// Remote AI Console - Relay Server v3
// WebSocket for BOTH admin and client extension
// HTTP polling fallback + /poll /result endpoints

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;
const SESSION_TTL_MS = 5 * 60 * 1000; // 5 min inactive cleanup

// sessionId → { client: ws|null, admin: ws|null, commands: [], lastSeen: Date }
const sessions = new Map();

function getSession(id) {
  if (!sessions.has(id)) {
    sessions.set(id, {
      client: null,
      admin:  null,
      commands: [],
      lastSeen: Date.now()
    });
  }
  return sessions.get(id);
}

// ─── Session cleanup every 2 min ─────────────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (const [id, sess] of sessions.entries()) {
    const noClient = !sess.client || sess.client.readyState !== WebSocket.OPEN;
    const noAdmin  = !sess.admin  || sess.admin.readyState  !== WebSocket.OPEN;
    if (noClient && noAdmin && (now - sess.lastSeen) > SESSION_TTL_MS) {
      sessions.delete(id);
      console.log('[relay] cleaned up session:', id);
    }
  }
}, 120000);

// ─── HTTP Server ──────────────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);

  // CORS for all origins
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-api-key');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  const json = (data, code = 200) => {
    res.writeHead(code, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(data));
  };

  const readBody = () => new Promise((resolve, reject) => {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => { try { resolve(JSON.parse(body)); } catch(e) { reject(e); } });
    req.on('error', reject);
  });

  // ── GET /health ───────────────────────────────────────────────────────────
  if (url.pathname === '/health') {
    return json({ ok: true, sessions: sessions.size, uptime: Math.round(process.uptime()) });
  }

  // ── GET / ─────────────────────────────────────────────────────────────────
  if (url.pathname === '/' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Remote AI Console Relay v3.0');
    return;
  }

  // ── GET /poll?sessionId=XXXX  (HTTP fallback for extension) ──────────────
  if (req.method === 'GET' && url.pathname === '/poll') {
    const sid = url.searchParams.get('sessionId');
    if (!sid) return json({ error: 'Missing sessionId' }, 400);

    const sess = getSession(sid);
    sess.lastSeen = Date.now();

    // Drain pending commands
    const commands = sess.commands.splice(0);

    // Notify admin if connected via WS
    if (sess.admin?.readyState === WebSocket.OPEN) {
      sess.admin.send(JSON.stringify({ type: 'client_connected', sessionId: sid }));
    }

    return json({ ok: true, commands });
  }

  // ── POST /result  (HTTP fallback — extension posts result) ────────────────
  if (req.method === 'POST' && url.pathname === '/result') {
    readBody().then(body => {
      const { sessionId: sid, commandId, result, generatedCode, ts } = body;
      if (!sid || !commandId) return json({ error: 'Missing fields' }, 400);

      const sess = sessions.get(sid);
      if (!sess) return json({ error: 'Unknown session' }, 404);

      // Forward to admin
      if (sess.admin?.readyState === WebSocket.OPEN) {
        sess.admin.send(JSON.stringify({ type: 'result', commandId, sessionId: sid, result, generatedCode, ts }));
      }

      json({ ok: true });
    }).catch(() => json({ error: 'Bad JSON' }, 400));
    return;
  }

  // ── GET /sessions ─────────────────────────────────────────────────────────
  if (req.method === 'GET' && url.pathname === '/sessions') {
    const now = Date.now();
    const list = [];
    for (const [id, sess] of sessions.entries()) {
      list.push({
        id,
        clientConnected: sess.client?.readyState === WebSocket.OPEN,
        adminConnected:  sess.admin?.readyState  === WebSocket.OPEN,
        pendingCommands: sess.commands.length,
        lastSeenAgo: Math.round((now - sess.lastSeen) / 1000) + 's'
      });
    }
    return json(list);
  }

  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not found');
});

// ─── WebSocket Server — handles BOTH client (extension) and admin ─────────────
const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  const url  = new URL(req.url, `http://localhost:${PORT}`);
  const sid  = url.searchParams.get('sessionId');
  const role = url.searchParams.get('role'); // 'client' or 'admin'

  if (!sid) { ws.close(1008, 'Missing sessionId'); return; }
  if (role !== 'client' && role !== 'admin') { ws.close(1008, 'Invalid role — use client or admin'); return; }

  const sess = getSession(sid);
  sess.lastSeen = Date.now();

  console.log(`[relay] ${role} connected — session: ${sid}`);

  // ── CLIENT (Chrome Extension) ─────────────────────────────────────────────
  if (role === 'client') {
    sess.client = ws;

    // Tell admin browser is connected
    if (sess.admin?.readyState === WebSocket.OPEN) {
      sess.admin.send(JSON.stringify({ type: 'client_connected', sessionId: sid }));
    }

    // Send any queued commands immediately
    if (sess.commands.length > 0) {
      const pending = sess.commands.splice(0);
      pending.forEach(cmd => ws.send(JSON.stringify({ type: 'command', ...cmd })));
    }

    ws.on('message', raw => {
      try {
        const msg = JSON.parse(raw);
        sess.lastSeen = Date.now();

        if (msg.type === 'result') {
          // Forward result to admin
          if (sess.admin?.readyState === WebSocket.OPEN) {
            sess.admin.send(raw.toString());
          }
        }
      } catch(e) { console.error('[relay] client msg error:', e.message); }
    });

    ws.on('close', () => {
      sess.client = null;
      if (sess.admin?.readyState === WebSocket.OPEN) {
        sess.admin.send(JSON.stringify({ type: 'client_disconnected', sessionId: sid }));
      }
      console.log(`[relay] client disconnected — session: ${sid}`);
    });

    ws.on('error', err => console.error(`[relay] client error [${sid}]:`, err.message));
  }

  // ── ADMIN (Admin App) ─────────────────────────────────────────────────────
  if (role === 'admin') {
    sess.admin = ws;

    // Tell admin current state
    ws.send(JSON.stringify({
      type: 'session_state',
      sessionId: sid,
      clientConnected: sess.client?.readyState === WebSocket.OPEN
    }));

    ws.on('message', raw => {
      try {
        const msg = JSON.parse(raw);
        sess.lastSeen = Date.now();

        if (msg.type === 'command') {
          if (sess.client?.readyState === WebSocket.OPEN) {
            // Client connected via WS — send directly
            sess.client.send(raw.toString());
            ws.send(JSON.stringify({ type: 'command_sent', commandId: msg.commandId, via: 'websocket' }));
          } else {
            // Queue for HTTP polling fallback
            msg.ts = Date.now();
            sess.commands.push(msg);
            ws.send(JSON.stringify({
              type: 'command_queued',
              commandId: msg.commandId,
              note: 'Client using HTTP poll — will pick up within 3s'
            }));
          }
        }

        if (msg.type === 'ping') {
          ws.send(JSON.stringify({ type: 'pong', ts: Date.now() }));
        }
      } catch(e) { console.error('[relay] admin msg error:', e.message); }
    });

    ws.on('close', () => {
      sess.admin = null;
      console.log(`[relay] admin disconnected — session: ${sid}`);
    });

    ws.on('error', err => console.error(`[relay] admin error [${sid}]:`, err.message));
  }

  // ── Keepalive ping every 25s to prevent Render sleeping the WS ────────────
  const pingInterval = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.ping();
    } else {
      clearInterval(pingInterval);
    }
  }, 25000);

  ws.on('close', () => clearInterval(pingInterval));
});

server.listen(PORT, () => {
  console.log(`[relay] Remote AI Console Relay v3.0 running on port ${PORT}`);
  console.log(`[relay] WS client: ws://HOST?sessionId=XXXX&role=client`);
  console.log(`[relay] WS admin:  ws://HOST?sessionId=XXXX&role=admin`);
  console.log(`[relay] HTTP poll: GET /poll?sessionId=XXXX`);
  console.log(`[relay] Health:    GET /health`);
});
