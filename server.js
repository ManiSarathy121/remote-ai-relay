// Remote AI Console - Relay Server v2
// Supports both WebSocket (admin) and HTTP polling (extension client)

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;
const COMMAND_TTL_MS = 30000;   // commands expire after 30s
const SESSION_TTL_MS = 120000;  // sessions expire after 2min of no poll

// sessionId → { admin: ws|null, commands: [], results: {}, lastPoll: Date, created: Date }
const sessions = new Map();

function getOrCreateSession(id) {
  if (!sessions.has(id)) {
    sessions.set(id, {
      admin: null,
      commands: [],      // queue of pending commands for the client to pick up
      results: {},       // commandId → result (for admin to pick up)
      lastPoll: Date.now(),
      created: new Date()
    });
  }
  return sessions.get(id);
}

// ─── Cleanup stale sessions every 60s ────────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (const [id, sess] of sessions.entries()) {
    if (now - sess.lastPoll > SESSION_TTL_MS && !sess.admin) {
      sessions.delete(id);
      console.log('[relay] cleaned session:', id);
    }
  }
}, 60000);

// ─── HTTP Server ──────────────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);

  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
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

  // ── POST /claude  (proxies Claude API calls — fixes browser CORS issue) ────
  if (req.method === 'POST' && url.pathname === '/claude') {
    readBody().then(async body => {
      const apiKey = req.headers['x-api-key'] || '';
      if (!apiKey || !apiKey.startsWith('sk-ant-')) {
        return json({ error: 'Missing or invalid Claude API key in x-api-key header' }, 401);
      }
      try {
        // Use dynamic import for node-fetch or built-in fetch (Node 18+)
        const response = await fetch('https://api.anthropic.com/v1/messages', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'x-api-key': apiKey,
            'anthropic-version': '2023-06-01'
          },
          body: JSON.stringify(body)
        });
        const data = await response.json();
        res.writeHead(response.status, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(data));
      } catch(e) {
        json({ error: 'Claude proxy error: ' + e.message }, 500);
      }
    }).catch(() => json({ error: 'Bad JSON' }, 400));
    return;
  }

  // ── GET /poll?sessionId=XXXX  (extension polls this) ───────────────────────
  if (req.method === 'GET' && url.pathname === '/poll') {
    const sid = url.searchParams.get('sessionId');
    if (!sid) return json({ error: 'Missing sessionId' }, 400);

    const sess = getOrCreateSession(sid);
    sess.lastPoll = Date.now();

    // Drain pending commands
    const commands = sess.commands.splice(0);

    // Notify admin client is alive
    if (sess.admin?.readyState === WebSocket.OPEN) {
      sess.admin.send(JSON.stringify({ type: 'client_connected', sessionId: sid }));
    }

    return json({ ok: true, commands });
  }

  // ── POST /result  (extension posts result back) ─────────────────────────────
  if (req.method === 'POST' && url.pathname === '/result') {
    readBody().then(body => {
      const { sessionId: sid, commandId, result, generatedCode, ts } = body;
      if (!sid || !commandId) return json({ error: 'Missing fields' }, 400);

      const sess = sessions.get(sid);
      if (!sess) return json({ error: 'Unknown session' }, 404);

      // Forward to admin via WebSocket
      if (sess.admin?.readyState === WebSocket.OPEN) {
        sess.admin.send(JSON.stringify({ type: 'result', commandId, sessionId: sid, result, generatedCode, ts }));
      }

      json({ ok: true });
    }).catch(() => json({ error: 'Bad JSON' }, 400));
    return;
  }

  // ── POST /command  (REST fallback for admin) ────────────────────────────────
  if (req.method === 'POST' && url.pathname === '/command') {
    readBody().then(body => {
      const sess = sessions.get(body.sessionId);
      if (!sess) return json({ error: 'Session not found' }, 404);

      // Expire old commands
      const now = Date.now();
      sess.commands = sess.commands.filter(c => now - (c.ts || 0) < COMMAND_TTL_MS);

      body.ts = now;
      sess.commands.push(body);
      json({ ok: true, commandId: body.commandId, queued: sess.commands.length });
    }).catch(() => json({ error: 'Bad JSON' }, 400));
    return;
  }

  // ── GET /sessions  (admin dashboard) ───────────────────────────────────────
  if (req.method === 'GET' && url.pathname === '/sessions') {
    const now = Date.now();
    const list = [];
    for (const [id, sess] of sessions.entries()) {
      list.push({
        id,
        clientAlive: (now - sess.lastPoll) < 8000,
        lastPollAgo: Math.round((now - sess.lastPoll) / 1000) + 's',
        adminConnected: sess.admin?.readyState === WebSocket.OPEN,
        pendingCommands: sess.commands.length,
      });
    }
    return json(list);
  }

  // ── GET /health ─────────────────────────────────────────────────────────────
  if (url.pathname === '/health') {
    return json({ ok: true, sessions: sessions.size, uptime: Math.round(process.uptime()) });
  }

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Remote AI Console Relay v2.0');
});

// ─── WebSocket (admin app uses this) ─────────────────────────────────────────
const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  const sid = url.searchParams.get('sessionId');
  const role = url.searchParams.get('role');

  if (!sid || role !== 'admin') { ws.close(1008, 'Admin only'); return; }
  console.log('[relay] admin connected:', sid);

  const sess = getOrCreateSession(sid);
  sess.admin = ws;

  // Tell admin current state
  const now = Date.now();
  ws.send(JSON.stringify({
    type: 'session_state',
    sessionId: sid,
    clientConnected: (now - sess.lastPoll) < 8000
  }));

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);

      if (msg.type === 'command') {
        const s = getOrCreateSession(sid);
        msg.ts = Date.now();

        // Expire stale commands first
        s.commands = s.commands.filter(c => Date.now() - (c.ts || 0) < COMMAND_TTL_MS);
        s.commands.push(msg);

        const clientAlive = (Date.now() - s.lastPoll) < 8000;
        ws.send(JSON.stringify({
          type: 'command_sent',
          commandId: msg.commandId,
          clientAlive
        }));

        if (!clientAlive) {
          ws.send(JSON.stringify({
            type: 'error',
            commandId: msg.commandId,
            message: 'Client browser is not polling — extension may be asleep. It will pick up the command when it wakes (within 30s).'
          }));
        }
      }

      if (msg.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong', ts: Date.now() }));
      }
    } catch (e) { console.error('[relay] admin msg error:', e); }
  });

  ws.on('close', () => {
    const s = sessions.get(sid);
    if (s) s.admin = null;
    console.log('[relay] admin disconnected:', sid);
  });

  ws.on('error', err => console.error('[relay] ws error:', err));
});

server.listen(PORT, () => {
  console.log(`[relay] Remote AI Console Relay v2.0 on port ${PORT}`);
  console.log(`[relay] HTTP poll: GET /poll?sessionId=XXXX`);
  console.log(`[relay] HTTP result: POST /result`);
  console.log(`[relay] WebSocket admin: ws://localhost:${PORT}?sessionId=XXXX&role=admin`);
});
