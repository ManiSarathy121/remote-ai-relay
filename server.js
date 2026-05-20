// relay-server/server.js
// WebSocket relay server — bridges Admin app <-> Chrome Extension
// Deploy free on: Glitch, Railway, Render, Fly.io

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;

// sessionId -> { client: ws, admin: ws, created: Date }
const sessions = new Map();
const pendingCommands = new Map(); // commandId -> { resolve, reject, timer }

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);

  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') { res.writeHead(200); res.end(); return; }

  // Health check
  if (url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, sessions: sessions.size, uptime: process.uptime() }));
    return;
  }

  // Active sessions list (for admin)
  if (url.pathname === '/sessions') {
    const list = [];
    for (const [id, sess] of sessions.entries()) {
      list.push({
        id,
        clientConnected: sess.client?.readyState === WebSocket.OPEN,
        adminConnected: sess.admin?.readyState === WebSocket.OPEN,
        created: sess.created
      });
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(list));
    return;
  }

  // REST fallback: POST /command (for admin if WebSocket fails)
  if (url.pathname === '/command' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try {
        const msg = JSON.parse(body);
        const sess = sessions.get(msg.sessionId);
        if (!sess || sess.client?.readyState !== WebSocket.OPEN) {
          res.writeHead(404, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Client not connected' }));
          return;
        }
        sess.client.send(JSON.stringify({ type: 'command', ...msg }));
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, commandId: msg.commandId }));
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Remote AI Console Relay v1.0');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  const sessionId = url.searchParams.get('sessionId');
  const role = url.searchParams.get('role'); // 'client' or 'admin'

  if (!sessionId) { ws.close(1008, 'Missing sessionId'); return; }

  console.log(`[relay] ${role} connected: ${sessionId}`);

  // Ensure session entry exists
  if (!sessions.has(sessionId)) {
    sessions.set(sessionId, { client: null, admin: null, created: new Date() });
  }

  const sess = sessions.get(sessionId);

  if (role === 'client') {
    // Chrome extension connects here
    sess.client = ws;
    // Notify admin if connected
    if (sess.admin?.readyState === WebSocket.OPEN) {
      sess.admin.send(JSON.stringify({ type: 'client_connected', sessionId }));
    }

    ws.on('message', (data) => {
      try {
        const msg = JSON.parse(data);
        if (msg.type === 'result') {
          // Forward result to admin
          if (sess.admin?.readyState === WebSocket.OPEN) {
            sess.admin.send(data.toString());
          }
          // Resolve pending promise if any
          if (pendingCommands.has(msg.commandId)) {
            const { resolve, timer } = pendingCommands.get(msg.commandId);
            clearTimeout(timer);
            pendingCommands.delete(msg.commandId);
            resolve(msg);
          }
        }
      } catch (e) { console.error('[relay] client msg error:', e); }
    });

    ws.on('close', () => {
      sess.client = null;
      if (sess.admin?.readyState === WebSocket.OPEN) {
        sess.admin.send(JSON.stringify({ type: 'client_disconnected', sessionId }));
      }
      cleanupSession(sessionId);
    });

  } else if (role === 'admin') {
    // Admin app connects here
    sess.admin = ws;

    // Tell admin current client state
    ws.send(JSON.stringify({
      type: 'session_state',
      sessionId,
      clientConnected: sess.client?.readyState === WebSocket.OPEN
    }));

    ws.on('message', (data) => {
      try {
        const msg = JSON.parse(data);

        if (msg.type === 'command') {
          // Forward command to client
          if (sess.client?.readyState === WebSocket.OPEN) {
            sess.client.send(data.toString());
            ws.send(JSON.stringify({ type: 'command_sent', commandId: msg.commandId }));
          } else {
            ws.send(JSON.stringify({ type: 'error', message: 'Client browser not connected', commandId: msg.commandId }));
          }
        }

        if (msg.type === 'ping') {
          ws.send(JSON.stringify({ type: 'pong', ts: Date.now() }));
        }
      } catch (e) { console.error('[relay] admin msg error:', e); }
    });

    ws.on('close', () => {
      sess.admin = null;
      cleanupSession(sessionId);
    });
  } else {
    ws.close(1008, 'Invalid role');
  }

  ws.on('error', (err) => console.error(`[relay] ws error [${sessionId}/${role}]:`, err));
});

function cleanupSession(sessionId) {
  const sess = sessions.get(sessionId);
  if (sess && !sess.client && !sess.admin) {
    setTimeout(() => {
      // Give 30s for reconnection before cleanup
      const s = sessions.get(sessionId);
      if (s && !s.client && !s.admin) {
        sessions.delete(sessionId);
        console.log(`[relay] session cleaned up: ${sessionId}`);
      }
    }, 30000);
  }
}

server.listen(PORT, () => {
  console.log(`[relay] Remote AI Console Relay running on port ${PORT}`);
  console.log(`[relay] WebSocket: ws://localhost:${PORT}?sessionId=XXXX-XXXX&role=client|admin`);
  console.log(`[relay] Health: http://localhost:${PORT}/health`);
});
