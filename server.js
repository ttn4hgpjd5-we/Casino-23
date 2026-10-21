// Casino 23 server: serves the game page and keeps lobbies/games in a small realtime store over WebSocket.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const DATA_FILE = path.join(__dirname, 'data.json');
const docs = new Map(); // "lobbies/abc" -> object
try { Object.entries(JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'))).forEach(([k, v]) => docs.set(k, v)); } catch (e) {}
let dirty = false;
setInterval(() => { if (dirty) { dirty = false; fs.writeFile(DATA_FILE, JSON.stringify(Object.fromEntries(docs)), () => {}); } }, 5000);

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') { res.writeHead(200); return res.end('ok'); }
  const first = path.join(__dirname, 'index.html'), second = path.join(__dirname, 'public', 'index.html');
  fs.readFile(fs.existsSync(first) ? first : second, (err, buf) => {
    if (err) { res.writeHead(500); return res.end('Missing index.html'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.end(buf);
  });
});
const wss = new WebSocketServer({ server, maxPayload: 512 * 1024 });
const clients = new Set();

function merge(a, b) {
  for (const k in b) {
    const v = b[k];
    if (v && typeof v === 'object' && !Array.isArray(v)) { if (!a[k] || typeof a[k] !== 'object') a[k] = {}; merge(a[k], v); } else a[k] = v;
  }
}
const validPath = p => typeof p === 'string' && /^[A-Za-z0-9_\-./:@+]{1,300}$/.test(p) && !p.includes('..');
const depth = p => p.split('/').length;

function colDocs(col) {
  const out = [];
  for (const [k, v] of docs) if (k.startsWith(col + '/') && depth(k) === depth(col) + 1) out.push({ id: k.split('/').pop(), data: v });
  return out;
}
function pushTo(c, sid, sub) {
  if (sub.kind === 'doc') c.send(JSON.stringify({ op: 'doc', sid, exists: docs.has(sub.path), data: docs.get(sub.path) || null }));
  else c.send(JSON.stringify({ op: 'col', sid, docs: colDocs(sub.path) }));
}
function changed(p) {
  dirty = true;
  for (const c of clients) {
    if (c.readyState !== 1) continue;
    for (const [sid, sub] of c.subs) {
      if (sub.kind === 'doc' && sub.path === p) pushTo(c, sid, sub);
      else if (sub.kind === 'col' && p.startsWith(sub.path + '/') && depth(p) === depth(sub.path) + 1) pushTo(c, sid, sub);
    }
  }
}

wss.on('connection', ws => {
  ws.subs = new Map(); clients.add(ws);
  ws.isAlive = true; ws.on('pong', () => { ws.isAlive = true; });
  ws.on('close', () => clients.delete(ws));
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    const ack = (ok, err) => { if (m.rid) ws.send(JSON.stringify({ op: 'ack', rid: m.rid, ok, err })); };
    if (m.op === 'sub') {
      if (!validPath(m.path)) return;
      const sub = { kind: m.kind === 'col' ? 'col' : 'doc', path: m.path };
      ws.subs.set(m.sid, sub); pushTo(ws, m.sid, sub); return;
    }
    if (m.op === 'unsub') { ws.subs.delete(m.sid); return; }
    if (!validPath(m.path) || depth(m.path) % 2 !== 0) return ack(false, 'bad path');
    if (m.op === 'get') { if (!m.rid) return; return ws.send(JSON.stringify({ op: 'ack', rid: m.rid, ok: true, exists: docs.has(m.path), data: docs.get(m.path) || null })); }
    if (m.op === 'set') {
      if (!m.data || typeof m.data !== 'object') return ack(false, 'bad data');
      docs.set(m.path, m.data); ack(true); changed(m.path);
    } else if (m.op === 'update') {
      const d = docs.get(m.path); if (!d) return ack(false, 'missing');
      merge(d, m.data || {}); ack(true); changed(m.path);
    } else if (m.op === 'delete') { docs.delete(m.path); ack(true); changed(m.path); }
  });
});
setInterval(() => wss.clients.forEach(ws => { if (!ws.isAlive) return ws.terminate(); ws.isAlive = false; ws.ping(); }), 30000);

// remove tables older than a day
setInterval(() => {
  const cutoff = Date.now() - 864e5;
  for (const [k, v] of Array.from(docs)) if (k.startsWith('lobbies/') && depth(k) === 2 && v.created < cutoff) { docs.delete(k); docs.delete('games/' + k.split('/')[1]); dirty = true; }
}, 600000);

server.listen(PORT, () => console.log('Casino 23 running on port ' + PORT));
