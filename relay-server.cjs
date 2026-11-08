/* 炸金花联机 · 中转服务器（零依赖，仅用 Node 内置模块）
 * 职责：① 提供静态页面（index.html） ② 房间内消息中转（WebSocket）
 * 不持有任何游戏逻辑，纯传声筒，因此极轻量、极稳定。
 *
 * 本地运行：  node relay-server.cjs            （默认端口 3000）
 * 环境变量：  PORT=8080 node relay-server.cjs
 */
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

const rooms = new Map();      // room -> Map(uid -> conn)
const conns = new Map();      // uid  -> conn

function log(...a) { console.log('[' + new Date().toISOString() + ']', ...a); }

/* ---------------- 静态文件服务 ---------------- */
const MIME = { '.html':'text/html; charset=utf-8', '.js':'text/javascript; charset=utf-8',
  '.css':'text/css; charset=utf-8', '.json':'application/json', '.ico':'image/x-icon' };

const server = http.createServer((req, res) => {
  if (req.url === '/healthz' || req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok'); return;
  }
  // 仅允许 GET
  if (req.method !== 'GET') { res.writeHead(405); res.end('Method Not Allowed'); return; }
  let urlPath = decodeURIComponent(req.url.split('?')[0]);
  if (urlPath === '/') urlPath = '/index.html';
  // 防目录穿越
  const safe = path.normalize(urlPath).replace(/^(\.\.[/\\])+/, '');
  const filePath = path.join(__dirname, safe);
  if (!filePath.startsWith(__dirname)) { res.writeHead(403); res.end('Forbidden'); return; }
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('Not Found'); return; }
    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    res.end(data);
  });
});

/* ---------------- WebSocket 握手 ---------------- */
server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n'
  );
  const uid = Math.random().toString(36).slice(2, 10);
  const conn = { socket, uid, room: null, name: '', isHost: false, buf: Buffer.alloc(0) };
  conns.set(uid, conn);
  socket.on('data', chunk => onData(conn, chunk));
  socket.on('close', () => onClose(conn));
  socket.on('error', () => { try { socket.destroy(); } catch (_) {} });
  // 立即下发 welcome（客户端连上即拿到自己的 uid）
  sendFrame(socket, JSON.stringify({ type: 'welcome', uid }));
  log('connect', uid);
});

/* ---------------- 帧解码（客户端→服务器，必带掩码） ---------------- */
function onData(conn, chunk) {
  conn.buf = Buffer.concat([conn.buf, chunk]);
  while (true) {
    if (conn.buf.length < 2) return;
    const b0 = conn.buf[0];
    const fin = (b0 & 0x80) !== 0;
    const opcode = b0 & 0x0f;
    const b1 = conn.buf[1];
    const masked = (b1 & 0x80) !== 0;
    let len = b1 & 0x7f;
    let offset = 2;
    if (len === 126) { if (conn.buf.length < 4) return; len = conn.buf.readUInt16BE(2); offset = 4; }
    else if (len === 127) { if (conn.buf.length < 10) return; len = Number(conn.buf.readBigUInt64BE(2)); offset = 10; }
    let maskKey;
    if (masked) { if (conn.buf.length < offset + 4) return; maskKey = conn.buf.slice(offset, offset + 4); offset += 4; }
    if (conn.buf.length < offset + len) return;
    let payload = conn.buf.slice(offset, offset + len);
    if (masked) { const out = Buffer.alloc(len); for (let i = 0; i < len; i++) out[i] = payload[i] ^ maskKey[i % 4]; payload = out; }
    conn.buf = conn.buf.slice(offset + len);
    // 本游戏消息均为小包、单帧（fin=1）。若对端分片，这里按首帧 opcode 处理即可。
    if (!fin && opcode === 0x0) continue; // 续帧：本场景不会出现，跳过以防卡死
    handleFrame(conn, opcode, payload);
  }
}

function handleFrame(conn, opcode, payload) {
  if (opcode === 0x8) { try { conn.socket.end(); } catch (_) {} return; } // close
  if (opcode === 0x9) { sendFrame(conn.socket, '', 0xA); return; }         // ping -> pong
  if (opcode === 0xA) return;                                              // pong
  if (opcode !== 0x1 && opcode !== 0x0) return;                            // 仅处理文本
  let msg;
  try { msg = JSON.parse(payload.toString('utf8')); } catch (_) { return; }
  handleMessage(conn, msg);
}

/* ---------------- 消息路由 ---------------- */
function handleMessage(conn, msg) {
  if (!msg || typeof msg !== 'object') return;
  if (msg.type === 'join') {
    conn.room = String(msg.room || '').slice(0, 12) || 'default';
    conn.name = String(msg.name || '玩家').slice(0, 12);
    conn.isHost = !!msg.isHost;
    if (!rooms.has(conn.room)) rooms.set(conn.room, new Map());
    rooms.get(conn.room).set(conn.uid, conn);
    log('join', conn.uid, 'room=' + conn.room, 'host=' + conn.isHost);
    return;
  }
  if (msg.type === 'game') {
    const room = rooms.get(conn.room);
    if (!room) return;
    const out = JSON.stringify({ type: 'fwd', from: conn.uid, data: msg.data || {} });
    if (msg.to) {
      const t = room.get(String(msg.to));
      if (t) sendFrame(t.socket, out);
    } else {
      room.forEach((c, u) => { if (u !== conn.uid) sendFrame(c.socket, out); });
    }
    return;
  }
}

function onClose(conn) {
  const room = conn.room ? rooms.get(conn.room) : null;
  if (room) {
    room.delete(conn.uid);
    if (room.size === 0) rooms.delete(conn.room);
    else { const out = JSON.stringify({ type: 'peer-left', uid: conn.uid }); room.forEach(c => sendFrame(c.socket, out)); }
  }
  conns.delete(conn.uid);
  log('disconnect', conn.uid, 'room=' + conn.room);
}

/* ---------------- 帧编码（服务器→客户端，不掩码） ---------------- */
function sendFrame(socket, str, opcode = 0x1) {
  const payload = Buffer.from(str, 'utf8');
  const len = payload.length;
  let header;
  if (len < 126) { header = Buffer.alloc(2); header[1] = len; }
  else if (len < 65536) { header = Buffer.alloc(4); header[1] = 126; header.writeUInt16BE(len, 2); }
  else { header = Buffer.alloc(10); header[1] = 127; header.writeUInt32BE(0, 2); header.writeUInt32BE(len, 6); }
  header[0] = 0x80 | (opcode & 0x0f);
  try { socket.write(Buffer.concat([header, payload])); } catch (_) {}
}

server.listen(PORT, () => log('炸金花中转服务器已启动： http://localhost:' + PORT));
