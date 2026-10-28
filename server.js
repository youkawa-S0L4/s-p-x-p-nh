/*
 * ARCANE SURVIVOR — máy chủ phòng đua online
 * Không cần cài thêm gì: chỉ cần Node.js 16 trở lên.
 *
 *   node server.js            (cổng mặc định 8080)
 *   PORT=3000 node server.js
 *
 * Máy chủ này vừa phát file game (index.html + thư mục sounds/, đặt cùng thư mục)
 * (số màn tự đọc từ index.html) vừa làm máy chủ WebSocket, nên mọi người chỉ cần mở http://<địa-chỉ-máy>:<cổng>
 * và game sẽ tự điền đúng địa chỉ phòng.
 */
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = +process.env.PORT || 8080;
const MAX_PLAYERS = 8, COUNTDOWN_MS = 3000;
/* Tự đồng bộ số màn với index.html: đọc PER (màn mỗi chapter) và CHAPS (số chapter). Thêm chapter mới ở game là server tự theo. */
/* Danh sách nhân vật: đọc CHAR_IDS trong index.html để server tự theo khi thêm nhân vật mới */
function readChars() {
  try {
    const t = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
    const m = /CHAR_IDS\s*=\s*\[([^\]]*)\]/.exec(t);
    if (m) { const ids = m[1].split(',').map(x => x.replace(/['"\s]/g, '')).filter(Boolean); if (ids.length) return ids; }
  } catch (e) { /* dùng mặc định */ }
  return ['luna'];
}
const CHAR_IDS = readChars();
const cleanCh = (v, d) => (typeof v === 'string' && CHAR_IDS.includes(v)) ? v : d;
function readStages() {
  try {
    const t = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
    const m = /PER\s*=\s*(\d+)\s*,\s*CHAPS\s*=\s*(\d+)/.exec(t);
    if (m && +m[1] > 0 && +m[2] > 0) return { per: +m[1], chaps: +m[2] };
  } catch (e) { /* dùng mặc định */ }
  return { per: 10, chaps: 2 };
}
const { per: PER, chaps: CHAPS } = readStages();
const STAGES = PER * CHAPS;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const GAME_FILES = ['index.html'];

/* ---------------- HTTP: phát file game ---------------- */
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/' || url === '/index.html' || GAME_FILES.some(f => url === '/' + f)) {
    const f = GAME_FILES.map(n => path.join(__dirname, n)).find(p => fs.existsSync(p));
    if (f) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      fs.createReadStream(f).pipe(res);
      return;
    }
  }
  /* Phát file âm thanh trong thư mục sounds/ */
  if (url.startsWith('/sounds/')) {
    let rel = '';
    try { rel = decodeURIComponent(url.slice(8)); } catch (e) { rel = ''; }
    const dir = path.join(__dirname, 'sounds');
    const fp = path.join(dir, path.normalize(rel));
    const MIME = { '.mp3': 'audio/mpeg', '.ogg': 'audio/ogg', '.wav': 'audio/wav', '.m4a': 'audio/mp4', '.txt': 'text/plain; charset=utf-8' };
    if (rel && fp.startsWith(dir + path.sep) && fs.existsSync(fp) && fs.statSync(fp).isFile()) {
      res.writeHead(200, { 'Content-Type': MIME[path.extname(fp).toLowerCase()] || 'application/octet-stream', 'Cache-Control': 'public, max-age=86400' });
      fs.createReadStream(fp).pipe(res);
      return;
    }
  }
  if (url === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok'); return; }
  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Không tìm thấy. Hãy đặt index.html cùng thư mục với server.js.');
});

/* ---------------- WebSocket (RFC 6455) tối giản ---------------- */
function frame(op, payload) {
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | op, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([head, payload]);
}

class Conn {
  constructor(socket) {
    this.socket = socket; this.buf = Buffer.alloc(0); this.alive = true; this.closed = false;
    this.frags = []; this.fragLen = 0; this.id = crypto.randomBytes(4).toString('hex'); this.room = null;
    socket.on('data', d => this.onData(d));
    socket.on('close', () => this.onClose());
    socket.on('error', () => this.onClose());
  }
  send(obj) {
    if (this.closed) return;
    try { this.socket.write(frame(1, Buffer.from(JSON.stringify(obj)))); } catch (e) { /* bỏ qua */ }
  }
  close(code) {
    if (this.closed) return;
    try { const b = Buffer.alloc(2); b.writeUInt16BE(code || 1000); this.socket.write(frame(8, b)); } catch (e) { /* bỏ qua */ }
    try { this.socket.end(); } catch (e) { /* bỏ qua */ }
    this.onClose();
  }
  onClose() {
    if (this.closed) return;
    this.closed = true;
    try { this.socket.destroy(); } catch (e) { /* bỏ qua */ }
    leaveRoom(this);
  }
  onData(d) {
    this.buf = Buffer.concat([this.buf, d]);
    if (this.buf.length > 1 << 20) return this.close(1009);
    for (;;) {
      const b = this.buf;
      if (b.length < 2) return;
      const fin = !!(b[0] & 0x80), op = b[0] & 0x0f, masked = !!(b[1] & 0x80);
      let len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (len > 65536) return this.close(1009);
      if (!masked) return this.close(1002);
      if (b.length < off + 4 + len) return;
      const mask = b.subarray(off, off + 4);
      const data = Buffer.from(b.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < data.length; i++) data[i] ^= mask[i & 3];
      this.buf = b.subarray(off + 4 + len);
      if (op === 8) return this.close(1000);
      if (op === 9) { try { this.socket.write(frame(10, data)); } catch (e) { /* bỏ qua */ } continue; }
      if (op === 10) { this.alive = true; continue; }
      if (op === 0 || op === 1 || op === 2) {
        if (op !== 0) { this.frags = []; this.fragLen = 0; this.fragOp = op; }
        this.frags.push(data); this.fragLen += data.length;
        if (this.fragLen > 65536) return this.close(1009);
        if (fin) {
          const msg = Buffer.concat(this.frags); this.frags = []; this.fragLen = 0;
          if (this.fragOp === 1) onMessage(this, msg.toString('utf8'));
        }
      }
    }
  }
}

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade).toLowerCase() !== 'websocket') { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  conns.add(new Conn(socket));
});
const conns = new Set();
setInterval(() => {
  for (const c of [...conns]) {
    if (c.closed) { conns.delete(c); continue; }
    if (!c.alive) { c.close(1001); conns.delete(c); continue; }
    c.alive = false;
    try { c.socket.write(frame(9, Buffer.alloc(0))); } catch (e) { /* bỏ qua */ }
  }
}, 25000).unref();

/* ---------------- Logic phòng ---------------- */
const rooms = new Map();
const clampI = (v, a, b, d) => { v = Math.floor(+v); return isFinite(v) ? Math.max(a, Math.min(b, v)) : d; };
const cleanName = s => String(s == null ? '' : s).replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, 16) || 'Khách';

function newCode() {
  for (let t = 0; t < 200; t++) {
    let c = '';
    for (let i = 0; i < 4; i++) c += CODE_CHARS[crypto.randomInt(CODE_CHARS.length)];
    if (!rooms.has(c)) return c;
  }
  return null;
}
function maxStage(r) {
  let m = STAGES;
  for (const p of r.players.values()) if (p.ws) m = Math.min(m, p.mx);
  return Math.max(1, m);
}
function maxDif(r) {
  let m = 3;
  for (const p of r.players.values()) if (p.ws) m = Math.min(m, p.md);
  return Math.max(1, m);
}
function pub(p) {
  return { id: p.id, name: p.name, ready: !!p.ready, status: p.status, hp: p.hp, max: p.max, lvl: p.lvl,
    kills: p.kills, tm: p.tm, win: p.win ? 1 : 0, quit: p.quit ? 1 : 0, ch: p.ch };
}
function snapshot(r, you) {
  return { t: 'room', you, code: r.code, host: r.host, phase: r.phase, stage: r.stage, maxStage: maxStage(r), dif: r.dif, maxDif: maxDif(r),
    players: [...r.players.values()].map(pub), rows: r.rows };
}
function broadcast(r) {
  r.dirty = false; r.lastB = Date.now();
  for (const p of r.players.values()) if (p.ws) p.ws.send(snapshot(r, p.id));
}
function broadcastSoon(r) {
  const wait = 350 - (Date.now() - (r.lastB || 0));
  if (wait <= 0) return broadcast(r);
  if (r.timer) return;
  r.timer = setTimeout(() => { r.timer = null; if (rooms.get(r.code) === r) broadcast(r); }, wait);
}
function resetPlayer(p) {
  Object.assign(p, { status: 'wait', hp: 0, max: 0, lvl: 1, kills: 0, tm: 0, win: 0, quit: 0 });
}
function err(c, msg) { c.send({ t: 'err', msg }); }

function finishIfDone(r) {
  if (r.phase !== 'run') return;
  const list = [...r.players.values()];
  if (!list.length || list.some(p => p.status !== 'done')) return;
  const rows = list.map(p => ({ name: p.name, ch: p.ch, win: p.win ? 1 : 0, tm: p.tm, kills: p.kills, quit: p.quit ? 1 : 0 }));
  rows.sort((a, b) => (b.win - a.win) || (a.win ? a.tm - b.tm : b.kills - a.kills));
  r.rows = rows; r.phase = 'results';
  for (const [id, p] of r.players) if (!p.ws) r.players.delete(id);
  pickHost(r);
  broadcast(r);
}
function pickHost(r) {
  const cur = r.players.get(r.host);
  if (cur && cur.ws) return;
  const nx = [...r.players.values()].find(p => p.ws);
  r.host = nx ? nx.id : '';
}
function leaveRoom(c) {
  const r = c.room; if (!r) return;
  c.room = null;
  const p = r.players.get(c.id); if (!p) return;
  if (r.phase === 'run' || r.phase === 'count') {
    p.ws = null; p.ready = 0;
    if (p.status !== 'done') { p.status = 'done'; p.quit = 1; p.win = 0; }
  } else r.players.delete(c.id);
  if (![...r.players.values()].some(x => x.ws)) {
    if (r.timer) clearTimeout(r.timer); if (r.cd) clearTimeout(r.cd);
    rooms.delete(r.code); return;
  }
  pickHost(r);
  if (r.phase === 'run') finishIfDone(r);
  if (rooms.get(r.code) === r && r.phase !== 'results') broadcast(r);
  else if (rooms.get(r.code) === r) broadcast(r);
}
function addPlayer(r, c, name, mx, md, ch) {
  const p = { id: c.id, name, ws: c, mx: clampI(mx, 1, STAGES, 1), md: clampI(md, 0, 3, 1), ready: 0, ch: cleanCh(ch, CHAR_IDS[0]) };
  resetPlayer(p); r.players.set(c.id, p); c.room = r; return p;
}

function onMessage(c, text) {
  let m; try { m = JSON.parse(text); } catch (e) { return; }
  if (!m || typeof m !== 'object' || typeof m.t !== 'string') return;
  const r = c.room, me = r && r.players.get(c.id);
  if (me && m.mx !== undefined) me.mx = clampI(m.mx, 1, STAGES, me.mx);
  if (me && m.md !== undefined) me.md = clampI(m.md, 0, 3, me.md);
  /* nhân vật chỉ đổi được khi chưa đua (sảnh / bảng kết quả) để bảng xếp hạng không bị đổi giữa trận */
  if (me && m.ch !== undefined && (r.phase === 'lobby' || r.phase === 'results')) me.ch = cleanCh(m.ch, me.ch);

  switch (m.t) {
    case 'create': {
      if (r) leaveRoom(c);
      const code = newCode(); if (!code) return err(c, 'Máy chủ đang quá tải, thử lại sau.');
      const room = { code, host: c.id, phase: 'lobby', stage: 1, dif: 1, players: new Map(), rows: [] };
      rooms.set(code, room); addPlayer(room, c, cleanName(m.name), m.mx, m.md, m.ch);
      return broadcast(room);
    }
    case 'join': {
      const code = String(m.code || '').toUpperCase().trim();
      const room = rooms.get(code);
      if (!room) return err(c, 'Không tìm thấy phòng ' + code + '.');
      if (room === r) return broadcast(room);
      if (room.phase === 'run' || room.phase === 'count') return err(c, 'Phòng đang đua, hãy vào lại khi ván kết thúc.');
      if ([...room.players.values()].filter(p => p.ws).length >= MAX_PLAYERS) return err(c, 'Phòng đã đầy (' + MAX_PLAYERS + ' người).');
      if (r) leaveRoom(c);
      addPlayer(room, c, cleanName(m.name), m.mx, m.md, m.ch);
      if (room.stage > maxStage(room)) room.stage = maxStage(room);
      if (room.dif > maxDif(room)) room.dif = maxDif(room);
      return broadcast(room);
    }
    case 'dif': {
      if (!r || r.host !== c.id || r.phase !== 'lobby') return;
      r.dif = clampI(m.d, 0, maxDif(r), r.dif);
      return broadcast(r);
    }
    case 'sync': return r ? broadcast(r) : undefined;
    case 'leave': return leaveRoom(c);
    case 'ready': {
      if (!r || !me || r.phase === 'run' || r.phase === 'count') return;
      me.ready = m.v ? 1 : 0;
      return broadcast(r);
    }
    case 'again': {
      if (!r || r.host !== c.id || r.phase !== 'results') return;
      r.phase = 'lobby'; r.rows = [];
      for (const p of r.players.values()) { resetPlayer(p); p.ready = 0; }
      r.stage = Math.min(r.stage, maxStage(r));
      return broadcast(r);
    }
    case 'start': {
      if (!r || r.host !== c.id) return err(c, 'Chỉ chủ phòng mới được bắt đầu.');
      if (r.phase !== 'lobby') return err(c, 'Hãy bấm "Ván mới" trước.');
      const waiting = [...r.players.values()].filter(p => p.id !== r.host && p.ws && !p.ready);
      if (waiting.length) return err(c, 'Còn ' + waiting.length + ' người chưa sẵn sàng: ' + waiting.map(p => p.name).join(', '));
      const n = clampI(m.n, 1, maxStage(r), 1);
      r.stage = n; r.phase = 'count'; r.seed = crypto.randomInt(1, 2147483647); /* hạt giống chung: mọi người gặp cùng đợt quái */ r.dif = Math.min(r.dif, maxDif(r));
      for (const p of r.players.values()) { resetPlayer(p); p.status = 'play'; p.ready = 0; }
      broadcast(r);
      r.cd = setTimeout(() => {
        r.cd = null;
        if (rooms.get(r.code) !== r || r.phase !== 'count') return;
        r.phase = 'run';
        for (const p of r.players.values()) if (p.ws) p.ws.send({ t: 'go', stage: r.stage, d: r.dif, seed: r.seed });
        broadcast(r);
        finishIfDone(r);
      }, COUNTDOWN_MS);
      return;
    }
    case 'prog': {
      if (!r || !me || r.phase !== 'run' || me.status === 'done') return;
      me.hp = clampI(m.hp, 0, 1e6, me.hp); me.max = clampI(m.max, 0, 1e6, me.max);
      me.lvl = clampI(m.lvl, 1, 999, me.lvl); me.kills = clampI(m.kills, 0, 1e7, me.kills);
      me.tm = clampI(m.tm, 0, 1e7, me.tm);
      return broadcastSoon(r);
    }
    case 'done': {
      if (!r || !me || (r.phase !== 'run' && r.phase !== 'count') || me.status === 'done') return;
      me.status = 'done'; me.win = m.win && !m.quit ? 1 : 0; me.quit = m.quit ? 1 : 0;
      me.tm = clampI(m.tm, 0, 1e7, me.tm); me.kills = clampI(m.kills, 0, 1e7, me.kills);
      if (r.phase === 'run') { finishIfDone(r); if (r.phase === 'run') broadcast(r); }
      else broadcast(r);
      return;
    }
  }
}

server.listen(PORT, () => {
  const nets = require('os').networkInterfaces(), ips = [];
  for (const k in nets) for (const n of nets[k]) if (n.family === 'IPv4' && !n.internal) ips.push(n.address);
  console.log('Arcane Survivor — máy chủ phòng đang chạy (' + CHAPS + ' chapter × ' + PER + ' màn = ' + STAGES + ' màn)');
  console.log('  Trên máy này : http://localhost:' + PORT);
  for (const ip of ips) console.log('  Cùng mạng LAN: http://' + ip + ':' + PORT);
  console.log('Bạn bè mở đúng địa chỉ trên (hoặc địa chỉ tunnel https) rồi bấm "Chơi online".');
});
