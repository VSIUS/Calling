'use strict';

const express = require('express');
const http = require('node:http');
const { randomInt } = require('node:crypto');
const { Server } = require('socket.io');

const app = express();
app.disable('x-powered-by');
const httpServer = http.createServer(app);
const PORT = Number(process.env.PORT || 3000);
const MAX_MEMBERS = 2;
const rooms = new Map(); // code -> { members: Set(socketId), createdAt }
const socketRoom = new Map();

const io = new Server(httpServer, {
  cors: { origin: '*' },
  transports: ['websocket', 'polling'],
  pingInterval: 25000,
  pingTimeout: 20000,
  maxHttpBufferSize: 1e6
});

app.get('/', (_req, res) => res.json({ ok: true, service: 'call-signaling', protocol: 'v7' }));
app.get('/health', (_req, res) => res.json({
  ok: true,
  service: 'call-signaling',
  protocol: 'v7',
  rooms: rooms.size,
  connectedClients: io.engine.clientsCount
}));

function iceServers() {
  const list = [
    { urls: ['stun:stun.l.google.com:19302'] },
    { urls: ['stun:stun.cloudflare.com:3478'] }
  ];
  const urls = (process.env.TURN_URLS || process.env.TURN_URL || '')
    .split(',').map(v => v.trim()).filter(Boolean);
  const username = process.env.TURN_USERNAME || '';
  const credential = process.env.TURN_CREDENTIAL || '';
  if (urls.length && username && credential) list.push({ urls, username, credential });
  return list;
}

function newCode() {
  let code;
  do { code = String(randomInt(100000, 1000000)); } while (rooms.has(code));
  return code;
}

function roomError(socket, code, reason, message) {
  socket.emit('room-error', { code, reason, message });
}

function leaveRoom(socket, notifyPeer = true) {
  const code = socketRoom.get(socket.id);
  if (!code) return;
  socketRoom.delete(socket.id);
  socket.leave(code);
  const room = rooms.get(code);
  if (!room) return;
  room.members.delete(socket.id);
  if (notifyPeer) socket.to(code).emit('peer-left', { room: code });
  if (room.members.size === 0) rooms.delete(code);
}

function announceJoined(socket, code, role) {
  socket.emit('room-joined', { room: code, code, role, iceServers: iceServers() });
  // Compatibility with clients that only listen for the older success event.
  socket.emit('joined-room', { room: code, code, role, iceServers: iceServers() });
}

function createRoom(socket) {
  if (socketRoom.has(socket.id)) {
    roomError(socket, 'ALREADY_IN_ROOM', 'already-in-room', 'Leave the current room first');
    return;
  }
  const code = newCode();
  rooms.set(code, { members: new Set([socket.id]), createdAt: Date.now() });
  socketRoom.set(socket.id, code);
  socket.join(code);
  announceJoined(socket, code, 'host');
}

function joinRoom(socket, payload) {
  if (socketRoom.has(socket.id)) {
    roomError(socket, 'ALREADY_IN_ROOM', 'already-in-room', 'Leave the current room first');
    return;
  }
  const code = payload && typeof payload.room === 'string'
    ? payload.room.trim()
    : (payload && typeof payload.code === 'string' ? payload.code.trim() : '');
  if (!/^\d{6}$/.test(code)) {
    roomError(socket, 'INVALID_CODE', 'invalid-room', 'Room code must contain six digits');
    return;
  }
  const room = rooms.get(code);
  if (!room) {
    roomError(socket, 'ROOM_NOT_FOUND', 'room-not-found', 'Room does not exist');
    return;
  }
  for (const id of [...room.members]) {
    if (!io.sockets.sockets.has(id)) room.members.delete(id);
  }
  if (room.members.size >= MAX_MEMBERS) {
    roomError(socket, 'ROOM_FULL', 'room-full', 'Room already has two members');
    return;
  }
  room.members.add(socket.id);
  socketRoom.set(socket.id, code);
  socket.join(code);
  announceJoined(socket, code, 'guest');
  socket.to(code).emit('peer-joined', { room: code });
}

io.on('connection', socket => {
  socket.on('create-room', () => createRoom(socket));
  socket.on('join-room', payload => joinRoom(socket, payload));
  socket.on('signal', payload => {
    const code = socketRoom.get(socket.id);
    if (!code || !payload || payload.room !== code || !payload.data || typeof payload.data !== 'object') return;
    const room = rooms.get(code);
    if (!room || !room.members.has(socket.id)) return;
    const data = payload.data;
    if (!['offer', 'answer', 'candidate'].includes(data.type)) return;
    if (typeof data.sdp === 'string' && data.sdp.length > 200000) return;
    if (typeof data.candidate === 'string' && data.candidate.length > 10000) return;
    socket.to(code).emit('signal', data);
  });
  socket.on('leave-room', () => leaveRoom(socket));
  socket.on('disconnect', () => leaveRoom(socket));
});

setInterval(() => {
  for (const [code, room] of rooms) {
    for (const id of [...room.members]) {
      if (!io.sockets.sockets.has(id)) room.members.delete(id);
    }
    if (room.members.size === 0) rooms.delete(code);
  }
}, 30000).unref();

httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`Call signaling V7 listening on ${PORT}`);
});
