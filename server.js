'use strict';

const express = require('express');
const http = require('node:http');
const { randomInt } = require('node:crypto');
const { Server } = require('socket.io');

const app = express();
app.disable('x-powered-by');
const httpServer = http.createServer(app);
const PORT = Number(process.env.PORT || 3000);
// No fixed participant limit. Capacity is constrained only by hosting resources.
const rooms = new Map(); // code -> { code, name, hostId, members: Map(socketId -> member), createdAt }
const socketRoom = new Map();

const io = new Server(httpServer, {
  cors: { origin: '*' },
  transports: ['websocket', 'polling'],
  pingInterval: 25000,
  pingTimeout: 20000,
  maxHttpBufferSize: 1e6
});

app.get('/', (_req, res) => res.json({ ok: true, service: 'call-signaling', protocol: 'v8' }));
app.get('/health', (_req, res) => res.json({
  ok: true,
  service: 'call-signaling',
  protocol: 'v8',
  rooms: rooms.size,
  connectedClients: io.engine.clientsCount,
  participants: [...rooms.values()].reduce((n, room) => n + room.members.size, 0)
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

function cleanText(value, fallback, max = 32) {
  if (typeof value !== 'string') return fallback;
  const text = value.trim().replace(/[\u0000-\u001f\u007f]/g, '').slice(0, max);
  return text || fallback;
}

function newCode() {
  let code;
  do { code = String(randomInt(100000, 1000000)); } while (rooms.has(code));
  return code;
}

function roomError(socket, code, reason, message) {
  socket.emit('room-error', { code, reason, message });
}

function roomSnapshot(room) {
  return {
    room: room.code,
    code: room.code,
    roomName: room.name,
    hostId: room.hostId,
    members: [...room.members.values()].map(member => ({
      id: member.id,
      displayName: member.displayName,
      role: member.role
    }))
  };
}

function emitRoomState(room) {
  io.to(room.code).emit('room-state', roomSnapshot(room));
  io.to(room.code).emit('room-updated', roomSnapshot(room));
}

function leaveRoom(socket, notifyPeer = true) {
  const code = socketRoom.get(socket.id);
  if (!code) return;
  socketRoom.delete(socket.id);
  socket.leave(code);
  const room = rooms.get(code);
  if (!room) return;
  room.members.delete(socket.id);
  if (room.hostId === socket.id) {
    const nextHost = room.members.keys().next().value || null;
    room.hostId = nextHost;
    if (nextHost) {
      const member = room.members.get(nextHost);
      if (member) member.role = 'host';
    }
  }
  if (notifyPeer) socket.to(code).emit('peer-left', { room: code, memberId: socket.id });
  if (room.members.size === 0) {
    rooms.delete(code);
  } else {
    emitRoomState(room);
  }
}

function announceJoined(socket, room, role) {
  const data = { ...roomSnapshot(room), role, iceServers: iceServers(), protocol: 'v8' };
  socket.emit('room-joined', data);
  socket.emit('joined-room', data); // legacy event compatibility
}

function createRoom(socket, payload = {}) {
  if (socketRoom.has(socket.id)) {
    roomError(socket, 'ALREADY_IN_ROOM', 'already-in-room', 'Leave the current room first');
    return;
  }
  const code = newCode();
  const displayName = cleanText(payload.displayName, 'Người gọi');
  const name = cleanText(payload.roomName, `Phòng ${code}`, 48);
  const member = { id: socket.id, displayName, role: 'host' };
  const room = { code, name, hostId: socket.id, members: new Map([[socket.id, member]]), createdAt: Date.now() };
  rooms.set(code, room);
  socketRoom.set(socket.id, code);
  socket.join(code);
  announceJoined(socket, room, 'host');
  emitRoomState(room);
}

function joinRoom(socket, payload = {}) {
  if (socketRoom.has(socket.id)) {
    roomError(socket, 'ALREADY_IN_ROOM', 'already-in-room', 'Leave the current room first');
    return;
  }
  const code = typeof payload.room === 'string' ? payload.room.trim()
    : (typeof payload.code === 'string' ? payload.code.trim() : '');
  if (!/^\d{6}$/.test(code)) {
    roomError(socket, 'INVALID_CODE', 'invalid-room', 'Room code must contain six digits');
    return;
  }
  const room = rooms.get(code);
  if (!room) {
    roomError(socket, 'ROOM_NOT_FOUND', 'room-not-found', 'Room does not exist');
    return;
  }
  const displayName = cleanText(payload.displayName, 'Người tham gia');
  const member = { id: socket.id, displayName, role: 'guest' };
  room.members.set(socket.id, member);
  socketRoom.set(socket.id, code);
  socket.join(code);
  announceJoined(socket, room, 'guest');
  socket.to(code).emit('peer-joined', { room: code, member: { ...member } });
  emitRoomState(room);
}

io.on('connection', socket => {
  socket.on('create-room', payload => createRoom(socket, payload || {}));
  socket.on('join-room', payload => joinRoom(socket, payload || {}));

  socket.on('update-display-name', payload => {
    const code = socketRoom.get(socket.id);
    const room = code && rooms.get(code);
    const member = room && room.members.get(socket.id);
    if (!member) return;
    member.displayName = cleanText(payload && payload.displayName, member.displayName);
    socket.emit('display-name-updated', { displayName: member.displayName });
    emitRoomState(room);
  });

  socket.on('rename-room', payload => {
    const code = socketRoom.get(socket.id);
    const room = code && rooms.get(code);
    if (!room || room.hostId !== socket.id) {
      roomError(socket, 'NOT_HOST', 'not-host', 'Only the room host can rename the room');
      return;
    }
    room.name = cleanText(payload && payload.roomName, room.name, 48);
    emitRoomState(room);
  });

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
    for (const id of [...room.members.keys()]) {
      if (!io.sockets.sockets.has(id)) room.members.delete(id);
    }
    if (!room.members.has(room.hostId)) {
      room.hostId = room.members.keys().next().value || null;
      if (room.hostId && room.members.get(room.hostId)) room.members.get(room.hostId).role = 'host';
    }
    if (room.members.size === 0) rooms.delete(code);
    else emitRoomState(room);
  }
}, 30000).unref();

httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`Call signaling V8 listening on ${PORT}`);
});
