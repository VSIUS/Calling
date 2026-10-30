const express = require("express");
const http = require("node:http");
const { Server } = require("socket.io");

const app = express();
app.disable("x-powered-by");
app.get("/", (_req, res) => res.status(200).send("Call signaling server is running"));
app.get("/health", (_req, res) => res.json({ ok: true, service: "call-signaling" }));

const httpServer = http.createServer(app);
const io = new Server(httpServer, {
  cors: { origin: "*" },
  transports: ["websocket", "polling"],
  pingInterval: 25000,
  pingTimeout: 20000,
  maxHttpBufferSize: 64 * 1024
});

const rooms = new Map(); // roomCode -> Set(socketId)
const socketRoom = new Map(); // socketId -> roomCode
const recentEvents = new Map();

function validRoom(value) {
  return typeof value === "string" && /^\d{6}$/.test(value);
}
function allowRate(socket, eventName, max, windowMs) {
  const now = Date.now();
  const key = `${socket.id}:${eventName}`;
  const old = recentEvents.get(key) || [];
  const fresh = old.filter(t => now - t < windowMs);
  if (fresh.length >= max) return false;
  fresh.push(now);
  recentEvents.set(key, fresh);
  return true;
}
function leaveRoom(socket, notify = true) {
  const room = socketRoom.get(socket.id);
  if (!room) return;
  socket.leave(room);
  socketRoom.delete(socket.id);
  const members = rooms.get(room);
  if (members) {
    members.delete(socket.id);
    if (notify) socket.to(room).emit("peer-left");
    if (members.size === 0) rooms.delete(room);
  }
}

io.on("connection", socket => {
  socket.on("join-room", payload => {
    if (!allowRate(socket, "join-room", 8, 60_000)) {
      socket.emit("room-error", { reason: "rate-limit" });
      return;
    }
    const room = payload && payload.room;
    if (!validRoom(room)) {
      socket.emit("room-error", { reason: "invalid-room" });
      return;
    }
    if (socketRoom.has(socket.id)) leaveRoom(socket, false);

    let members = rooms.get(room);
    if (!members) {
      members = new Set();
      rooms.set(room, members);
    }
    if (members.size >= 2) {
      socket.emit("room-error", { reason: "room-full" });
      if (members.size === 0) rooms.delete(room);
      return;
    }

    members.add(socket.id);
    socketRoom.set(socket.id, room);
    socket.join(room);
    if (members.size === 2) socket.to(room).emit("peer-joined");
  });

  socket.on("signal", payload => {
    if (!allowRate(socket, "signal", 120, 60_000)) return;
    const room = payload && payload.room;
    const data = payload && payload.data;
    if (!validRoom(room) || socketRoom.get(socket.id) !== room || !data) return;

    const type = data.type;
    if (type === "offer" || type === "answer") {
      if (typeof data.sdp !== "string" || data.sdp.length > 48_000) return;
      socket.to(room).emit("signal", { type, sdp: data.sdp });
    } else if (type === "candidate") {
      if (typeof data.candidate !== "string" || data.candidate.length > 4_000) return;
      if (typeof data.sdpMid !== "string" || data.sdpMid.length > 64) return;
      const line = Number(data.sdpMLineIndex);
      if (!Number.isInteger(line) || line < 0 || line > 32) return;
      socket.to(room).emit("signal", {
        type, candidate: data.candidate,
        sdpMid: data.sdpMid, sdpMLineIndex: line
      });
    }
  });

  socket.on("leave-room", () => leaveRoom(socket));
  socket.on("disconnect", () => {
    leaveRoom(socket);
    for (const key of recentEvents.keys()) {
      if (key.startsWith(`${socket.id}:`)) recentEvents.delete(key);
    }
  });
});

// Remove empty rooms defensively; occupied rooms are removed on disconnect.
setInterval(() => {
  for (const [room, members] of rooms) {
    for (const id of members) {
      if (!io.sockets.sockets.has(id)) members.delete(id);
    }
    if (members.size === 0) rooms.delete(room);
  }
}, 60_000).unref();

const port = Number(process.env.PORT || 3000);
httpServer.listen(port, "0.0.0.0", () => {
  console.log(`Call signaling server listening on ${port}`);
});
