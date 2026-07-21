/**
 * RemoteDesk Signaling Server v2
 * Features:
 *  - Multi-session support
 *  - Password-based auto-accept signaling
 *  - Permission system relay
 *  - Voice/video/clipboard/file signaling
 *  - Session management
 *  - Health + stats API
 */

const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  maxHttpBufferSize: 1e8,
});

// =============================
// Data Stores
// =============================

const clients = new Map();
// clients: remoteId → {
//   socketId,
//   connectedTo: Set<remoteId>,   // active sessions
//   joinedAt: Date,
// }

const socketToId = new Map();
// socketId → remoteId

const sessions = new Map();
// sessionId → { hostId, viewerId, startedAt, permissions }

// =============================
// Utilities
// =============================

function generateRemoteId() {
  let id;
  do {
    id = Math.floor(100000000 + Math.random() * 900000000).toString();
  } while (clients.has(id));
  return id;
}

function generateSessionId() {
  return `sess_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function getSocketId(remoteId) {
  return clients.get(remoteId)?.socketId;
}

function getRemoteId(socketId) {
  return socketToId.get(socketId);
}

function log(tag, msg, data = '') {
  console.log(`[${new Date().toISOString()}] [${tag}] ${msg}`, data);
}

// =============================
// REST API
// =============================

app.get("/", (req, res) => {
  res.json({
    success: true,
    service: "RemoteDesk Signaling Server v2",
    connectedClients: clients.size,
    activeSessions: sessions.size,
    uptime: process.uptime(),
  });
});

app.get("/stats", (req, res) => {
  const sessionList = [];
  sessions.forEach((s, id) => {
    sessionList.push({
      sessionId: id,
      hostId: s.hostId,
      viewerId: s.viewerId,
      startedAt: s.startedAt,
      permissions: s.permissions,
    });
  });
  res.json({
    connectedClients: clients.size,
    activeSessions: sessions.size,
    sessions: sessionList,
  });
});

app.get("/client/:remoteId", (req, res) => {
  const client = clients.get(req.params.remoteId);
  if (!client) return res.status(404).json({ error: "Not found" });
  res.json({
    remoteId: req.params.remoteId,
    connectedTo: [...client.connectedTo],
    joinedAt: client.joinedAt,
  });
});

// =============================
// Socket.IO
// =============================

io.on("connection", (socket) => {
  const remoteId = generateRemoteId();

  clients.set(remoteId, {
    socketId: socket.id,
    connectedTo: new Set(),
    joinedAt: new Date(),
  });
  socketToId.set(socket.id, remoteId);

  log("CONNECT", `RemoteID: ${remoteId}`, `Socket: ${socket.id}`);

  socket.emit("registered", { remoteId });

  // ---- Connection Request ----
  // Viewer → Host
  socket.on("connect-request", ({ targetId, password, viewerInfo }) => {
    const targetSocketId = getSocketId(targetId);
    const fromId = getRemoteId(socket.id);

    if (!targetSocketId) {
      socket.emit("connect-error", { message: "Target ID not found." });
      return;
    }

    log("REQUEST", `${fromId} → ${targetId}`);

    io.to(targetSocketId).emit("incoming-request", {
      fromId,
      fromSocket: socket.id,
      password,
      viewerInfo: viewerInfo || {},
    });
  });

  // ---- Connection Response ----
  // Host → Viewer
  socket.on("connect-response", ({ toSocket, accepted, reason, permissions, sessionId }) => {
    const hostId = getRemoteId(socket.id);
    const viewerId = getRemoteId(toSocket);

    if (accepted && hostId && viewerId) {
      const sid = sessionId || generateSessionId();

      // Register session
      sessions.set(sid, {
        hostId,
        viewerId,
        startedAt: new Date(),
        permissions: permissions || defaultPermissions(),
      });

      // Track connections
      clients.get(hostId)?.connectedTo.add(viewerId);
      clients.get(viewerId)?.connectedTo.add(hostId);

      io.to(toSocket).emit("connect-response", {
        accepted,
        hostSocket: socket.id,
        hostId,
        sessionId: sid,
        permissions: permissions || defaultPermissions(),
      });

      log("SESSION", `Started: ${sid}`, `Host: ${hostId} ← Viewer: ${viewerId}`);
    } else {
      io.to(toSocket).emit("connect-response", {
        accepted: false,
        reason: reason || "Rejected",
      });
      log("REJECT", `Host: ${hostId} rejected viewer: ${getRemoteId(toSocket)}`);
    }
  });

  // ---- WebRTC Signal Relay ----
  socket.on("signal", ({ toSocket, data, sessionId }) => {
    const fromId = getRemoteId(socket.id);
    io.to(toSocket).emit("signal", {
      fromSocket: socket.id,
      fromId,
      sessionId,
      data,
    });
  });

  // ---- Permission Update ----
  // Host can update permissions live
  socket.on("update-permissions", ({ toSocket, permissions, sessionId }) => {
    const hostId = getRemoteId(socket.id);

    if (sessions.has(sessionId)) {
      sessions.get(sessionId).permissions = permissions;
    }

    io.to(toSocket).emit("permissions-updated", {
      permissions,
      sessionId,
      fromId: hostId,
    });

    log("PERMS", `Updated for session ${sessionId}`, JSON.stringify(permissions));
  });

  // ---- Session Terminate ----
  socket.on("terminate-session", ({ toSocket, sessionId, reason }) => {
    const fromId = getRemoteId(socket.id);

    if (sessions.has(sessionId)) {
      const sess = sessions.get(sessionId);
      clients.get(sess.hostId)?.connectedTo.delete(sess.viewerId);
      clients.get(sess.viewerId)?.connectedTo.delete(sess.hostId);
      sessions.delete(sessionId);
    }

    io.to(toSocket).emit("session-terminated", {
      sessionId,
      reason: reason || "Remote side disconnected",
      byId: fromId,
    });

    log("TERMINATE", `Session: ${sessionId}`, `By: ${fromId}`);
  });

  // ---- Chat Message Relay ----
  socket.on("chat-message", ({ toSocket, message, sessionId }) => {
    const fromId = getRemoteId(socket.id);
    io.to(toSocket).emit("chat-message", {
      fromId,
      message,
      sessionId,
      timestamp: Date.now(),
    });
  });

  // ---- System Info Relay ----
  socket.on("system-info", ({ toSocket, info, sessionId }) => {
    io.to(toSocket).emit("system-info", {
      info,
      sessionId,
      fromId: getRemoteId(socket.id),
    });
  });

  // ---- Quality Change Request ----
  socket.on("quality-change", ({ toSocket, quality, sessionId }) => {
    io.to(toSocket).emit("quality-change", { quality, sessionId });
  });

  // ---- Disconnect ----
  socket.on("disconnect", () => {
    const remoteId = getRemoteId(socket.id);
    if (!remoteId) return;

    const client = clients.get(remoteId);
    if (client) {
      // Notify all connected peers
      client.connectedTo.forEach((peerId) => {
        const peerSocketId = getSocketId(peerId);
        if (peerSocketId) {
          io.to(peerSocketId).emit("peer-disconnected", {
            remoteId,
            socketId: socket.id,
          });
        }
        // Clean peer's connectedTo
        clients.get(peerId)?.connectedTo.delete(remoteId);
      });
    }

    // Clean sessions involving this client
    sessions.forEach((sess, sid) => {
      if (sess.hostId === remoteId || sess.viewerId === remoteId) {
        sessions.delete(sid);
        log("SESSION", `Auto-closed: ${sid}`, `Reason: ${remoteId} disconnected`);
      }
    });

    clients.delete(remoteId);
    socketToId.delete(socket.id);

    log("DISCONNECT", `RemoteID: ${remoteId}`);
  });
});

// =============================
// Helpers
// =============================

function defaultPermissions() {
  return {
    mouse: true,
    keyboard: true,
    clipboard: true,
    fileTransfer: true,
    audio: true,
    viewOnly: false,
    remoteReboot: false,
    chat: true,
  };
}

// =============================
// Listen
// =============================

const PORT = process.env.PORT || 4000;
server.listen(PORT, "0.0.0.0", () => {
  console.log("====================================");
  console.log(" RemoteDesk Signaling Server v2");
  console.log("====================================");
  console.log(` Port    : ${PORT}`);
  console.log(` Host    : 0.0.0.0`);
  console.log("====================================");
});