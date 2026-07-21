/**
 * RemoteDesk Signaling Server v2
 * Deployed on Railway
 * ─────────────────────────────────────────────────────────
 * Features:
 *   - Multi-session support
 *   - Password-based auto-accept signaling
 *   - Permission system relay
 *   - Voice/video/clipboard/file signaling
 *   - Chat message relay
 *   - System info relay
 *   - Quality control relay
 *   - Health check + stats API
 *   - Keep-alive self ping (Railway free tier)
 *   - Auto cleanup on disconnect
 */

const express  = require("express");
const http     = require("http");
const cors     = require("cors");
const { Server } = require("socket.io");

const app = express();

// ─── CORS ──────────────────────────────────────────────────
app.use(cors({
  origin:  "*",
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"],
  credentials: false,
}));

app.use(express.json());
app.options("*", cors()); // Pre-flight for all routes

// ─── HTTP Server ───────────────────────────────────────────
const server = http.createServer(app);

// ─── Socket.IO ─────────────────────────────────────────────
const io = new Server(server, {
  cors: {
    origin:      "*",
    methods:     ["GET", "POST"],
    credentials: false,
  },
  // ✅ Railway: allow polling + websocket
  transports:        ["polling", "websocket"],
  allowEIO3:         true,
  maxHttpBufferSize: 1e8,    // 100 MB (for large signal payloads)
  pingTimeout:       60000,  // 60s
  pingInterval:      25000,  // 25s
  connectTimeout:    45000,
});

// ─── Data Stores ───────────────────────────────────────────

/**
 * clients: remoteId → {
 *   socketId:    string,
 *   connectedTo: Set<remoteId>,
 *   joinedAt:    Date,
 * }
 */
const clients    = new Map();

/**
 * socketToId: socketId → remoteId
 */
const socketToId = new Map();

/**
 * sessions: sessionId → {
 *   hostId:      string,
 *   viewerId:    string,
 *   startedAt:   Date,
 *   permissions: object,
 * }
 */
const sessions   = new Map();

// ─── Utilities ─────────────────────────────────────────────

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
  return clients.get(remoteId)?.socketId || null;
}

function getRemoteId(socketId) {
  return socketToId.get(socketId) || null;
}

function defaultPermissions() {
  return {
    mouse:        true,
    keyboard:     true,
    clipboard:    true,
    fileTransfer: true,
    audio:        true,
    viewOnly:     false,
    remoteReboot: false,
    chat:         true,
  };
}

function log(tag, msg, data = "") {
  const time = new Date().toISOString();
  if (data) {
    console.log(`[${time}] [${tag}] ${msg} | ${data}`);
  } else {
    console.log(`[${time}] [${tag}] ${msg}`);
  }
}

// ─── REST API ──────────────────────────────────────────────

// Root — service info
app.get("/", (req, res) => {
  res.json({
    success:          true,
    service:          "RemoteDesk Signaling Server v2",
    connectedClients: clients.size,
    activeSessions:   sessions.size,
    uptime:           Math.floor(process.uptime()),
    timestamp:        new Date().toISOString(),
    version:          "2.0.0",
  });
});

// Health check — Railway uses this
app.get("/health", (req, res) => {
  res.json({
    status:  "ok",
    uptime:  Math.floor(process.uptime()),
    clients: clients.size,
    sessions: sessions.size,
  });
});

// Stats — detailed session info
app.get("/stats", (req, res) => {
  const sessionList = [];
  sessions.forEach((s, id) => {
    sessionList.push({
      sessionId:   id,
      hostId:      s.hostId,
      viewerId:    s.viewerId,
      startedAt:   s.startedAt,
      permissions: s.permissions,
    });
  });

  const clientList = [];
  clients.forEach((c, id) => {
    clientList.push({
      remoteId:    id,
      connectedTo: [...c.connectedTo],
      joinedAt:    c.joinedAt,
    });
  });

  res.json({
    connectedClients: clients.size,
    activeSessions:   sessions.size,
    sessions:         sessionList,
    clients:          clientList,
  });
});

// Single client info
app.get("/client/:remoteId", (req, res) => {
  const client = clients.get(req.params.remoteId);
  if (!client) {
    return res.status(404).json({ error: "Client not found or offline" });
  }
  res.json({
    remoteId:    req.params.remoteId,
    connectedTo: [...client.connectedTo],
    joinedAt:    client.joinedAt,
  });
});

// ─── Socket.IO Events ──────────────────────────────────────

io.on("connection", (socket) => {

  // Assign a unique 9-digit remote ID
  const remoteId = generateRemoteId();

  clients.set(remoteId, {
    socketId:    socket.id,
    connectedTo: new Set(),
    joinedAt:    new Date(),
  });
  socketToId.set(socket.id, remoteId);

  log("CONNECT", `RemoteID: ${remoteId}`, `Socket: ${socket.id}`);

  // Send the assigned ID to the client
  socket.emit("registered", { remoteId });

  // ── Connection Request (Viewer → Host) ──────────────────
  socket.on("connect-request", ({ targetId, password, viewerInfo }) => {
    const fromId        = getRemoteId(socket.id);
    const targetSocketId = getSocketId(targetId);

    if (!targetSocketId) {
      socket.emit("connect-error", {
        message: `Target ID "${targetId}" not found or offline.`,
      });
      log("REQUEST_FAIL", `${fromId} → ${targetId}`, "Target not found");
      return;
    }

    log("REQUEST", `${fromId} → ${targetId}`);

    // Forward request to host
    io.to(targetSocketId).emit("incoming-request", {
      fromId,
      fromSocket: socket.id,
      password:   password || null,
      viewerInfo: viewerInfo || {},
    });
  });

  // ── Connection Response (Host → Viewer) ─────────────────
  socket.on("connect-response", ({ toSocket, accepted, reason, permissions, sessionId }) => {
    const hostId   = getRemoteId(socket.id);
    const viewerId = getRemoteId(toSocket);

    if (accepted && hostId && viewerId) {
      const sid = sessionId || generateSessionId();

      // Register the session
      sessions.set(sid, {
        hostId,
        viewerId,
        startedAt:   new Date(),
        permissions: permissions || defaultPermissions(),
      });

      // Track connections both ways
      clients.get(hostId)?.connectedTo.add(viewerId);
      clients.get(viewerId)?.connectedTo.add(hostId);

      // Notify viewer — accepted
      io.to(toSocket).emit("connect-response", {
        accepted:    true,
        hostSocket:  socket.id,
        hostId,
        sessionId:   sid,
        permissions: permissions || defaultPermissions(),
      });

      log("SESSION_START", `ID: ${sid}`, `Host: ${hostId} ← Viewer: ${viewerId}`);

    } else {
      // Notify viewer — rejected
      io.to(toSocket).emit("connect-response", {
        accepted: false,
        reason:   reason || "Connection rejected by host",
      });
      log("SESSION_REJECT", `Host: ${hostId}`, `Viewer: ${viewerId}`);
    }
  });

  // ── WebRTC Signal Relay ──────────────────────────────────
  // Relays SDP offers/answers and ICE candidates between peers
  socket.on("signal", ({ toSocket, data, sessionId }) => {
    const fromId = getRemoteId(socket.id);

    // Validate target socket exists
    if (!io.sockets.sockets.has(toSocket)) {
      log("SIGNAL_FAIL", `Target socket not found: ${toSocket}`);
      return;
    }

    io.to(toSocket).emit("signal", {
      fromSocket: socket.id,
      fromId,
      sessionId:  sessionId || null,
      data,
    });
  });

  // ── Permission Update (Host → Viewer) ───────────────────
  socket.on("update-permissions", ({ toSocket, permissions, sessionId }) => {
    const hostId = getRemoteId(socket.id);

    // Update stored session permissions
    if (sessions.has(sessionId)) {
      sessions.get(sessionId).permissions = permissions;
    }

    io.to(toSocket).emit("permissions-updated", {
      permissions,
      sessionId,
      fromId: hostId,
    });

    log("PERMISSIONS", `Session: ${sessionId}`, JSON.stringify(permissions));
  });

  // ── Session Terminate (Either side) ─────────────────────
  socket.on("terminate-session", ({ toSocket, sessionId, reason }) => {
    const fromId = getRemoteId(socket.id);

    // Clean up session data
    if (sessions.has(sessionId)) {
      const sess = sessions.get(sessionId);
      clients.get(sess.hostId)?.connectedTo.delete(sess.viewerId);
      clients.get(sess.viewerId)?.connectedTo.delete(sess.hostId);
      sessions.delete(sessionId);
    }

    // Notify the other peer
    io.to(toSocket).emit("session-terminated", {
      sessionId,
      reason: reason || "Remote side ended the session",
      byId:   fromId,
    });

    log("SESSION_END", `ID: ${sessionId}`, `By: ${fromId} | Reason: ${reason}`);
  });

  // ── Chat Message Relay ───────────────────────────────────
  socket.on("chat-message", ({ toSocket, message, sessionId }) => {
    const fromId = getRemoteId(socket.id);

    io.to(toSocket).emit("chat-message", {
      fromId,
      message,
      sessionId,
      timestamp: Date.now(),
    });
  });

  // ── System Info Relay ────────────────────────────────────
  socket.on("system-info", ({ toSocket, info, sessionId }) => {
    const fromId = getRemoteId(socket.id);

    io.to(toSocket).emit("system-info", {
      info,
      sessionId,
      fromId,
    });
  });

  // ── Quality Change Request ───────────────────────────────
  socket.on("quality-change", ({ toSocket, quality, sessionId }) => {
    io.to(toSocket).emit("quality-change", {
      quality,
      sessionId,
    });
  });

  // ── Disconnect ───────────────────────────────────────────
  socket.on("disconnect", (reason) => {
    const remoteId = getRemoteId(socket.id);
    if (!remoteId) return;

    log("DISCONNECT", `RemoteID: ${remoteId}`, `Reason: ${reason}`);

    const client = clients.get(remoteId);

    if (client) {
      // Notify all peers this client was connected to
      client.connectedTo.forEach((peerId) => {
        const peerSocketId = getSocketId(peerId);
        if (peerSocketId) {
          io.to(peerSocketId).emit("peer-disconnected", {
            remoteId,
            socketId: socket.id,
          });
        }
        // Clean up peer's connectedTo
        clients.get(peerId)?.connectedTo.delete(remoteId);
      });
    }

    // Clean up sessions involving this client
    sessions.forEach((sess, sid) => {
      if (sess.hostId === remoteId || sess.viewerId === remoteId) {
        sessions.delete(sid);
        log("SESSION_AUTO_CLOSE", `ID: ${sid}`, `Reason: ${remoteId} disconnected`);
      }
    });

    // Remove client data
    clients.delete(remoteId);
    socketToId.delete(socket.id);
  });

  // ── Error handler ────────────────────────────────────────
  socket.on("error", (err) => {
    log("SOCKET_ERROR", `RemoteID: ${getRemoteId(socket.id)}`, err.message);
  });

});

// ─── Keep-Alive (Railway free tier sleeps after inactivity) ─
const APP_URL =
  process.env.APP_URL ||
  "https://node-js-api-production-6b70.up.railway.app";

// Self-ping every 4 minutes to prevent Railway from sleeping
setInterval(async () => {
  try {
    const res = await fetch(`${APP_URL}/health`);
    log("KEEPALIVE", `Ping → ${res.status}`);
  } catch (err) {
    log("KEEPALIVE_FAIL", err.message);
  }
}, 4 * 60 * 1000);

// ─── Periodic cleanup (every 10 min) ───────────────────────
// Remove stale sessions where peers are no longer connected
setInterval(() => {
  let cleaned = 0;
  sessions.forEach((sess, sid) => {
    const hostOnline   = clients.has(sess.hostId);
    const viewerOnline = clients.has(sess.viewerId);
    if (!hostOnline || !viewerOnline) {
      sessions.delete(sid);
      cleaned++;
    }
  });
  if (cleaned > 0) {
    log("CLEANUP", `Removed ${cleaned} stale sessions`);
  }
}, 10 * 60 * 1000);

// ─── Server Stats Log (every 5 min) ────────────────────────
setInterval(() => {
  log(
    "STATS",
    `Clients: ${clients.size}`,
    `Sessions: ${sessions.size} | Uptime: ${Math.floor(process.uptime())}s`
  );
}, 5 * 60 * 1000);

// ─── Listen ────────────────────────────────────────────────
// ✅ Railway automatically sets PORT env variable
const PORT = process.env.PORT || 4000;

server.listen(PORT, "0.0.0.0", () => {
  console.log("============================================");
  console.log("  RemoteDesk Signaling Server v2");
  console.log("============================================");
  console.log(`  Port    : ${PORT}`);
  console.log(`  URL     : ${APP_URL}`);
  console.log(`  Env     : ${process.env.NODE_ENV || "development"}`);
  console.log("============================================");
});

// ─── Graceful shutdown ─────────────────────────────────────
process.on("SIGTERM", () => {
  log("SHUTDOWN", "SIGTERM received — closing server");
  server.close(() => {
    log("SHUTDOWN", "Server closed");
    process.exit(0);
  });
});

process.on("SIGINT", () => {
  log("SHUTDOWN", "SIGINT received — closing server");
  server.close(() => {
    log("SHUTDOWN", "Server closed");
    process.exit(0);
  });
});

process.on("uncaughtException", (err) => {
  log("UNCAUGHT_EXCEPTION", err.message, err.stack);
});

process.on("unhandledRejection", (reason) => {
  log("UNHANDLED_REJECTION", String(reason));
});