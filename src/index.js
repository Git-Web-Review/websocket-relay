import http from "node:http";
import Redis from "ioredis";
import { WebSocketServer, WebSocket } from "ws";

const port = Number(process.env.PORT ?? 3001);
const redisUrl = process.env.REDIS_URL ?? "redis://localhost:6379";
const backendUrl = (process.env.BACKEND_URL ?? "http://localhost:3000").replace(
  /\/$/,
  "",
);
const redisPattern =
  process.env.REDIS_CHANNEL_PATTERN ?? "notifications:user:*";
const userChannelPrefix =
  process.env.REDIS_USER_CHANNEL_PREFIX ?? "notifications:user:";
const heartbeatIntervalMs = Number(
  process.env.WS_HEARTBEAT_INTERVAL_MS ?? 30000,
);

const parseAllowedHosts = (value) => {
  const hosts = value
    ?.split(",")
    .map((host) => host.trim().toLowerCase())
    .filter(Boolean);

  if (!hosts?.length) {
    return undefined;
  }

  if (hosts.some((host) => host === "*" || host === "true")) {
    return true;
  }

  return hosts;
};

const stripPort = (host) => {
  if (host.startsWith("[")) {
    const ipv6End = host.indexOf("]");
    return ipv6End === -1 ? host : host.slice(1, ipv6End);
  }

  return host.split(":")[0] ?? host;
};

const isHostAllowed = (requestHost, allowedHosts) => {
  const normalizedHost = requestHost.trim().toLowerCase();
  const hostname = stripPort(normalizedHost);

  return allowedHosts.some((allowedHost) => {
    if (allowedHost === normalizedHost || allowedHost === hostname) {
      return true;
    }

    if (allowedHost.startsWith(".")) {
      const suffix = allowedHost.slice(1);
      return hostname === suffix || hostname.endsWith(allowedHost);
    }

    return false;
  });
};

const allowedHosts = parseAllowedHosts(process.env.WEBSOCKET_ALLOWED_HOSTS);

const clientsByUserId = new Map();
const subscriber = new Redis(redisUrl);
const server = http.createServer((request, response) => {
  if (request.url === "/health") {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ status: "ok" }));
    return;
  }

  response.writeHead(200, { "content-type": "text/plain" });
  response.end("websocket-relay\n");
});
const wss = new WebSocketServer({ noServer: true });

const addClient = (userId, websocket) => {
  const userClients = clientsByUserId.get(userId) ?? new Set();
  userClients.add(websocket);
  clientsByUserId.set(userId, userClients);
};

const removeClient = (userId, websocket) => {
  const userClients = clientsByUserId.get(userId);
  if (!userClients) {
    return;
  }

  userClients.delete(websocket);
  if (userClients.size === 0) {
    clientsByUserId.delete(userId);
  }
};

const userIdFromChannel = (channel) => {
  if (channel.startsWith(userChannelPrefix)) {
    return channel.slice(userChannelPrefix.length);
  }

  return channel.split(":").at(-1) ?? "";
};

const tokenFromRequest = (request) => {
  const url = new URL(request.url ?? "/", `http://${request.headers.host}`);
  const queryToken = url.searchParams.get("token");
  if (queryToken) {
    return queryToken;
  }

  const authorization = request.headers.authorization;
  if (authorization?.toLowerCase().startsWith("bearer ")) {
    return authorization.slice("bearer ".length).trim();
  }

  return null;
};

const currentUserFromToken = async (token) => {
  const response = await fetch(`${backendUrl}/v1/me`, {
    headers: { authorization: `Bearer ${token}` },
  });
  if (!response.ok) {
    throw new Error(`Backend auth failed with ${response.status}`);
  }

  const user = await response.json();
  if (!user || typeof user.id !== "string" || !user.id) {
    throw new Error("Backend auth response did not include user.id");
  }

  return user;
};

const rejectUpgrade = (socket, statusCode, message) => {
  socket.write(
    `HTTP/1.1 ${statusCode} ${message}\r\nConnection: close\r\n\r\n`,
  );
  socket.destroy();
};

server.on("upgrade", (request, socket, head) => {
  if (
    Array.isArray(allowedHosts) &&
    (!request.headers.host || !isHostAllowed(request.headers.host, allowedHosts))
  ) {
    rejectUpgrade(socket, 403, "Forbidden");
    return;
  }

  const url = new URL(request.url ?? "/", `http://${request.headers.host}`);
  if (url.pathname !== "/" && url.pathname !== "/ws") {
    rejectUpgrade(socket, 404, "Not Found");
    return;
  }

  const token = tokenFromRequest(request);
  if (!token) {
    rejectUpgrade(socket, 401, "Unauthorized");
    return;
  }

  void currentUserFromToken(token)
    .then((user) => {
      wss.handleUpgrade(request, socket, head, (websocket) => {
        websocket.userId = user.id;
        wss.emit("connection", websocket, request);
      });
    })
    .catch(() => rejectUpgrade(socket, 401, "Unauthorized"));
});

wss.on("connection", (websocket) => {
  const userId = websocket.userId;
  websocket.isAlive = true;
  addClient(userId, websocket);

  websocket.on("pong", () => {
    websocket.isAlive = true;
  });
  websocket.on("close", () => removeClient(userId, websocket));
  websocket.on("error", () => removeClient(userId, websocket));
});

subscriber.on("pmessage", (_pattern, channel, message) => {
  const userId = userIdFromChannel(channel);
  const userClients = clientsByUserId.get(userId);
  if (!userClients) {
    return;
  }

  for (const websocket of userClients) {
    if (websocket.readyState === WebSocket.OPEN) {
      websocket.send(message);
    }
  }
});

subscriber.on("error", (error) => {
  console.error("Redis subscriber error", error);
});

await subscriber.psubscribe(redisPattern);

const heartbeat = setInterval(() => {
  for (const websocket of wss.clients) {
    if (!websocket.isAlive) {
      websocket.terminate();
      continue;
    }

    websocket.isAlive = false;
    websocket.ping();
  }
}, heartbeatIntervalMs);

const shutdown = async () => {
  clearInterval(heartbeat);
  for (const websocket of wss.clients) {
    websocket.close(1001, "Server shutdown");
  }
  wss.close();
  server.close();
  subscriber.disconnect();
};

process.on("SIGINT", () => void shutdown().finally(() => process.exit(0)));
process.on("SIGTERM", () => void shutdown().finally(() => process.exit(0)));

server.listen(port, "0.0.0.0", () => {
  console.log(
    `websocket-relay listening on :${port}, redis pattern ${redisPattern}`,
  );
});
