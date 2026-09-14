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
const revalidateIntervalMs = Number(
  process.env.WS_REVALIDATE_INTERVAL_MS ?? 300000,
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

/**
 * Origins allowed to open a connection from a browser. A non-browser client
 * sends no `Origin` header and is therefore out of scope: it authenticates with
 * `Authorization`, which a third-party site's browser cannot set.
 */
const allowedOrigins = (process.env.FRONTEND_ORIGIN ?? "")
  .split(",")
  .map((origin) => origin.trim().toLowerCase())
  .filter(Boolean);

const maxConnectionsPerUser = Number(
  process.env.WS_MAX_CONNECTIONS_PER_USER ?? 10,
);

const isOriginAllowed = (origin) => {
  if (!origin) {
    return true;
  }

  if (allowedOrigins.length === 0) {
    return true;
  }

  return allowedOrigins.includes(origin.trim().toLowerCase());
};

const clientsByUserId = new Map();
// The password is passed as an option rather than inside the URL: a
// base64-generated password contains `/`, `+` and `=`, which make the URL invalid.
const subscriber = new Redis(redisUrl, {
  password: process.env.REDIS_PASSWORD || undefined,
});
const server = http.createServer((request, response) => {
  if (request.url === "/health") {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ status: "ok" }));
    return;
  }

  response.writeHead(200, { "content-type": "text/plain" });
  response.end("websocket-relay\n");
});
const wss = new WebSocketServer({
  noServer: true,
  // Answers `bearer` when the client offered it; the token itself is never
  // selected as the subprotocol, which would only echo it back.
  handleProtocols: (protocols) =>
    protocols.has(BEARER_SUBPROTOCOL) ? BEARER_SUBPROTOCOL : false,
});

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

const BEARER_SUBPROTOCOL = "bearer";

/**
 * The token no longer travels in the URL.
 *
 * A query string ends up in proxy logs, browser history and telemetry; a
 * Firebase token stayed readable there for an hour. Browsers cannot set a header
 * on a WebSocket, but they can advertise subprotocols: `bearer` goes there,
 * followed by the token. Non-browser clients keep the `Authorization` header.
 */
const tokenFromRequest = (request) => {
  const authorization = request.headers.authorization;
  if (authorization?.toLowerCase().startsWith("bearer ")) {
    return { token: authorization.slice("bearer ".length).trim(), protocol: null };
  }

  const offered = (request.headers["sec-websocket-protocol"] ?? "")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);

  const marker = offered.indexOf(BEARER_SUBPROTOCOL);
  if (marker !== -1 && offered[marker + 1]) {
    // The selected subprotocol has to be echoed back, or the browser closes the
    // connection for a failed negotiation.
    return { token: offered[marker + 1], protocol: BEARER_SUBPROTOCOL };
  }

  return { token: null, protocol: null };
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

  if (!isOriginAllowed(request.headers.origin)) {
    rejectUpgrade(socket, 403, "Forbidden");
    return;
  }

  const { token, protocol } = tokenFromRequest(request);
  if (!token) {
    rejectUpgrade(socket, 401, "Unauthorized");
    return;
  }

  void currentUserFromToken(token)
    .then((user) => {
      // Every upgrade triggers a call to the backend: without a cap, one account
      // can open as many as it likes and use them as an amplifier.
      if ((clientsByUserId.get(user.id)?.size ?? 0) >= maxConnectionsPerUser) {
        rejectUpgrade(socket, 429, "Too Many Requests");
        return;
      }

      wss.handleUpgrade(request, socket, head, (websocket) => {
        websocket.userId = user.id;
        websocket.authToken = token;
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

/**
 * Authentication only happened at open: a disabled service account or a deleted
 * user kept receiving notifications until it disconnected. The token is
 * revalidated and whatever no longer passes is closed.
 */
const revalidate = setInterval(() => {
  for (const websocket of wss.clients) {
    if (websocket.readyState !== WebSocket.OPEN || !websocket.authToken) {
      continue;
    }

    void currentUserFromToken(websocket.authToken)
      .then((user) => {
        if (user.id !== websocket.userId) {
          websocket.close(1008, "Identity changed");
        }
      })
      .catch(() => websocket.close(1008, "Token no longer valid"));
  }
}, revalidateIntervalMs);

const shutdown = async () => {
  clearInterval(heartbeat);
  clearInterval(revalidate);
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
