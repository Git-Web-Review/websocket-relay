# websocket-relay

The websocket relay between Redis and connected users.

It subscribes to Redis channels matching `REDIS_CHANNEL_PATTERN`, extracts the
target user id from the channel name, and forwards the Redis message unchanged to
all websocket clients authenticated as that user.

Default channel contract:

```text
notifications:user:<userId>
```

Clients connect to `/ws?token=<firebase id token>`. The relay validates the token
through the backend `/v1/me` endpoint and uses the returned `id` as the connected
user id.

Environment variables:

- `PORT` default `3001`
- `REDIS_URL` default `redis://localhost:6379`
- `BACKEND_URL` default `http://localhost:3000`
- `WEBSOCKET_ALLOWED_HOSTS` optional comma-separated HTTP `Host` allowlist; `*` allows every host
- `REDIS_CHANNEL_PATTERN` default `notifications:user:*`
- `REDIS_USER_CHANNEL_PREFIX` default `notifications:user:`
