# 20. Overload Control (P-FLOW / P-DEADLINE)

Protect the server from overload: a global **in-flight cap** rejects excess
inference with `503 + Retry-After`, per-model **queue timeouts** reject
long-waited requests, and clients can **bound their own wait** with a
deadline header.

[中文版](README_zh.md)

## What this example shows

- `server.max_inflight: 2` — at most 2 inferences run concurrently. Requests
  beyond the cap are rejected immediately with `503` and a `Retry-After: 1`
  header (health/admin endpoints stay reachable — probes keep working under
  load). Default `0` = unlimited.
- `queue_timeout_secs` + `queue_timeout_action: reject` (per model) — a
  request waiting in the queue longer than 1s is rejected with `503
  (queue_full)` + `Retry-After`.
- `x-lite-timeout` — client-specified relative deadline (seconds, float).
  The server stops waiting at the deadline and returns `504 Gateway Timeout`.
  On gRPC the standard `grpc-timeout` metadata does the same.
- `x-lite-priority` — integer request header (higher = dispatched first,
  default 0) for priority queues (demonstrated in the commands below).

## Layout

```
model_repo/
  slow_echo/1/    — 0.8s per inference, single worker
server.yaml       — max_inflight: 2
```

## Running

```bash
lite-server serve --config server.yaml
```

## Verify

```bash
# 1. Saturate max_inflight with 6 concurrent requests (only 2 slots exist):
for i in $(seq 1 6); do
  curl -s -o /dev/null -w "%{http_code} " -X POST \
    http://localhost:8000/v2/models/slow_echo/infer \
    -H 'Content-Type: application/json' -d '{"input": 1}' &
done; wait; echo
# => 200 200 503 503 503 503   (the 503s carry Retry-After: 1)

# 2. See the 503 + Retry-After header explicitly:
curl -s -D - -o /dev/null -X POST http://localhost:8000/v2/models/slow_echo/infer \
  -H 'Content-Type: application/json' -d '{"input": 1}' &
sleep 0.05
curl -s -D - -o /dev/null -X POST http://localhost:8000/v2/models/slow_echo/infer \
  -H 'Content-Type: application/json' -d '{"input": 1}' &
sleep 0.05
curl -s -D - -o /dev/null -X POST http://localhost:8000/v2/models/slow_echo/infer \
  -H 'Content-Type: application/json' -d '{"input": 1}'
wait
# third request => HTTP/1.1 503 Service Unavailable
#                 retry-after: 1

# 3. Bound your own wait with a deadline (the model takes 0.8s):
curl -s -w "\nHTTP %{http_code}\n" -X POST http://localhost:8000/v2/models/slow_echo/infer \
  -H 'Content-Type: application/json' -H 'x-lite-timeout: 0.1' \
  -d '{"input": 1}'
# => HTTP 504 Gateway Timeout (fast — no 0.8s wait)

# 4. Priority queue: two queued requests, the higher x-lite-priority is
#    dispatched first. Fire 2 slow requests to fill the slots, then queue a
#    low-priority and a high-priority one:
curl -s -o /dev/null -X POST http://localhost:8000/v2/models/slow_echo/infer \
  -H 'Content-Type: application/json' -d '{"input": 1}' &
curl -s -o /dev/null -X POST http://localhost:8000/v2/models/slow_echo/infer \
  -H 'Content-Type: application/json' -d '{"input": 1}' &
sleep 0.05
( time curl -s -o /dev/null -X POST http://localhost:8000/v2/models/slow_echo/infer \
    -H 'Content-Type: application/json' -H 'x-lite-priority: 0' -d '{"input": 1}' ) 2>&1 &
( time curl -s -o /dev/null -X POST http://localhost:8000/v2/models/slow_echo/infer \
    -H 'Content-Type: application/json' -H 'x-lite-priority: 5' -d '{"input": 1}' ) 2>&1 &
wait
# The priority-5 request completes before the priority-0 one.
```

## Notes

- `max_inflight` is global across models. To cap only one model, run it in
  its own server (or scale workers).
- The queue-timeout demo needs `max_inflight: 0` (unlimited) so excess
  requests actually queue — see `config.yaml` for the per-model settings and
  try it with `server.max_inflight` removed.
- Without `x-lite-timeout` and with `server.timeout: 0`, a stuck worker wait
  is unbounded — set a server-level `timeout` in production.
