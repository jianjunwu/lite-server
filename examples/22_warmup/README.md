# 22. Model Warmup (P-WARM)

Run **dummy inferences before serving**: a version stays `WarmingUp`
(`/ready` = false) until its warmup passes complete, then flips to `Ready`.

[中文版](README_zh.md)

## What this example shows

- `policies.warmup.enabled: true` — the version goes through `WarmingUp` →
  `Ready` instead of straight to `Ready`.
- `samples: [{input_ref: warmup/input.json, iterations: 2}]` — two dummy
  inferences of `{"input": 42}` are executed (the model sleeps 0.5s per call,
  so readiness is delayed ~1s). The samples list (M7) covers multiple input
  shapes/batches, one file per sample.
- `timeout_secs` — per-warmup budget; a warmup failure marks the version
  `Failed` (with `last_failure`) instead of serving cold.

## Layout

```
model_repo/
  warmup_echo/v1/
    model.py          — counts dummy calls (input 42), exposes /stats route
    warmup/input.json — the dummy request body
server.yaml           — loads the model (warmup policy lives in config.yaml)
```

## Running

```bash
lite-server serve --config server.yaml
```

## Verify

```bash
# 1. Watch readiness flip false → true (~1s of warmup work):
for i in $(seq 1 20); do
  curl -s http://localhost:8000/v2/models/warmup_echo/ready; echo
  sleep 0.2
done
# => {"ready": false, ...} × ~5   (WarmingUp — not yet ready)
# => {"ready": true,  ...}        (warmup done)

# 2. The model saw exactly the configured number of dummy inferences:
curl -s http://localhost:8000/v2/models/warmup_echo/stats
# => {"warmup_count": 2}

# 3. A normal inference is unaffected:
curl -s -X POST http://localhost:8000/v2/models/warmup_echo/infer \
  -H 'Content-Type: application/json' -d '{"input": 21}'
# => {"output": {"warmup_count": 2}}
```

## Notes

- With `enabled: false` (default) the version goes straight to `Ready` — no
  behavior change, no warmup cost.
- A warmup failure (exception / timeout) marks the version `Failed`; the
  registry records `last_failure` and serving stays unavailable until a
  reload succeeds.
