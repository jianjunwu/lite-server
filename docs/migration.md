# Migration Guide

[中文版](zh/migration.md)

This major release is **deliberately breaking** (D30): no deprecated compatibility
switches, no cross-version migration window. This guide is the per-item old → new
reference. At startup the server runs a **config preflight** that detects legacy
config shapes and logs `warn` lines pointing at the M-entries below
(`config-check` prints them too). Rollback = roll back to the previous tag
(proto is additive-only, so wire compatibility holds in both directions).

| Entry | Phase | Breaking change |
|---|---|---|
| M1 | P-XFF | Client `X-Forwarded-For` / `X-Real-IP` no longer trusted by default |
| M2 | P-CORS | Per-model CORS ACAO single-valued; preflight strictly validated |
| M3 | P5-2 | `x-lite-version` header ignored unless `canary_override` enabled |
| M4 | P7-1 | Admin endpoints are loopback-only when `access_control` is unconfigured |
| M5 | P-TRACE | tonic 0.13 upgrade; OTel requires the `telemetry` cargo feature |
| M6 | P-TRACE | `telemetry.protocol: http` fails at startup; inbound baggage dropped by default |
| M7 | P-WARM | `policies.warmup.dummy_input_ref`/`iterations` removed → `samples` list (config fails to load) |
| M8 | 0.8.0 | `features.*` toggles now enforced; 3 reserved fields removed; `custom_metrics` now opt-in |
| M9 | 0.8.0 | Callback lifecycle hooks renamed (`on_*` → positional `before_*`/`after_*`); duck-typed `pre_setup` removed |

Non-breaking phases (no action needed): P-MW, P-ENSEMBLE-GRPC, P-FLOW, P-DEADLINE,
P-WARM (pure additions; defaults preserve prior behavior). P-OAI is deferred to 0.9
(not part of this release).

> **Semantics note (P-DEADLINE):** an expired request budget returns **HTTP `504`
> (not `408`)** / gRPC `DEADLINE_EXCEEDED`. `408` would mean "the client was too
> slow sending", which is not the case here — the request was fully received and
> the server-side budget (client-specified via `x-lite-timeout` / `grpc-timeout`,
> or the `server.timeout` fallback) expired while waiting on the worker. The
> existing `InferenceTimeout → 504` mapping is kept deliberately so clients
> alerting on 504 do not silently break. Distinct neighbours: queue-wait timeout
> REJECT → `503` / `Unavailable`; rate limiting → `429` / `RESOURCE_EXHAUSTED`.
> See architecture.md "Deadline Propagation".

> **Semantics note (stream idle reclaim):** `decoupled_idle_timeout_secs`
> defaults to `300` and applies to EVERY streaming mode (regular stream / bidi
> / SSE / WS / custom-route / gRPC). A stream that produces no chunk for that
> long is closed and reclaimed, where previously a stalled stream with no
> client deadline could hang unbounded. This is on by design (a stuck stream
> is recovered instead of leaking). Set `decoupled_idle_timeout_secs: 0` to
> restore the old unbounded-idle behavior.

## M1 — XFF/X-Real-IP no longer trusted (P-XFF)

**What changed:** `client_ip` is now derived from the direct peer address. Client
supplied `X-Forwarded-For` / `X-Real-IP` headers are ignored unless the peer is in
`server.trusted_proxies`. This affects IP-keyed rate limiting, access logs and any
risk control keyed on client IP.

**Migrate:**

```yaml
# old (relied on the client/gateway XFF being trusted implicitly)
server:
  trusted_proxies: []        # default / absent

# new (deployments behind a gateway/LB must declare it)
server:
  trusted_proxies: ["10.0.0.0/8", "192.168.0.0/16"]   # CIDRs or bare IPs of your proxies
```

Malformed entries fail fast at startup. The preflight warns (entry M1) when a model
uses `rate_limit: { key: ip }` while `trusted_proxies` is empty — the tell-tale of a
deployment that relied on XFF.

## M2 — CORS single-valued ACAO + strict preflight (P-CORS)

**What changed:**

1. Multiple `allow_origins` used to be joined into a single (invalid)
   `Access-Control-Allow-Origin` header. Now the request `Origin` is matched
   exactly (or via `*.host` subdomain wildcard) and that one origin is echoed.
2. A preflight (`OPTIONS` + `Access-Control-Request-Method`) only receives CORS
   headers when the origin matches **and** the requested method/headers are all in
   `allow_methods` / `allow_headers`; otherwise it is a bare `204`.
3. `Vary: Origin` is attached to all responses under a configured policy
   (including requests without an `Origin` header).
4. WebSocket handshakes validate `Origin` against the effective policy when CORS
   is configured (CSWSH protection); unconfigured CORS leaves WS to `access_control`.
5. New global `server.cors` policy; per-model `policies.cors` still wins on model
   routes. `allow_credentials: true` + wildcard origin `*` is refused.

**Migrate:**

```yaml
# old (multi origins were joined into one broken ACAO value)
policies:
  cors:
    allow_origins: ["https://a.example", "https://b.example"]

# new — same YAML shape, corrected semantics: each request's Origin is matched
# against the list; no config change required, but *verify* your browser clients
# send one of the listed origins exactly (scheme + host + port).
```

The preflight warns (entry M2) for any multi-origin CORS config so operators
re-confirm intent. Browsers previously "working" by luck of the invalid join may
now be correctly rejected — list every legitimate origin explicitly.

## M3 — `x-lite-version` ignored by default (P5-2)

**What changed:** the `x-lite-version` request header (canary pin override) is
ignored unless explicitly enabled.

**Migrate:**

```yaml
# gray / debug environments only — restore the header's effect
features:
  canary_override: true
```

Production deployments should leave it `false` (clients can no longer pin
themselves onto canary versions).

## M4 — Admin endpoints loopback-only when unconfigured (P7-1)

**What changed:** with no `access_control` configured, admin endpoints (HTTP
`/admin/*`, gRPC Admin service) are reachable **only from loopback** (UDS counts
as loopback). Previously, binding a non-loopback address implicitly exposed admin
("bind = open").

**Migrate (pick one):**

```yaml
# a) key-based admin access control (both protocols, or per-protocol)
access_control:
  admin:
    http: { mode: key, key: x-admin-key, value_env: ADMIN_KEY }
    grpc: { mode: key, key: x-admin-key, value_env: ADMIN_KEY }

# b) keep admin local-only and bind it separately (UDS = this host only)
grpc:
  admin_bind: unix:/var/run/lite-admin.sock
```

Prometheus scraping should move to the dedicated `metrics_port` (not affected by
admin control). The preflight warns (entry M4) on a non-loopback bind with
unconfigured admin access control.

## M5 — tonic 0.13 / telemetry cargo feature (P-TRACE)

**What changed:** the gRPC stack moved to tonic 0.13 (wire-compatible — existing
clients, including Python `liteserver_pb2`, need no change). OpenTelemetry export
is now gated behind a **compile-time cargo feature** plus the runtime switch:

```bash
cargo build --features telemetry        # binary must be built with the feature
```

```yaml
telemetry:
  enabled: true                          # runtime switch (default false, zero cost)
  otlp_endpoint: http://collector:4317
```

A binary built without the feature ignores `telemetry.*` silently by design.

## M6 — telemetry protocol / inbound baggage (P-TRACE hardening)

**What changed:**

1. `telemetry.protocol: http` is **reserved** (OTLP/gRPC only this period). It now
   fails config validation at startup instead of disabling telemetry with a warning.
2. Inbound W3C `baggage` is **untrusted** and dropped by default — it no longer
   flows into the worker request headers. Forward only the keys you mean to trust:

```yaml
telemetry:
  baggage_allowlist: ["tenant", "experiment"]   # default [] = drop all inbound baggage
  baggage_max_entries: 16                        # cap on kept entries
  baggage_max_entry_bytes: 128                   # per-entry key+value byte cap
```

3. `health_admin_sample_ratio` is now honored: health/admin endpoint spans are
   sampled at this independent rate (default `0.0`) so probes do not burn
   collector quota; other endpoints use `sample_ratio`.

## M7 — warmup single-sample fields removed (P-WARM)

**What changed:** `policies.warmup.dummy_input_ref` and `policies.warmup.iterations`
were removed in favor of a `samples` list (multi-shape warmup, Triton
ModelWarmup style). A config.yaml using the old keys **fails to load** with an
error naming this entry — it never silently skips warmup (a silently skipped
warmup quietly brings back the first-request latency spike).

**Migrate:**

```yaml
# old
policies:
  warmup:
    enabled: true
    iterations: 2
    dummy_input_ref: warmup/input.json

# new — one file per input shape/batch, consumed in order
policies:
  warmup:
    enabled: true
    samples:
      - input_ref: warmup/input.json
        iterations: 2
      - input_ref: warmup/batch8.json   # iterations defaults to 1
```

## M8 — feature toggles enforced; reserved fields removed (0.8.0)

**What changed:** the `features.*` toggles were previously declared but ignored
("(reserved)"). They now control real behavior:

- `system_overview`, `benchmarks`, `playground` were **removed** from the
  schema. They never had any effect; leaving them in an old `server.yaml` is
  harmless (unknown keys are ignored), but they no longer appear in generated
  configs.
- `timeline` / `alerts` / `version_compare` now gate their HTTP routes: when
  off, the routes are unmounted (404) and the timeline background sampler is a
  no-op.
- `streaming` is the master switch for SSE + WebSocket routes; `sse` and
  `websocket_streaming` gate their own transport. Turning `streaming` off truly
  unmounts the routes (previously they were always mounted).
- `grpc_streaming` gates the three streaming RPCs (`stream_infer`,
  `decoupled_infer`, `bidi_stream`); when off they return `UNIMPLEMENTED` before
  admission. `batch_infer` (unary) is unaffected.
- `custom_metrics` is now **opt-in** (default `false`): worker-declared custom
  metrics are registered only when this is `true`. Previously they were always
  registered; if your workers declare metrics, set `custom_metrics: true`.

**Migrate:** no action required unless you relied on custom metrics (set
`features.custom_metrics: true`) or on a streaming route being mounted while its
toggle was `false` (set the relevant toggle to `true`).

## M9 — Callback lifecycle hooks renamed; `pre_setup` removed (0.8.0)

**What changed:** the three Callback lifecycle hooks were renamed to positional
`before_*`/`after_*` names, and a fourth was added:

| old (0.7) | new (0.8) | position |
|---|---|---|
| `on_before_setup(config, device)` | `before_setup(config, device)` | before `LitAPI.setup()` |
| `on_after_setup(lit_api)` | `after_setup(lit_api)` | after `setup()` succeeds |
| `on_teardown(lit_api)` | `before_teardown(lit_api)` | before `LitAPI.teardown()` |
| — | `after_teardown(lit_api)` | after `teardown()` succeeds (new; skipped when teardown raises) |

The naming rule is now consistent: stage wrappers use `before_X`/`after_X`;
event handlers keep `on_*` (`on_error`, `on_stream_close`). A callback that
still defines an old name fails at load time with a `RuntimeError` naming the
exact rename — nothing silently stops running.

Two related changes:

- The undocumented duck-typed `pre_setup()` call on `LitAPI` was **removed
  without a load-time error**: a model defining `pre_setup` simply never has it
  called (it overlapped with `before_setup`, the supported position). Move any
  `pre_setup` logic into `setup()` or a `before_setup` callback.
- `LitAPI.teardown()` and the `before_teardown`/`after_teardown` callbacks now
  actually run in production (previously workers were always SIGKILLed before
  Python could run them): on unload/reload/eviction/graceful shutdown the
  worker gets a stop message and `worker_kill_timeout` to finish teardown
  before the SIGKILL fallback. See model-authoring.md `teardown()` for the
  full trigger matrix.

**Migrate:** rename the three hooks per the table; move `pre_setup` logic into
`setup()` or a `before_setup` callback.

