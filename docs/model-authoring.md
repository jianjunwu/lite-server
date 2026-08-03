# Model Authoring Guide

This guide covers how to write model code for lite-server. Models are Python classes that implement the `LitAPI` interface.

[中文版](zh/model-authoring.md)

## Quick Start

```python
from lite_server import LitAPI

class MyModel(LitAPI):
    def setup(self, device):
        """Load model weights and initialize resources."""
        self.model = load_my_model()

    def decode_request(self, request):
        """Parse the raw HTTP request body."""
        return request.get("input", "")

    def predict(self, x):
        """Run inference. Receives decoded input, returns output."""
        return self.model(x)

    def encode_response(self, output):
        """Format the prediction into an HTTP response body."""
        return {"result": output}
```

Save as `model_repo/{model_name}/{version}/model.py`.

## Directory Structure

```
model_repo/
  {model_name}/
    {version}/
      model.py          # Required: LitAPI subclass (except for ensemble models)
      config.yaml        # Optional: model configuration (defines ensemble models)
```

- `model_name`: alphanumeric, underscores, hyphens, max 64 characters (e.g., `my_model`, `resnet-v2`). Dots `.` are not allowed.
- `version`: alphanumeric, dots, underscores, hyphens, max 64 characters. Must start with an alphanumeric character, must not start/end with a dot, and must not contain `..` (e.g., `1`, `v2`, `latest`, `1.0.0`)

For **ensemble models**, `model.py` can be omitted — define a top-level `ensemble` key in `config.yaml` instead. The model is then entirely configuration-driven.

The model root directory (`{model_name}/`) may also contain `requirements.txt` (Python dependencies) and `README.md`, which are automatically included when packaging as `.lma`.

## LitAPI Interface

### Required Methods

#### `setup(self, device)`

Called once when the worker starts. Load your model and any resources here.

```python
def setup(self, device):
    self.device = device
    self.model = torch.load("weights.pt", map_device=device)
    self.model.eval()
```

- `device` has the format `{accelerator}:{index}` (e.g., `"cpu:0"`, `"cuda:0"`, `"cuda:1"`, `"rocm:0"`, `"mps:0"`). It is controlled by the `accelerator` (default `"cpu"`), `devices`, and `workers_per_device` fields in `config.yaml`. The framework does **not** auto-detect hardware — `device` is an opaque label forwarded from config; your `setup()` is responsible for interpreting it.
- Resources stored on `self` persist for the worker's lifetime

#### `decode_request(self, request)`

Parse the raw HTTP request body (dict from JSON) into the format your model expects.

```python
def decode_request(self, request):
    return {
        "text": request["text"],
        "max_length": request.get("max_length", 128),
    }
```

#### `predict(self, x)`

Run inference. Receives the output of `decode_request()`.

```python
def predict(self, x):
    tokens = self.tokenizer(x["text"], max_length=x["max_length"])
    return self.model(**tokens)
```

When batching is enabled (`max_batch_size > 1`), `x` is a **list** of decoded inputs:

```python
def predict(self, x):
    # x is a list when batching is active
    if isinstance(x, list):
        return [self._infer(item) for item in x]
    return self._infer(x)
```

**Accessing per-request context in batch mode.** `batch`, `unbatch`, and
`predict` (when batching is active) may all declare a `ctx` parameter —
injected as a `list[RequestContext]` aligned positionally with the inputs
(one entry per batched request):

```python
def batch(self, inputs, ctx):
    for c in ctx:
        self.logger.info("batching request %s", c.meta.request_id)
    return torch.stack(inputs)

def predict(self, batched, ctx):
    # ctx[i] corresponds to inputs[i]; ctx[i].state writes are per-item
    return self.model(batched)

def unbatch(self, output, ctx):
    return list(output)
```

`ctx[i]` always aligns with `inputs[i]` — **do not reorder** the inputs
inside `batch`, or results are written back to the wrong requests. Not
declaring `ctx` behaves exactly as before (the list is ignored).

#### `encode_response(self, output)`

Format the prediction output into an HTTP response body:

- `dict` / `list` / other JSON-serializable values are serialized as JSON (compact form; NaN/Infinity become `null`).
- `str` is sent verbatim as UTF-8 — **not** JSON-quoted. Pair with a matching `media_type` (e.g. `Response(content=html, media_type="text/html")`) for non-JSON payloads.
- `bytes` / `bytearray` are sent verbatim (e.g. images, protobuf payloads).

```python
def encode_response(self, output):
    return {"prediction": output.tolist(), "confidence": float(output.max())}
```

### Optional Methods

#### `stream_predict(self, request)`

Generator for streaming output. Each yielded value is sent as a chunk via SSE/WebSocket/gRPC.

```python
def stream_predict(self, request):
    prompt = request.get("prompt", "")
    for token in self.model.generate(prompt):
        yield {"token": token}
        time.sleep(0.02)  # simulate generation latency
```

Enable streaming in `config.yaml`:

```yaml
stream: true
```

If `stream_predict()` is not implemented, the server falls back to `predict()` and sends the result as a single chunk.

#### `before_decode_request(self, ctx)`

Called before `decode_request()`, on the raw request. Use for auth, logging, or request modification. Receives a single :class:`RequestContext` argument (same contract as Callback hooks).

```python
def before_decode_request(self, ctx):
    self.logger.info(f"Request from {ctx.meta.client_ip}: {ctx.meta.request_id}")
    if not self._check_auth(ctx.meta.headers):
        raise PermissionError("Unauthorized")
    return ctx.request
```

``ctx.meta`` is a `RequestMeta` object with: `route`, `headers`, `client_ip`, `request_id`, `timestamp_ns`.

#### `after_encode_response(self, ctx)`

Called after `encode_response()`, before sending to client. Use for response modification or logging. Also called in the streaming path (after each chunk is encoded).

```python
def after_encode_response(self, ctx):
    ctx.response["latency_ms"] = (time.time_ns() - ctx.meta.timestamp_ns) / 1_000_000
    return ctx.response
```

To attach custom HTTP response headers, use :meth:`ctx.respond() <lite_server.RequestContext.respond>`:

```python
def after_encode_response(self, ctx):
    return ctx.respond(
        ctx.response,
        headers={"X-Request-ID": ctx.meta.request_id},
    )
```

#### `on_file_changed(self, changed_files)`

Called when files in the model directory change (hot reload). Override to implement custom reload logic.

```python
def on_file_changed(self, changed_files):
    if any(f.endswith(".pt") for f in changed_files):
        self.logger.info("Reloading model weights...")
        self.model = torch.load("weights.pt")
```

If not overridden, the default behavior restarts the worker (re-runs `setup()`).

Notes:

- A single `FILE_CHANGED` round-trip times out after 60 seconds by default
  (tunable via `tunables.file_changed_timeout_secs` in server.yaml). If any
  worker times out, errors, or does not reply `{"handled": true}`, the server
  falls back to restarting the whole version.
- Hot reloads of the same model/version are rate-limited by a 3-second
  cooldown (`tunables.hot_reload_cooldown_secs`).

#### `teardown(self)`

Called when the model is unloaded. Release resources here.

```python
def teardown(self):
    del self.model
    torch.cuda.empty_cache()
```

## Callbacks

Callbacks are a **composable, declarative** way to intercept the inference request lifecycle. Unlike inline `before_decode_request`/`after_encode_response` hooks, Callbacks are standalone classes that can be reused, shared, and combined across models.

### Callback Base Class

Subclass `Callback` and override the hooks you care about. All hooks have default no-op implementations — only define the methods you need. Data hooks receive a single `ctx` (`RequestContext`) and may be sync or `async def`.

```python
from lite_server import Callback

class MyCallback(Callback):
    def before_decode_request(self, ctx):
        """Called on the raw request, before decode_request."""
        ctx.request["_timestamp"] = ctx.meta.timestamp_ns

    def after_predict(self, ctx):
        """Called after predict, before encode_response."""
        ctx.output["_latency_ns"] = time.time_ns() - ctx.meta.timestamp_ns
```

**Hook points** (pipeline order):

```
before_decode_request → decode_request → after_decode_request → predict → after_predict → encode_response → after_encode_response
```

When any stage or hook raises, the pipeline short-circuits to ``on_error``, then an error response is returned to the client.

| Hook | When | Reads / writes |
|------|------|----------------|
| `before_decode_request` | Raw request, before `decode_request` | `ctx.request` |
| `after_decode_request` | After `decode_request`, before `predict` | `ctx.input` |
| `after_predict` | After `predict`, before `encode_response` (per chunk when streaming) | `ctx.output` |
| `after_encode_response` | After `encode_response`, before sending (per chunk when streaming) | `ctx.response` |
| `on_stream_close` | End of a streaming run (once per stream) | `ctx` + `reason`: `"done"` \| `"error"` \| `"cancel"`; read `ctx.stream_stats` |
| `after_batch` | Batch mode: after `batch()`, before `predict()` (whole batch tensor) | `ctx_list` + `batched`; raising `HTTPException` rejects the whole batch |
| `after_unbatch` | Batch mode: after `unbatch()` (per-item outputs) | `ctx_list` + `outputs` |
| `on_error` | Any hook or stage raises an exception | `ctx` + `exc` (the exception) |
| `before_setup` | Before `LitAPI.setup()` | `(config, device)` |
| `after_setup` | After `LitAPI.setup()` succeeds | `(lit_api)` |
| `before_teardown` | Before `LitAPI.teardown()` (model unload / worker shutdown) | `(lit_api)` |
| `after_teardown` | After `LitAPI.teardown()` succeeds (unload done) | `(lit_api)` |

A data hook may mutate `ctx` in place or return a replacement value (`None` = pass through).

### RequestContext

| Field | Content |
|-------|---------|
| `ctx.meta` | `RequestMeta`: HTTP headers, route, client IP, request ID, timestamp |
| `ctx.request` / `ctx.input` / `ctx.output` / `ctx.response` | Pipeline values at each stage |
| `ctx.state` | Per-request scratch dict shared across hooks — use this, **never** `self` attributes (shared across concurrent requests) |
| `ctx.early` | Set → pipeline short-circuits |
| `ctx.mode` | Scenario tag: `"unary"` \| `"stream"` \| `"bidi"` \| `"decoupled"` \| `"batch"` \| `"cb"` (`None` on `@route` models) — lets a callback skip e.g. streaming |
| `ctx.stage` | Current pipeline stage (`decode_request` / `predict` / `batch_predict` / `encode_response`) — `on_error` reads it to see which stage raised |
| `ctx.stream_stats` | `{chunks, bytes}` of a uni-stream run; populated by the stream consumer, read at `on_stream_close` |
| `ctx.elapsed_ms()` | Milliseconds since the request started (based on `meta.timestamp_ns`) |
| `ctx.deadline_remaining_ms()` | Milliseconds until the request deadline, or `None` (no deadline); negative once expired — check it per chunk to stop streaming cooperatively |

### Early Return and Validation

- **Early return** (e.g. cache hit): call `ctx.respond(body, status_code=..., headers=...)` or return a `Response` from any hook. Later stages and remaining hooks of earlier chains are skipped. The terminal `after_encode_response` chain is the exception: every registered hook runs there even after a `respond(...)` — no stages follow, so responding is header-attach, not short-circuit (this keeps later validation/audit hooks effective regardless of registration order).
- **Validation / rejection**: raise `HTTPException` (`BadRequestError`, `UnauthorizedError`, ...) from any hook. The client receives the structured error with the exception's status code — data-hook exceptions are **not** swallowed.
- Lifecycle hooks (`before_setup` / `after_setup` / `before_teardown` / `after_teardown`) and `on_error` are exception-isolated: failures are logged, never propagated (a failing `on_error` also never masks the original error).

```python
from lite_server import Callback, BadRequestError

class Validator(Callback):
    def before_decode_request(self, ctx):
        if "input" not in (ctx.request or {}):
            raise BadRequestError("missing field", param="input")

class Cache(Callback):
    def before_decode_request(self, ctx):
        hit = self._cache.get(key(ctx))
        if hit is not None:
            ctx.respond(hit, headers={"X-Cache-Hit": "1"})
```

### Declarative Loading

Declare callback class paths in `config.yaml` under the `callbacks` key. The server loads and registers them automatically on startup. Each entry is either a class-path string (no-arg construction) or a **single-key map** `{path: kwargs}` passing constructor arguments:

```yaml
# config.yaml
callbacks:
  - my_package.callbacks.AuditLogger        # no-arg
  - my_package.callbacks.MetricsCollector   # no-arg
  - lite_server.callbacks.JsonSchemaValidator:  # map entry → cls(**kwargs)
      input_schema:
        type: object
        required: [prompt]
        properties:
          prompt: { type: string, minLength: 1 }
```

Both forms are interchangeable with the class-attribute path (`LitAPI.callbacks = (JsonSchemaValidator(input_schema=...),)`) — they are the same constructor arguments. Loading fails loudly on import errors or pre-0.7 hook signatures — a silently skipped callback could mean auth/validation logic that never runs.

### Complete Example: Audit Logger

```python
"""Audit-logging callback: records input/output and latency per request."""
from lite_server import Callback

class AuditLogger(Callback):
    def before_decode_request(self, ctx):
        ctx.request["_audit_id"] = ctx.meta.request_id

    def after_predict(self, ctx):
        print(f"[AUDIT] request_id={ctx.meta.request_id} latency={ctx.elapsed_ms():.2f}ms")

    def before_teardown(self, lit_api):
        print(f"[AUDIT] model torn down, total handled: {lit_api.call_count}")
```

### Built-in: JsonSchemaValidator (Schema Validation)

`lite_server.callbacks.JsonSchemaValidator` validates the request body
(`before_decode_request`, before `decode_request`) and the response body (`after_encode_response`,
after `encode_response`) against JSON Schemas — declarative, no model-code
changes. Both schemas describe the **wire payload** (what the client sends /
receives), so an invalid request is rejected with 400 before any model code
(decode included) runs:

```yaml
# config.yaml — requires `pip install lite-server[validation]`
callbacks:
  - lite_server.callbacks.JsonSchemaValidator:
      input_schema:
        type: object
        required: [prompt]
        additionalProperties: false
        properties:
          prompt: { type: string, minLength: 1, maxLength: 4096 }
          max_tokens: { type: integer, minimum: 1, maximum: 2048 }
      output_schema:                 # optional; validate model output too
        type: object
        required: [text]
```

- **Failure → structured 400**: `param` is the JSON Pointer of the single
  best-match error (prefixed `body/`, e.g. `body/prompt`), `message` is the
  error text. The schema draft is auto-detected from `$schema` (default
  Draft 7); a malformed schema is rejected at load time (loud — a silent
  skip would mean validation never ran).
- **Output validation scope**: unary/batch and custom-route responses —
  streaming chunks are partial JSON and never match a full schema, so they
  are skipped via `ctx.mode`.
- **Skipped payloads**: `ctx.request` is always the parsed JSON body, so on
  the request side every value is validated — a scalar / `null` body fails
  an `object`/`array` schema's top-level type. On the response side a
  text/bytes passthrough payload is genuinely non-JSON, so `object`/`array`
  schemas leave it untouched (a scalar top-level schema such as
  `type: string` still validates the value itself). No
  `input_schema`/`output_schema` → that direction is not validated. In
  batch mode each item is validated independently.
- **Custom routes**: the validator works there too — `before_decode_request` runs
  before the route handler, so `input_schema` rejects an invalid route body
  the same way, and `output_schema` validates the route's (complete)
  response payload.

### Policies

Auth, rate limiting, CORS, and access logging are HTTP-layer concerns. They
are declared per model in `config.yaml` and enforced by the Rust server
(scoped to each model version):

```yaml
# model_repo/my_model/1/config.yaml
policies:
  # API-key auth: reads the X-API-Key header by default; empty keys = any
  # non-empty value passes. ${VAR} entries are read from the environment —
  # an unset variable fails the config load (fail-closed).
  auth: { header: "X-API-Key", keys: ["${API_KEYS}"] }

  # Rate limit: key="route" shares one bucket per route, key="ip" limits per client IP
  rate_limit: { requests_per_minute: 60, key: ip, burst: 100 }

  # CORS
  cors:
    allow_origins: ["https://example.com"]
    allow_methods: ["GET", "POST"]
    allow_headers: ["Content-Type", "Authorization"]

  # Access log (method, path, status, elapsed — including rejections)
  request_log: {}
```

### Warmup (P-WARM)

Engines lazy-initialize (CUDA graph capture, `torch.compile`, allocator pools)
on the first request — the very first user request after a load, scale-up, or
rolling upgrade can stall for 20–30s. Warmup runs dummy inference at load time
so that cost is paid before the version accepts traffic. It is **off by
default**; enabling it blocks readiness (D33): the version stays in the
`warming_up` state (`/readyz` 503, gRPC health `NOT_SERVING`, `/startupz`
`initializing`) until the warmup completes, then flips to `ready`. A warmup
failure (bad dummy input, error response, or timeout) marks the version
`failed` with a `last_failure` reason instead of serving cold.

The dummy input is the raw `/predict` request body stored in a file next to the
model, sent verbatim through the normal inference path:

```yaml
# model_repo/my_model/1/config.yaml
policies:
  warmup:
    enabled: true
    samples:                            # one file per input shape/batch (M7)
      - input_ref: warmup/input.json    # relative to the model dir
        iterations: 3                   # run this sample N times (default 1)
    timeout_secs: 30.0                  # 0 = use request_timeout
```

```json
// model_repo/my_model/1/warmup/input.json — same body a client would POST
{ "prompt": "hello", "max_tokens": 8 }
```

> Since 0.7.6, the four Python policy callbacks (`RequireApiKey`, `Cors`,
> `RateLimit`, `LogRequests`) are removed — they duplicated the Rust-side
> enforcement, and per-worker declaration had a last-declaration-wins
> consistency hazard. Referencing them in a `callbacks:` list is a load-time
> error with migration instructions.

### Where Hooks Live (0.8.0)

All request hooks live on `Callback` subclasses — `LitAPI` only carries the
pipeline stages (`setup` / `decode_request` / `predict` / `encode_response`
+ mode-specific methods). The 0.7–0.8 `LitAPI.on_request` / `on_response`
methods were removed in 0.8.0: defining a hook on the model class is a
load-time error pointing to the `Callback` migration. Callbacks register via
the `callbacks:` field in config.yaml or the `LitAPI.callbacks` class
attribute.

See [examples/14_lifecycle_hooks](../examples/14_lifecycle_hooks/) for a runnable demo.

## Custom Routes (`@route`)

Declare extra HTTP endpoints on a model with the `@route` decorator. They are
served under `/v2/models/<model>/<tail>` and dispatched to the model's worker
over the same channel as inference — no separate process.

```python
from lite_server import LitAPI, route
from lite_server.response import Response

class PetsAPI(LitAPI):
    @route.get("/pets/{pet_id}")
    def get_pet(self, ctx):
        pet_id = int(ctx.state["path_params"]["pet_id"])
        pet = self.pets.get(pet_id)
        if pet is None:
            return Response(content={"error": "pet not found"}, status_code=404)
        return pet
```

Handlers receive a `RequestContext`:

- `ctx.request` — parsed JSON body (dict, or `{}` when absent)
- `ctx.meta.method` / `ctx.meta.query` / `ctx.meta.headers` — HTTP metadata
- `ctx.state["path_params"]` — path params from `{name}` segments
- `ctx.server` — a `ServerProxy` for the hosting server (see below)
- return a plain value (→ `200 application/json`) or a `Response` (custom
  status / headers / media type)

System routes (`infer`, `events`, `stream`, `ready`, `health`, `reload`,
`versions`, `compare`) are reserved: declaring `@route` at one of them is
skipped with a warning at load time.

### `ctx.server` (ServerProxy)

Route handlers can query the hosting server over loopback HTTP:

| API | Behavior |
|-----|----------|
| `ctx.server.registry.list_loaded()` | Live list of loaded models: `[{"name", "version", "status", "model_type", "workers"}, ...]` |
| `ctx.server.registry.get(name)` | First entry for one model, or `None` |
| `await ctx.server.inference.infer(model_name, input_data, version=None)` | Run inference on another model; returns the model's JSON output |
| `ctx.server.metrics.query(name, **labels)` | Current value of a Prometheus metric (scrapes `/metrics`); `None` when absent |

```python
@route.get("/models")
def models(self, ctx):
    return {"loaded": ctx.server.registry.list_loaded()}

@route.post("/embed_query")
async def embed_query(self, ctx):
    out = await ctx.server.inference.infer("embedder", {"text": ctx.request["q"]})
    return {"embedding": out["output"]}
```

- Registry methods are **synchronous** (safe in sync handlers, which run on a
  worker thread; async handlers should wrap them in `asyncio.to_thread`).
- `inference.infer` is **async**. A sync handler can drive it with
  `asyncio.run(...)` (sync handlers run on a thread without a running loop).
- `metrics.query` is **synchronous** and suited to counters/gauges;
  histograms are exposed as separate `<name>_bucket` / `_sum` / `_count`
  samples.
- **Self-inference is rejected.** A route handler occupies its worker, so
  calling `infer` back into the same model+version would deadlock with a
  single worker — `infer()` raises `ValueError` for the handler's own
  model/version. Call a *different* model, or use a direct method call for
  own-model logic.

### Streaming routes

Return a `StreamingResponse` to stream the body chunk by chunk:

```python
from lite_server.response import StreamingResponse

@route.get("/ticks")
def ticks(self, ctx):
    async def gen():
        for n in range(10):
            yield {"n": n}
    return StreamingResponse(content=gen())
```

- `content` may be an async iterator or a plain (sync) iterable — sync
  iterables are pulled on a thread so a slow `next()` never blocks the
  worker loop.
- Each yielded item is serialized per chunk: `bytes` verbatim, `str` as
  UTF-8, anything else as JSON.
- With the default `text/event-stream` media type, each chunk is framed as
  one SSE event (one `data:` line per payload line). Any other
  `media_type="..."` passes chunk bytes through verbatim with that
  content-type — e.g. `application/octet-stream` for file-like downloads.
- `status_code` / `headers` on the `StreamingResponse` become the HTTP
  response head; they must be set before the first chunk is yielded.
- Raising `HTTPException` mid-stream sends a terminal structured error
  event (SSE mode) or truncates the body (other media types) — the status
  line is already on the wire by then.

See [examples/06_custom_route](../examples/06_custom_route/) for a runnable demo.

## Async Models

Every model runs on the worker's unified asyncio loop — there is no separate async base class (the pre-0.7 `AsyncLitAPI` is gone). Any method except `setup()` may be `async def`; the worker adapts at load time.

### Usage

```python
import asyncio
from lite_server import LitAPI

class AsyncModel(LitAPI):
    def setup(self, device):
        # setup() is always synchronous
        self.client = create_client()

    async def decode_request(self, request):
        return request.get("input", "")

    async def predict(self, x):
        # Async I/O: e.g. remote API call or async model inference
        result = await self.client.predict(x)
        return {"output": result}

    def encode_response(self, output):
        return output
```

### How It Works

- **Fully-sync models** run inline on the event loop — zero adaptation overhead, same behavior as the pre-0.7 standard loop.
- **When anything is async** (any model method or callback hook), sync model stages run on a single-thread executor: sync code never executes concurrently (thread-safety assumptions preserved) and never blocks the event loop.
- Batching, streaming, bidirectional streaming, and continuous batching all work with sync or async methods.

## Continuous Batching (LLM)

For LLM workloads, enable continuous batching to process multiple sequences simultaneously with iterative generation.

```yaml
# config.yaml
continuous_batching: true
```

Implement three hooks:

```python
class LLMModel(LitAPI):
    def prefill(self, uid, decoded_input):
        """Initialize a new sequence in the KV cache."""
        tokens = self.tokenizer.encode(decoded_input["prompt"])
        self.kv_cache.add(uid, tokens)

    def step(self, active_sequences):
        """Run one generation step for all active sequences."""
        new_tokens = []
        for seq in active_sequences:
            token = self.model.generate_step(seq["uid"])
            new_tokens.append(token)
        return new_tokens

    def has_finished(self, uid, token, generated_sequence):
        """Check if a sequence is done generating."""
        return token == self.eos_token or len(generated_sequence) >= self.max_length
```

Each element in `active_sequences` has keys: `uid`, `input`, `output` (list of tokens so far).

## Batching

Enable batching to process multiple requests in a single `predict()` call:

```yaml
# config.yaml
max_batch_size: 8
batch_timeout: 0.01
adaptive_batching: true
```

When batching is active, `predict()` receives a **list** of decoded inputs:

```python
def predict(self, x):
    # x is a list of decoded inputs
    batch_input = [item["text"] for item in x]
    results = self.model(batch(batch_input))
    return [{"output": r} for r in results]  # must return list, one per input
```

Key rules:
- Return a **list** with one result per input
- The order must match the input order
- `batch_timeout` controls how long to wait for more requests (adaptive batching adjusts this automatically)

#### Custom `batch()` / `unbatch()`

Override `batch()` to reshape decoded inputs before prediction, and `unbatch()` to split batch output back into per-request responses. The full pipeline becomes:

```
decode_request → batch → predict → unbatch → encode_response
```

When only one request is queued, `batch()` and `unbatch()` are both skipped — `predict()` receives the decoded request directly.

```python
class CustomBatchModel(LitAPI):
    def decode_request(self, request):
        return {"value": request["input"], "weight": request.get("weight", 1.0)}

    def batch(self, inputs):
        """Merge decoded requests into a single batch dict."""
        return {
            "values": [x["value"] for x in inputs],
            "weights": [x["weight"] for x in inputs],
            "batch_size": len(inputs),
        }

    def predict(self, batch):
        if isinstance(batch, dict) and "values" in batch:
            # Multiple requests — came through batch()
            results = [v * w for v, w in zip(batch["values"], batch["weights"])]
            return {"results": results, "batch_size": batch["batch_size"]}
        # Single request — batch() skipped
        return {"output": batch["value"] * batch["weight"], "batch_size": 1}

    def unbatch(self, output):
        """Split batch output back into per-request responses."""
        return [
            {"output": r, "batch_size": output["batch_size"]}
            for r in output["results"]
        ]

    def encode_response(self, output):
        return output
```

See [examples/02_batching](../examples/02_batching/) for a runnable demo.

## Bidirectional Streaming

For real-time bidirectional communication (e.g., ASR):

```python
class ASRModel(LitAPI):
    def bidi_stream(self):
        class Handler:
            def on_chunk(self, chunk):
                # Process incoming audio chunk, return partial result
                return self.model.process_audio(chunk)

            def on_close(self):
                # Finalize and return final result
                return self.model.finalize()
        return Handler()
```

Implementing ``bidi_stream()`` is all that's needed — sessions are detected
from the method itself and served over gRPC; no config flag is required.

> **Note:** During a bidi session, ``ctx.request`` and ``ctx.input`` in hooks
> always hold the original open payload — they do not change as individual
> chunks arrive.  The per-chunk data is available via the handler's
> ``on_chunk(chunk)`` argument.

## Custom Metrics

Collect application-level metrics (gauges, counters, histograms) from your model code. Metrics flow through the same Prometheus endpoint as built-in server metrics (`/metrics`).

### How It Works

1. **Pre-register** metrics in `setup()` — returns a numeric ID
2. **Report** values in `predict()` / `stream_predict()` using the ID
3. Metrics are automatically attached to the response and recorded in Prometheus

Pre-registration lets the server pre-allocate Prometheus objects, keeping the hot path zero-allocation (~50ns per `report_metric` call).

### API

```python
def register_metric(self, name: str, metric_type: str) -> int
```

Pre-register a metric. Call in `setup()`. Returns a numeric ID.

- `name`: Prometheus metric name (e.g. `"batch_size"`, `"cache_hit_rate"`)
- `metric_type`: `"gauge"`, `"counter"`, or `"histogram"`

```python
def report_metric(self, metric_id: int, value: float) -> None
```

Report a metric value by ID. Call in `predict()` or `stream_predict()`.

### Example

```python
import time
from lite_server import LitAPI

class MyModel(LitAPI):
    def setup(self, device):
        self.model = load_model()
        # Pre-register metrics — one-time cost
        self.g_batch_size = self.register_metric("my_batch_size", "gauge")
        self.c_predictions = self.register_metric("my_predictions", "counter")
        self.h_latency = self.register_metric("my_inference_ms", "histogram")

    def predict(self, x):
        start = time.time()
        output = self.model(x)
        elapsed_ms = (time.time() - start) * 1000

        # Report metrics — hot path, ~50ns each
        self.report_metric(self.g_batch_size, len(x) if isinstance(x, list) else 1)
        self.report_metric(self.c_predictions, 1.0)
        self.report_metric(self.h_latency, elapsed_ms)

        return output
```

### Prometheus Output

After sending requests, check `/metrics`:

```
# Gauge
lite_server_my_batch_size{model="mymodel"} 32

# Counter
lite_server_my_predictions_total{model="mymodel"} 1542

# Histogram
lite_server_my_inference_ms_count{model="mymodel"} 1542
lite_server_my_inference_ms_sum{model="mymodel"} 462.6
lite_server_my_inference_ms_bucket{model="mymodel",le="0.1"} 1200
lite_server_my_inference_ms_bucket{model="mymodel",le="0.5"} 1400
...
```

### Metric Types

| Type | Prometheus Type | Use Case |
|------|----------------|----------|
| `gauge` | Gauge | Current value: queue length, cache hit rate, GPU utilization |
| `counter` | Counter (cumulative) | Monotonic count: total predictions, total errors, total tokens |
| `histogram` | Histogram | Distribution: latency, batch size, token count per request |

### Streaming Support

Metrics work in all modes — standard, batch, streaming, and continuous batching. In streaming mode, metrics are collected after the generator completes and attached to the `StreamDone` message.

```python
def stream_predict(self, request):
    for token in self.model.generate(request["prompt"]):
        yield {"token": token}
    # Metrics reported during generation are automatically collected
    self.report_metric(self.c_predictions, 1.0)
```

### Notes

- **Counter metrics must NOT end with `_total`** — Prometheus automatically appends `_total` to counter names. Naming a counter `my_predictions_total` produces `my_predictions_total_total` in `/metrics` output. Use `my_predictions` instead.
- Metric names must not conflict with built-in Prometheus metrics (e.g. `liteserver_requests_total`)
- IDs are per-LitAPI-instance — different models can register the same metric name (values are separated by the `model` label)
- Default histogram buckets: `[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]`

See [examples/09_custom_metrics](../examples/09_custom_metrics/) for a runnable demo.

## Custom Parameters

All fields in `config.yaml` are accessible in your model code via `self.config`. This lets you tune behavior without changing code.

### Defining Parameters

Add any custom fields to `config.yaml` alongside the standard fields:

```yaml
# model_repo/my_model/1/config.yaml
max_batch_size: 1
stream: false

# Custom parameters
threshold: 0.5
label: "positive"
model_path: "/opt/models/weights.pt"
```

### Accessing in model.py

Use `self.config.get(key, default)` in `setup()` or anywhere in your model:

```python
class MyModel(LitAPI):
    def setup(self, device):
        self.threshold = self.config.get("threshold", 0.5)
        self.label = self.config.get("label", "default")
        model_path = self.config.get("model_path", "model.pt")
        self.model = load_model(model_path)

    def predict(self, x):
        if x["score"] >= self.threshold:
            return {"label": self.label}
        return {"label": "other"}
```

### When to Use

- **Thresholds and hyperparameters**: confidence cutoffs, temperature, max_length
- **File paths**: model weights, label files, lookup tables
- **Feature flags**: enable/disable behaviors per model version
- **A/B testing**: different configs for different versions

See [examples/07_custom_params](../examples/07_custom_params/) for a runnable demo.

## Logging

Every `LitAPI` instance has a `self.logger` property (a standard Python `logging.Logger`) that is bound to the model class name. Use it to emit structured logs at any stage of the inference lifecycle.

### Basic Usage

```python
class MyModel(LitAPI):
    def setup(self, device):
        self.logger.info("Loading model on device=%s", device)
        self.model = load_model()

    def predict(self, x):
        self.logger.debug("predict input=%s", x)
        output = self.model(x)
        self.logger.info("predict output=%s", output)
        return output
```

### Log Levels

| Method | Use Case |
|--------|----------|
| `logger.debug(...)` | Verbose diagnostics: raw inputs/outputs, intermediate tensors |
| `logger.info(...)` | Lifecycle events: model loaded, request received, response sent |
| `logger.warning(...)` | Recoverable issues: deprecated feature used, fallback triggered |
| `logger.error(...)` | Errors that will fail the request |

### Controlling Verbosity

The worker configures the root logger so that all model loggers inherit the same handler and level. Control it via the `--log-level` CLI flag:

```bash
python -m lite_server serve --config server.yaml --log-level info
```

Or in `server.yaml`:

```yaml
server:
  log_level: info
```

### Per-Request Tracing

Use `before_decode_request` and `after_encode_response` to log request metadata:

```python
def before_decode_request(self, ctx):
    self.logger.info(
        "Request from %s | route=%s | request_id=%s",
        ctx.meta.client_ip, ctx.meta.route, ctx.meta.request_id,
    )
    return ctx.request

def after_encode_response(self, ctx):
    self.logger.info(
        "Response ready | request_id=%s | latency_ms=%.2f",
        ctx.meta.request_id,
        (time.time_ns() - ctx.meta.timestamp_ns) / 1_000_000,
    )
    return ctx.response
```

``ctx.meta`` is a `RequestMeta` object with: `route`, `headers`, `client_ip`, `request_id`, `timestamp_ns`.

See [examples/11_logging](../examples/11_logging/) for a runnable demo.

## Best Practices

### Resource Management

- Load heavy resources (model weights, tokenizers) in `setup()`, not in `predict()`
- Use `teardown()` to release GPU memory and file handles
- Store all state on `self` — workers are long-lived processes

### Error Handling

- Raise exceptions in `predict()` to signal errors — the server retries on a different worker
- Use `before_decode_request()` for input validation — raise to reject early
- Avoid bare `except:` — let unexpected errors propagate for debugging

#### Typed HTTP Errors

Use `HTTPException` subclasses to return typed HTTP errors with structured responses. Subclasses work in **all hooks** (`predict`, `stream_predict`, `bidi_stream`, `decode_request`, `encode_response`, `before_decode_request`, `after_encode_response`, `prefill`, `step`) and across all protocols (HTTP, SSE, WebSocket, gRPC).

```python
from lite_server.exceptions import (
    BadRequestError,
    UnauthorizedError,
    ForbiddenError,
    NotFoundError,
    InternalServerError,
    ServiceUnavailableError,
)

class MyModel(LitAPI):
    def predict(self, x):
        if x.get("value") < 0:
            raise BadRequestError("input must be non-negative", "INVALID_INPUT")
        if self.model is None:
            raise ServiceUnavailableError("model not loaded yet")
        return self.model(x)

    def before_decode_request(self, ctx):
        if not self._check_auth(ctx.meta.headers):
            raise UnauthorizedError("invalid or missing token")
        return ctx.request
```

| Exception | HTTP Status | Default error_type |
|-----------|-------------|--------------------|
| `BadRequestError` | 400 | `invalid_request_error` |
| `UnauthorizedError` | 401 | `authentication_error` |
| `ForbiddenError` | 403 | `permission_denied_error` |
| `NotFoundError` | 404 | `not_found_error` |
| `InternalServerError` | 500 | `server_error` |
| `ServiceUnavailableError` | 503 | `service_unavailable` |

All accept a custom `error_type` as the second argument, plus optional `code` and `param` keyword arguments for programmatic error handling (OpenAI convention):

```python
raise BadRequestError("input must be non-negative", code="invalid_input", param="value")
```

The client always receives a four-field structured response:

```json
{"error": {"type": "INVALID_INPUT", "message": "input must be non-negative", "code": "invalid_input", "param": "value"}}
```

- `code` — machine-readable error code (snake_case), `null` when not set. Server-generated errors always carry one (e.g. `model_not_found`, `queue_full`, `invalid_request_body`).
- `param` — name of the parameter that caused the error, `null` when not applicable.

On gRPC, `code`/`param` are delivered as standard [ErrorInfo](https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto) details (`reason` = code, `metadata` = {error_type, param}) alongside the `[error_type] message` status message.

`HTTPException` works in custom route handlers too — the route returns the exception's status code with the same structured error body.

Custom status codes are supported via subclassing `HTTPException` directly:

```python
from lite_server.exceptions import HTTPException

class PaymentRequiredError(HTTPException):
    def __init__(self, detail, error_type="payment_required"):
        super().__init__(402, detail, error_type)
```

#### Response Headers

Every HTTP response (success or error) carries:

| Header | Description |
|--------|-------------|
| `x-request-id` | Request ID for log/tracing correlation. Echoes the client's `x-client-request-id` (1–512 ASCII chars) when provided; otherwise a UUID v4. The same ID propagates to inference workers and callbacks. |
| `x-processing-time-ms` | Total server-side wall-clock processing time in milliseconds. |

Framework-level errors are standardized as well: unknown routes return 404 (`code: route_not_found`), unsupported methods 405 (`code: method_not_allowed`), and malformed JSON bodies 400 (`code: invalid_request_body`) — all in the four-field format above.

### Performance

- Keep `decode_request()` and `encode_response()` lightweight — they run on every request
- For batch inference, ensure `predict()` returns results in the same order as inputs
- Use `adaptive_batching: true` for variable-load workloads

### Testing

Models can be tested independently without starting the server:

```python
import json, asyncio
from lite_server.pipeline import Pipeline
from lite_server.context import RequestMeta, Headers

api = MyModel(max_batch_size=1)
api.setup("cpu")

pipe = Pipeline.build(api)
data = json.dumps({"input": 42}).encode()
meta = RequestMeta(route="/predict", headers=Headers(), client_ip="",
                   request_id="", timestamp_ns=0)
resp_bytes, status, _, _ = asyncio.run(pipe.run_single(data, meta))
assert json.loads(resp_bytes) == {"result": 84}
```

## Example: Complete Model

```python
"""Image classification model with preprocessing and batch support."""

import numpy as np
from lite_server import LitAPI

class ImageClassifier(LitAPI):
    def setup(self, device):
        self.device = device
        self.model = load_model("resnet50.pt", device=device)
        self.labels = load_labels("imagenet_labels.txt")

    def decode_request(self, request):
        # request: {"image": base64_encoded_string}
        import base64
        img_bytes = base64.b64decode(request["image"])
        return preprocess_image(img_bytes)

    def predict(self, x):
        if isinstance(x, list):
            # Batching: x is a list of preprocessed images
            batch = np.stack(x)
            outputs = self.model(batch)
            return [self._decode_output(o) for o in outputs]
        return self._decode_output(self.model(x))

    def encode_response(self, output):
        return output  # already a dict with label + confidence

    def _decode_output(self, logits):
        idx = int(np.argmax(logits))
        return {"label": self.labels[idx], "confidence": float(logits[idx])}

    def teardown(self):
        del self.model
```

## Config Reference

See [configuration.md](configuration.md) for the full model config field reference.
