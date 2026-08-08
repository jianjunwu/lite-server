# Known Deviations from KServe V2 / Triton (protocol-compat)

> Created in batch 1 (protocol-compat-plan.md C18: documentation debt is cleared
> per batch — first four items are C17, appended every batch). Records the
> **intentional deviations** from the KServe V2 dataplane / Triton HTTP
> protocols, for ecosystem clients to compare against. Plan:
> [.claude/protocol-compat-plan.md](../.claude/protocol-compat-plan.md).

## Batch 1 (stage 1 Triton Binary Tensor Data Extension, first C17 set)

| # | Deviation | Note |
|---|---|---|
| ① | **KServe binary responses carry no Content-Type** | KServe `encode()` writes only the `Inference-Header-Content-Length` header; we keep `application/octet-stream` (Triton convention) — see [raw-bytes-request.md](raw-bytes-request.md). |
| ② | **KServe CloudEvents envelope not implemented** | Neither structured nor binary CloudEvents wrapping; the Triton JSON-head + binary-tail channel is the target (G10 targets tritonclient). |
| ③ | **KServe model not ready → 503 (optional alignment)** | KServe checks ready before infer and returns 503; our ready-gate semantics already exist and are not force-aligned. |
| ④ | **Triton `/statistics` and `/config` endpoints not implemented** | `GET /v2/models/:m/statistics` and `/v2/models/:m/config` are non-goals (G20); tritonclient `get_inference_statistics` will 404. |

## Batch 2 (error body duality, protocol-seam dispatch)

| # | Deviation | Note |
|---|---|---|
| ⑤ | **Error body duality** | KServe-mode requests (`Inference-Header-Content-Length` present, or a KServe envelope body) get the flat `{"error": "<message>"}` shape for 4xx/5xx; non-KServe requests keep the OpenAI style `{"error":{type,message,code,param}}` — dispatched via `src/protocol/` render. |

## Batch 3 (management surface)

| # | Deviation | Note |
|---|---|---|
| ⑥ | **bare load does not trigger post-upload loading** | `POST /v2/repository/models/:m/load` aliases the active version (idempotent 200); the post-upload flow must versioned load/activate first. |
| ⑦ | **worker `get_metadata()` callback not landed** | `/v2/models/:m` metadata returns empty `inputs`/`outputs` (legal degradation; tritonclient does not require non-empty); a worker callback is a later optional enhancement. |

## Batch 4 (Triton Generate extension)

| # | Deviation | Note |
|---|---|---|
| ⑧ | **own SSE format vs `generate_stream`** | `/events` uses the own format (`data: chunk-N` + terminating `data: [DONE]`); `generate_stream` is the Triton-compatible channel (`data: <full JSON>` per chunk, errors carried inside events, connection closes at the end — no `[DONE]`). Both coexist; `generate_stream` rides the `streaming+sse` gate family, `/generate` (unary) is ungated. **Caveat (D9):** the HTTP status is fixed by the first SSE response; mid-stream errors arrive in later `data:` events and clients must check per-event. |
| ⑧a | **built-in `generate`/`generate_stream` shadow same-named custom `@route`** | Since batch 4 these paths are built-in routes; a model declaring a custom `@route` named `generate` / `generate_stream` no longer reaches the fallback dispatcher on those paths (G17 behavior change). Rename the custom route if you relied on it. |

## Batch 5 (openai-compact)

| # | Deviation | Note |
|---|---|---|
| ⑨ | **`/v1/rerank` not implemented** | Not an OpenAI API (KServe's own extension); openai-compact is exactly 5 endpoints (chat/completions/embeddings/models/models/{model}). |
| ⑩ | **translation layer lives on the worker side** | The server thin-forwards /v1 (body parsed only for `model`/`stream` routing+demux and SSE frame encoding); chat request parsing / completion·chunk·embeddings construction live in the worker-side helper `lite_server/helpers/openai.py` (chat→tensor is model semantics; the server cannot translate generically). |

## Follow-ups

- (none — batches 0–5 complete, this plan is wrapped up.)
