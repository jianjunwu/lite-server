# Modality Transport Guide

Which transport/compression to use per payload type. The short version:
**compressed-at-source bytes (audio, tensors) go as raw bytes; large textual
payloads use gzip or gRPC; token streams stay uncompressed.**

| Payload | Recommended path | Why |
|---|---|---|
| Audio | Encode at the codec layer (Opus/AAC) → raw-byte envelope | Already entropy-coded; transport gzip adds latency for zero gain |
| Tensors / binary | Raw bytes (KServe binary extension / `x-lite-bidi` envelope) | Same — no generic compression on high-entropy data |
| Large JSON between services | `server.request_decompression` + client gzip, or gRPC | Text compresses well; gRPC does per-message gzip both ways |
| Token streams (SSE) | Never gzip | gzip buffering breaks per-token flush (TTFT) |
| h2 bidi / WS frames | Never transport-compressed | Frame timeliness; codec layer owns audio compression |

## Request decompression (gzip)

Off by default. Enable for ingress that receives large gzipped bodies:

```yaml
server:
  request_decompression: true
```

- Applies to **all HTTP routes except h2 `/bidi`** (inference and admin,
  including `.lma` uploads). `/bidi` keeps its 415 rejection.
- Only `gzip` is accepted; any other `Content-Encoding` → 415 envelope.
  `identity` is treated as no encoding (header stripped).
- Decompressed bytes count against `server.max_request_body_bytes` (default
  64 MiB) — a zip bomb trips 413 after decoding, not before.
- KServe `Inference-Header-Content-Length` keeps working: decoding is 1:1,
  so the header byte offset is unchanged.

Client side:

```bash
curl -X POST http://localhost:8000/v2/models/m/infer \
  -H 'Content-Type: application/json' \
  -H 'Content-Encoding: gzip' \
  --data-binary @<(gzip -c payload.json)
```

## Response compression

`server.compression: true` gzips textual responses when the client sends
`Accept-Encoding: gzip`. SSE responses are excluded by predicate; WS
upgrades carry no body.

## gRPC

`grpc.response_compression: true` enables per-message gzip in both
directions on the inference service (`accept_compressed` +
`send_compressed`). Per-message framing keeps streaming safe — unlike HTTP
gzip, there is no cross-message buffer. Prefer gRPC for service-to-service
traffic with large payloads.
