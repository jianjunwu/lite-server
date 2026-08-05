# HTTP 双向流式

lite-server 提供两条 HTTP 双向流式传输，二者都翻译到与 gRPC `BidiStream`
RPC 相同的 worker 流协议（模型侧的 `on_open` / `on_chunk` / `on_close`——
见[模型开发](../model-authoring.md)）。模型与 worker 无需任何传输层特定代码。

| 传输 | 端点 | 帧格式 | 开关 |
|------|------|--------|------|
| WebSocket | `GET /v2/models/{m}/stream`（含 `/versions/{v}/stream`） | WS 消息（JSON + 二进制） | `features.streaming && features.websocket_streaming` |
| HTTP/2 | `POST /v2/models/{m}/bidi`（含 `/versions/{v}/bidi`） | LPM 帧（protobuf） | `features.streaming && features.http_bidi`（默认开） |

两条传输均支持 `x-sequence-id`(worker 亲和）、鉴权、限流、deadline
(`x-lite-timeout`）与推理回调。

## WebSocket `/stream` 双向帧约定

端点向后兼容：只发一帧然后只读的老客户端行为与之前完全一致。双向客户端
可在首帧之后继续发送。

| 方向 | 帧 | 格式 | 含义 |
|------|----|----|------|
| C→S | 首帧 | Text JSON payload(Binary 按 lossy UTF-8 解码为 JSON) | 初始输入(`on_open`) |
| C→S | Data ×N | **Binary**——原始字节 | 追加输入(`on_chunk`) |
| C→S | Close | Text `{"type":"close"}` | 优雅结束输入(`on_close`)；下行继续 |
| C→S | 其他 Text | — | 协议错误：服务端发 `{"error":"unknown control frame"}`、关闭连接、Cancel worker |
| S→C | Data ×N | Binary | worker 输出块 |
| S→C | Error | Text `{"error":...}` | 终止帧 |
| S→C | Done | Text `{"done":true}` | 终止帧，随后关闭连接 |

客户端断开会立即 Cancel worker（不等 idle 超时）。

```
C→S  Text   {"prompt":"..."}      # 首帧 = 初始 payload
S→C  Binary <chunk 1>
C→S  Binary <追加输入>             # 双向：首帧后可继续发
S→C  Binary <chunk 2>
C→S  Text   {"type":"close"}      # 优雅结束输入 → on_close
S→C  Text   {"done":true}         # 终止帧 → 关闭连接
```

## HTTP/2 `/bidi`(LPM 帧格式）

面向机器对机器客户端的二进制 protobuf 帧；复用 gRPC `BidiChunk` 消息，
一份 protobuf 定义覆盖两条传输。

**LPM 帧**(Lite Protocol Message):

```
+--------+------------------+-----------------+
| 1B flag| 4B length (BE)   | prost BidiChunk |
|  = 0   |  = N             | N 字节          |
+--------+------------------+-----------------+
```

- `flag` 必须为 0（压缩预留）；非 0 即协议错误。
- 单帧上限 16 MiB；超限声明在分配内存前拒绝。

**会话：**

```
POST /v2/models/asr/bidi   (h2;headers: authorization, x-sequence-id, x-lite-timeout…)
C→S  LPM(BidiChunk{open:{initial_data}})    # 首帧必须是 open;
                                            # model/version/sequence_id 字段被忽略——
                                            # 以 URL path 与 HTTP header 为准
C→S  LPM(BidiChunk{data:{…}}) ×N
C→S  LPM(BidiChunk{close:{}})               # 或直接结束 body(EOF → 服务端补 on_close)
S→C  200, content-type: application/x-lite-bidi
S→C  LPM(BidiChunk{data:{…}}) ×N
S→C  LPM(BidiChunk{close:{}})               # worker Done;失败时为 LPM(BidiChunk{error:{message,error_type}})
```

- `stream_id` 由服务端生成（`http-bidi-<uuid>`)，在每一帧下行帧中回显。
- 首帧非 `open` → 400（响应尚未提交，普通 HTTP 错误）。鉴权 / 未就绪 /
  限流失败同样是普通 4xx/404/503。
- 请求 body 结束而未发 `close` 帧时，服务端仍会优雅结束 worker 输入
  (half-close，与 gRPC 语义一致）。

**要求与限制：**

- **仅 h2。**HTTP/1.1 返回 `426 Upgrade Required`——不退化。客户端须用
  prior-knowledge h2c(`curl --http2-prior-knowledge`，或
  `reqwest::Client::builder().http2_prior_knowledge()`）或 TLS ALPN
  （服务端广播 `h2, http/1.1`)。不支持协商式 h2c upgrade。
- **必须立即流式发送。**服务端在提交 200 之前等待首个 LPM 帧（以
  `server.timeout` 兜底）。等到响应到达才发请求体的客户端会死锁至该
  超时。
- **不适用于浏览器。**`fetch` 无法全双工流式发送请求体；Web 应用请用
  WebSocket 端点。
- **代理不得缓冲。**nginx 默认 `proxy_request_buffering on` 会破坏同时
  性——须设为 `off`（或直连）。

## 可观测性

- 流式指标(`record_stream_open/ttft/tbt/chunk/close`）对该端点携带
  protocol label `http2`（其他传输为 `websocket` / `sse` / `grpc`)。
- 推理回调以 protocol `http2` 触发：`InferenceRequest` 在 worker 流打开
  时触发，`InferenceResponse` 在终止帧恰好触发一次。
