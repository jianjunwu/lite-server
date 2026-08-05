# HTTP Decoupled 流式

decoupled 流的生命周期由**模型**驱动：worker 从 `predict_decoupled` 立即返回，
经 `ResponseSender` 异步推送 N 个 chunk，显式 `sender.close()` 结束。客户端
纯接收——首次请求 payload 之后无 C→S 数据流。

lite-server 提供两条 HTTP decoupled 传输，二者都翻译到与 gRPC `DecoupledInfer`
RPC 相同的 worker 流协议（模型侧的 `predict_decoupled`——见
[模型开发](../model-authoring.md)）。模型与 worker 无需任何传输层特定代码。

| 传输 | 端点 | 帧格式 | 开关 |
|------|------|--------|------|
| SSE | `POST /v2/models/{m}/decoupled`（含 `/versions/{v}/decoupled`） | `text/event-stream` | `features.streaming && features.sse && features.decoupled` |
| WebSocket | `GET /v2/models/{m}/decoupled-stream`（含 `/versions/{v}/decoupled-stream`） | WS 消息（JSON + 二进制） | `features.streaming && features.websocket_streaming && features.decoupled` |

`x-sequence-id` 二者均支持（worker affinity），auth、rate limit、deadline
（`x-lite-timeout`）、`x-lite-worker-id`（直连 pin）及推理回调也同样支持。

`features.decoupled` 默认 `true`。设为 `false` 则在路由层卸载两条路由（404）。

> **注意：** 名为 `decoupled` 或 `decoupled-stream` 的 `@route` 声明会被内置
> 端点遮蔽——与 `/bidi`、`/stream` 相同的 tradeoff。

## SSE `POST .../decoupled`

```
请求:  POST /v2/models/{m}/decoupled
       headers: authorization / x-sequence-id / x-lite-timeout / x-lite-worker-id
       body: JSON（任意模型 payload）
响应:  200 OK, content-type: text/event-stream
       data: <chunk 1>            ← 模型推送（String::from_utf8_lossy）
       data: <chunk 2>
       data: {"error":{...}}      ← 终端错误（结构化 HTTPException 透传）
       data: [DONE]               ← 终端（模型 close()）
```

终端帧（Error 或 Done）后服务端结束响应，并发 **定向** cancel 给 worker
（仅该流的属主 worker，非广播）。

客户端断开 → `event_tx` 失败 → forwarder 退出 → 定向 cancel（复用现有回收路径）。

## WebSocket `GET .../decoupled-stream`

```
握手: GET /v2/models/{m}/decoupled-stream  → 101
      （CORS：浏览器 WS 无 preflight，升级时校验 Origin——ws_origin_allowed）
C→S   Text {"prompt": ...}         首帧 = 请求 payload
                                   （Binary 亦可，UTF-8 lossy 解码）
S→C   Binary <chunk>               模型推送 ×N
C→S   Text {"type":"cancel"}       可选取消
C→S   Text {"type":"close"}        cancel 别名（行为相同）
S→C   Text {"done":true}           终端（模型 close()）
S→C   Text {"error":...}           终端错误 / 协议错误
```

### 控制帧（首帧之后的 C→S）

| C→S 帧 | 服务端动作 |
|--------|-----------|
| Text `{"type":"cancel"}` | 发定向 `build_stream_cancel` 给 worker → 正常关闭 WS（1000） |
| Text `{"type":"close"}` | cancel 别名——与 `{"type":"cancel"}` 完全一致 |
| Binary（首帧之后） | 发 `{"error":"decoupled stream accepts no data frames"}` → cancel worker → 关闭 |
| 其他 Text | 发 `{"error":"unknown control frame"}` → cancel worker → 关闭 |
| 硬断连 | 立即定向 cancel（gone 信号，不等 idle timeout） |

S→C 帧：**Binary** = chunk；**Text** `{"error":...}` = 终端错误；
**Text** `{"done":true}` = 终端。终端帧后服务端关闭 socket。

## 共有语义

- **路由**：`RequestMeta.route = "/predict"`，`Protocol::Sse` / `Protocol::WebSocket`，
  `InferenceContext.route = "/predict"`——与 gRPC decoupled 及 coupled SSE/WS 一致。
- **超时**（方案 C，与 coupled 零差异）：overall deadline 仅当客户端指定
  `x-lite-timeout` 时生效；chunk-idle（`server.decoupled_idle_timeout_secs`，
  默认 300s）始终开。
- **背压**：forwarder `mpsc(64)` 有界通道（继承自 coupled forwarder）。
- **worker 未实现 `predict_decoupled`**：worker 发 Error 帧 → 走现有终端错误
  映射（worker 侧保证 FailedPrecondition 语义）。
- **Canary**：SSE/WS 无 canary 路径（与 coupled SSE/WS 一致）。
- **Auth / rate-limit**：与 SSE infer / WS stream 完全相同——模型 policies
  同序评估（先 auth 后 rate-limit；WS 错误走帧不走 HTTP 状态码）。
- **Cancel 幂等**：同一流上 cancel 可能发两次（reader + main task）；
  worker 对未知/已终结 stream_id 的 Cancel 忽略——构造安全。

## 可观测性

- 流式指标（`record_stream_open/ttft/tbt/chunk/close`）复用 `"sse"` /
  `"websocket"` label——与 coupled SSE/WS 相同（gRPC decoupled 也复用 `"grpc"`）。
- 推理回调携带 `Protocol::Sse` / `Protocol::WebSocket`；`InferenceRequest`
  在 worker 流打开时触发，`InferenceResponse` 在终端帧精确触发一次。
- 长跑 decoupled 流（可能跨数分钟）被 idle timeout 回收时也照常记录
  `stream_close`。

## 与 gRPC DecoupledInfer 对比

| 维度 | gRPC `DecoupledInfer` | HTTP SSE | HTTP WS |
|------|----------------------|----------|---------|
| Worker 方法 | `predict_decoupled` | 相同 | 相同 |
| `StreamOpen.decoupled` | `true` | `true` | `true` |
| 超时语义 | 方案 C（overall + idle） | 相同 | 相同 |
| Cancel | 定向 | 定向（D4） | 定向 |
| 指标 label | `"grpc"` | `"sse"` | `"websocket"` |
| 功能开关 | `features.grpc_streaming` | `streaming && sse && decoupled` | `streaming && websocket_streaming && decoupled` |

## 快速示例

### SSE（curl）

```bash
curl -N -X POST http://localhost:8900/v2/models/my-model/decoupled \
  -H "Content-Type: application/json" \
  -d '{"input": 3}'
# data: {"index":0}
# data: {"index":1}
# data: {"index":2}
# data: [DONE]
```

### WebSocket（wscat）

```bash
wscat -c ws://localhost:8900/v2/models/my-model/decoupled-stream
> {"input": 3}
< {"index":0}     # Binary
< {"index":1}     # Binary
< {"index":2}     # Binary
< {"done":true}   # Text
```
