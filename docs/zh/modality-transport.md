# 模态传输指引

按负载类型选择传输与压缩方式。一句话：**源头已编码的字节(音频、tensor)走原生字节;大文本用 gzip 或 gRPC;token 流永不压缩。**

| 负载 | 推荐路径 | 原因 |
|---|---|---|
| 音频 | 编解码层压缩(Opus/AAC)→ 原生字节信封 | 已经熵编码;传输层 gzip 零收益反增延迟 |
| tensor / 二进制 | 原生字节(KServe 二进制扩展 / `x-lite-bidi` 信封) | 同上,高熵数据不做通用压缩 |
| 服务间大 JSON | `server.request_decompression` + 客户端 gzip,或 gRPC | 文本压缩率高;gRPC 双向逐消息 gzip |
| token 流(SSE) | 永不 gzip | gzip 缓冲破坏逐 token 冲刷(TTFT) |
| h2 bidi / WS 帧 | 永不走传输压缩 | 帧及时性;音频压缩归编解码层 |

## 请求解压(gzip)

默认关闭。接收大 gzip 请求体的入口可开启:

```yaml
server:
  request_decompression: true
```

- 覆盖**除 h2 `/bidi` 外的全部 HTTP 路由**(推理与 admin,含 `.lma` 上传);`/bidi` 维持 415 拒止。
- 仅接受 `gzip`;其他 `Content-Encoding` → 415 错误信封。`identity` 视为无编码(剥头放行)。
- 解压后字节计入 `server.max_request_body_bytes`(默认 64 MiB)——zip bomb 在解压后触发 413。
- KServe `Inference-Header-Content-Length` 语义不变:解压 1:1 还原,头部字节偏移不受影响。

客户端:

```bash
curl -X POST http://localhost:8000/v2/models/m/infer \
  -H 'Content-Type: application/json' \
  -H 'Content-Encoding: gzip' \
  --data-binary @<(gzip -c payload.json)
```

## 响应压缩

`server.compression: true` 在客户端带 `Accept-Encoding: gzip` 时压缩文本响应。SSE 响应按谓词排除;WS 升级无 body 不受影响。

## gRPC

`grpc.response_compression: true` 在推理服务上启用双向逐消息 gzip(`accept_compressed` + `send_compressed`)。逐消息成帧保证流式安全——不同于 HTTP gzip,没有跨消息缓冲。服务间大 payload 流量优先走 gRPC。
