# 与 KServe V2 / Triton 的已知偏差（protocol-compat）

> 创建于批次 1（protocol-compat-plan.md C18：文档债随批清零——首批 C17 四条，
> 每批追加）。记录与 KServe V2 dataplane / Triton HTTP 协议的**有意偏差**，
> 供生态客户端对接时对照。对应方案：[.claude/protocol-compat-plan.md](../../.claude/protocol-compat-plan.md)。

## 批次 1（阶段 1 Triton Binary Tensor Data Extension，首批 C17）

| # | 偏差 | 说明 |
|---|---|---|
| ① | **KServe 二进制响应无 Content-Type** | KServe `encode()` 只写 `Inference-Header-Content-Length` header、不设 Content-Type；我们保留 `application/octet-stream`（Triton 惯例）——见 [raw-bytes-request.md](raw-bytes-request.md)。 |
| ② | **KServe CloudEvents 信封不做** | structured/binary 两种 CloudEvents 包装均不实现；Triton 的「JSON 头 + 二进制尾」是目标通道（G10 面向 tritonclient）。 |
| ③ | **KServe model not ready → 503（可选对齐）** | KServe 在 infer 前置 ready 检查、未就绪返回 503；我们 ready gate 语义既有，未做强制对齐。 |
| ④ | **Triton `/statistics`、`/config` 端点不做** | `GET /v2/models/:m/statistics`、`/v2/models/:m/config` 非目标（G20）；tritonclient `get_inference_statistics` 会 404。 |

## 待追加（后续批次）

- 批次 2：错误体双形状——KServe-mode 请求返回扁平 `{"error": "<message>"}`、
  非 KServe 请求维持 OpenAI 风格（经协议层 seam 分派）；SSE 自有格式与
  Triton `generate_stream` 的区别（批次 4）。
- 批次 6：`/v1/rerank` 不做（非 OpenAI API，KServe 自家扩展）。
