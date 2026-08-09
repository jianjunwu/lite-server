# CLI 参考手册

[English](../cli.md)

## 安装

```bash
pip install miraserver
```

## 全局选项

```bash
lite-server -v, --version    显示版本
lite-server -h, --help       显示帮助
```

## 子命令

### `serve` — 启动推理服务器

```bash
lite-server serve [选项]
```

#### 配置文件

| 参数 | 类型 | 说明 |
|------|------|------|
| `--config`, `-c` | string | YAML 配置文件路径 |

#### 服务器选项

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--port` | int | 8000 | HTTP 服务端口 |
| `--host` | string | 0.0.0.0 | 绑定地址。使用 `unix:/path/to/sock` 启用 Unix 域套接字 |
| `--timeout` | float | 30.0 | 全局请求超时（秒） |
| `--log-level` | string | info | 日志级别：`trace`、`debug`、`info`、`warn`、`error` |
| `--log-info-output` | string | — | info 级别日志的独立文件 |
| `--log-error-output` | string | — | error 级别日志的独立文件 |
| `--log-rotation` | string | none | 日志轮转策略：`none`、`size`、`daily`、`hourly` |
| `--threads` | int | 自动 | Tokio 工作线程数 |
| `--graceful-timeout` | float | 30.0 | 优雅关闭时等待进行中请求的最大秒数 |
| `--keepalive-timeout` | float | 5.0 | HTTP keep-alive 超时（秒），0 = 禁用 |

#### 端口选项

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--grpc-port` | int | 8001 | gRPC 服务端口 |
| `--metrics-port` | int | 8002 | Prometheus `/metrics` 端点端口 |

#### 模型仓库

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--model-repo` | string | ./model_repo | 模型仓库目录 |

#### 功能开关

| 参数 | 说明 |
|------|------|
| `--no-metrics` | 禁用 Prometheus 指标端点 |
| `--no-grpc` | 禁用 gRPC 服务 |
| `--no-streaming-metrics` | 禁用流式指标采集 |

#### 模型默认值（覆盖所有模型）

这些参数设置全局默认值，会覆盖各模型 `config.yaml` 中的对应配置。

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--max-queue-size` | int | 1000 | 每个 worker 的最大待处理请求数 |
| `--max-requests` | int | 0 | worker 处理 N 个请求后自动重启（0 = 禁用） |
| `--max-requests-jitter` | int | 0 | `max_requests` 的随机抖动范围，防止惊群效应 |
| `--request-timeout` | float | 0.0 | 单请求硬超时（秒），0 = 禁用 |
| `--health-check-interval` | float | 15.0 | 主动健康检查间隔（秒），0 = 禁用 |
| `--ejection-error-threshold` | int | 3 | 连续错误 N 次后剔除 worker（0 = 禁用） |
| `--ejection-timeout` | float | 30.0 | 被剔除 worker 自动恢复前的秒数 |
| `--ejection-max-percent` | int | 50 | 同一时刻最多可剔除的 worker 比例（1-100） |
| `--max-retries` | int | 3 | 失败 batch 换 worker 重试次数（0 = 禁用） |
| `--startup-timeout` | float | 60.0 | 等待 worker ready 握手的最大秒数 |
| `--health-check-timeout` | float | 5.0 | 单次健康探测超时（秒） |
| `--worker-kill-timeout` | float | 10.0 | 杀死 worker 后等待 OS 回收的秒数 |
| `--hook-http-timeout` | float | 5.0 | 生命周期 HTTP 钩子请求超时（秒） |

#### 示例

```bash
# 最简启动 — 从 ./model_repo 加载模型
lite-server serve

# 使用配置文件
lite-server serve --config server.yaml

# 覆盖端口和日志级别
lite-server serve --config server.yaml --port 9090 --log-level debug

# 生产环境 — 多 worker、长优雅关闭超时
lite-server serve --config server.yaml \
  --graceful-timeout 60 \
  --keepalive-timeout 10 \
  --max-requests 1000 \
  --max-requests-jitter 100

# 禁用 gRPC 和指标
lite-server serve --no-grpc --no-metrics
```

---

### `config-check` — 验证配置

```bash
lite-server config-check <配置文件>
```

验证 YAML 配置文件并报告错误。

```bash
lite-server config-check server.yaml
```

---

### `benchmark` — 运行基准测试

```bash
lite-server benchmark --model <模型名> [选项]
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--url` | string | http://127.0.0.1:8000 | 服务器 URL |
| `--model` | string | （必填） | 要测试的模型名 |
| `--version` | string | （最新版） | 模型版本 |
| `--concurrency` | int \| start:end:step | 8 | 并发数，或 sweep 范围（如 `1:16:2` → 1,3,5,…,15） |
| `--duration` | float | 30.0 | 持续 N 秒（与 `--requests` 互斥） |
| `--requests` | int | — | 固定发送 N 个请求（与 `--duration` 互斥） |
| `--warmup-requests` | int | 0 | 预热请求数，样本丢弃（推荐 ≈ 并发数） |
| `--grace-period` | float | 30.0 | 到期后等待在途请求完成的上限（秒） |
| `--rate` | float | — | 固定到达速率 req/s（开环）；消除负载发生器侧的 coordinated omission |
| `--latency-threshold` | float | — | sweep 模式下 p99 超过 MS 毫秒时提前停止 |
| `--payload` | string | `{"input": 1.0}` | 内联 JSON 请求体 |
| `--payload-file` | path | — | JSON 请求体文件；可重复指定，轮询发送 |
| `--payload-random` | string | — | 每次请求随机化 id/request_id/uuid，使用 TEMPLATE 作为基础 JSON |
| `--export` | path | — | 将权威 JSON 记录写入 PATH（stdout 表格不变） |
| `--max-error-rate` | float | — | 错误率超过 R 时退出码 99（如 0.01） |
| `--max-p99` | float | — | p99 延迟超过 MS 毫秒时退出码 99 |
| `--stream` | flag | off | 使用 SSE 流式端点 `/v2/models/{m}/events`（与 `--bidi` 互斥） |
| `--bidi` | flag | off | WS `/stream` bidi 模式的会话压测；payload 必须是 JSON 数组 `[open, chunk1, ...]` |
| `--model-type` | llm\|tts\|stt\|generic | llm | 流式指标的语义解释（`generic`：仅通用节） |
| `--endpoint` | events\|decoupled | events | 流式端点变体（`decoupled` → `/v2/models/{m}/decoupled`，需配合 `--stream`） |
| `--transport` | sse\|ws\|grpc\|h2 | sse（`--bidi` 时为 ws） | 流式传输（ws/grpc 需配合 `--stream`）。`ws` → `/stream`\|`/decoupled-stream`；`grpc` → StreamInfer\|DecoupledInfer（向 `--url` 的 host:port 建 insecure channel）；`h2` → `/bidi`（仅 bidi,h2c prior-knowledge） |
| `--pace` | float | — | bidi 实时节奏：chunk 间隔秒数（需配合 `--bidi`；默认 lock-step） |
| `--rt-factor` | float | — | bidi 倍速：`--pace` 除以 N（需配合 `--pace`） |
| `--min-sessions` | int | 30 | bidi：样本量告警的最少完成会话数 |
| `--cancel-after` | int | — | 每流消费 N chunk 后取消——客户端取消场景（需配合 `--stream`）；取消计入 `error_kinds` 的 `canceled` 桶 |
| `--read-delay-ms` | float | — | 慢消费者场景：每 chunk 后睡眠 MS 毫秒（需配合 `--stream`） |
| `--goodput` | string | — | SLO 表达式，如 `ttft:500 tpot:50 e2el:2000`（毫秒；需配合 `--stream`；`tpot` 仅 llm） |
| `--slo-attainment` | float | 0.95 | SLO 达标率低于 R 时退出码 99（需配合 `--goodput`） |
| `--tokenizer` | string | — | 客户端精确 token 计数（本地文件或 hub id；需 `--stream` + `--model-type llm`；需 `pip install miraserver[benchmark]`） |
| `--text-field` | string | text→token | chunk JSON 中待分词文本所在字段（需配合 `--tokenizer`） |
| `--stream-read-timeout` | float | 300.0 | 流式 chunk 间超时秒数 |
| `--max-ttft-ms` | float | — | TTFT p99 超过 MS 毫秒时退出码 99（需配合 `--stream`） |
| `--max-rtf` | float | — | RTF p99 超过 VAL 时退出码 99（需 `--stream` + `--model-type tts/stt`） |

**测量口径**（闭环：service-time；开环：`--rate`）：

- **闭环**（`load_mode: closed-loop`、`latency_basis: service-time`）：延迟为纯服务时间，不含负载发生器侧排队（闭环固有）。
- **开环**（`load_mode: open-loop`、`latency_basis: service-time`）：按固定间隔调度请求。在吞吐之外报告实际发出速率（`achieved_rate`）。发生器无法维持目标速率时（调度落后或信号量饱和）显式告警。
- 预热样本丢弃；到期在途请求按 `--grace-period` 有界 drain（grace 内完成计入统计，其余记为 `dropped_inflight`）。
- 吞吐 = `成功数 / (末响应 − 首请求)`（实测窗口）；百分位统一 numpy `linear` 插值（报告 p50/p90/p95/p99/max）。
- 样本不足（`< max(300, 10 × 并发数)`）与客户端 CPU 饱和（单核 >70%）会在 stdout 与 JSON 中显式告警。

**流式测量口径**（`--stream`）：

- 流式适配器将 SSE 响应包装为单次调用；`latency_ms` 仍报告端到端流延迟。每个 chunk 的指标（TTFT、ITL、TPOT）在 JSON `"stream"` 段中。
- 空 chunk（keepalive、`data: [DONE]`）不计入 TTFT/ITL/chunk 数，但其字节仍计入总量。
- **LLM**（`--model-type llm`，默认）：token 数默认按 `chunk_count` 估算（estimated）。当模型在 chunk 元数据中提供 `token_count` 时标记为 exact。部分有、部分无标记为 mixed，仍计算指标但附注警告。
- **TTS**（`--model-type tts`）：RTF = `total_ms / audio_duration_ms`，从 chunk 元数据提取。
- **STT**（`--model-type stt`）：RTF = `total_ms / audio_duration_ms`，从请求 payload 提取。**约定**：在 JSON payload 中包含 `"audio_duration_ms"`（float，毫秒）——CLI 会自动提取。不含此字段的请求不参与 RTF 计算。
- **Generic**（`--model-type generic`）：仅通用节（chunks_per_request / TTFT / e2e），不计算 ITL/tokens/RTF。面向 decoupled 及其他非 token 流。
- **Decoupled**（`--endpoint decoupled`）：压测 `POST /v2/models/{m}/decoupled`（服务端异步推送，`is_final` 终止）。与 `/events` 同一 SSE wire 格式；通常搭配 `--model-type generic`。压测前把 `decoupled_idle_timeout_secs` 设足够大——idle 截断在客户端无法与正常关流区分。
- **传输**（`--transport`）：`sse`（默认，httpx）· `ws`（websockets；Binary 帧 = chunk，Text 帧仅 `{"done":true}`/`{"error":...}` 控制帧）· `grpc`（StreamInfer/DecoupledInfer，insecure channel，payload 为 JSON bytes）。`--endpoint` 在各传输下选择对应端点变体（`events` ↔ `/stream` ↔ StreamInfer）。注意：`grpc` 下 `--stream-read-timeout` 是整个 RPC 的 deadline（gRPC 语义），不是逐 chunk 空闲预算。
- **组合矩阵**：`--stream` 与 `--concurrency start:end:step`、`--rate`、`--version` 自由组合，无需额外参数。
- **阈值**：`--max-ttft-ms` 与 `--max-rtf` 按 p99 门禁；fail-closed（缺少 `--stream` 时 exit 2）。

**Bidi 会话口径**（`--bidi`，暂仅 WS 传输）：

- 压测单元是**会话**：open → 按节奏推 chunk → close。payload 为 JSON 数组——第 0 元素是 open 载荷（作为 Text 首帧发送 → `on_open`），其余为数据 chunk（各自 JSON 序列化为 Binary 帧 → `on_chunk`）。
- 会话指标（JSON `"bidi"` 段）：open 延迟、close→final 延迟、会话 e2e 时长、每会话 chunk 数、会话/秒；逐 chunk 往返百分位仅 **lock-step** 模式。
- **lock-step**（默认）要求模型每个 `on_chunk` 都返回响应**且** `on_open` 返回 ready 响应——稀疏响应模型须用 `--pace`（实时）或 `--pace` + `--rt-factor`（倍速），这两种模式不做 chunk↔响应配对。
- `--stream-read-timeout` 是逐帧空闲预算；超时的会话计为失败。`--max-p99` 门禁会话 e2e 时长。样本量告警阈值是 `--min-sessions`（默认 30），而非请求模型的 300。

**流式场景**（仅 `--stream`，全传输通用）：

- **mid-stream error**（E1）：指向注错模型（如 `examples/03_streaming` 的 `stream_errors`，`mode=server_error`），用可重复的 `--payload-file` 混合正常/错误负载；错误帧计入 `error_kinds` 的 `stream` 桶，由 `--max-error-rate` 门禁。
- **client cancel**（E2）：`--cancel-after N` 在每流 N 个 chunk 后中止（连接拆除 → 服务端 promptly 取消 worker）。取消计入 `canceled` 桶——不与失败混淆。
- **慢消费者**（E3）：`--read-delay-ms M` 每 chunk 后睡眠 M；ITL 膨胀即服务端发送阻塞信号。注意：内核缓冲区会吸收小 chunk 反压——本场景测"慢排空"行为而非 TCP 级反压。e2e 含一个尾部延迟。

**Goodput / SLO**（`--goodput`，仅 `--stream`）：

- SLO 表达式：空格分隔的 `键:阈值毫秒`——`ttft`（首 token 延迟）、`tpot`（每请求逐 token 时间，仅 llm）、`e2el`（端到端延迟）。请求在**所有**指定指标内即达标（逐请求判定，非百分位）。
- 输出（JSON `stream.goodput`）：`attainment`（达标数/成功数）、`goodput_req_per_sec`（= attainment × 吞吐，vLLM 语义）、`attainment_target`。
- 门禁：attainment 低于 `--slo-attainment`（默认 0.95）→ exit 99。
- 缺失指定指标的记录（如零 chunk 流）计为不达标。

**精确 token 计数**（`--tokenizer`，仅 `--stream --model-type llm`）：

- 从本地文件（`Tokenizer.from_file`）或 HuggingFace hub id（`from_pretrained`，需联网）加载 `tokenizers` 分词器。需 `pip install miraserver[benchmark]`。
- 逐 chunk 对文本字段（`--text-field`，默认先试 `"text"` 再试 `"token"`）做客户端分词并馈入 token 指标——TPOT / tokens/sec 变为精确值。chunk meta 已带 `token_count` 时保留服务端值（绝不重复计数）；无文本字段的 chunk 计 0 并产生告警。
- 分词增加客户端 CPU；留意内置的 CPU 饱和告警。

```bash
# SLO 门禁:低于 95% 达标率则 exit 99
lite-server benchmark --model llama --stream --duration 60 \
  --goodput "ttft:500 tpot:50 e2el:2000" --slo-attainment 0.95

# 模型无法上报 token_count 时的客户端精确 token 指标
lite-server benchmark --model llama --stream --duration 60 \
  --tokenizer ./tokenizer.json --text-field text
```

```bash
# 客户端取消场景:每流 5 chunk 后取消
lite-server benchmark --model llama --stream --cancel-after 5 --requests 100

# 错误混合负载:正常+错误 payload 轮替,错误率门禁
lite-server benchmark --model stream_errors --stream --requests 200 \
  --payload-file ok.json --payload-file err.json --max-error-rate 0.6
```

```bash
# bidi lock-step：逐 chunk 往返延迟(echo 型模型)
lite-server benchmark --model asr --bidi --duration 300 --concurrency 4 \
  --payload-file tests/fixtures/asr_session.json   # ["open", chunk1, chunk2, ...]

# bidi 实时节奏:25 fps ASR 节奏(每 chunk 320ms)
lite-server benchmark --model asr --bidi --pace 0.32 --duration 300 \
  --payload-file tests/fixtures/asr_session.json

# bidi 2 倍速:寻找过载拐点
lite-server benchmark --model asr --bidi --pace 0.32 --rt-factor 2 --duration 300 \
  --payload-file tests/fixtures/asr_session.json
```

**退出码**：`0` 通过 · `1` 执行错误（如无请求完成） · `2` 参数/payload 错误 · `99` 阈值违例。

```bash
# 对 my_model 做 60 秒、16 并发的基准测试
lite-server benchmark --model my_model --concurrency 16 --duration 60

# CI 冒烟：固定次数 + 预热 + JSON 导出 + 错误率门禁
lite-server benchmark --model my_model --requests 200 --concurrency 4 \
  --warmup-requests 4 --max-error-rate 0.01 --export smoke.json

# LLM 流式：SSE 端点 + token 级指标
lite-server benchmark --model llama --stream --model-type llm --duration 60 --concurrency 16

# TTS 流式：基于 RTF 的评估
lite-server benchmark --model xtts --stream --model-type tts --concurrency 4 \
  --payload '{"text": "你好世界"}'

# STT 流式：RTF 从 payload 的 audio_duration_ms 计算
lite-server benchmark --model whisper --stream --model-type stt --duration 60 \
  --payload '{"audio_duration_ms": 5000}'

# 流式 + 延迟阈值（超出则 exit 99）
lite-server benchmark --model llama --stream --requests 100 \
  --max-ttft-ms 200 --max-p99 500

# Decoupled 流式：服务端推送端点 + generic 指标
lite-server benchmark --model detector --stream --endpoint decoupled \
  --model-type generic --duration 60 --concurrency 8

# WS 传输流式(websockets)
lite-server benchmark --model llama --stream --transport ws --duration 60

# gRPC 传输流式(StreamInfer,insecure channel)
lite-server benchmark --model llama --stream --transport grpc \
  --url http://127.0.0.1:8001 --duration 60

# 流式并发扫描
lite-server benchmark --model llama --stream --concurrency 1:16:2 --duration 30
```

---

### `analyze` — 模型分析器

```bash
lite-server analyze --model <模型名> [选项]
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--model-repo` | string | ./model_repo | 模型仓库路径 |
| `--model` | string | （必填） | 要分析的模型名 |
| `--version` | string | （最新版） | 模型版本（缺省按 latest 解析并产生 LS111 警告） |
| `--format` | json\|markdown | json | 输出格式；markdown 由同一份 schema v1 数据渲染 |
| `--output-dir` | string | — | 额外将报告文件（json+md）保存到 DIR |
| `--fail-severity` | error\|warning | error | 触发退出码 1 的最低严重度 |
| `--strict` | flag | false | `--fail-severity warning` 的简写 |
| `--deep` | flag | false | 在隔离子进程中 import model.py 以解析静态无法确定的类（**会执行模型代码**——需显式启用） |
| `--deep-timeout` | float | 30.0 | `--deep` 导入的超时秒数 |
| `--interop` | kserve-v2 | — | 运行可选的互操作 profile 检查（kserve-v2：KServe V2 推理协议；protocol-compat 批次 3 由 `--profile` 改名——`--profile` 保留为废弃别名） |

**纯静态分析——用户代码绝不执行。** model.py 以 AST 解析（无 import 副
作用）；路径限定在仓库根内（`..`/符号链接逃逸以退出码 2 拒绝）；
config.yaml 走与 `config-check` 相同的 Rust serde 校验路径。

**退出码**：`0` 无达到 `--fail-severity` 的发现 · `1` 有达到该级别的发现 · `2` 分析本身失败（模型/版本不存在、路径越界）。

| rule_id | 严重度 | 触发条件 |
|---------|--------|---------|
| LS001 | error | `predict` 未实现（LitAPI 基类抛 NotImplementedError） |
| LS002 | error | LitAPI 子类为零或多个（按最派生类计），或非 ensemble 模型缺少 model.py |
| LS004 | error | config.yaml 未通过校验（Rust serde）或顶层非 mapping |
| LS005 | error | .py 文件语法错误 |
| LS101 | warning | `max_batch_size > 1` 但 `batch`/`unbatch` 均未覆写 |
| LS102 | warning | `setup` 未覆写（基类默认 pass） |
| LS103 | warning | `stream: true` 但 `stream_predict` 未覆写或非 generator |
| LS104 | warning | requirements.txt 存在无法解析的行 |
| LS111 | warning | 未指定 `--version`，按 latest(1) 解析 |
| LS201 | info | 生命周期钩子（`teardown`/`on_file_changed`）未覆写 |
| LS202 | info | 疑似 LitAPI 子类但基类无法静态解析（假阴性不静默） |
| LS203 | warning | `--deep` 导入失败（超时、非零退出、无效输出或运行时错误） |
| LS204 | info | `--deep` 在运行时解析了 API 类 |
| LS205 | info | `--deep` 解析出与 AST 不同的 API 类 |
| LS301 | warning | 动态代码执行：`eval()`/`exec()`/`compile()` |
| LS302 | warning | 系统调用：`os.system()`/`subprocess.*` |
| LS303 | warning | 网络调用：`socket`/`urllib`/`requests`/`httpx` |
| LS304 | warning | 反序列化：`pickle.load()`/`torch.load()`/`yaml.load()` |
| LS305 | warning | 破坏性文件操作：`os.remove()`/`os.unlink()`/`shutil.rmtree()` |
| LS401 | info | KServe V2：`decode_request`/`encode_response` 覆写不对称 |
| LS402 | info | KServe V2：config.yaml 缺少 `name`/`version` 用于模型元数据端点 |
| LS403 | info | KServe V2：`stream: true` 但 `stream_predict` 非 generator |
| LS404 | warning | KServe V2：`predict` 未实现；V2 infer 将返回 500 |

JSON 报告为 schema v1（`schema_version: 1`），是唯一权威表示——CI 门
禁与下游工具应消费 JSON（经 stdout 或 `--output-dir`），而非 markdown
渲染层。

---

### `profile` — 配置空间搜索

```bash
lite-server profile --model <MODEL> [OPTIONS]
```

对**运行中**的服务器搜索配置空间（config 点 × concurrency）。配置变更
经 Admin ReloadModel 应用——服务端从磁盘重读 config.yaml
（validate-then-swap），服务器进程不重启；worker 按 config 点重建。
资源指标为通用口径（CPU/RAM，非 GPU），经 Prometheus `/metrics` 端点
与本机 psutil 采样获取。

| Flag | 类型 | 默认值 | 说明 |
|------|------|---------|-------------|
| `--model` | string | （必填） | 模型名 |
| `--version` | string | （解析） | 模型版本 |
| `--repo` | path | ./model_repo | 模型仓库路径（预检 batching 检测） |
| `--admin-url` | url | http://127.0.0.1:8000 | Admin 端点（兼作 trial 的推理 URL） |
| `--metrics-url` | url | admin 主机:8002 | Prometheus /metrics 端点 |
| `--server-pid` | int | — | 本机资源采样的服务器 PID（缺省按端口反查监听进程） |
| `--sweep-knob` | KEY=v1,v2,v3 | — | 被扫配置键（可重复）。batch 键要求已声明 batch/unbatch；`continuous_batching` 下剔除 `workers_per_device` |
| `--concurrency` | list | 1,2,4,8,16 | 内层网格 concurrency 档（档间零 reload） |
| `--search-mode` | grid\|quick | grid | 搜索策略：全网格，或 quick（单键爬山，约 <40% 点数） |
| `--max-trials` | int | 64 | 跨积上限（grid）/ 测点上限（quick） |
| `--duration` | float | 30.0 | 单 trial 时长（秒） |
| `--requests` | int | — | 单 trial 固定 N 个请求（与 `--duration` 互斥） |
| `--export` | dir | — | 写出逐 trial JSON checkpoint + summary.json + report.md 到 DIR |
| `--resume` | dir | — | 换约束重分析完整 checkpoint，或续跑中断的运行（campaign 哈希须匹配） |
| `--reload-timeout` | float | 120.0 | ReloadModel 后等待 Ready 的秒数 |
| `--max-trial-failures` | int | 3 | 熔断前允许的连续 config 点失败数 |
| `--objective` | throughput\|goodput\|sessions_per_sec | throughput | 排名目标（goodput 需 `--goodput`；sessions_per_sec 仅 bidi） |
| `--top-n` | int | 3 | top-N 推荐 |
| `--max-p99` | float | — | 约束：p99 延迟预算（ms） |
| `--min-throughput` | float | — | 约束：最低 req/s |
| `--max-error-rate` | float | — | 约束：最大 failed/total |
| `--max-ttft-ms` | float | — | 约束：TTFT p99 预算（ms，流式） |
| `--max-rtf` | float | — | 约束：RTF p99 预算（TTS/STT） |
| `--max-session-ms` / `--max-chunk-roundtrip-ms` | float | — | 约束：bidi 会话时长 / chunk roundtrip p99 预算 |
| `--max-rss-mb` | float | — | 约束：进程树 RSS（仅本机服务器） |
| `--apply-recommendation` | flag | false | 跑完后保留 top-1 配置并 reload 生效 |
| `--dry-run` | flag | false | 只打印预检结论 + 有效网格 + 预估墙钟；零副作用 |
| `--force` | flag | false | 覆盖独占使用守卫（他方流量会污染结果） |
| `--recover` | flag | false | 从残留 `.profile.backup` 逐字节恢复 config.yaml 后退出 |

**benchmark 透传**（流式/bidi 场景，方案 §2.11）：
`--stream`、`--bidi`、`--model-type`、`--endpoint`、`--transport`、
`--payload` / `--payload-file` / `--payload-random`、`--rate`、
`--warmup-requests`、`--grace-period`、`--goodput`、`--slo-attainment`、
`--tokenizer`、`--text-field`、`--pace`、`--rt-factor`、`--min-sessions`、
`--cancel-after`、`--read-delay-ms`。

**预检门禁（任一失败 → exit 2）**：服务器可达 + 模型已加载 +
`/metrics` 可读；服务器版本 ≥ 0.8.4（reload_model 磁盘重读修复——已
发布的 v0.8.4-rc0 tag 早于该修复，被拒绝；rc1+ 与正式版放行）；独占
（所有模型的 `liteserver_queue_depth` == 0 且
`liteserver_in_flight_requests` == 0，`--force` 可覆盖）；StaticAnalyzer
AST 检测（零执行）的 batching 声明状态决定扫描键集（未声明/不确定 →
剔除 batch 键；已声明 → batch 网格从 2 起、禁 1；
`continuous_batching` → 剔除 `workers_per_device`）。

**机制**：每个 config 点——原子重写 config.yaml（ruamel round-trip，
注释保留；pyyaml 校验网 fail-closed；tmp + os.replace）→ Admin
ReloadModel → 轮询 Ready（+ ACTIVE_WORKERS == 期望值）→ 内层
concurrency 扫描（零 reload）→ 下一点。全部结束后原始 config.yaml
**逐字节**恢复并把服务器 reload 回基线；恢复失败即 profile 失败
（exit 2）。SIGINT → best-effort 恢复。残留 `.profile.backup`
（SIGKILL 遗留）会阻塞运行，直至 `--recover` 或手动清理。

**退出码**：`0` 有推荐 · `1` 无满足约束的 trial · `2` 失败（预检拒绝、
网格冲突、恢复失败、熔断、campaign 不匹配）。

---

### `pack` — 打包模型为制品

```bash
lite-server pack <模型目录> --version <版本> [选项]
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `model_dir` | string | （位置参数） | 要打包的模型目录 |
| `--version`, `-v` | string | （必填） | 版本号 |
| `--name`, `-n` | string | （自动推断） | 模型名称（默认从目录名推断） |
| `--output`, `-o` | string | ./artifacts | 输出目录 |

```bash
lite-server pack model_repo/my_model/1 --version 1 --output ./artifacts
```

---

### `unpack` — 解包制品

```bash
lite-server unpack <制品文件> [选项]
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `artifact` | string | （位置参数） | .lma 制品文件路径 |
| `--to` | string | . | 目标目录 |
| `--flat` | flag | false | 直接解压文件，不创建模型名子目录 |

---

### `init` — 初始化项目

```bash
lite-server init [项目名] [选项]
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `project_name` | string | （位置参数） | 项目目录名（`--model-only` 时为模型名） |
| `--wizard`, `-w` | flag | false | 交互式向导模式 |
| `--model-only` | flag | false | 只生成 `model_repo/<name>/1/`（model.py、callbacks.py、config.yaml、config.yaml.example）— 不生成项目外壳；目录已存在时报错 |

```bash
# 创建新项目
lite-server init my-server

# 交互式向导
lite-server init --wizard

# 给已有项目添加模型（不生成项目外壳）
lite-server init --model-only my_model
# -> 生成 model_repo/my_model/1/ — 通过 orchestration.load_models 加载
```

---

## 配置优先级

参数按以下顺序解析（优先级从高到低）：

1. **CLI 参数** — 最高优先级
2. **YAML 配置文件**（`--config`）
3. **内置默认值**

模型配置优先级：

1. **CLI 模型默认值**（`--max-queue-size`、`--max-requests` 等）
2. **模型 `config.yaml`**（`model_repo/<name>/<version>/config.yaml`）
3. **内置默认值**

## 环境变量

| 变量 | 说明 |
|------|------|
| `RUST_LOG` | Rust tracing 过滤器（高级用法） |
