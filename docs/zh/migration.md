# 迁移指南

[English](../migration.md)

本 major 版本**彻底 breaking**（D30）：不设 deprecated 兼容开关、不留跨版本迁移
窗口。本指南是逐条「旧配置 → 新配置」对照。服务启动时会运行**配置预检**，检测到
旧配置形态即打 `warn` 日志并点名下列 M 条目（`config-check` 子命令同样输出）。
回滚 = 回退到上一 tag（proto additive-only，wire 双向兼容）。

| 条目 | 阶段 | breaking |
|---|---|---|
| M1 | P-XFF | 默认不再信任客户端 `X-Forwarded-For` / `X-Real-IP` |
| M2 | P-CORS | per-model CORS ACAO 单值化；预检严格校验 |
| M3 | P5-2 | `x-lite-version` 头默认被忽略 |
| M4 | P7-1 | 未配置 access_control 时 admin 仅 loopback 可达 |
| M5 | P-TRACE | tonic 0.13 升级；OTel 需 `telemetry` cargo feature |
| M6 | P-TRACE | `telemetry.protocol: http` 启动 fail-fast；入站 baggage 默认丢弃 |
| M7 | P-WARM | `policies.warmup.dummy_input_ref`/`iterations` 已移除 → `samples` 列表（配置加载失败） |
| M8 | 0.8.0 | `features.*` 开关现已生效；移除 3 个预留字段；`custom_metrics` 改为显式开启 |
| M9 | 0.8.0 | Callback 生命周期钩子改名（`on_*` → 位置化 `before_*`/`after_*`）；鸭子类型 `pre_setup` 已删除 |
| M10 | 0.8.3 | WebSocket Binary 首帧改为原始字节（原为 lossy UTF-8 解码当 JSON）——JSON 请用 Text 帧发送 |

无 breaking 的阶段（无需动作）：P-MW、P-ENSEMBLE-GRPC、P-FLOW、P-DEADLINE、
P-WARM（纯新增，默认值保持旧行为）。P-OAI 延后至 0.9（不在本版本范围内）。

> **语义说明（P-DEADLINE）**：请求预算耗尽返回 **HTTP `504`（而非 `408`）** /
> gRPC `DEADLINE_EXCEEDED`。`408` 意为"客户端发送太慢"，与实况不符——请求
> 已完整到达，是服务端预算（客户端经 `x-lite-timeout` / `grpc-timeout` 指定，
> 或 `server.timeout` 兜底）在等待 worker 时耗尽。既有 `InferenceTimeout → 504`
> 映射有意保留，避免静默打破已在 504 上告警的客户端。邻近区分：排队超时
> REJECT → `503` / `Unavailable`；限流 → `429` / `RESOURCE_EXHAUSTED`。
> 详见 architecture.md「Deadline 传播与超时状态码」。

## M1 — 不再信任 XFF/X-Real-IP（P-XFF）

**变化**：`client_ip` 改为取直连 peer 地址；仅当 peer 属于
`server.trusted_proxies` 时才采信 `X-Forwarded-For` / `X-Real-IP`。影响 ip 维度
限流、访问日志及一切按客户端 IP 的风控。

**迁移**：

```yaml
# 旧（隐式信任客户端/网关 XFF）
server:
  trusted_proxies: []        # 默认 / 缺省

# 新（前置网关/LB 部署必须声明）
server:
  trusted_proxies: ["10.0.0.0/8", "192.168.0.0/16"]   # 代理的 CIDR 或裸 IP
```

坏条目启动 fail-fast。当某模型配置 `rate_limit: { key: ip }` 且
`trusted_proxies` 为空时，预检告警（M1）——这正是「依赖 XFF」的旧形态特征。

## M2 — CORS ACAO 单值化 + 预检严格校验（P-CORS）

**变化**：

1. 多 `allow_origins` 旧版被 join 成单个（非法的）`Access-Control-Allow-Origin`
   值；新版按请求 `Origin` 精确匹配（或 `*.host` 子域通配）并回写命中的单个
   origin。
2. 预检（`OPTIONS` + `Access-Control-Request-Method`）仅当 Origin 命中**且**
   请求的 method/headers 全在 `allow_methods` / `allow_headers` 清单内才附
   CORS 头；否则裸 `204`。
3. 策略生效的所有响应始终附 `Vary: Origin`（含无 `Origin` 头的请求）。
4. 配置了 CORS 时 WebSocket 握手校验 `Origin`（防 CSWSH）；未配置 CORS 的 WS
   仍由 `access_control` 把守。
5. 新增全局 `server.cors`；模型路由上 per-model `policies.cors` 优先。
   `allow_credentials: true` + 通配 `*` 被拒绝。

**迁移**：

```yaml
# 旧（多 origin 被 join 成一个坏 ACAO 值）
policies:
  cors:
    allow_origins: ["https://a.example", "https://b.example"]

# 新——YAML 形态不变、语义修正：逐请求 Origin 精确匹配清单。配置本身无需改，
# 但请确认浏览器客户端发出的 Origin 与清单条目逐字符一致（scheme + host + 端口）。
```

预检对任意多 origin CORS 配置告警（M2），提示运维重新确认意图。旧版靠非法
join「侥幸可用」的浏览器端，新版会被正确地拒绝——请把每个合法 origin 显式列出。

## M3 — `x-lite-version` 默认被忽略（P5-2）

**变化**：`x-lite-version` 请求头（canary pin 覆盖）默认不再生效。

**迁移**：

```yaml
# 仅灰度/调试环境——恢复该头的效果
features:
  canary_override: true
```

生产环境应保持 `false`（客户端无法再自行 pin 到 canary 版本）。

## M4 — 未配置 access_control 时 admin 仅 loopback（P7-1）

**变化**：未配置 `access_control` 时，admin 端点（HTTP `/admin/*`、gRPC Admin
服务）**仅 loopback 可达**（UDS 视为 loopback）。旧版绑定非 loopback 地址即
隐式开放 admin（「绑定即开放」）。

**迁移（三选一）**：

```yaml
# a) 给 admin 配 key 访问控制（双协议或单协议）
access_control:
  admin:
    http: { mode: key, key: x-admin-key, value_env: ADMIN_KEY }
    grpc: { mode: key, key: x-admin-key, value_env: ADMIN_KEY }

# b) admin 保持本机专用，单独绑定（UDS = 仅本机）
grpc:
  admin_bind: unix:/var/run/lite-admin.sock
```

Prometheus 抓取改用独立 `metrics_port`（不受 admin 控制影响）。绑定非
loopback 且 admin 未配置时，预检告警（M4）。

## M5 — tonic 0.13 / telemetry cargo feature（P-TRACE）

**变化**：gRPC 全家桶升级到 tonic 0.13（wire 兼容——既有客户端含 Python
`liteserver_pb2` 无需改动）。OpenTelemetry 导出现在需要**编译期 cargo
feature** 叠加运行时开关：

```bash
cargo build --features telemetry        # 二进制须带 feature 编译
```

```yaml
telemetry:
  enabled: true                          # 运行时开关（默认 false，零开销）
  otlp_endpoint: http://collector:4317
```

未带 feature 编译的二进制按设计忽略 `telemetry.*`。

## M6 — telemetry protocol / 入站 baggage（P-TRACE 加固）

**变化**：

1. `telemetry.protocol: http` 为**预留**（本期仅 OTLP/gRPC）。现在启动校验
   fail-fast，取代旧版「warn 一条后 telemetry 整体静默关闭」。
2. 入站 W3C `baggage` **不受信**，默认丢弃——不再流入 worker 请求头。只透传
   你明确信任的键：

```yaml
telemetry:
  baggage_allowlist: ["tenant", "experiment"]   # 默认 [] = 入站 baggage 全丢弃
  baggage_max_entries: 16                        # 保留条目数上限
  baggage_max_entry_bytes: 128                   # 单条目 key+value 字节上限
```

3. `health_admin_sample_ratio` 现已生效：health/admin 端点 span 按此独立比率
   采样（默认 `0.0`），探活不再刷 collector 配额；其余端点用 `sample_ratio`。

## M7 — warmup 单样本字段移除（P-WARM）

**变化：** `policies.warmup.dummy_input_ref` 与 `policies.warmup.iterations`
已移除，由 `samples` 列表取代（多形状预热，Triton ModelWarmup 范式）。使用旧
键的 config.yaml **加载直接失败**并点名本条目——绝不静默跳过预热（静默跳过
= 悄悄放回首请求尖峰）。

**迁移：**

```yaml
# 旧
policies:
  warmup:
    enabled: true
    iterations: 2
    dummy_input_ref: warmup/input.json

# 新——每样本一个文件，覆盖一种输入形状/batch，按序消费
policies:
  warmup:
    enabled: true
    samples:
      - input_ref: warmup/input.json
        iterations: 2
      - input_ref: warmup/batch8.json   # iterations 缺省 1
```

## M8 — feature 开关生效；移除预留字段（0.8.0）

**变更内容：** `features.*` 开关此前只是声明但不生效（"预留"）。现在它们真实门控行为：

- `system_overview`、`benchmarks`、`playground` 已从 schema **移除**。它们从未生效；旧的 `server.yaml` 里保留这些键无害（未知键被忽略），但生成配置不再包含。
- `timeline` / `alerts` / `version_compare` 现在门控各自的 HTTP 路由：关闭时路由卸载（404），timeline 后台采样任务也变 no-op。
- `streaming` 是 SSE + WebSocket 路由总开关；`sse` 与 `websocket_streaming` 各自门控自己的传输。关闭 `streaming` 会真正卸载路由（此前始终挂载）。
- `grpc_streaming` 门控三个流式 RPC（`stream_infer`、`decoupled_infer`、`bidi_stream`）；关闭时在 admission 之前返回 `UNIMPLEMENTED`。`batch_infer`（unary）不受影响。
- `custom_metrics` 现为**显式开启**（默认 `false`）：仅当为 `true` 时才注册 worker 声明的自定义指标。此前总是注册；若你的 worker 声明了指标，请设置 `custom_metrics: true`。

**迁移：** 除非你依赖自定义指标（设 `features.custom_metrics: true`），或依赖某个流式路由在开关为 `false` 时仍被挂载（把对应开关设为 `true`），否则无需动作。

## M9 — Callback 生命周期钩子改名；`pre_setup` 已删除（0.8.0）

**变更内容：** 三个 Callback 生命周期钩子改为位置化 `before_*`/`after_*` 命名，并新增第四个：

| 旧（0.7） | 新（0.8） | 触发位置 |
|---|---|---|
| `on_before_setup(config, device)` | `before_setup(config, device)` | `LitAPI.setup()` 之前 |
| `on_after_setup(lit_api)` | `after_setup(lit_api)` | `setup()` 成功之后 |
| `on_teardown(lit_api)` | `before_teardown(lit_api)` | `LitAPI.teardown()` 之前 |
| — | `after_teardown(lit_api)` | `teardown()` 成功之后（新增；teardown 抛异常时不触发） |

命名规则就此统一：阶段包装器用 `before_X`/`after_X`，事件处理器保留 `on_*`
（`on_error`、`on_stream_close`）。仍定义旧名的回调会在**加载时报
`RuntimeError`**，错误信息给出确切的新名字——不会静默失效。

两项相关变更：

- `LitAPI` 上未文档化的鸭子类型 `pre_setup()` 调用被**静默删除**（无加载时报错）：
  定义了 `pre_setup` 的模型升级后它根本不会被调用（其位置与受支持的
  `before_setup` 重叠）。请把 `pre_setup` 逻辑迁入 `setup()` 或 `before_setup` 回调。
- `LitAPI.teardown()` 与 `before_teardown`/`after_teardown` 回调现在在生产中
  **真正执行**（此前 worker 一律被 SIGKILL,Python 侧从无机会运行）：卸载/重载/
  驱逐/优雅关闭时 worker 会收到 stop 消息，并有 `worker_kill_timeout` 时长完成
  teardown，超时才回退 SIGKILL。完整触发矩阵见 model-authoring.md 的
  `teardown()` 章节。

**迁移：** 按上表改名三个钩子；把 `pre_setup` 逻辑迁入 `setup()` 或
`before_setup` 回调。

## M10 — WebSocket Binary 首帧改为原始字节（0.8.3）

**变更内容：** 在 WS 流式端点（`/stream`、`/decoupled-stream` 及其版本化
变体）上，首帧的解释方式改为由帧*类型*单独决定。0.8.2 中 Binary 首帧会
被 lossy UTF-8 解码并要求内容是 JSON 文本；现在按不透明字节处理、原样
转发给 worker,worker 看到的 Content-Type 在升级请求缺失或携带 JSON 值
时归一化为 `application/octet-stream`（非 JSON 值如 `image/png` 保留为
payload 元数据）。

**迁移：** 如果客户端把 JSON payload 放在 **Binary** WS 帧里发送，请改用
Text 帧（浏览器用 `ws.send(JSON.stringify(payload))`,tungstenite 用
`Message::Text(...)`)。使用 Text 帧的客户端不受影响。

0.8.3 的相关新增（非破坏）:h2 bidi 在边界校验 `open.initial_data` 的
JSON（400 取代 worker 侧报错——原始字节类 content type 跳过校验）;
ensemble 模型接受原始字节根输入（见[原始字节 / Tensor 请求](raw-bytes-request.md)
的 *Ensemble 模型* 一节）。

