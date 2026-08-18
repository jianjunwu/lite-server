# 配置参考

[English](../configuration.md)

lite-server 采用三层配置：**服务器配置**（YAML 文件或 CLI）、**模型配置**（每模型 `config.yaml`）和**编排配置**（`server.yaml` 中的 `orchestration` 段落）。CLI 参数覆盖 YAML 值。

> **从旧版本升级？** breaking 变更与旧→新对照见[迁移指南](migration.md)。

## 服务器配置（`server.yaml`）

路径：`server.yaml`（通过 `--config` 或 `-c` 传入）

```yaml
server:
  http_port: 8000              # HTTP 服务端口
  grpc_port: 8001              # gRPC 服务端口
  metrics_port: 8002           # Prometheus 指标端口
  host: 0.0.0.0                # 绑定地址（支持 unix:/path/to/sock 使用 UDS）
  timeout: 30.0                # 全局请求超时（秒）
  threads: null                # Tokio 工作线程数（null = 自动 = CPU 核数）
  cache_registry: false        # 停机时把注册表（策略 + 激活版本 pin）快照到
                               # <repo>/.lite-server-registry.json，启动时恢复。
                               # 容忍损坏文件；删除该文件即重置。
  graceful_timeout: 30.0       # 优雅关闭时等待进行中请求的最大秒数
  keepalive_timeout: 5.0       # HTTP keep-alive 超时（秒）；空闲连接超过该窗口被回收
                               #（h1 空闲回收 + slowloris 头防护）。0 = 完全禁用 keep-alive
                               #（强制 h1-only；TLS 下撤销 h2 ALPN——h2 无 close 语义）
  stream_keepalive_interval_secs: 30.0  # 流上服务端活性帧间隔（WS Ping / SSE `: keepalive`
                               # 注释）；0 = 关闭。用于检出静默流上的死对端、保活 NAT/LB 状态
  stream_channel_size: 64      # 每流 chunk 通道深度（worker→服务端、SSE、gRPC）；消费滞后
                               # 超过该深度即截断流。突发容忍可调大（内存 ≈ 深度×chunk×并发流）
  request_body_timeout_secs: 0.0  # 请求体读取空闲超时（slowloris 体防护）；有字节流动即
                               # 重置，大上传不受影响。0 = 关闭。h2 /bidi 请求体豁免
  http2_keepalive_interval_secs: null  # HTTP h2 PING 间隔（仅死对端检测）；null = 关闭。
                               # 与 grpc.http2_keepalive_* 不同面
  http2_keepalive_timeout_secs: null   # h2 PING ACK 超时（须先设间隔）
  max_connections: 0           # HTTP 连接数硬顶（TCP+TLS）；超限连接在 accept 即关闭。
                               # 0 = 不限（默认）
  compression: false           # gzip HTTP 响应；排除 SSE，不影响 WS
  request_decompression: false # gzip 请求体在进 handler 前解码（覆盖除 h2 /bidi 外的
                               # 全部 HTTP 路由，/bidi 维持 415）。解压后字节计入
                               # max_request_body_bytes（zip-bomb 防护）。仅支持 gzip，
                               # 其他编码 → 415。
  socket_mode: 0o666           # unix: UDS host 的 chmod。HTTP UDS 同时服务 admin，
                               # 多租户主机建议 0o600（仅 owner）
  # TLS/mTLS（见下文「TLS / mTLS」一节）——均可选，默认关闭
  tls_cert_path: null          # 服务器证书链 PEM；须与 tls_key_path 同设
  tls_key_path: null           # 服务器私钥 PEM；须与 tls_cert_path 同设
  mtls_ca_path: null           # 客户端 CA  bundle PEM；设置后强制客户端证书（mTLS）
  tls_min_version: null        # "1.2"（默认）或 "1.3"
  # sequence_id 粘性路由——按请求经 x-sequence-id / gRPC sequence_id 字段
  # 显式开启；缺省时调度与现状完全一致。
  sequence_ttl_secs: 3600.0    # sequence_id→worker 映射在末次使用后保留的秒数
  max_sequences: 65536         # 追踪的 sequence_id 条目上限（近似 LRU）
  balance_abs_threshold: 2     # 粘性 worker 在途数超过最少负载 worker 多少即回退
                               # （SGLang --balance-abs-threshold 语义；0 = 关闭）
  balance_rel_threshold: 1.5   # 相对阈值（…× 倍数；0.0 = 关闭）
  decoupled_idle_timeout_secs: 300.0  # DecoupledInfer 流的 idle 超时（秒）——窗口内无
                               # chunk 到达 → 服务端关闭并取消 worker。0 = 关闭
                               # （流存活至模型 close / 客户端取消）
  # 过载保护——全部默认关闭，行为不变
  max_inflight: 0             # 全局在途推理上限。>0 → 超过此并发数的推理请求被拒（503 /
                               # gRPC Unavailable + Retry-After）。health/admin 端点豁免（探活
                               # 不能挂）。0 = 无限。
  max_request_body_bytes: 67108864 # 单请求体上限（字节）。超限 → HTTP 413 / gRPC
                               # ResourceExhausted。默认 64 MiB；null = 平台默认
                               #（axum 2MB / tonic 4MB）。内存预算：该值 × 在途请求数。
                               # 仅约束推理请求体——制品上传不受此上限约束。
  max_upload_bytes: null       # 模型仓库「上传」上限（字节）：单次上传请求总字节数，
                               # 流式中实时计数（HTTP multipart）/ 逐消息累计（gRPC UploadModel）。
                               # 超限 → HTTP 413 / gRPC ResourceExhausted。
                               # null（默认）= 无上限——制品合法地可达 GB 级。
  max_concurrent_streaming_dags: 128  # 并发「流式 ensemble DAG」全局上限。流式 step 直连不过队列
                               #（无背压），该信号量即内存上界：最坏驻留 ≈ 该值 × 64 × 单 chunk 大小。
                               # 超限立即拒绝（HTTP 429 / gRPC ResourceExhausted，不排队）。
                               # 0 = 无上限。
  # 受信代理 client-IP 清洗——默认 fail-safe。
  trusted_proxies: []          # 前置代理的 CIDR/IP，其 X-Forwarded-For / X-Real-IP 才被信任。
                               # 空（默认）= 一律用直连 TCP peer、忽略客户端代理头（防伪造 IP 绕过
                               # key=ip 限流）。网关/代理需在此列出，其转发的客户端 IP 才能参与限流。
  # 全局 CORS 策略（无 per-model policies.cors 覆盖时生效，且覆盖非模型路由）。
  # null（默认）= CORS 直通（不附任何头）。字段同 per-model policies.cors。
  cors: null

logging:
  level: info                  # 日志级别：trace, debug, info, warn, error
  info_output: null            # info 级别日志的独立文件
  error_output: null           # error 级别日志的独立文件
  rotation: none               # none, size, daily, hourly
  max_size: 100                # 最大日志文件大小（MB），rotation=size 时生效
  backup_count: 7              # 保留的轮转日志文件数
  hostname_in_log_name: false  # 注入系统主机名到文件名:server.log -> server-<host>.log

grpc:
  enabled: true                # 启用 gRPC 服务
  host: null                   # gRPC 绑定地址；null = 跟随 server.host（"unix:/路径" = UDS）
  # LiteAdmin 服务独立绑定。推荐 UDS——UDS admin socket 默认属主独占（0o600）创建
  admin_bind: null             # 如 unix:/var/run/lite-admin.sock 或 127.0.0.1:9001
  http2_keepalive_interval_secs: null  # HTTP/2 PING 间隔；null = 关闭
  http2_keepalive_timeout_secs: null   # PING ACK 超时（须先设间隔）
  http2_adaptive_window: false         # BDP 自适应 HTTP/2 流控窗口
  http2_max_frame_size: null           # HTTP/2 帧载荷上限（字节）；null = tonic 默认
  response_compression: false          # gzip gRPC 响应；仅推理服务
  reflection: false                    # gRPC server reflection（opt-in）：grpcurl/grpcui 服务发现；挂 Admin 访问类（未配置 access_control admin 时 fail-closed 仅 loopback）
  socket_mode: 0o666                   # unix: gRPC UDS 的 chmod
  # TLS/mTLS——与 server.* 的 TLS 键语义相同，作用于 gRPC 监听器
  tls_cert_path: null          # 服务器证书链 PEM；须与 tls_key_path 同设
  tls_key_path: null           # 服务器私钥 PEM；须与 tls_cert_path 同设
  mtls_ca_path: null           # 客户端 CA bundle PEM；设置后强制客户端证书（mTLS）
  tls_min_version: null        # "1.2"（默认）或 "1.3"

metrics:
  enabled: true                # 运行独立 Prometheus 监听器（server.metrics_port，明文——见下文 TLS 注记）。
                               # 作用域注意：主端口 /metrics 路由恒挂载（Admin 端点类），不受此开关影响。
  # GIE/EPP 兼容指标命名空间：在 /metrics 暴露
  # {namespace}:total_queued_requests / {namespace}:kv_cache_utilization
  # （vllm 兼容命名，对接 K8s LLM 自动扩缩生态）。非法命名空间启动 fail-fast。
  metric_namespace: liteserver
  # /metrics/timeline 窗口（每 model/version 环形缓冲）
  timeline_max_points: 30      # 每条序列保留的数据点数（点数 × 间隔 = 历史深度）
  timeline_sample_interval_secs: 10  # 采样间隔，即 /metrics/timeline 分辨率
  # /metrics/timeline p99 滑窗（每 model/version 延迟样本）
  p99_window_max_samples: 1000 # 样本数上限；高 QPS 版本很快触顶
  p99_window_max_age_secs: 0   # 样本年龄界（秒）；0 = 关闭（仅按数量界）。
                               # 低 QPS 部署建议设置，避免 p99 跨小时陈旧。

alerts:
  # /alerts 求值阈值（开关仍是 features.alerts）。
  # 图示为默认值；引擎按 GET /alerts 请求时求值。
  queue_depth_warning: 100
  queue_depth_critical: 500
  p99_ms_warning: 500
  p99_ms_critical: 2000

rate_limit:
  max_buckets: 65536           # 限流桶数量上限（按 IP/路由 key），
                               # 防 IP 伪造洪泛导致内存无限增长。
                               # 0 = 无限制。
                               # 进程内 per-instance：N 副本时实际限额 =
                               # N×配置值；全局限流由上游网关负责。

model_repository:
  path: ./model_repo           # 模型仓库目录

features:
  # Breaking（迁移 M3）：是否响应 x-lite-version canary pin 请求头。
  # 默认 false = 该头被忽略（客户端无法自行 pin 到 canary 版本）。仅灰度/调试环境开启
  canary_override: false
  timeline: false              # 挂载 /metrics/timeline* 并运行后台采样任务
  custom_metrics: false        # 注册 worker 声明的自定义指标（需显式开启）
  alerts: true                 # 挂载 /metrics/alerts
  version_compare: false       # 挂载 /v2/models/:model_name/compare
  streaming: true              # SSE + WebSocket 路由总开关
  grpc_streaming: true         # stream_infer / decoupled_infer / bidi_stream RPC（关闭时返回 Unimplemented）
  sse: true                    # SSE 路由（另需 streaming: true）
  websocket_streaming: true    # WebSocket 路由（另需 streaming: true）
  http_bidi: true              # h2 /bidi 端点（另需 streaming: true）
  decoupled: true             # SSE /decoupled + WS /decoupled-stream（另需 streaming + 传输开关）
  streaming_metrics: true      # 流式生命周期指标族（liteserver_stream*/streaming_*）。
                               # 门控边界与豁免指标见 observability.md「Streaming metrics」节。
                               # 与 metrics.enabled 相互独立——后者控制独立监听器，本开关控制记录哪些序列。

model_defaults:                # CLI 级别默认值，应用于所有模型
  max_queue_size: null         # 覆盖所有模型的 max_queue_size
  max_requests: null           # 覆盖所有模型的 max_requests
  max_requests_jitter: null    # 覆盖所有模型的 max_requests_jitter
  request_timeout: null        # 覆盖所有模型的 request_timeout
  health_check_interval: null  # 覆盖所有模型的 health_check_interval
  max_retries: null            # 覆盖所有模型的 max_retries
  ejection_error_threshold: null   # 覆盖所有模型的 ejection_error_threshold
  ejection_timeout: null       # 覆盖所有模型的 ejection_timeout
  ejection_max_percent: null   # 覆盖所有模型的 ejection_max_percent
  ejection_max_timeout: null   # 覆盖所有模型的 ejection_max_timeout
  startup_timeout: null        # 覆盖所有模型的 startup_timeout
  health_check_timeout: null   # 覆盖所有模型的 health_check_timeout
  health_check_kill_threshold: null  # 覆盖所有模型的 health_check_kill_threshold
  worker_kill_timeout: null    # 覆盖所有模型的 worker_kill_timeout
  hook_http_timeout: null      # 覆盖所有模型的 hook_http_timeout

tunables:                      # 服务器级旋钮（默认值即下列值，一般无需调整）
  reconcile_coalesce_secs: 2.0     # 文件事件合并窗口：一批事件合并为一次 reconcile
  hot_reload_cooldown_secs: 3.0    # 同一模型/版本两次热重载之间的冷却期
  watcher_debounce_secs: 2.5       # 文件监听器防抖窗口
  file_changed_timeout_secs: 60.0  # 单个 worker FILE_CHANGED 往返的超时
  worker_stderr_tail_bytes: 65536  # worker 崩溃诊断保留的最大 stderr 字节数
  worker_stderr_drain_secs: 5.0    # 等待已退出 worker 冲刷 stderr 的时限
  unpack_timeout_secs: 120.0       # 单次 .lma unpack 命令的上限（防挂起阻塞 reconcile）
```

## TLS / mTLS

两个监听器（HTTP `server.*` 与 gRPC `grpc.*`）均支持基于 rustls（纯 Rust，ring provider）的 TLS 与双向 TLS。TLS 按监听器独立启用：同设 `tls_cert_path` + `tls_key_path` 即开启。

```yaml
server:
  tls_cert_path: /etc/lite/tls/server.crt   # PEM 证书链（叶子在前）
  tls_key_path: /etc/lite/tls/server.key    # PEM 私钥（建议 chmod 600）
  mtls_ca_path: /etc/lite/tls/clients-ca.crt # 可选：强制客户端证书（mTLS）
  tls_min_version: "1.3"                     # 可选；默认 "1.2"
grpc:
  tls_cert_path: /etc/lite/tls/server.crt   # 与 HTTP 监听器相互独立
  tls_key_path: /etc/lite/tls/server.key
```

启动时强制校验的规则：

- `tls_cert_path` / `tls_key_path` 必须**成对设置**——只设一个为启动错误。
- `mtls_ca_path` 依赖该证书对（没有服务器证书的 mTLS 无意义）。
- TLS 与 **UDS（`unix:` 主机）互斥**——Unix socket 本身已有对等凭证。
- `tls_min_version` 仅接受 `"1.2"`（默认）或 `"1.3"`，建议 TLS 1.3。
- PEM 非法、私钥与证书不匹配、CA bundle 为空均为启动错误。

**证书热轮换（免重启）。** 服务器监视 PEM 文件（10 秒内容轮询 + Unix 下 `SIGHUP` 即时触发）：文件变更后——如 cert-manager/Let's Encrypt 续期或 k8s secret 卷符号链接交换——新连接立即使用新证书，已建立连接不受影响。轮换失败（文件损坏、轮换中途 cert/key 不匹配）保留旧证书继续服务并记录错误，下一轮轮询自动重试。mTLS CA bundle 同样支持热轮换。

**ALPN。** gRPC 监听器仅通告 `h2`；HTTP 监听器通告 `h2` 与 `http/1.1`，探活与简单 HTTPS 客户端不受影响。

**mTLS 客户端身份。** 已通过验证的客户端证书 principal（URI SAN → DNS SAN → Subject DN → SHA-256 指纹）写入请求上下文，供访问日志/审计使用。访问控制本期尚不消费该身份——按模型的 API key 鉴权（`policies.auth`）独立配置。

**既定策略说明：**

- **不做 CRL/OCSP 吊销检查**——吊销检查超出范围；剔除被入侵客户端请轮换 CA bundle。
- `metrics_port` 监听器保持**明文且无鉴权**——请绑定内网或 loopback。启用 TLS 后，主端口的 Prometheus 抓取/探活/内部客户端须走 HTTPS（ALPN 含 `http/1.1`，简单客户端可用）。
- 私钥文件为组/全员可读时启动仅告警（建议 `chmod 600`），不阻断基于用户组的部署。

## 访问控制

端点类别：`admin`（HTTP `/admin/*` + gRPC LiteAdmin 服务）、`inference`、`health`。`admin` / `inference` 按协议（`http` / `grpc`）分别配置；`health` 为单条简写、双协议同生效。

- **默认（admin fail-closed）**：未配置 `admin` → **仅 loopback 可达**（UDS 视为 loopback）；未配置 `inference` / `health` → 公开。breaking 变更——见[迁移指南](migration.md) M4。
- **模式**（`mode` 标签）：`public`（显式开放——逃生门）或 `key`（API key：`key` = header 名；密钥取 `value` / `value_env` / `value_file` 首个存在者，启动期解析，缺源 fail-fast）。
- key 比较为恒定时间。拒绝返回 HTTP 401 / gRPC Unauthenticated。`metrics_port` 监听器不受此约束——Prometheus 抓取走该端口。
- **`key` 模式务必与 TLS 联合启用**（P5-1）——否则 API key 明文传输，可被链路直接截获。
- **key 轮换**：密钥在启动期一次性解析。轮换=更新密钥来源（`value_env` / `value_file`）后滚动重启；建议用密钥来源而非内联 `value`——轮换只动密钥库，不动配置文件。

```yaml
access_control:
  admin:
    http: { mode: key, key: x-admin-key, value_env: ADMIN_KEY }
    grpc: { mode: key, key: x-admin-key, value_env: ADMIN_KEY }
  inference:
    http: { mode: public }       # 显式声明——与默认相同
  health: { mode: public }       # 简写，http + grpc 同生效
```

per-model `policies.auth` 独立于此端点级控制，叠加在其后。

### OpenAI-Compact（`/v1`）专属鉴权

`openai_compact.auth` 门（openai-compact 协议，阶段 6）**只锁 /v1 5 个端点**（`/v1/chat/completions`、`/v1/completions`、`/v1/embeddings`、`/v1/models`、`/v1/models/{model}`）。KServe `/v2`、gRPC、自定义路由、admin 均不受影响；**无 loopback 豁免**（配了之后每个 /v1 请求都必须带 key）。

- 与 `access_control` 相同的 `mode` 标签与 secret 来源（`value` / `value_env` / `value_file`，首个生效，启动期解析——缺源 fail-fast）。单 key，轮换改 secret 源 + 滚动重启。
- header 为默认的 `authorization` 时，`Authorization: Bearer <key>`（RFC 6750，官方 `openai` SDK 只发这种形式）与裸值均接受，常量时间比对；自定义 header（如 `x-api-key`）仅全值比对。
- 拒绝：401 + OpenAI 形状错误体（`{"error": {message, type, param, code}}`）。未配置 → 现状公开，零行为变化。
- 与 `access_control`、per-model `policies.auth` 相互独立，同时配置时叠加。

```yaml
openai_compact:
  auth:
    mode: key
    key: authorization            # OpenAI 标准:Authorization: Bearer <key>
    value_env: OPENAI_API_KEY     # 或 value: "sk-..." / value_file: <路径>
```

### 控制面审计（D27）

所有控制面**变更**操作（HTTP admin 与 gRPC Admin，双侧同形状）都输出一条结构化审计记录；只读端点（`ListModels`/`GetInfo`/健康探活）不审计。字段：`action`（`load`/`unload`/`reload`/`delete`/`activate`/`set_routing`）、`model`/`version`、`request_id`、`client_ip`、`principal`（mTLS）、`key_fingerprint`、`details`（含前后值，如 `weights {"1": 70} -> {"2": 100}`；`activate` 失败也留痕）。

`key_fingerprint` = 配置 key 的 SHA-256 前 6 字节 hex（12 字符）——日志可区分轮换前后的 key，不落密钥明文；public / loopback / 未配置为 `None`。

记录走独立 log target `lite_server::audit`，`info` 级别即出，无需额外配置。EnvFilter 用**下划线**形式：

```sh
RUST_LOG=lite_server::audit=info
```

完整字段表见 [observability.md](../../observability.md)（en，无 zh 镜像）。

## CORS

通过 `server.cors` 全局配置，或通过 `policies.cors` 按模型配置（per-model 覆盖全局策略；省略则回退到 `server.cors`；`null` 默认为透传——不附加任何头）。CORS **不是** `tower-http::cors`：per-model 策略覆盖需要在请求时按路径解析模型，静态挂载的 `CorsLayer` 做不到。中间件按有效策略（per-model → 全局）应用下述规则。

```yaml
server:
  cors:
    allow_origins: ["https://example.com"]  # 精确匹配；"*" = 任意；"*.example.com" = 子域通配
    allow_methods: ["GET", "POST"]
    allow_headers: ["Content-Type", "Authorization"]
    expose_headers: ["x-request-id", "x-processing-time-ms"]  # JS 可见的响应头
    allow_credentials: false     # true → ACAC: true；与 "*" 互斥
    max_age_secs: 7200           # preflight 缓存（秒）；Chrome 上限 7200
```

强制 8 条安全属性：

1. **精确 Origin 匹配** — `Origin` 经规范化（scheme/host 小写、默认端口剥离）后与配置的 `allow_origins` 精确匹配。无模糊匹配。
2. **不回显** — `Access-Control-Allow-Origin` 永不为请求原始 `Origin` 的 echo。仅当 (a) 请求匹配到某个已配置 origin，或 (b) 字面量 `*` 时设置；未配置的 origin 得不到**任何** ACAO。
3. **拒绝 `null`** — `Origin: null`（沙箱 iframe、`file://`、data URI）视为无 origin——不附加任何 CORS 头。
4. **无后缀混淆** — `https://evil-example.com` 不匹配 `https://example.com`，`https://a.notexample.com` 不匹配 `https://*.example.com`。子域通配（`*.example.com`）要求前置标签（`a.example.com`），永不含顶域（`example.com`）。
5. **Credentials + `*` 拒绝** — `allow_credentials: true` 时不回显通配 `*`——不发 ACAO（浏览器禁止 `Access-Control-Allow-Origin: *` 与 `Access-Control-Allow-Credentials: true` 共存）。请配置显式 origins。
6. **`Vary: Origin` 恒有** — 所有 CORS 相关响应携带 `Vary: Origin`（preflight 额外携带 `Vary: Access-Control-Request-Method` / `-Headers`），共享缓存不会把某个 Origin 的响应发给另一个 Origin。
7. **Preflight 校验方法与请求头** — preflight（`OPTIONS` + `Access-Control-Request-Method`）仅当 Origin 被允许**且**请求的方法（`ACRM`）与全部请求头（`ACRH`）都在 `allow_methods` / `allow_headers` 清单内时才附加 CORS 头。不合格的 preflight 返回 204、**无** CORS 头。
8. **`max_age` ≤ 7200** — `max_age_secs` 默认 7200——Chrome 的 preflight 缓存上限。超出值反正会被浏览器截断；配置 ≤ 7200。

**分层** — CORS 中间件挂载在访问控制**外侧**：preflight `OPTIONS` 在鉴权之前以 204 短路（preflight 不带凭证）。位于 observability 内侧，故 204 携带 `x-request-id`。

**WebSocket** — 浏览器对 WS 握手不发 preflight 也不强制 ACAO，因此 CORS 中间件无法阻止跨站 WebSocket 劫持（CSWSH）。WS upgrade handler 用同一引擎独立校验 `Origin`（`ws_origin_allowed`）。未配置 CORS 策略时，WS 安全完全依赖访问控制 key 鉴权。

**Admin 端点** — admin 类端点非浏览器面向；CORS 中间件跳过它们（不附加 ACAO）。仅在需要跨域 admin 访问时才配置全局 `server.cors` 策略。

## 遥测 / OpenTelemetry

全量 OpenTelemetry 追踪 + metrics SDK，经 **OTLP/gRPC** 导出。两级 opt-in：编译期 cargo feature（`--features telemetry`）+ 运行时开关（`telemetry.enabled`，默认 `false`→零开销）。两者皆关 ⇒ 无 OTel layer、无 propagator、无 exporter，行为与无 OTel 逐字节一致。trace context 经既有 `RequestMeta.headers` map（W3C `traceparent`/`tracestate`/`baggage`）到达 Python worker——worker 读 header 关联但不创 span（Rust-only，见 [observability.md](../observability.md)）。

```yaml
telemetry:
  enabled: false                       # opt-in。false=无 OTel（零开销）
  otlp_endpoint: "http://localhost:4317"  # OTLP/gRPC collector（4317）
  protocol: grpc                       # 本期仅 grpc；http = 启动 fail-fast（预留，M6）
  sample_ratio: 1.0                    # ParentBased(TraceIdRatioBased(ratio))
  health_admin_sample_ratio: 0.0       # health/admin span 独立采样率（0 = 探活不采样）
  service_name: "lite-server"
  resource_attributes: {}              # 与 OTEL_RESOURCE_ATTRIBUTES env 合并
  otlp_headers: {}                     # OTLP 认证，如 {"Authorization":"Bearer ..."}
  export_interval_millis: 5000
  max_queue_size: 2048
  metrics_enabled: false               # OTel metrics SDK 叠加（C4 exemplars）
  exemplars_enabled: false             # （预留）exemplar filter，见 observability.md
  # 入站 W3C baggage 不受信（M6）：仅白名单键被保留并透传到 worker。
  # 默认 [] = 入站 baggage 全丢弃
  baggage_allowlist: []                # 如 ["tenant", "experiment"]
  baggage_max_entries: 16              # 保留条目数上限
  baggage_max_entry_bytes: 128         # 单条目 key+value 字节上限
```

- **构建**：`cargo build --features telemetry`（telemetry 测试用 `cargo test --features telemetry`）。默认构建不编译 OTel SDK/exporter。
- **采样**：root 按 `sample_ratio` 采样；子 span 遵循入站 sampled 标志。health/admin root 用独立的 `health_admin_sample_ratio`（默认 `0.0`），高频探活不刷 collector 配额。
- **Exemplars（C4）**：`metrics_enabled` 时在既有 `/metrics` 之外经 OTLP/metrics 叠加记录 `liteserver.request.duration` histogram。注意：`opentelemetry_sdk 0.30` 的 exemplar 池为占位（exemplars 空），真正挂 trace_id 的 exemplar 需 SDK 升级（已记）。Prometheus exemplar-storage + Grafana 补全 metrics→trace 链路。
- **停机**：优雅停机期间以 5s 上限 force_flush traces/metrics。

## 模型配置

路径：`model_repo/{模型名}/{版本}/config.yaml`

所有字段均可选，省略时使用默认值。

```yaml
# 动态 Batching
max_batch_size: 1              # 每批最大请求数（1 = 不批处理）
batch_timeout: 0.0             # 等待 batch 填满的最大秒数（0 = 不等待）
adaptive_batching: false       # 根据队列压力动态调整 batch 超时
min_batch_timeout: 0.001       # adaptive_batching 启用时的最小 batch 超时
adaptive_queue_threshold: 10   # 自适应 batching 的队列深度阈值

# 流式输出
stream: false                  # 启用流式输出（需要 model.py 中实现 stream_predict）

# Continuous Batching（LLM）
continuous_batching: false     # 启用 continuous batching 模式

# Worker 管理
accelerator: null              # 设备类型标签，透传到 device 字符串（如 "cuda:0"）；null = cpu
devices: null                  # 设备分配（null = 自动，或整数如 1）
workers_per_device: null       # 每设备 worker 数（null = 1）
max_queue_size: 1000           # 每个 worker 的最大待处理请求数
request_timeout: 0.0           # 单请求硬超时（秒），0 = 禁用
# 过载队列控制。每请求用 `x-lite-priority` header
# （整数；越大越先派发，默认 0）指定优先级。
queue_timeout_secs: 0.0        # 请求在队列中最长等待秒数，超过则按 queue_timeout_action
                               # 处理（0 = 禁用，默认）。
queue_timeout_action: delay    # delay（默认；交给 request_timeout 兜底）| reject
                               # （超过 queue_timeout_secs 返回 503 / gRPC Unavailable）
max_requests: 0                # worker 处理 N 个请求后自动重启（0 = 禁用）
max_requests_jitter: 0         # max_requests 的随机抖动，防止惊群效应
health_check_interval: 15.0    # 主动健康检查间隔（秒），0 = 禁用

# Worker 韧性（Resilience）
max_retries: 3                 # 失败的 batch 换其它 worker 重试次数（0 = 禁用）
ejection_error_threshold: 3    # 连续错误达到此次数后剔除 worker（0 = 禁用剔除）
ejection_timeout: 30.0         # 熔断基础退避秒数；连续熔断按 ×2 指数增长
                               #（每次退避期满进入半开：一次成功即闭合，一次失败即更久重熔断）
ejection_max_timeout: 300.0    # per-worker 熔断器退避上限（B1）
ejection_max_percent: 50       # 同时最多剔除的 worker 比例（1-100）
startup_timeout: 60.0          # 等待 worker "ready" 握手的最大秒数
health_check_timeout: 5.0      # 单次健康探测超时秒数
health_check_kill_threshold: 0 # [实验性] 连续探测失败达到此次数后杀死并重启 worker（0 = 从不）；respawn 有已知缺陷（inference-queue 客户端快照变孤儿、死 slot 卫生），0.9 修复
worker_kill_timeout: 10.0      # 优雅停止预算：卸载/关闭时 worker 收到 stop 消息后须在此秒数内
                               # 运行完 teardown() 并退出，否则被 SIGKILL；兼作杀死后等待 OS 回收的秒数

# Worker 生命周期钩子
hooks:
  on_ready: null               # worker 就绪时执行的 shell 命令
  on_exit: null                # worker 退出时执行的 shell 命令
  on_error: null               # worker 异常退出时执行的 shell 命令
  on_ready_http: null          # worker 就绪时的 HTTP 回调
  on_exit_http: null           # worker 退出时的 HTTP 回调
  on_error_http: null          # worker 异常退出时的 HTTP 回调
  hook_http_timeout: 5.0       # 生命周期 HTTP 钩子请求的超时秒数
  # HTTP 钩子格式：
  # on_ready_http:
  #   url: "http://notify.internal/worker-ready"
  #   method: POST             # GET 或 POST（默认：POST）
  #   body_template: '{"model":"$MODEL","worker":$WORKER_ID}'
  # 可用变量：$MODEL, $VERSION, $WORKER_ID, $EXIT_CODE, $REASON

# 热重载
hot_reload: false              # 启用文件监听热重载
hot_reload_patterns:           # 监听的 glob 模式
  - "*.py"

# 策略（由 Rust 服务端按模型版本执行）
policies:
  auth: { header: "X-API-Key", keys: ["${API_KEYS}"] }  # ${VAR} = 环境变量；keys 为空 = 任意非空值通过
  rate_limit: { requests_per_minute: 60, key: ip, burst: 100 }  # key: "route" | "ip"
  cors:                          # Per-model 策略（覆盖 server.cors）。省略 = 回退全局。
    allow_origins: ["https://example.com"]  # 精确匹配；"*" = 任意；"*.example.com" = 子域通配
    allow_methods: ["GET", "POST"]
    allow_headers: ["Content-Type", "Authorization"]
    expose_headers: ["x-request-id", "x-processing-time-ms"]  # 暴露给 JS 的响应头
    allow_credentials: false     # true → ACAC: true；与 "*" 互斥
    max_age_secs: 7200           # 预检缓存（秒）；Chrome 上限 7200
  request_log: {}                # 访问日志：方法、路径、状态码、耗时
  warmup:                        # 服务前预热引擎（默认关闭）
    enabled: true                #   false = 版本直接置 Ready（行为不变）
    scope: worker                #   worker（默认）= 每个 worker 进程都跑全量样本；
                                 #   version = 总量不变，轮转摊到各 worker
    respawn: true                #   respawn 后对替补 worker 重新预热（默认 true）
    samples:                     #   dummy 输入列表，按序消费——每样本一个文件覆盖一种
                                 #   输入形状/batch（M7；旧 dummy_input_ref/iterations 已移除）
      - input_ref: warmup/batch1.json   # dummy 请求体 JSON 路径，相对模型目录
        iterations: 3            #   该样本的 dummy 推理次数（默认 1）
      - input_ref: warmup/batch8.json   # 另一形状/batch（iterations 缺省 1）
    timeout_secs: 30.0           #   单次 dummy 推理预算（0 = 回退到 request_timeout；二者皆 0 = 无上限）
    total_timeout_secs: 0        #   整个预热运行的总预算（0 = 无，默认）
    concurrency: 1               #   每个 worker 组内并发的 dummy 推理数（默认 1 = 串行）
    retries: 0                   #   单样本失败重试次数，间隔 500ms（默认 0 = 快速失败）

# Callback（推理管线数据钩子）
callbacks:                     # Worker 启动时加载的 callback 类路径列表
  - my_package.callbacks.AuditLogger
```

> **关于 `hot_reload` 的作用范围**：它对**已加载**版本做 worker 重启
> （或经 `on_file_changed` 钩子进程内刷新）。`config.yaml` 自身的变更
> **绕过 `on_file_changed` 钩子**、总是按磁盘配置重启 worker
> （`max_batch_size` 等构造参数无法进程内刷新）；校验失败的 reload 在
> **unload 之前**被拒绝,旧 worker 继续服务。`control_mode: "auto"` 时，
> 版本目录的新增/删除完全由 reconcile 任务负责。在非 auto 的
> `control_mode` 下 `hot_reload: true` 自动**加载**新版本目录的旧行为已
> 于 **0.7.7 移除**——仅**目录创建事件**会以 WARN 提示新版本出现（不会
> 自动加载）；在已存在但未加载的版本目录内修改文件只记 debug 日志，
> 不打扰正常开发。加载请改用 Admin API 显式加载或切换到
> `control_mode: "auto"`。

## 编排配置

路径：`server.yaml`（`orchestration` 段落）

控制启动时加载哪些模型和版本。

```yaml
control_mode: explicit         # explicit（显式）、auto（自动同步仓库）或 all（加载仓库中所有模型）
poll_interval: 30              # 兜底重同步间隔（秒），control_mode=auto 时生效
load_models:                   # 启动时加载的模型列表
  - my_model
  - another_model
models:                        # 每个模型的版本策略
  - name: my_model
    load_policy: explicit      # explicit, latest, all
    versions_to_load:          # 要加载的版本（load_policy=explicit 时）
      - "1"
      - "2"
    default_version: "2"       # 默认激活的版本
    max_loaded_versions: null  # 最多保留的已加载版本数（null = 无限制）
    weights:                   # 金丝雀/加权流量分配（未列出的版本权重为 0）
      "1": 80
      "2": 20
```

### 加载策略

| 策略 | 行为 |
|------|------|
| `explicit` | 仅加载 `versions_to_load` 中列出的版本 |
| `latest` | 仅加载最新版本（最大版本号） |
| `all` | 加载所有可用版本 |

### auto 模式（reconcile）

`control_mode: auto` 时，后台 reconcile 任务保持注册表与模型仓库一致。
磁盘上版本目录的出现/消失会通过文件监听器近实时触发 reconcile（2 秒
合并窗口）；每隔 `poll_interval` 秒（最小 1，默认 30）做一次全量重同步
作为兜底（watch 事件在网络文件系统上可能丢失）：

- **托管集合**：`load_models` 中列出的模型。磁盘上新出现的版本目录会按
  各模型的 `load_policy` 自动加载；从磁盘删除的版本会自动卸载。
- **声明式语义**：orchestration 配置是托管模型的唯一事实源。通过 Admin
  API 对托管模型手动 load/unload 会在下一次 reconcile 被回正。不在
  `load_models` 中的模型不受影响。
- **单一决策者**：auto 模式下文件监听器不再直接加载/卸载版本——它只对
  已加载版本做 worker 重启（`hot_reload`），生命周期事件全部交给
  reconcile 任务。所有策略决策（`load_policy`、`max_loaded_versions`）
  只有这一个执行点。
- **静态配置**：orchestration 位于 `server.yaml`，启动时读取一次，修改
  后需重启生效。
- **容量保护**：超出模型 `max_loaded_versions` 的版本会跳过并告警（不会
  出现驱逐/重载循环）。
- 大仓库（>1000 模型）建议调大 `poll_interval` 以降低重同步开销。

## CLI 参数

所有服务器配置字段均可通过 CLI 参数覆盖，详见 [CLI 参考手册](cli.md)。

```bash
lite-server serve [参数]
```

| 参数 | 说明 | 覆盖 |
|------|------|------|
| `--config`, `-c` | YAML 配置文件路径 | — |
| `--port` | HTTP 端口 | `server.http_port` |
| `--host` | 绑定地址 | `server.host` |
| `--model-repo` | 模型仓库路径 | `model_repository.path` |
| `--timeout` | 全局请求超时 | `server.timeout` |
| `--log-level` | 日志级别 | `logging.level` |
| `--log-info-output` | info 日志输出文件 | `logging.info_output` |
| `--log-error-output` | error 日志输出文件 | `logging.error_output` |
| `--log-rotation` | 日志轮转策略（none/size/daily/hourly） | `logging.rotation` |
| `--no-metrics` | 禁用独立指标监听器（主端口 `/metrics` 仍挂载） | `metrics.enabled` |
| `--grpc-port` | gRPC 端口 | `server.grpc_port` |
| `--no-grpc` | 禁用 gRPC | `grpc.enabled` |
| `--no-streaming-metrics` | 禁用流式指标 | `features.streaming_metrics` |
| `--max-queue-size` | 所有模型的最大队列 | `model_defaults.max_queue_size` |
| `--max-requests` | N 个请求后自动重启 | `model_defaults.max_requests` |
| `--max-requests-jitter` | max_requests 抖动 | `model_defaults.max_requests_jitter` |
| `--request-timeout` | 单请求超时 | `model_defaults.request_timeout` |
| `--health-check-interval` | 健康检查间隔 | `model_defaults.health_check_interval` |
| `--threads` | Tokio 工作线程数 | `server.threads` |
| `--metrics-port` | 指标端口 | `server.metrics_port` |
| `--graceful-timeout` | 优雅关闭超时 | `server.graceful_timeout` |
| `--keepalive-timeout` | HTTP keep-alive 超时 | `server.keepalive_timeout` |
| `--ejection-error-threshold` | 剔除 worker 的错误次数（0=禁用） | `model_defaults.ejection_error_threshold` |
| `--ejection-timeout` | 熔断基础退避（秒） | `model_defaults.ejection_timeout` |
| `--ejection-max-timeout` | 熔断器退避上限（秒） | `model_defaults.ejection_max_timeout` |
| `--ejection-max-percent` | 最多剔除的 worker 比例 | `model_defaults.ejection_max_percent` |
| `--max-retries` | 失败 batch 换 worker 重试 | `model_defaults.max_retries` |
| `--startup-timeout` | worker ready 握手超时（秒） | `model_defaults.startup_timeout` |
| `--health-check-timeout` | 健康探测超时（秒） | `model_defaults.health_check_timeout` |
| `--health-check-kill-threshold` | 连续探测失败 N 次后杀死并重启 worker（0=禁用） | `model_defaults.health_check_kill_threshold` |
| `--worker-kill-timeout` | 优雅停止/teardown 预算，超时 SIGKILL；兼作杀死后 OS 回收等待（秒） | `model_defaults.worker_kill_timeout` |
| `--hook-http-timeout` | 生命周期 HTTP 钩子超时（秒） | `model_defaults.hook_http_timeout` |

## 优先级

参数按以下顺序解析（优先级从高到低）：

1. CLI 参数
2. YAML 配置文件（`--config`）
3. 内置默认值

模型配置优先级：

1. CLI `--max-queue-size`、`--max-requests` 等（通过 `model_defaults`）
2. 模型 `config.yaml`
3. 内置默认值

## 配置示例

### 开发环境（单模型，无配置文件）

```bash
lite-server serve --model-repo ./my_models
```

### 生产环境（多 worker，自定义端口）

```yaml
# server.yaml
server:
  http_port: 8080
  host: 0.0.0.0
  graceful_timeout: 60.0
  keepalive_timeout: 10.0

model_repository:
  path: /opt/models

features:
  alerts: true
  streaming: true

logging:
  level: info
  info_output: /var/log/lite-server/server.log
  rotation: size
  max_size: 100
  backup_count: 10
  hostname_in_log_name: false
```

### LLM 推理（流式 + continuous batching）

```yaml
# model_repo/my_model/1/config.yaml
stream: true
continuous_batching: true
max_batch_size: 4
batch_timeout: 0.05
workers_per_device: 1
request_timeout: 120.0
```

### 生产环境 + 健康检查杀进程 + 生命周期钩子

```yaml
# model_repo/my_model/1/config.yaml
max_requests: 500
max_requests_jitter: 50

# 每 10s 探测一次；连续 3 次探测失败的 worker 将被杀死并重启
health_check_interval: 10.0
health_check_timeout: 5.0
health_check_kill_threshold: 3

hooks:
  on_ready: 'echo "Worker $WORKER_ID ready for $MODEL"'
  on_error: 'curl -s -X POST http://alerts.internal/worker-error -d "{\"model\":\"$MODEL\",\"worker\":$WORKER_ID,\"reason\":\"$REASON\"}"'
  on_exit_http:
    url: "http://notify.internal/worker-exit"
    method: POST
    body_template: '{"model":"$MODEL","version":"$VERSION","worker":$WORKER_ID}'
```

## 构建期性能选项

### CPU target(`target-cpu`)

Rust release 构建默认面向基线指令集(x86-64 / 通用 aarch64)。调整
`target-cpu` 可让编译器生成更新的指令(AVX2 等),但为新 CPU 构建的
二进制**在老 CPU 上会 SIGILL 崩溃** —— 必须按部署目标选择,绝不能按
"在我机器上最快"选择。

三层策略:

1. **本地开发** —— 在机器本地的 `.cargo/config.toml`(**不要提交**)设置
   `target-cpu=native`,获得本机最佳性能。
2. **CI / 面向已知机群的发布构建** —— 按机群中最老的 CPU 代际选择:
   `x86-64-v2`(SSE4.2/POPCNT,约 2009+ 年)或 `x86-64-v3`(AVX2,约
   2013+ 年),如 `RUSTFLAGS="-C target-cpu=x86-64-v3" cargo build --release`。
3. **发布的安装包(pip wheel)** —— 不设置(保持基线)。wheel 安装在未知
   硬件上,基线是唯一安全选择。

Apple Silicon / aarch64:`target-cpu` 可设 `apple-m1` 等已知目标;机群
规则同上。
