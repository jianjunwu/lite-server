# 配置参考

[English](../configuration.md)

lite-server 采用三层配置：**服务器配置**（YAML 文件或 CLI）、**模型配置**（每模型 `config.yaml`）和**编排配置**（`server.yaml` 中的 `orchestration` 段落）。CLI 参数覆盖 YAML 值。

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
  cache_registry: false        # 缓存模型注册表到磁盘（预留 — 尚未实现）
  graceful_timeout: 30.0       # 优雅关闭时等待进行中请求的最大秒数
  keepalive_timeout: 5.0       # HTTP keep-alive 超时（秒），0 = 禁用
  # TLS/mTLS（见下文「TLS / mTLS」一节）——均可选，默认关闭
  tls_cert_path: null          # 服务器证书链 PEM；须与 tls_key_path 同设
  tls_key_path: null           # 服务器私钥 PEM；须与 tls_cert_path 同设
  mtls_ca_path: null           # 客户端 CA  bundle PEM；设置后强制客户端证书（mTLS）
  tls_min_version: null        # "1.2"（默认）或 "1.3"
  # sequence_id 粘性路由（P8-1）——按请求经 x-sequence-id / gRPC sequence_id 字段
  # 显式开启；缺省时调度与现状完全一致。
  sequence_ttl_secs: 3600.0    # sequence_id→worker 映射在末次使用后保留的秒数
  max_sequences: 65536         # 追踪的 sequence_id 条目上限（近似 LRU）
  balance_abs_threshold: 2     # B2：粘性 worker 在途数超过最少负载 worker 多少即回退
                               # （SGLang --balance-abs-threshold 语义；0 = 关闭）
  balance_rel_threshold: 1.5   # B2：相对阈值（…× 倍数；0.0 = 关闭）

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
  max_workers: 10              # gRPC 最大工作线程数（预留 — 尚未实现）
  # TLS/mTLS——与 server.* 的 TLS 键语义相同，作用于 gRPC 监听器
  tls_cert_path: null          # 服务器证书链 PEM；须与 tls_key_path 同设
  tls_key_path: null           # 服务器私钥 PEM；须与 tls_cert_path 同设
  mtls_ca_path: null           # 客户端 CA bundle PEM；设置后强制客户端证书（mTLS）
  tls_min_version: null        # "1.2"（默认）或 "1.3"

metrics:
  enabled: true                # 启用 Prometheus 指标端点

rate_limit:
  max_buckets: 65536           # 限流桶数量上限（按 IP/路由 key），
                               # 防 IP 伪造洪泛导致内存无限增长。
                               # 0 = 无限制。

model_repository:
  path: ./model_repo           # 模型仓库目录

features:
  timeline: false              # 启用历史指标时间线
  system_overview: true        # （预留 — 尚未实现）
  custom_metrics: false        # （预留 — 尚未实现）
  benchmarks: true             # （预留 — 尚未实现）
  playground: false            # （预留 — 尚未实现）
  alerts: true                 # 启用告警引擎
  version_compare: false       # （预留 — 尚未实现）
  streaming: true              # 启用流式端点
  grpc_streaming: true         # 启用 gRPC 流式
  sse: true                    # 启用 SSE 流式
  websocket_streaming: true    # 启用 WebSocket 流式
  streaming_metrics: true      # 启用流式专用指标

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
max_requests: 0                # worker 处理 N 个请求后自动重启（0 = 禁用）
max_requests_jitter: 0         # max_requests 的随机抖动，防止惊群效应
health_check_interval: 15.0    # 主动健康检查间隔（秒），0 = 禁用

# Worker 韧性（Resilience）
max_retries: 3                 # 失败的 batch 换其它 worker 重试次数（0 = 禁用）
ejection_error_threshold: 3    # 连续错误达到此次数后剔除 worker（0 = 禁用剔除）
ejection_timeout: 30.0         # 被剔除 worker 自动恢复前的等待秒数
ejection_max_percent: 50       # 同时最多剔除的 worker 比例（1-100）
startup_timeout: 60.0          # 等待 worker "ready" 握手的最大秒数
health_check_timeout: 5.0      # 单次健康探测超时秒数
health_check_kill_threshold: 0 # 连续探测失败达到此次数后杀死并重启 worker（0 = 从不）
worker_kill_timeout: 10.0      # 杀死 worker 后等待 OS 回收的秒数

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
  cors:
    allow_origins: ["https://example.com"]
    allow_methods: ["GET", "POST"]
    allow_headers: ["Content-Type", "Authorization"]
  request_log: {}                # 访问日志：方法、路径、状态码、耗时

# Callback（推理管线数据钩子）
callbacks:                     # Worker 启动时加载的 callback 类路径列表
  - my_package.callbacks.AuditLogger
```

> **关于 `hot_reload` 的作用范围**：它对**已加载**版本做 worker 重启
> （或经 `on_file_changed` 钩子进程内刷新）。`control_mode: "auto"` 时，
> 版本目录的新增/删除完全由 reconcile 任务负责。在非 auto 的
> `control_mode` 下 `hot_reload: true` 自动**加载**新版本目录的旧行为已
> 于 **0.7.7 移除**——新版本目录仅记录日志，请改用 Admin API 显式加载或
> 切换到 `control_mode: "auto"`。

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
| `--no-metrics` | 禁用指标 | `metrics.enabled` |
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
| `--ejection-timeout` | 被剔除 worker 自动恢复（秒） | `model_defaults.ejection_timeout` |
| `--ejection-max-percent` | 最多剔除的 worker 比例 | `model_defaults.ejection_max_percent` |
| `--max-retries` | 失败 batch 换 worker 重试 | `model_defaults.max_retries` |
| `--startup-timeout` | worker ready 握手超时（秒） | `model_defaults.startup_timeout` |
| `--health-check-timeout` | 健康探测超时（秒） | `model_defaults.health_check_timeout` |
| `--health-check-kill-threshold` | 连续探测失败 N 次后杀死并重启 worker（0=禁用） | `model_defaults.health_check_kill_threshold` |
| `--worker-kill-timeout` | 杀死后等待 OS 回收（秒） | `model_defaults.worker_kill_timeout` |
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
