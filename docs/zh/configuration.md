# 配置参考

[English](../configuration.md)

lite-server 采用三层配置：**服务器配置**（YAML 文件或 CLI）、**模型配置**（每模型 `config.yaml`）和**编排配置**（`server.yaml` 中的 `orchestration` 段落）。CLI 参数覆盖 YAML 值。

## 服务器配置

路径：`server.yaml`（通过 `--config` 或 `-c` 传入）

```yaml
server:
  http_port: 8000              # HTTP 服务端口
  grpc_port: 8001              # gRPC 服务端口
  metrics_port: 8002           # Prometheus 指标端口
  host: 0.0.0.0                # 绑定地址（支持 unix:/path/to/sock 使用 UDS）
  timeout: 30.0                # 全局请求超时（秒）
  threads: null                # Tokio 工作线程数（null = 自动 = CPU 核数）
  cache_registry: false        # 缓存模型注册表到磁盘
  graceful_timeout: 30.0       # 优雅关闭时等待进行中请求的最大秒数
  keepalive_timeout: 5.0       # HTTP keep-alive 超时（秒），0 = 禁用

logging:
  level: info                  # 日志级别：trace, debug, info, warn, error
  info_output: null            # info 级别日志的独立文件
  error_output: null           # error 级别日志的独立文件
  rotation: none               # none, size, daily, hourly
  max_size: 100                # 最大日志文件大小（MB），rotation=size 时生效
  backup_count: 7              # 保留的轮转日志文件数

grpc:
  enabled: true                # 启用 gRPC 服务
  max_workers: 10              # gRPC 最大工作线程数

metrics:
  enabled: true                # 启用 Prometheus 指标端点

model_repository:
  path: ./model_repo           # 模型仓库目录

endpoints_dir: ./endpoints     # 自定义 HTTP 端点目录（可选）
                               # 递归扫描 *.py 文件

features:
  timeline: false              # 启用历史指标时间线
  system_overview: true        # 启用系统概览
  custom_metrics: false        # 启用自定义用户指标
  benchmarks: true             # 启用内置基准测试
  playground: false            # 启用 API 沙盒
  alerts: true                 # 启用告警引擎
  version_compare: false       # 启用版本对比
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
```

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
bidirectional: false           # 启用双向流式

# Continuous Batching（LLM）
continuous_batching: false     # 启用 continuous batching 模式
max_sequence_length: 2048      # 最大序列长度

# Worker 管理
accelerator: null              # 加速器类型：cpu, cuda, auto（null = cpu）
devices: null                  # 设备分配（null = 自动，或整数如 1）
workers_per_device: null       # 每设备 worker 数（null = 1）
max_queue_size: 1000           # 每个 worker 的最大待处理请求数
queue_mode: per_worker         # 队列模式：per_worker 或 shared
request_timeout: 0.0           # 单请求硬超时（秒），0 = 禁用
max_requests: 0                # worker 处理 N 个请求后自动重启（0 = 禁用）
max_requests_jitter: 0         # max_requests 的随机抖动，防止惊群效应
health_check_interval: 15.0    # 主动健康检查间隔（秒），0 = 禁用

# 心跳检测（Worker 存活检测）
heartbeat_interval: 0.0        # 心跳探测间隔（秒），0 = 禁用
heartbeat_timeout: 5.0         # 等待探测响应的最大秒数
heartbeat_max_failures: 3      # 连续失败次数达到此值后杀死 worker

# Worker 生命周期钩子
hooks:
  on_ready: null               # worker 就绪时执行的 shell 命令
  on_exit: null                # worker 退出时执行的 shell 命令
  on_error: null               # worker 异常退出时执行的 shell 命令
  on_ready_http: null          # worker 就绪时的 HTTP 回调
  on_exit_http: null           # worker 退出时的 HTTP 回调
  on_error_http: null          # worker 异常退出时的 HTTP 回调
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
hot_reload_interval: 1.0       # 轮询间隔（秒）
```

## 编排配置

路径：`server.yaml`（`orchestration` 段落）

控制启动时加载哪些模型和版本。

```yaml
control_mode: explicit         # explicit（手动）或 auto（轮询变更）
poll_interval: 5               # 轮询间隔（秒），control_mode=auto 时生效
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
```

### 加载策略

| 策略 | 行为 |
|------|------|
| `explicit` | 仅加载 `versions_to_load` 中列出的版本 |
| `latest` | 仅加载最新版本（最大版本号） |
| `all` | 加载所有可用版本 |

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
| `--endpoints-dir` | 自定义端点目录 | `endpoints_dir` |
| `--timeout` | 全局请求超时 | `server.timeout` |
| `--log-level` | 日志级别 | `logging.level` |
| `--no-metrics` | 禁用指标 | `metrics.enabled` |
| `--grpc-port` | gRPC 端口 | `server.grpc_port` |
| `--no-grpc` | 禁用 gRPC | `grpc.enabled` |
| `--no-streaming-metrics` | 禁用流式指标 | `features.streaming_metrics` |
| `--max-queue-size` | 所有模型的最大队列 | `model_defaults.max_queue_size` |
| `--max-requests` | N 个请求后自动重启 | `model_defaults.max_requests` |
| `--max-requests-jitter` | max_requests 抖动 | `model_defaults.max_requests_jitter` |
| `--request-timeout` | 单请求超时 | `model_defaults.request_timeout` |
| `--health-check-interval` | 健康检查间隔 | `model_defaults.health_check_interval` |
| `--graceful-timeout` | 优雅关闭超时 | `server.graceful_timeout` |
| `--keepalive-timeout` | HTTP keep-alive 超时 | `server.keepalive_timeout` |

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
```

### LLM 推理（流式 + continuous batching）

```yaml
# model_repo/llm/1/config.yaml
stream: true
continuous_batching: true
max_sequence_length: 4096
max_batch_size: 4
batch_timeout: 0.05
workers_per_device: 1
request_timeout: 120.0
```

### 生产环境 + 心跳检测 + 生命周期钩子

```yaml
# model_repo/my_model/1/config.yaml
max_requests: 500
max_requests_jitter: 50

heartbeat_interval: 10.0
heartbeat_timeout: 5.0
heartbeat_max_failures: 3

hooks:
  on_ready: 'echo "Worker $WORKER_ID ready for $MODEL"'
  on_error: 'curl -s -X POST http://alerts.internal/worker-error -d "{\"model\":\"$MODEL\",\"worker\":$WORKER_ID,\"reason\":\"$REASON\"}"'
  on_exit_http:
    url: "http://notify.internal/worker-exit"
    method: POST
    body_template: '{"model":"$MODEL","version":"$VERSION","worker":$WORKER_ID}'
```
