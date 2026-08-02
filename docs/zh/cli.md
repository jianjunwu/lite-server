# CLI 参考手册

[English](../cli.md)

## 安装

```bash
pip install lite-server
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
| `--concurrency` | int | 8 | 并发请求数 |
| `--duration` | float | 30.0 | 测试持续时间（秒） |

```bash
# 对 my_model 做 60 秒、16 并发的基准测试
lite-server benchmark --model my_model --concurrency 16 --duration 60
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
| `--output-dir` | string | ./reports | 报告输出目录 |

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
