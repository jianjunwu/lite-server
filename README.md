# lite-server

High-performance inference server with a Rust core and Python workers.

## Features

- Rust HTTP server (axum/tokio) for high throughput and low latency
- Python workers for model inference via subprocess + UDS
- Model repository with hot-reload support
- Ensemble and orchestration strategies
- Prometheus metrics endpoint
- CLI tools: benchmark, analyze, pack/unpack artifacts, project scaffolding

## Installation

### From Wheel (Recommended)

Pre-built wheels are available for:

- **Linux**: x86_64, aarch64 (manylinux2014)
- **macOS**: x86_64, aarch64 (Apple Silicon)
- **Windows**: x86_64, aarch64
- **Python**: 3.9 - 3.14

```bash
pip install lite-server-<version>-py3-none-<platform>.whl
```

After installation:

- `lite-server-core` — Rust inference server binary
- `python -m lite_server` — Python CLI wrapper (serve, benchmark, analyze, pack, unpack, init)

### From Source

Requires Rust >= 1.70 and Python >= 3.9.

```bash
# Install maturin
pip install maturin

# Build and install locally
maturin develop

# Or build a wheel
maturin build --release
```

## Usage

### Start the server

```bash
# Direct Rust binary
lite-server-core serve --config server.yaml

# Or via Python wrapper
python -m lite_server serve --config server.yaml
```

### CLI Commands

```bash
python -m lite_server serve       # Start inference server
python -m lite_server config-check server.yaml
python -m lite_server benchmark --model my_model
python -m lite_server analyze --model my_model
python -m lite_server pack ./my_model --version 1
python -m lite_server unpack my_model_v1.lma
python -m lite_server init my_project
```

## Multi-Platform Wheel Builds

Wheels are built automatically via GitHub Actions (`.github/workflows/build-wheels.yml`).

Trigger on push of a `v*` tag or manually via `workflow_dispatch`:

```bash
git tag v0.1.0
git push origin v0.1.0
```

Artifacts are uploaded per platform and can be downloaded from the Actions run page.

### Supported Build Matrix

| Platform | Architecture | Wheel Tag |
|----------|-------------|-----------|
| Linux | x86_64 | manylinux2014_x86_64 |
| Linux | aarch64 | manylinux2014_aarch64 |
| macOS | x86_64 | macosx_10_12_x86_64 |
| macOS | aarch64 | macosx_11_0_arm64 |
| Windows | x86_64 | win_amd64 |
| Windows | aarch64 | win_arm64 |

All wheels are tagged `py3-none`, meaning they work with any Python 3.9+ installation on the matching platform.

## Development

### Rust

```bash
cargo build --release
cargo test
```

### Python

```bash
cd python
python -m pytest tests/
```

### Project Structure

```
.
├── src/              # Rust source
├── python/           # Python package (lite_server)
├── tests/            # Rust integration tests
├── examples/         # Example model repository
└── Cargo.toml        # Rust manifest
└── pyproject.toml    # Python packaging (maturin)
```

## License

MIT
