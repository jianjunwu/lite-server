"""Command-line interface for lite-server."""

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path


from lite_server import __version__ as _pkg_version

_logger = logging.getLogger("lite_server.cli")


def main(argv=None):
    parser = argparse.ArgumentParser(prog="lite-server")
    parser.add_argument("-v", "--version", action="version", version=f"lite-server {_pkg_version}")
    subparsers = parser.add_subparsers(dest="command")

    # serve
    serve_parser = subparsers.add_parser("serve", help="Start the inference server")
    serve_parser.add_argument("--config", "-c", help="Path to YAML configuration file")
    serve_parser.add_argument("--port", type=int, help="HTTP server port")
    serve_parser.add_argument("--host", help="Bind address")
    serve_parser.add_argument("--model-repo", help="Model repository path")
    serve_parser.add_argument("--endpoints-dir", help="Custom endpoint directory (overrides model_repository.path for endpoints)")
    serve_parser.add_argument("--timeout", type=float, help="Request timeout")
    serve_parser.add_argument("--log-level", help="Log level")
    serve_parser.add_argument("--log-info-output", help="Info log file path")
    serve_parser.add_argument("--log-error-output", help="Error log file path")
    serve_parser.add_argument("--log-rotation", help="Log rotation: none, size, daily, hourly")
    serve_parser.add_argument("--metrics-port", type=int, help="Metrics server port")
    serve_parser.add_argument("--no-metrics", action="store_true", help="Disable metrics")
    serve_parser.add_argument("--grpc-port", type=int, help="gRPC server port")
    serve_parser.add_argument("--no-grpc", action="store_true", help="Disable gRPC server")
    serve_parser.add_argument("--no-streaming-metrics", action="store_true", help="Disable streaming metrics")
    serve_parser.add_argument("--log-verbose", action="store_true", help="Also log to stderr")
    serve_parser.add_argument("--max-queue-size", type=int, help="Max queue size per model (overrides model config)")
    serve_parser.add_argument("--max-requests", type=int, help="Auto-restart worker after N requests (0=disabled)")
    serve_parser.add_argument("--max-requests-jitter", type=int, help="Jitter range for max_requests to prevent thundering herd")
    serve_parser.add_argument("--request-timeout", type=float, help="Per-request hard timeout in seconds (0=disabled)")
    serve_parser.add_argument("--health-check-interval", type=float, help="Active health check interval in seconds (0=disabled)")
    serve_parser.add_argument("--graceful-timeout", type=float, help="Graceful shutdown timeout in seconds")
    serve_parser.add_argument("--threads", type=int, help="Number of Tokio worker threads (default: auto = CPU cores)")
    serve_parser.add_argument("--keepalive-timeout", type=float, help="HTTP keep-alive timeout in seconds (0=disabled)")

    # config-check
    check_parser = subparsers.add_parser("config-check", help="Validate configuration")
    check_parser.add_argument("config", help="Path to YAML configuration file")

    # benchmark
    bench_parser = subparsers.add_parser("benchmark", help="Run benchmark")
    bench_parser.add_argument("--url", default="http://127.0.0.1:8000")
    bench_parser.add_argument("--model", required=True)
    bench_parser.add_argument("--version", default=None)
    bench_parser.add_argument("--concurrency", type=int, default=8)
    bench_parser.add_argument("--duration", type=float, default=30.0)

    # analyze
    analyze_parser = subparsers.add_parser("analyze", help="Run model analyzer")
    analyze_parser.add_argument("--model-repo", default="./model_repo")
    analyze_parser.add_argument("--model", required=True)
    analyze_parser.add_argument("--output-dir", default="./reports")

    # pack
    pack_parser = subparsers.add_parser("pack", help="Pack model into artifact")
    pack_parser.add_argument("model_dir", help="Model directory")
    pack_parser.add_argument("--version", "-v", required=True)
    pack_parser.add_argument("--name", "-n", default=None, help="Model name (auto-inferred from directory if omitted)")
    pack_parser.add_argument("--output", "-o", default="./artifacts")

    # unpack
    unpack_parser = subparsers.add_parser("unpack", help="Unpack artifact")
    unpack_parser.add_argument("artifact", help="Path to .lma file")
    unpack_parser.add_argument("--to", dest="target_dir", default=".")
    unpack_parser.add_argument("--flat", action="store_true", help="Extract files directly without prepending model name directory")

    # init
    init_parser = subparsers.add_parser("init", help="Initialize project")
    init_parser.add_argument("project_name", nargs="?")
    init_parser.add_argument("--template", "-t", default="empty")
    init_parser.add_argument("--wizard", "-w", action="store_true", help="Interactive wizard mode")

    args = parser.parse_args(argv)

    if args.command == "serve":
        return _cmd_serve(args)
    elif args.command == "config-check":
        return _cmd_config_check(args)
    elif args.command == "benchmark":
        return _cmd_benchmark(args)
    elif args.command == "analyze":
        return _cmd_analyze(args)
    elif args.command == "pack":
        return _cmd_pack(args)
    elif args.command == "unpack":
        return _cmd_unpack(args)
    elif args.command == "init":
        return _cmd_init(args)
    else:
        parser.print_help()
        return 0


def _cmd_serve(args):
    """Start the Rust server."""
    from lite_server import serve

    if serve is None:
        _logger.error("lite-server Rust extension not built. Run 'maturin develop'.")
        return 1

    try:
        serve(
            config=args.config,
            port=args.port,
            host=args.host,
            model_repo=args.model_repo,
            endpoints_dir=args.endpoints_dir,
            threads=args.threads,
            timeout=args.timeout,
            log_level=args.log_level,
            log_info_output=args.log_info_output,
            log_error_output=args.log_error_output,
            log_rotation=args.log_rotation,
            no_metrics=args.no_metrics,
            metrics_port=args.metrics_port,
            grpc_port=args.grpc_port,
            no_grpc=args.no_grpc,
            no_streaming_metrics=args.no_streaming_metrics,
            log_verbose=args.log_verbose,
            max_queue_size=args.max_queue_size,
            max_requests=args.max_requests,
            max_requests_jitter=args.max_requests_jitter,
            request_timeout=args.request_timeout,
            health_check_interval=args.health_check_interval,
            graceful_timeout=args.graceful_timeout,
            keepalive_timeout=args.keepalive_timeout,
        )
    except KeyboardInterrupt:
        print("\nShutting down...")
    except RuntimeError as e:
        _logger.error("%s", e)
        return 1
    return 0


def _cmd_config_check(args):
    try:
        import yaml
        with open(args.config, "r") as f:
            cfg = yaml.safe_load(f)
        if cfg is None:
            cfg = {}
        print(f"Configuration OK: {args.config}")
        if "server" in cfg:
            s = cfg["server"]
            print(f"  HTTP port: {s.get('http_port', 8000)}")
            print(f"  gRPC port: {s.get('grpc_port', 8001)}")
            print(f"  Metrics port: {s.get('metrics_port', 8002)}")
        if "model_repository" in cfg:
            print(f"  Model repo: {cfg['model_repository'].get('path', './model_repo')}")
        # Model-level config (no server section)
        if "server" not in cfg:
            if "max_batch_size" in cfg:
                print(f"  max_batch_size: {cfg['max_batch_size']}")
            if "stream" in cfg:
                print(f"  stream: {cfg['stream']}")
            if "accelerator" in cfg:
                print(f"  accelerator: {cfg['accelerator']}")
            if "max_queue_size" in cfg:
                print(f"  max_queue_size: {cfg['max_queue_size']}")
        return 0
    except Exception as e:
        _logger.error("Configuration error: %s", e)
        return 1


def _cmd_benchmark(args):
    """Run benchmark against running server using async HTTP with precise concurrency control."""
    import asyncio
    import time
    import statistics
    import json
    import sys

    payload = {"input": 1.0}

    url = f"{args.url}/v2/models/{args.model}/infer"
    if args.version:
        url = f"{args.url}/v2/models/{args.model}/versions/{args.version}/infer"

    async def run_benchmark():
        import httpx

        print(f"Benchmarking {args.model} (concurrency={args.concurrency}, duration={args.duration}s)")

        results: list[dict] = []
        sem = asyncio.Semaphore(args.concurrency)
        running = True

        async def send_request(client: httpx.AsyncClient):
            async with sem:
                t0 = time.monotonic()
                try:
                    resp = await client.post(
                        url, json=payload,
                        timeout=httpx.Timeout(30.0, connect=5.0),
                    )
                    results.append({
                        "success": resp.status_code == 200,
                        "latency_ms": (time.monotonic() - t0) * 1000,
                    })
                except Exception as e:
                    results.append({
                        "success": False,
                        "latency_ms": (time.monotonic() - t0) * 1000,
                        "error": str(e),
                    })

        async with httpx.AsyncClient(
            limits=httpx.Limits(max_connections=args.concurrency * 2, max_keepalive_connections=args.concurrency),
        ) as client:
            tasks: list[asyncio.Task] = []
            deadline = time.monotonic() + args.duration

            while time.monotonic() < deadline:
                tasks.append(asyncio.create_task(send_request(client)))
                # Trim completed tasks periodically to bound memory
                if len(tasks) > args.concurrency * 4:
                    tasks = [t for t in tasks if not t.done()]

            # Wait for remaining tasks to finish
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        return results

    # Windows: use SelectorEventLoop for subprocess compat
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        results = asyncio.run(run_benchmark())
    except KeyboardInterrupt:
        print("\nBenchmark interrupted.")
        return 130

    total = len(results)
    if total == 0:
        print("No requests completed — is the server running?")
        return 1

    success = sum(1 for r in results if r.get("success"))
    failed = total - success
    latencies = [r["latency_ms"] for r in results if r.get("success")]

    throughput = total / args.duration if args.duration > 0 else 0

    print(f"\nBenchmark Results ({args.model}):")
    print(f"  Duration:        {args.duration}s")
    print(f"  Total requests:  {total}")
    print(f"  Success:         {success}")
    print(f"  Failed:          {failed}")
    print(f"  Throughput:      {throughput:.2f} req/s")

    if latencies:
        latencies.sort()
        print(f"  Latency (ms):")
        print(f"    mean: {statistics.mean(latencies):.2f}")
        print(f"    p50:  {statistics.median(latencies):.2f}")
        print(f"    p90:  {latencies[int(len(latencies) * 0.9)]:.2f}")
        print(f"    p99:  {latencies[int(len(latencies) * 0.99)]:.2f}")
        print(f"    min:  {min(latencies):.2f}")
        print(f"    max:  {max(latencies):.2f}")

    return 0


def _cmd_analyze(args):
    """Static model analyzer: inspect model.py, config.yaml, deps, generate JSON report."""
    import importlib.util
    from datetime import datetime, timezone

    repo = Path(args.model_repo)
    model_dir = repo / args.model

    if not model_dir.exists():
        _logger.error("Model not found: %s", model_dir)
        return 1

    # Find version directory (use first numeric dir, or '1')
    version_dirs = [d for d in model_dir.iterdir() if d.is_dir() and d.name.isdigit()]
    version_dir = version_dirs[0] if version_dirs else model_dir / "1"
    version = version_dir.name

    model_py = version_dir / "model.py"
    config_yaml = version_dir / "config.yaml"
    requirements_txt = model_dir / "requirements.txt"

    report = {
        "model_name": args.model,
        "version": version,
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
        "has_model_py": model_py.exists(),
        "has_config": config_yaml.exists(),
        "has_requirements": requirements_txt.exists(),
        "config": {},
        "methods": [],
        "warnings": [],
    }

    # Parse config
    if config_yaml.exists():
        try:
            import yaml
            report["config"] = yaml.safe_load(config_yaml.read_text()) or {}
        except Exception as e:
            report["warnings"].append(f"config.yaml parse error: {e}")

    # Analyze model.py
    if model_py.exists():
        try:
            spec = importlib.util.spec_from_file_location("analyzed_model", model_py)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Find the API class
            api_class = None
            for name in dir(module):
                obj = getattr(module, name)
                if isinstance(obj, type) and name != "LitAPI":
                    api_class = obj
                    break

            if api_class:
                for method in ("setup", "decode_request", "predict", "encode_response",
                               "batch", "unbatch", "predict_step", "stream_open",
                               "stream_chunk", "stream_close", "stream_cancel"):
                    if hasattr(api_class, method) and callable(getattr(api_class, method)):
                        report["methods"].append(method)

                if "predict" not in report["methods"]:
                    report["warnings"].append("No predict() method found")
            else:
                report["warnings"].append("No LitAPI subclass found")
        except Exception as e:
            report["warnings"].append(f"model.py load error: {e}")
    else:
        report["warnings"].append("model.py not found")

    # Check requirements
    if requirements_txt.exists():
        deps = [line.strip() for line in requirements_txt.read_text().splitlines() if line.strip()]
        report["dependencies"] = deps

    # Write report
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / f"{args.model}_v{version}_report.json"
    report_path.write_text(json.dumps(report, indent=2))

    print(f"Analysis report: {report_path}")
    if report["warnings"]:
        for w in report["warnings"]:
            print(f"  Warning: {w}")
    return 0


def _cmd_pack(args):
    """Pack model into a signed .lma artifact."""
    from lite_server.artifact import ModelPacker, _load_or_create_key

    model_dir = Path(args.model_dir)
    if not model_dir.exists():
        _logger.error("Model directory not found: %s", model_dir)
        return 1

    # Load signing key if configured
    sign_key = None
    key_env = os.environ.get("LITE_SERVER_SIGN_KEY")
    if key_env:
        sign_key = _load_or_create_key(Path(key_env))

    try:
        name = args.name
        packer = ModelPacker(model_dir, version=args.version, name=name)
    except ValueError as e:
        _logger.error("%s", e)
        return 1

    try:
        artifact_path = packer.pack(args.output, sign_key=sign_key)
    except ValueError as e:
        _logger.error("%s", e)
        return 1

    print(f"Packed artifact: {artifact_path}")
    if sign_key:
        print(f"  Signed with key: {key_env}")
    return 0


def _cmd_unpack(args):
    """Unpack artifact with optional signature verification."""
    from lite_server.artifact import ModelUnpacker, _load_or_create_key

    artifact_path = Path(args.artifact)
    if not artifact_path.exists():
        _logger.error("Artifact not found: %s", artifact_path)
        return 1

    # Load verification key if configured
    verify_key = None
    key_env = os.environ.get("LITE_SERVER_SIGN_KEY")
    if key_env:
        verify_key = _load_or_create_key(Path(key_env))

    unpacker = ModelUnpacker(artifact_path)
    try:
        manifest = unpacker.validate(verify_key=verify_key)
        print(f"Artifact valid: {manifest.name} v{manifest.version}")
        print(f"  Files: {len(manifest.files)}")
    except Exception as e:
        _logger.error("Validation failed: %s", e)
        return 1

    target_dir = Path(args.target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    target = unpacker.unpack(target_dir, prepend_name=not args.flat)
    print(f"Extracted to: {target}")
    return 0


def _cmd_init(args):
    """Initialize a new lite-server project from a template."""
    if getattr(args, "wizard", False):
        from lite_server.init.wizard import run_wizard
        try:
            run_wizard(output_dir=str(Path(".")))
            return 0
        except SystemExit as e:
            return e.code if isinstance(e.code, int) else 1

    from lite_server.init import ProjectGenerator

    project_name = args.project_name or "my_project"
    template = args.template

    try:
        gen = ProjectGenerator(
            project_name=project_name,
            template=template,
            output_dir=Path("."),
        )
        root = gen.generate()
        print(f"Created project at: {root}")
        print(f"\nNext steps:")
        print(f"  cd {project_name}")
        print(f"  lite-server serve --config server.yaml")
        return 0
    except FileExistsError as e:
        _logger.error("%s", e)
        return 1
    except ValueError as e:
        _logger.error("%s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
