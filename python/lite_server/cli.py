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

    serve_parser.add_argument("--max-queue-size", type=int, help="Max queue size per model (overrides model config)")
    serve_parser.add_argument("--max-requests", type=int, help="Auto-restart worker after N requests (0=disabled)")
    serve_parser.add_argument("--max-requests-jitter", type=int, help="Jitter range for max_requests to prevent thundering herd")
    serve_parser.add_argument("--request-timeout", type=float, help="Per-request hard timeout in seconds (0=disabled)")
    serve_parser.add_argument("--health-check-interval", type=float, help="Active health check interval in seconds (0=disabled)")
    serve_parser.add_argument("--graceful-timeout", type=float, help="Graceful shutdown timeout in seconds")
    serve_parser.add_argument("--threads", type=int, help="Number of Tokio worker threads (default: auto = CPU cores)")
    serve_parser.add_argument("--keepalive-timeout", type=float, help="HTTP keep-alive timeout in seconds (0=disabled)")
    # Worker resilience (§3)
    serve_parser.add_argument("--ejection-error-threshold", type=int, help="Consecutive errors before a worker is ejected (0=disable)")
    serve_parser.add_argument("--ejection-timeout", type=float, help="Seconds a worker stays ejected before auto-recovery")
    serve_parser.add_argument("--ejection-max-percent", type=int, help="Max %% of workers ejectable at once (1-100)")
    serve_parser.add_argument("--ejection-max-timeout", type=float, help="Upper bound for per-worker ejection backoff (seconds)")
    serve_parser.add_argument("--max-retries", type=int, help="Retry a failed batch on a different worker up to N times (0=disable)")
    serve_parser.add_argument("--startup-timeout", type=float, help="Max seconds to wait for a worker ready handshake")
    serve_parser.add_argument("--health-check-timeout", type=float, help="Seconds per health-check probe before timeout")
    serve_parser.add_argument("--health-check-kill-threshold", type=int, help="Consecutive probe failures before kill + respawn (0=never)")
    serve_parser.add_argument("--worker-kill-timeout", type=float, help="Seconds to wait for the OS to reap a killed worker")
    serve_parser.add_argument("--hook-http-timeout", type=float, help="Seconds for a worker lifecycle HTTP hook request")

    # config-check
    check_parser = subparsers.add_parser("config-check", help="Validate configuration")
    check_parser.add_argument("config", help="Path to YAML configuration file")

    # benchmark
    bench_parser = subparsers.add_parser("benchmark", help="Run benchmark")
    bench_parser.add_argument("--url", default="http://127.0.0.1:8000")
    bench_parser.add_argument("--model", required=True)
    bench_parser.add_argument("--version", default=None)
    bench_parser.add_argument("--concurrency", type=int, default=8)
    term_group = bench_parser.add_mutually_exclusive_group()
    term_group.add_argument("--duration", type=float, default=None,
                            help="Run for N seconds (default: 30; mutually exclusive with --requests)")
    term_group.add_argument("--requests", type=int, default=None,
                            help="Run exactly N requests (mutually exclusive with --duration)")
    bench_parser.add_argument("--warmup-requests", type=int, default=0,
                              help="Warmup requests before measurement; samples discarded "
                                   "(recommended: ~= concurrency)")
    bench_parser.add_argument("--grace-period", type=float, default=30.0,
                              help="After the deadline, wait at most N seconds for in-flight "
                                   "requests to drain (duration mode)")
    payload_group = bench_parser.add_mutually_exclusive_group()
    payload_group.add_argument("--payload", default=None,
                               help="Inline JSON request body (default: '{\"input\": 1.0}')")
    payload_group.add_argument("--payload-file", action="append", default=None,
                               help="JSON file with request body; repeatable, round-robin")
    bench_parser.add_argument("--export", default=None,
                              help="Write authoritative JSON record to PATH (stdout table unchanged)")
    bench_parser.add_argument("--max-error-rate", type=float, default=None,
                              help="Exit 99 if failed/total exceeds R (e.g. 0.01)")
    bench_parser.add_argument("--max-p99", type=float, default=None,
                              help="Exit 99 if p99 latency exceeds MS milliseconds")

    # analyze
    analyze_parser = subparsers.add_parser("analyze", help="Run model analyzer")
    analyze_parser.add_argument("--model-repo", default="./model_repo")
    analyze_parser.add_argument("--model", required=True)
    analyze_parser.add_argument("--version", default=None,
                                help="Model version (default: latest; warns LS111)")
    analyze_parser.add_argument("--format", choices=["json", "markdown"], default="json",
                                help="Output format (default: json; markdown is rendered "
                                     "from the same schema v1 data)")
    analyze_parser.add_argument("--output-dir", default=None,
                                help="Additionally save report files (json+md) to DIR")
    analyze_parser.add_argument("--fail-severity", choices=["error", "warning"],
                                default="error",
                                help="Minimum severity that exits 1 (default: error)")
    analyze_parser.add_argument("--strict", action="store_true",
                                help="Shortcut for --fail-severity warning")

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
    init_parser.add_argument("--wizard", "-w", action="store_true", help="Interactive wizard mode")
    init_parser.add_argument(
        "--model-only",
        dest="model_only",
        action="store_true",
        help="Generate only model_repo/<name>/1/ (no project shell)",
    )

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
            max_queue_size=args.max_queue_size,
            max_requests=args.max_requests,
            max_requests_jitter=args.max_requests_jitter,
            request_timeout=args.request_timeout,
            health_check_interval=args.health_check_interval,
            graceful_timeout=args.graceful_timeout,
            keepalive_timeout=args.keepalive_timeout,
            ejection_error_threshold=args.ejection_error_threshold,
            ejection_timeout=args.ejection_timeout,
            ejection_max_percent=args.ejection_max_percent,
            ejection_max_timeout=args.ejection_max_timeout,
            max_retries=args.max_retries,
            startup_timeout=args.startup_timeout,
            health_check_timeout=args.health_check_timeout,
            health_check_kill_threshold=args.health_check_kill_threshold,
            worker_kill_timeout=args.worker_kill_timeout,
            hook_http_timeout=args.hook_http_timeout,
        )
    except KeyboardInterrupt:
        print("\nShutting down...")
    except RuntimeError as e:
        _logger.error("%s", e)
        return 1
    return 0


def _cmd_config_check(args):
    from lite_server import validate_model_config, validate_server_config

    try:
        import yaml
        with open(args.config, "r") as f:
            cfg = yaml.safe_load(f)
        if cfg is None:
            cfg = {}
        # Delegate value/type validation to the Rust serde path so that a
        # config passing config-check is guaranteed loadable by the server.
        if "server" in cfg:
            if validate_server_config is None:
                _logger.warning("lite-server Rust extension not built; type validation skipped.")
            else:
                validate_server_config(args.config)
        else:
            if validate_model_config is None:
                _logger.warning("lite-server Rust extension not built; type validation skipped.")
            else:
                validate_model_config(args.config)
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


def _resolve_benchmark_payloads(args) -> list[dict]:
    """Resolve request payloads from --payload / --payload-file / default."""
    if args.payload is not None:
        return [json.loads(args.payload)]
    if args.payload_file:
        return [
            json.loads(Path(p).read_text(encoding="utf-8"))
            for p in args.payload_file
        ]
    return [{"input": 1.0}]


def _cmd_benchmark(args):
    """Run benchmark: thin shell over BenchmarkEngine (closed-loop, worker pool).

    CLI responsibilities only: arg validation, httpx target construction,
    output rendering, threshold gate. All measurement logic lives in
    lite_server.analyzer.benchmark.BenchmarkEngine.
    """
    import itertools
    import sys

    from lite_server.analyzer.benchmark import (
        BenchmarkEngine,
        RequestConnectError,
        RequestStatusError,
        RequestTimeoutError,
        RequestTransportError,
    )

    try:
        payloads = _resolve_benchmark_payloads(args)
    except (OSError, ValueError) as e:
        _logger.error("payload error: %s", e)
        return 2

    duration = args.duration
    if duration is None and args.requests is None:
        duration = 30.0

    url = f"{args.url}/v2/models/{args.model}/infer"
    if args.version:
        url = f"{args.url}/v2/models/{args.model}/versions/{args.version}/infer"

    async def run_benchmark():
        import httpx

        mode = f"duration={duration}s" if duration is not None else f"requests={args.requests}"
        print(f"Benchmarking {args.model} (concurrency={args.concurrency}, "
              f"{mode}, warmup={args.warmup_requests})")

        payload_cycle = itertools.cycle(payloads)
        timeout = httpx.Timeout(30.0, connect=5.0, pool=5.0)
        limits = httpx.Limits(
            max_connections=max(args.concurrency, 1),
            max_keepalive_connections=max(args.concurrency, 1),
            keepalive_expiry=15.0,
        )
        async with httpx.AsyncClient(limits=limits) as client:
            async def target(payload: dict) -> dict:
                try:
                    resp = await client.post(url, json=payload, timeout=timeout)
                except httpx.TimeoutException as e:
                    raise RequestTimeoutError() from e
                except httpx.ConnectError as e:
                    raise RequestConnectError() from e
                except httpx.TransportError as e:
                    raise RequestTransportError() from e
                if resp.status_code != 200:
                    raise RequestStatusError(resp.status_code)
                return {"ok": True}

            engine = BenchmarkEngine()
            return await engine.run(
                target=target,
                payload=lambda: next(payload_cycle),
                concurrency=args.concurrency,
                duration=duration,
                total_requests=args.requests,
                warmup_requests=args.warmup_requests,
                grace_period=args.grace_period,
            )

    # Windows: use SelectorEventLoop for subprocess compat
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        result = asyncio.run(run_benchmark())
    except KeyboardInterrupt:
        print("\nBenchmark interrupted.")
        return 130

    if result.total_requests == 0:
        print("No requests completed — is the server running?")
        return 1

    print(f"\nBenchmark Results ({args.model}):")
    print("  Mode:            closed-loop (service-time latencies)")
    if duration is not None:
        print(f"  Duration:        {duration}s (measured window {result.window:.3f}s)")
    else:
        print(f"  Requests:        {args.requests} (measured window {result.window:.3f}s)")
    print(f"  Total requests:  {result.total_requests}")
    print(f"  Success:         {result.successful}")
    print(f"  Failed:          {result.failed}")
    if result.error_kinds:
        kinds = ", ".join(f"{k}={v}" for k, v in sorted(result.error_kinds.items()))
        print(f"    by kind:       {kinds}")
    print(f"  Throughput:      {result.throughput:.2f} req/s")
    if duration is not None:
        print(f"  Drained/grace:   {result.drained_in_grace}  "
              f"Dropped in-flight: {result.dropped_inflight}")

    if result.latencies:
        print("  Latency (ms) [percentile method: linear]:")
        print(f"    mean: {result.mean_latency:.2f}")
        print(f"    p50:  {result.p50:.2f}")
        print(f"    p90:  {result.p90:.2f}")
        print(f"    p95:  {result.p95:.2f}")
        print(f"    p99:  {result.p99:.2f}")
        print(f"    min:  {result.min_latency:.2f}")
        print(f"    max:  {result.max_latency:.2f}")

    for w in result.warnings:
        print(f"  WARNING: {w}")

    if args.export:
        export_data = {
            "config": {
                "url": url,
                "model": args.model,
                "version": args.version,
                "concurrency": args.concurrency,
                "duration": duration,
                "requests": args.requests,
                "warmup_requests": args.warmup_requests,
                "grace_period": args.grace_period,
            },
            **result.to_dict(),
        }
        Path(args.export).write_text(json.dumps(export_data, indent=2), encoding="utf-8")
        print(f"  Exported: {args.export}")

    violations = []
    if args.max_error_rate is not None:
        rate = result.failed / result.total_requests
        if rate > args.max_error_rate:
            violations.append(f"error rate {rate:.3f} > {args.max_error_rate}")
    if args.max_p99 is not None and result.p99 > args.max_p99:
        violations.append(f"p99 {result.p99:.2f}ms > {args.max_p99}ms")
    if violations:
        for v in violations:
            print(f"  THRESHOLD VIOLATION: {v}")
        return 99

    return 0


def _cmd_analyze(args):
    """Analyze a model: thin shell over StaticAnalyzer (pure AST, zero execution).

    Exit code protocol: 0 = no finding at --fail-severity, 1 = finding(s) at
    or above it, 2 = analysis itself failed (path escape, not found, ...).
    """
    from lite_server.analyzer.report import ReportGenerator
    from lite_server.analyzer.static import StaticAnalyzer

    try:
        analyzer = StaticAnalyzer(Path(args.model_repo))
        report = analyzer.analyze_model(args.model, version=args.version)
    except (ValueError, FileNotFoundError) as e:
        _logger.error("%s", e)
        return 2

    command = f"lite-server analyze --model {args.model}"
    if args.version:
        command += f" --version {args.version}"
    report_dict = report.to_dict(tool_version=f"lite-server {_pkg_version}",
                                 command=command)

    if args.format == "markdown":
        print(ReportGenerator.to_markdown(report_dict))
    else:
        print(ReportGenerator.to_json(report_dict))

    if args.output_dir:
        saved = ReportGenerator.save(report_dict, args.output_dir)
        print(f"Reports saved: {saved} (+ .md)", file=sys.stderr)

    fail_severity = "warning" if args.strict else args.fail_severity
    return report.exit_code(fail_severity)


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
    """Initialize a new lite-server project."""
    if getattr(args, "wizard", False):
        from lite_server.init.wizard import run_wizard
        try:
            run_wizard(output_dir=str(Path(".")))
            return 0
        except SystemExit as e:
            return e.code if isinstance(e.code, int) else 1

    from lite_server.init import ProjectGenerator

    project_name = args.project_name or "my_project"

    try:
        if getattr(args, "model_only", False):
            gen = ProjectGenerator(
                project_name=project_name,
                output_dir=Path("."),
                model_name=project_name,  # the positional arg is the MODEL name
            )
            model_dir = gen.generate_model_only()
            print(f"Created model at: {model_dir}")
            print(f"\nNext steps:")
            print(f"  Add '{project_name}' to orchestration.load_models in "
                  f"server.yaml (or use control_mode=auto)")
        else:
            gen = ProjectGenerator(
                project_name=project_name,
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


if __name__ == "__main__":
    sys.exit(main())
