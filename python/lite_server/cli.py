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
    bench_parser.add_argument("--concurrency", default="8",
                              help="Concurrency level (N), or sweep range "
                                   "(start:end:step, e.g. 1:16:2 => 1,3,5,...,15)")
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
    bench_parser.add_argument("--rate", type=float, default=None,
                              help="Constant arrival rate in req/s (open-loop); "
                                   "dispatch requests on a fixed-interval schedule "
                                   "independent of response times")
    bench_parser.add_argument("--latency-threshold", type=float, default=None,
                              help="During concurrency sweep, stop early when p99 "
                                   "exceeds MS milliseconds (perf_analyzer convention)")
    payload_group = bench_parser.add_mutually_exclusive_group()
    payload_group.add_argument("--payload", default=None,
                               help="Inline JSON request body (default: '{\"input\": 1.0}')")
    payload_group.add_argument("--payload-file", action="append", default=None,
                               help="JSON file with request body; repeatable, round-robin")
    payload_group.add_argument("--payload-random", default=None, metavar="TEMPLATE",
                               help="Randomize id/request_id/uuid fields per request "
                                    "using TEMPLATE as the base JSON body "
                                    "(default: '{\"input\": 1.0}')")
    bench_parser.add_argument("--export", default=None,
                              help="Write authoritative JSON record to PATH (stdout table unchanged)")
    bench_parser.add_argument("--max-error-rate", type=float, default=None,
                              help="Exit 99 if failed/total exceeds R (e.g. 0.01)")
    bench_parser.add_argument("--max-p99", type=float, default=None,
                              help="Exit 99 if p99 latency exceeds MS milliseconds")
    stream_mode_group = bench_parser.add_mutually_exclusive_group()
    stream_mode_group.add_argument("--stream", action="store_true", default=False,
                              help="Use SSE streaming endpoint /v2/models/{m}/events")
    stream_mode_group.add_argument("--bidi", action="store_true", default=False,
                              help="Run bidi session benchmark (WS /stream bidi mode; "
                                   "payload must be a JSON array [open, chunk1, ...])")
    bench_parser.add_argument("--model-type", choices=["llm", "tts", "stt", "generic"],
                              default="llm",
                              help="Streaming metric interpretation (default: llm); "
                                   "'generic' reports common section only (decoupled)")
    bench_parser.add_argument("--endpoint", choices=["events", "decoupled"],
                              default="events",
                              help="Streaming endpoint variant (default: events; "
                                   "decoupled → /v2/models/{m}/decoupled, requires --stream)")
    bench_parser.add_argument("--transport", choices=["sse", "ws", "grpc", "h2"],
                              default=None,
                              help="Streaming transport (default: sse; ws for --bidi). "
                                   "ws → /stream|/decoupled-stream; grpc → "
                                   "StreamInfer|DecoupledInfer over an insecure "
                                   "channel to the --url host:port; h2 → /bidi "
                                   "(bidi only, h2c prior-knowledge)")
    bench_parser.add_argument("--pace", type=float, default=None,
                              help="Bidi real-time pacing: seconds between chunks "
                                   "(requires --bidi; default: lock-step)")
    bench_parser.add_argument("--rt-factor", type=float, default=None,
                              help="Bidi speedup pacing: divide --pace by N "
                                   "(requires --pace)")
    bench_parser.add_argument("--min-sessions", type=int, default=30,
                              help="Bidi: minimum completed sessions before the "
                                   "sample-size warning fires (default: 30)")
    bench_parser.add_argument("--cancel-after", type=int, default=None,
                              help="Cancel each stream after N chunks "
                                   "(client-cancel scenario; requires --stream)")
    bench_parser.add_argument("--read-delay-ms", type=float, default=None,
                              help="Slow-consumer scenario: sleep MS after each "
                                   "chunk (requires --stream)")
    bench_parser.add_argument("--goodput", default=None, metavar="EXPR",
                              help="SLO expression, e.g. 'ttft:500 tpot:50 e2el:2000' "
                                   "(ms; requires --stream; tpot is llm-only)")
    bench_parser.add_argument("--slo-attainment", type=float, default=None,
                              help="Exit 99 if SLO attainment is below R "
                                   "(default: 0.95; requires --goodput)")
    bench_parser.add_argument("--tokenizer", default=None, metavar="PATH_OR_HUB_ID",
                              help="Client-side exact token counting with a "
                                   "tokenizers tokenizer (local file or hub id; "
                                   "requires --stream and --model-type llm; "
                                   "pip install lite-server[benchmark])")
    bench_parser.add_argument("--text-field", default=None, metavar="FIELD",
                              help="Chunk JSON field holding the text to tokenize "
                                   "(default: 'text' then 'token'; requires --tokenizer)")
    bench_parser.add_argument("--stream-read-timeout", type=float, default=300.0,
                              help="Seconds between stream chunks before timeout "
                                   "(default: 300)")
    bench_parser.add_argument("--max-ttft-ms", type=float, default=None,
                              help="Exit 99 if TTFT p99 exceeds MS (requires --stream)")
    bench_parser.add_argument("--max-rtf", type=float, default=None,
                              help="Exit 99 if RTF p99 exceeds VAL "
                                   "(requires --stream and --model-type tts/stt)")

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
    analyze_parser.add_argument("--deep", action="store_true", default=False,
                                help="Resolve statically-unresolvable classes by "
                                     "importing model.py in an isolated subprocess "
                                     "(EXECUTES MODEL CODE — opt-in)")
    analyze_parser.add_argument("--deep-timeout", type=float, default=30.0,
                                help="Seconds before --deep import is killed (default: 30)")
    analyze_parser.add_argument("--profile", choices=["kserve-v2"], default=None,
                                help="Run an optional interop profile check "
                                     "(kserve-v2: KServe V2 inference protocol)")

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


def _parse_concurrency(raw: str) -> list[int]:
    """Parse --concurrency argument: single int or start:end:step sweep."""
    if ":" in raw:
        parts = raw.split(":")
        if len(parts) != 3:
            raise ValueError(
                f"Concurrency sweep must be start:end:step, got {raw!r}"
            )
        start, end, step = int(parts[0]), int(parts[1]), int(parts[2])
        if start < 1 or end < start or step < 1:
            raise ValueError(
                f"Concurrency sweep requires 1 <= start <= end and step >= 1, "
                f"got {raw!r}"
            )
        levels = list(range(start, end + 1, step))
        if not levels:
            raise ValueError(f"Concurrency sweep {raw!r} produces no levels")
        return levels
    return [int(raw)]


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


def _random_payload_factory(template: dict) -> "Callable[[], dict]":
    """Return a callable that yields ``template`` with random id/uuid fields.

    Fields ``id``, ``request_id``, and ``uuid`` are replaced with random
    values on each call so the server cannot serve from a hot cache.
    A ``_r`` nonce is injected when none of those fields are present in the
    template (ensuring at least one per-request variation).
    """
    import random as _random
    import uuid as _uuid

    has_random_field = any(k in template for k in ("id", "request_id", "uuid"))

    def _fab() -> dict:
        d = dict(template)
        if "id" in d:
            d["id"] = str(_uuid.uuid4())
        if "request_id" in d:
            d["request_id"] = str(_uuid.uuid4())
        if "uuid" in d:
            d["uuid"] = str(_uuid.uuid4())
        if not has_random_field:
            d["_r"] = _random.randint(0, 2**63 - 1)
        return d

    return _fab


def _run_concurrency_sweep(args, levels: list[int], duration, run_one) -> int:
    """Run benchmark at each concurrency level; print sweep summary table.

    Returns the most severe exit code across all levels: 99 (threshold
    violation) > 1 (no results) > 0 (pass).  Exports all level results
    to ``--export`` path when specified.
    """
    import asyncio

    results: list[tuple[int, "BenchmarkResult"]] = []
    print(f"Concurrency sweep: {levels}")
    worst_rc = 0

    for c in levels:
        print(f"\n--- concurrency={c} ---")
        try:
            result = asyncio.run(run_one(c))
        except KeyboardInterrupt:
            print("\nSweep interrupted.")
            return 130
        if result.total_requests == 0:
            print(f"  (no requests completed — skipping remaining levels)")
            break
        _print_benchmark_summary(result, args, duration, c)
        if result.stream_metrics is not None:
            _print_stream_section(result)
        if result.bidi_metrics is not None:
            _print_bidi_section(result)
        results.append((c, result))

        # Per-level threshold gate (A4)
        level_rc = _check_threshold_gate(result, args)
        if level_rc == 99:
            worst_rc = 99

        if args.latency_threshold is not None and result.p99 > args.latency_threshold:
            print(f"  p99 {result.p99:.2f}ms > {args.latency_threshold}ms threshold — stopping sweep")
            break

    # Sweep summary table
    if results:
        print(f"\n{'='*70}")
        print(f"Sweep Summary ({args.model})")
        print(f"{'C':>4} {'Throughput':>10} {'p50':>8} {'p90':>8} {'p95':>8} {'p99':>8} {'max':>8}")
        print("-" * 62)
        for c, r in results:
            print(f"{c:>4} {r.throughput:>10.1f} {r.p50:>8.1f} {r.p90:>8.1f} "
                  f"{r.p95:>8.1f} {r.p99:>8.1f} {r.max_latency:>8.1f}")
        print(f"{'='*70}")
    else:
        # A5: all levels failed → exit 1 (consistent with single-run mode)
        return 1

    # Export sweep results
    if args.export:
        _export_sweep_results(args, results)

    return worst_rc


def _check_threshold_gate(result: "BenchmarkResult", args) -> int:
    """Check threshold violations for a single benchmark result.

    Returns 99 if any threshold is violated, 0 otherwise.
    """
    violations = []
    if args.max_error_rate is not None:
        rate = result.failed / result.total_requests if result.total_requests else 0
        if rate > args.max_error_rate:
            violations.append(f"error rate {rate:.3f} > {args.max_error_rate}")
    if args.max_p99 is not None and result.p99 > args.max_p99:
        violations.append(f"p99 {result.p99:.2f}ms > {args.max_p99}ms")
    # Stream thresholds (R3) — use getattr for backward compat
    max_ttft_ms = getattr(args, "max_ttft_ms", None)
    max_rtf = getattr(args, "max_rtf", None)
    if max_ttft_ms is not None and result.stream_metrics is not None:
        ttft_p99 = result.stream_metrics.ttft_ms.get("p99", 0.0)
        if ttft_p99 > max_ttft_ms:
            violations.append(
                f"TTFT p99 {ttft_p99:.2f}ms > {max_ttft_ms}ms"
            )
    if max_rtf is not None and result.stream_metrics is not None:
        rtf = result.stream_metrics.rtf
        if rtf is not None:
            rtf_p99 = rtf.get("p99", 0.0)
            if rtf_p99 > max_rtf:
                violations.append(
                    f"RTF p99 {rtf_p99:.2f} > {max_rtf}"
                )
    # goodput/SLO attainment gate (批次 4, plan §8.1 A2)
    if getattr(args, "goodput", None) is not None \
            and result.stream_metrics is not None \
            and result.stream_metrics.goodput is not None:
        g = result.stream_metrics.goodput
        if g["attainment"] < g["attainment_target"]:
            violations.append(
                f"SLO attainment {g['attainment']:.3f} < "
                f"{g['attainment_target']:g}"
            )
    if violations:
        for v in violations:
            print(f"  THRESHOLD VIOLATION: {v}")
        return 99
    return 0


def _export_sweep_results(args, results: list[tuple[int, "BenchmarkResult"]]) -> None:
    """Write all sweep level results to --export path."""
    import json
    from datetime import datetime, timezone
    from pathlib import Path

    is_stream = getattr(args, "stream", False)
    is_bidi = getattr(args, "bidi", False)

    export_data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "url": _benchmark_request_url(args),
            "model": args.model,
            "version": args.version,
            "concurrency": args.concurrency,
            "duration": args.duration,
            "requests": args.requests,
            "warmup_requests": args.warmup_requests,
            "grace_period": args.grace_period,
            "payload": _payload_source(args),
            "stream": is_stream,
            "model_type": getattr(args, "model_type", "llm"),
            "endpoint": getattr(args, "endpoint", "events") if is_stream else None,
            "transport": getattr(args, "transport", None) if (is_stream or is_bidi) else None,
            "bidi": is_bidi,
            "pacing_mode": _bidi_pacing_label(args) if is_bidi else None,
            "min_sessions": getattr(args, "min_sessions", 30) if is_bidi else None,
            "cancel_after": getattr(args, "cancel_after", None) if is_stream else None,
            "read_delay_ms": getattr(args, "read_delay_ms", None) if is_stream else None,
            "goodput": getattr(args, "goodput", None),
            "slo_attainment": getattr(args, "slo_attainment", None),
            "tokenizer": getattr(args, "tokenizer", None) if is_stream else None,
            "text_field": getattr(args, "text_field", None) if is_stream else None,
        },
        "mode": "sweep",
        "levels": [
            {"concurrency": c, **r.to_dict()}
            for c, r in results
        ],
    }
    Path(args.export).write_text(json.dumps(export_data, indent=2), encoding="utf-8")
    print(f"  Exported: {args.export}")


def _print_stream_section(result: "BenchmarkResult") -> None:
    """Print streaming-specific metrics section."""
    sm = result.stream_metrics
    if sm is None:
        return

    def _p(d: dict) -> str:
        return f"{d.get('mean', 0):.1f}/{d.get('p50', 0):.1f}/{d.get('p90', 0):.1f}/{d.get('p95', 0):.1f}/{d.get('p99', 0):.1f}"

    print(f"\n  Stream Metrics (mode={sm.model_type}):")
    print(f"    Requests:       {sm.requests}  "
          f"(zero-chunk: {sm.zero_chunk_requests})")
    print(f"    Chunks:         {sm.total_chunks} "
          f"({sm.chunks_per_request.get('mean', 0):.1f}/req)")
    print(f"    TTFT (ms)       [mean/p50/p90/p95/p99]: "
          f"{_p(sm.ttft_ms)}")
    print(f"    E2E (ms)        [mean/p50/p90/p95/p99]: "
          f"{_p(sm.total_ms)}")

    if sm.model_type == "llm":
        if sm.itl_ms is not None:
            print(f"    ITL (ms)        [mean/p50/p90/p95/p99]: "
                  f"{_p(sm.itl_ms)}")
        if sm.tpot_ms is not None:
            print(f"    TPOT (ms)       [mean/p50/p90/p95/p99]: "
                  f"{_p(sm.tpot_ms)}")
        if sm.tokens_per_sec is not None:
            print(f"    Tokens/sec      (decode): {sm.tokens_per_sec:.1f}")
        if sm.tokens_per_sec_e2e is not None:
            print(f"    Tokens/sec      (e2e):    {sm.tokens_per_sec_e2e:.1f}")
        if sm.tokens_per_sec_aggregate is not None:
            print(f"    Tokens/sec      (agg):    {sm.tokens_per_sec_aggregate:.1f}")
        if sm.token_count_basis is not None:
            print(f"    Token basis:    {sm.token_count_basis}")
    elif sm.model_type in ("tts", "stt") and sm.rtf is not None:
        print(f"    RTF             [mean/p50/p90/p95/p99]: "
              f"{_p(sm.rtf)}")

    if sm.goodput is not None:
        g = sm.goodput
        slo_str = " ".join(f"{k}<={v:g}ms" for k, v in g["slo"].items())
        print(f"    SLO ({slo_str}): attainment {g['attainment']:.3f} "
              f"(target {g['attainment_target']:g}) → "
              f"goodput {g['goodput_req_per_sec']:.2f} req/s")


def _print_bidi_section(result: "BenchmarkResult") -> None:
    """Print bidi session metrics section (批次 2)."""
    bm = result.bidi_metrics
    if bm is None:
        return

    def _p(d: dict) -> str:
        return f"{d.get('mean', 0):.1f}/{d.get('p50', 0):.1f}/{d.get('p90', 0):.1f}/{d.get('p95', 0):.1f}/{d.get('p99', 0):.1f}"

    print(f"\n  Bidi Session Metrics (transport={bm.transport}, "
          f"pacing={bm.pacing_mode}):")
    print(f"    Sessions:         {bm.sessions}  (failed: {bm.failed_sessions})")
    print(f"    Open latency (ms) [mean/p50/p90/p95/p99]: "
          f"{_p(bm.open_latency_ms)}")
    if bm.chunk_roundtrip_ms is not None:
        print(f"    Chunk RTT (ms)    [mean/p50/p90/p95/p99]: "
              f"{_p(bm.chunk_roundtrip_ms)}")
    print(f"    Close→final (ms)  [mean/p50/p90/p95/p99]: "
          f"{_p(bm.close_to_final_ms)}")
    print(f"    Session e2e (ms)  [mean/p50/p90/p95/p99]: "
          f"{_p(bm.session_duration_ms)}")
    print(f"    Chunks/session (consumer mean): "
          f"{bm.chunks_per_session.get('mean', 0):.1f}")
    if bm.sessions_per_sec is not None:
        print(f"    Sessions/sec:     {bm.sessions_per_sec:.2f}")


def _bidi_pacing_label(args) -> str:
    """Pacing mode label from CLI args (single source for Pacing + exports)."""
    if getattr(args, "pace", None) is None:
        return "lock_step"
    if getattr(args, "rt_factor", None) is None:
        return "real_time"
    return "speedup"


def _benchmark_request_url(args) -> str:
    """Request URL for benchmark runs — shared by single-run and sweep export.

    grpc bypasses the URL (channel address comes from the --url host:port);
    ws converts the scheme and maps endpoint → /stream|/decoupled-stream.
    """
    transport = getattr(args, "transport", None) or "sse"
    endpoint = getattr(args, "endpoint", "events")
    streamish = getattr(args, "stream", False) or getattr(args, "bidi", False)
    if streamish:
        if transport == "ws":
            base = args.url.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
            path = "stream" if endpoint == "events" else "decoupled-stream"
        elif transport == "h2":
            base, path = args.url, "bidi"
        else:
            base, path = args.url, endpoint
        if args.version:
            return f"{base}/v2/models/{args.model}/versions/{args.version}/{path}"
        return f"{base}/v2/models/{args.model}/{path}"
    if args.version:
        return f"{args.url}/v2/models/{args.model}/versions/{args.version}/infer"
    return f"{args.url}/v2/models/{args.model}/infer"


def _payload_source(args) -> str:
    """Describe the payload source for the export contract (A10)."""
    if args.payload_random is not None:
        return f"random-template: {args.payload_random}"
    if args.payload is not None:
        return "inline"
    if args.payload_file:
        return f"file: {', '.join(args.payload_file)}"
    return "default: {\"input\": 1.0}"


def _print_benchmark_summary(result, args, duration, concurrency) -> None:
    """Print single-run benchmark summary (used by sweep and single modes)."""
    print(f"  Mode:            {'open-loop' if args.rate else 'closed-loop'}")
    if args.rate:
        print(f"  Target rate:     {args.rate} req/s")
    print(f"  Throughput:      {result.throughput:.2f} req/s")
    print(f"  Success/Failed:  {result.successful}/{result.failed}")
    if result.latencies:
        print(f"  p50/p90/p95/p99: {result.p50:.1f}/{result.p90:.1f}/"
              f"{result.p95:.1f}/{result.p99:.1f} ms (max {result.max_latency:.1f})")
    for w in result.warnings:
        print(f"  WARNING: {w}")


def _cmd_benchmark(args):
    """Run benchmark: thin shell over BenchmarkEngine (closed-loop, worker pool).

    CLI responsibilities only: arg validation, httpx target construction,
    output rendering, threshold gate. All measurement logic lives in
    lite_server.benchmark.benchmark.BenchmarkEngine.
    """
    import itertools
    import sys

    # ── Streaming flag defaults (backward compat with non-CLI callers) ──
    if not hasattr(args, "stream"):
        args.stream = False
    if not hasattr(args, "model_type"):
        args.model_type = "llm"
    if not hasattr(args, "stream_read_timeout"):
        args.stream_read_timeout = 300.0
    if not hasattr(args, "max_ttft_ms"):
        args.max_ttft_ms = None
    if not hasattr(args, "max_rtf"):
        args.max_rtf = None
    if not hasattr(args, "endpoint"):
        args.endpoint = "events"
    if not hasattr(args, "transport"):
        args.transport = None
    if not hasattr(args, "bidi"):
        args.bidi = False
    if not hasattr(args, "pace"):
        args.pace = None
    if not hasattr(args, "rt_factor"):
        args.rt_factor = None
    if not hasattr(args, "min_sessions"):
        args.min_sessions = 30
    if not hasattr(args, "cancel_after"):
        args.cancel_after = None
    if not hasattr(args, "read_delay_ms"):
        args.read_delay_ms = None
    if not hasattr(args, "goodput"):
        args.goodput = None
    if not hasattr(args, "slo_attainment"):
        args.slo_attainment = None
    if not hasattr(args, "tokenizer"):
        args.tokenizer = None
    if not hasattr(args, "text_field"):
        args.text_field = None
    if args.transport is None:
        args.transport = "ws" if args.bidi else "sse"

    if args.bidi:
        if args.stream:
            _logger.error("--bidi and --stream are mutually exclusive")
            return 2
        if args.transport == "sse":
            _logger.error("--bidi requires --transport ws, grpc or h2 "
                          "(sse has no bidi mode)")
            return 2
        if args.endpoint != "events":
            _logger.error("--endpoint does not apply to --bidi")
            return 2
        if args.rt_factor is not None and args.pace is None:
            _logger.error("--rt-factor requires --pace")
            return 2
        if args.payload_random is not None:
            _logger.error("--payload-random does not apply to --bidi "
                          "(bidi payload must be a JSON array)")
            return 2
        if args.min_sessions < 1:
            _logger.error("--min-sessions must be >= 1")
            return 2
    elif args.pace is not None or args.rt_factor is not None:
        _logger.error("--pace/--rt-factor require --bidi")
        return 2

    if args.max_ttft_ms is not None and not args.stream:
        _logger.error("--max-ttft-ms requires --stream")
        return 2
    if args.max_rtf is not None:
        if not args.stream:
            _logger.error("--max-rtf requires --stream")
            return 2
        if args.model_type not in ("tts", "stt"):
            _logger.error("--max-rtf requires --model-type tts or stt, got %s",
                          args.model_type)
            return 2
    if args.endpoint != "events" and not args.stream and not args.bidi:
        _logger.error("--endpoint requires --stream")
        return 2
    if args.transport != "sse" and not args.stream and not args.bidi:
        _logger.error("--transport requires --stream")
        return 2
    if args.transport == "h2" and args.stream:
        _logger.error("--transport h2 is bidi-only (use --bidi)")
        return 2
    if (args.cancel_after is not None or args.read_delay_ms is not None) \
            and not args.stream:
        _logger.error("--cancel-after/--read-delay-ms require --stream")
        return 2

    # goodput/SLO (批次 4, plan §8.1)
    goodput_slo = None
    if args.goodput is not None:
        if not args.stream:
            _logger.error("--goodput requires --stream")
            return 2
        from lite_server.benchmark.stream_metrics import parse_goodput

        try:
            goodput_slo = parse_goodput(args.goodput)
        except ValueError as e:
            _logger.error("--goodput: %s", e)
            return 2
        if "tpot" in goodput_slo and args.model_type != "llm":
            _logger.error("--goodput tpot requires --model-type llm, got %s",
                          args.model_type)
            return 2
    if args.slo_attainment is not None:
        if goodput_slo is None:
            _logger.error("--slo-attainment requires --goodput")
            return 2
        if not 0.0 < args.slo_attainment <= 1.0:
            _logger.error("--slo-attainment must be in (0, 1]")
            return 2

    # tokenizer (批次 4, plan §8.2)
    token_counter = None
    if args.tokenizer is not None:
        if not args.stream:
            _logger.error("--tokenizer requires --stream")
            return 2
        if args.model_type != "llm":
            _logger.error("--tokenizer requires --model-type llm, got %s",
                          args.model_type)
            return 2
        from lite_server.benchmark.token_counter import (
            TokenizerCounter,
            TokenizerLoadError,
            load_tokenizer,
        )

        try:
            _tokenizer = load_tokenizer(args.tokenizer)
        except TokenizerLoadError as e:
            _logger.error("%s", e)
            return 2
        token_counter = TokenizerCounter(_tokenizer, text_field=args.text_field)
    elif args.text_field is not None:
        _logger.error("--text-field requires --tokenizer")
        return 2

    # Parse concurrency — single int or sweep range
    try:
        concurrency_levels = _parse_concurrency(args.concurrency)
    except ValueError as e:
        _logger.error("--concurrency: %s", e)
        return 2
    sweep_mode = len(concurrency_levels) > 1

    from lite_server.benchmark.benchmark import (
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

    if args.bidi:
        for p in payloads:
            if not isinstance(p, list):
                _logger.error("--bidi payload must be a JSON array: "
                              "[open_payload, chunk1, chunk2, ...]")
                return 2

    pacing = None
    if args.bidi:
        from lite_server.benchmark.bidi_session import Pacing

        pacing_mode_label = _bidi_pacing_label(args)
        if pacing_mode_label == "lock_step":
            pacing = Pacing(mode="lock_step")
        elif pacing_mode_label == "real_time":
            pacing = Pacing(mode="real_time", pace_secs=args.pace)
        else:
            pacing = Pacing(mode="speedup", pace_secs=args.pace / args.rt_factor)

    if args.payload_random is not None:
        try:
            template = json.loads(args.payload_random)
        except json.JSONDecodeError as e:
            _logger.error("payload-random template is not valid JSON: %s", e)
            return 2
        payload_factory = _random_payload_factory(template)
    else:
        payload_factory = None

    duration = args.duration
    if duration is None and args.requests is None:
        duration = 30.0

    # URL: streaming/bidi path depends on transport; grpc bypasses URL (uses
    # the --url host:port as channel address)
    url = _benchmark_request_url(args)

    async def run_benchmark(c: int | None = None):
        import httpx

        concurrency = c if c is not None else concurrency_levels[0]
        mode = f"duration={duration}s" if duration is not None else f"requests={args.requests}"
        if args.bidi:
            stream_label = f", bidi/{pacing.mode}"
        else:
            stream_label = ", streaming" if args.stream else ""
        print(f"Benchmarking {args.model} (concurrency={concurrency}, "
              f"{mode}{stream_label}, warmup={args.warmup_requests})")

        if payload_factory is not None:
            final_payload = payload_factory
        else:
            payload_cycle = itertools.cycle(payloads)
            final_payload = lambda: next(payload_cycle)

        if args.stream:
            read_timeout = args.stream_read_timeout
        else:
            read_timeout = 30.0
        timeout = httpx.Timeout(read_timeout, connect=5.0, pool=5.0)
        limits = httpx.Limits(
            max_connections=max(concurrency, 1),
            max_keepalive_connections=max(concurrency, 1),
            keepalive_expiry=15.0,
        )
        async with httpx.AsyncClient(limits=limits) as client:
            engine = BenchmarkEngine()

            if args.bidi:
                # Bidi session path: ws/grpc transport → run_bidi()
                grpc_channel = None
                if args.transport == "ws":
                    from websockets.asyncio.client import connect as _ws_connect

                    from lite_server.benchmark.ws_bidi_target import ws_bidi_session

                    session = ws_bidi_session(
                        _ws_connect, url, pacing=pacing,
                        idle_timeout=args.stream_read_timeout,
                    )
                elif args.transport == "h2":
                    from lite_server.benchmark.h2_bidi_target import h2_bidi_session

                    session = h2_bidi_session(
                        url, pacing=pacing,
                        idle_timeout=args.stream_read_timeout,
                    )
                else:  # grpc
                    import grpc as _grpc

                    from lite_server.benchmark.grpc_bidi_target import (
                        grpc_bidi_session,
                    )

                    grpc_addr = args.url.split("://", 1)[-1].rstrip("/")
                    grpc_channel = _grpc.aio.insecure_channel(grpc_addr)
                    session = grpc_bidi_session(
                        grpc_channel, args.model, version=args.version,
                        pacing=pacing, idle_timeout=args.stream_read_timeout,
                    )
                try:
                    return await engine.run_bidi(
                        session_runner=session,
                        payload=final_payload,
                        concurrency=concurrency,
                        duration=duration,
                        total_requests=args.requests,
                        warmup_requests=args.warmup_requests,
                        grace_period=args.grace_period,
                        transport=args.transport,
                        pacing_mode=pacing.mode,
                        min_sessions=args.min_sessions,
                    )
                finally:
                    if grpc_channel is not None:
                        await grpc_channel.close()

            if args.stream:
                # Streaming path: transport target → run_stream()
                grpc_channel = None
                if args.transport == "sse":
                    from lite_server.benchmark.sse_target import sse_stream_target

                    stream_target = sse_stream_target(client, url, timeout=timeout)
                elif args.transport == "ws":
                    from websockets.asyncio.client import connect as _ws_connect

                    from lite_server.benchmark.ws_target import ws_stream_target

                    stream_target = ws_stream_target(
                        _ws_connect, url, timeout=args.stream_read_timeout,
                    )
                else:  # grpc
                    import grpc as _grpc

                    from lite_server.benchmark.grpc_target import grpc_stream_target

                    grpc_addr = args.url.split("://", 1)[-1].rstrip("/")
                    grpc_channel = _grpc.aio.insecure_channel(grpc_addr)
                    stream_target = grpc_stream_target(
                        grpc_channel, args.model, version=args.version,
                        decoupled=(args.endpoint == "decoupled"),
                        timeout=args.stream_read_timeout,
                    )

                # Scenario wrappers (plan §7.2): cancel / slow-consumer injection
                if args.cancel_after is not None:
                    from lite_server.benchmark.scenario import with_cancel_after

                    stream_target = with_cancel_after(
                        stream_target, args.cancel_after,
                    )
                if args.read_delay_ms is not None:
                    from lite_server.benchmark.scenario import with_read_delay

                    stream_target = with_read_delay(
                        stream_target, args.read_delay_ms / 1000.0,
                    )

                # STT request_meta: extract audio_duration_ms from payload
                request_meta_fn = None
                if args.model_type == "stt":
                    def _stt_request_meta(p: dict) -> dict | None:
                        val = p.get("audio_duration_ms")
                        if isinstance(val, (int, float)):
                            return {"audio_duration_ms": float(val)}
                        return None
                    request_meta_fn = _stt_request_meta

                try:
                    return await engine.run_stream(
                        target=stream_target,
                        payload=final_payload,
                        concurrency=concurrency,
                        duration=duration,
                        total_requests=args.requests,
                        warmup_requests=args.warmup_requests,
                        grace_period=args.grace_period,
                        rate=args.rate,
                        model_type=args.model_type,
                        request_meta=request_meta_fn,
                        goodput_slo=goodput_slo,
                        slo_attainment_target=(
                            args.slo_attainment
                            if args.slo_attainment is not None else 0.95
                        ),
                        token_counter=token_counter,
                    )
                finally:
                    if grpc_channel is not None:
                        await grpc_channel.close()
            else:
                # Non-streaming path (unchanged)
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

                return await engine.run(
                    target=target,
                    payload=final_payload,
                    concurrency=concurrency,
                    duration=duration,
                    total_requests=args.requests,
                    warmup_requests=args.warmup_requests,
                    grace_period=args.grace_period,
                    rate=args.rate,
                )

    # Windows: use SelectorEventLoop for subprocess compat
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    if sweep_mode:
        return _run_concurrency_sweep(
            args, concurrency_levels, duration,
            lambda c: run_benchmark(c),
        )

    try:
        result = asyncio.run(run_benchmark())
    except ValueError as e:
        _logger.error("%s", e)
        return 2
    except KeyboardInterrupt:
        print("\nBenchmark interrupted.")
        return 130

    if result.total_requests == 0:
        print("No requests completed — is the server running?")
        return 1

    if token_counter is not None and token_counter.missed_chunks > 0:
        result.warnings.append(
            f"{token_counter.missed_chunks} chunks had no text to tokenize "
            f"(field: {args.text_field or 'text/token'}) — counted as 0 tokens"
        )

    print(f"\nBenchmark Results ({args.model}):")
    mode_label = "open-loop" if args.rate else "closed-loop"
    latency_label = "open-loop (realistic)" if args.rate else "closed-loop (service-time latencies)"
    print(f"  Mode:            {mode_label} ({latency_label})")
    if args.rate:
        print(f"  Target rate:     {args.rate} req/s")
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
        print("  Latency (ms) [service-time, percentile method: linear]:")
        print(f"    mean: {result.mean_latency:.2f}")
        print(f"    p50:  {result.p50:.2f}")
        print(f"    p90:  {result.p90:.2f}")
        print(f"    p95:  {result.p95:.2f}")
        print(f"    p99:  {result.p99:.2f}")
        print(f"    min:  {result.min_latency:.2f}")
        print(f"    max:  {result.max_latency:.2f}")
        # CO-corrected latencies (always computed; non-zero gap from
        # service-time is the coordinated-omission detection signal)
        if result.successful >= 2:
            print("  Latency (ms) [CO-corrected, percentile method: linear]:")
            print(f"    p50:  {result._co_corrected_percentile(0.50):.2f}")
            print(f"    p90:  {result._co_corrected_percentile(0.90):.2f}")
            print(f"    p95:  {result._co_corrected_percentile(0.95):.2f}")
            print(f"    p99:  {result._co_corrected_percentile(0.99):.2f}")
            c_max = max(result.corrected_latencies) if result.corrected_latencies else 0.0
            print(f"    max:  {c_max:.2f}")

    for w in result.warnings:
        print(f"  WARNING: {w}")

    # ── Stream section ──────────────────────────────────────────────────
    if result.stream_metrics is not None:
        _print_stream_section(result)
    if result.bidi_metrics is not None:
        _print_bidi_section(result)

    if args.export:
        from datetime import datetime, timezone
        export_data = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "config": {
                "url": url,
                "model": args.model,
                "version": args.version,
                "concurrency": concurrency_levels[0] if not sweep_mode else args.concurrency,
                "duration": duration,
                "requests": args.requests,
                "warmup_requests": args.warmup_requests,
                "grace_period": args.grace_period,
                "payload": _payload_source(args),
                "stream": args.stream,
                "model_type": args.model_type,
                "endpoint": args.endpoint if args.stream else None,
                "transport": args.transport if (args.stream or args.bidi) else None,
                "bidi": args.bidi,
                "pacing_mode": pacing.mode if args.bidi else None,
                "min_sessions": args.min_sessions if args.bidi else None,
                "cancel_after": args.cancel_after if args.stream else None,
                "read_delay_ms": args.read_delay_ms if args.stream else None,
                "goodput": args.goodput,
                "slo_attainment": args.slo_attainment,
                "tokenizer": args.tokenizer if args.stream else None,
                "text_field": args.text_field if args.stream else None,
            },
            **result.to_dict(),
        }
        Path(args.export).write_text(json.dumps(export_data, indent=2), encoding="utf-8")
        print(f"  Exported: {args.export}")

    return _check_threshold_gate(result, args)


def _cmd_analyze(args):
    """Analyze a model: thin shell over StaticAnalyzer (pure AST, zero execution).

    Exit code protocol: 0 = no finding at --fail-severity, 1 = finding(s) at
    or above it, 2 = analysis itself failed (path escape, not found, ...).
    """
    from lite_server.analyzer.report import ReportGenerator
    from lite_server.analyzer.static import StaticAnalyzer

    try:
        analyzer = StaticAnalyzer(Path(args.model_repo))
        report = analyzer.analyze_model(
            args.model, version=args.version,
            deep=args.deep, deep_timeout=args.deep_timeout,
            profile=args.profile,
        )
    except (ValueError, FileNotFoundError) as e:
        _logger.error("%s", e)
        return 2

    command = f"lite-server analyze --model {args.model}"
    if args.version:
        command += f" --version {args.version}"
    if args.deep:
        command += " --deep"
    if args.profile:
        command += f" --profile {args.profile}"
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
