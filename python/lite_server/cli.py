"""Command-line interface for lite-server."""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path


def main(argv=None):
    parser = argparse.ArgumentParser(prog="lite-server")
    parser.add_argument("-v", "--version", action="version", version="lite-server 0.1.0")
    subparsers = parser.add_subparsers(dest="command")

    # serve
    serve_parser = subparsers.add_parser("serve", help="Start the inference server")
    serve_parser.add_argument("--config", "-c", help="Path to YAML configuration file")
    serve_parser.add_argument("--port", type=int, help="HTTP server port")
    serve_parser.add_argument("--host", help="Bind address")
    serve_parser.add_argument("--model-repo", help="Model repository path")
    serve_parser.add_argument("--timeout", type=float, help="Request timeout")
    serve_parser.add_argument("--log-level", help="Log level")
    serve_parser.add_argument("--no-metrics", action="store_true", help="Disable metrics")

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
    pack_parser.add_argument("--output", "-o", default="./artifacts")

    # unpack
    unpack_parser = subparsers.add_parser("unpack", help="Unpack artifact")
    unpack_parser.add_argument("artifact", help="Path to .lma file")
    unpack_parser.add_argument("--to", dest="target_dir", default=".")

    # init
    init_parser = subparsers.add_parser("init", help="Initialize project")
    init_parser.add_argument("project_name", nargs="?")
    init_parser.add_argument("--template", "-t", default="basic")

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
    """Start the Rust server via subprocess."""
    import subprocess
    import shutil

    cmd = ["lite-server-core"]
    if args.config:
        cmd.extend(["serve", "--config", args.config])
    else:
        cmd.append("serve")
    if args.port:
        cmd.extend(["--port", str(args.port)])
    if args.host:
        cmd.extend(["--host", args.host])
    if args.model_repo:
        cmd.extend(["--model-repo", args.model_repo])
    if args.timeout:
        cmd.extend(["--timeout", str(args.timeout)])
    if args.log_level:
        cmd.extend(["--log-level", args.log_level])
    if args.no_metrics:
        cmd.append("--no-metrics")

    # Check if lite-server-core binary exists
    binary = shutil.which("lite-server-core")
    if not binary:
        print("Error: lite-server-core binary not found. Please install lite-server first.", file=sys.stderr)
        return 1

    try:
        subprocess.run(cmd, check=True)
    except KeyboardInterrupt:
        print("\nShutting down...")
    return 0


def _cmd_config_check(args):
    try:
        import yaml
        with open(args.config, "r") as f:
            cfg = yaml.safe_load(f)
        print(f"Configuration OK: {args.config}")
        if "server" in cfg:
            s = cfg["server"]
            print(f"  HTTP port: {s.get('http_port', 8000)}")
            print(f"  gRPC port: {s.get('grpc_port', 8001)}")
            print(f"  Metrics port: {s.get('metrics_port', 8002)}")
        if "model_repository" in cfg:
            print(f"  Model repo: {cfg['model_repository'].get('path', './model_repo')}")
        return 0
    except Exception as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 1


def _cmd_benchmark(args):
    """Run benchmark against running server."""
    import httpx
    import time
    import statistics
    from concurrent.futures import ThreadPoolExecutor, as_completed

    payload = {"input": 1.0}  # default payload

    url = f"{args.url}/v2/models/{args.model}/infer"
    if args.version:
        url = f"{args.url}/v2/models/{args.model}/versions/{args.version}/infer"

    print(f"Benchmarking {args.model} (concurrency={args.concurrency}, duration={args.duration}s)")

    results = []
    start_time = time.time()
    end_time = start_time + args.duration

    def send_request():
        try:
            t0 = time.time()
            resp = httpx.post(url, json=payload, timeout=30.0)
            t1 = time.time()
            return {"success": resp.status_code == 200, "latency_ms": (t1 - t0) * 1000}
        except Exception as e:
            return {"success": False, "error": str(e)}

    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = []
        while time.time() < end_time:
            futures.append(executor.submit(send_request))
            if len(futures) >= args.concurrency:
                for f in as_completed(futures[:args.concurrency]):
                    results.append(f.result())
                futures = futures[args.concurrency:]

        for f in as_completed(futures):
            results.append(f.result())

    total = len(results)
    success = sum(1 for r in results if r.get("success"))
    failed = total - success
    latencies = [r["latency_ms"] for r in results if r.get("success")]

    duration = args.duration
    throughput = total / duration if duration > 0 else 0

    print(f"\nBenchmark Results ({args.model}):")
    print(f"  Duration:        {duration}s")
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
        print(f"Model not found: {model_dir}", file=sys.stderr)
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
        print(f"Model directory not found: {model_dir}", file=sys.stderr)
        return 1

    # Load signing key if configured
    sign_key = None
    key_env = os.environ.get("LITE_SERVER_SIGN_KEY")
    if key_env:
        sign_key = _load_or_create_key(Path(key_env))

    packer = ModelPacker(model_dir, version=args.version)
    artifact_path = packer.pack(args.output, sign_key=sign_key)

    print(f"Packed artifact: {artifact_path}")
    if sign_key:
        print(f"  Signed with key: {key_env}")
    return 0


def _cmd_unpack(args):
    """Unpack artifact with optional signature verification."""
    from lite_server.artifact import ModelUnpacker, _load_or_create_key

    artifact_path = Path(args.artifact)
    if not artifact_path.exists():
        print(f"Artifact not found: {artifact_path}", file=sys.stderr)
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
        print(f"Validation failed: {e}", file=sys.stderr)
        return 1

    target_dir = Path(args.target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    unpacker.unpack(target_dir)
    print(f"Extracted to: {target_dir}")
    return 0


def _cmd_init(args):
    """Initialize a new lite-server project from a template."""
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
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
