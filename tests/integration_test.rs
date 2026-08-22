use serde_json::{json, Value};
use serial_test::serial;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tokio::time::sleep;

#[cfg(unix)]
use std::os::unix::process::CommandExt;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn project_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn lite_server_bin() -> std::path::PathBuf {
    project_root()
        .join("target")
        .join("debug")
        .join("lite-server-core")
}

/// Hand out a monotonically-unique TCP port for a dedicated-server test.
///
/// `kill_stale_on_port` pre-emptively `SIGKILL`s any `lite-server` holding the
/// port. If two tests shared a fixed port and ran at once — and a `#[serial]`
/// test is only excluded from *other serial* tests, not from non-serial ones —
/// one test's `kill_stale_on_port` would murder the other's live server
/// mid-flight. Under default-thread parallelism that surfaces as a server
/// dying mid-test and, depending on timing, a hang the runner `SIGKILL`s with
/// no FAILED line.
///
/// A per-process monotonic allocator guarantees no two concurrent tests ever
/// share a port, so `kill_stale_on_port` can only ever find THIS test's own
/// stale server (a previous crashed run reused the same reset counter value),
/// never a sibling's. The counter resets each process; a stale server left by
/// a crashed run is reclaimed by the next run's `kill_stale_on_port` on the
/// same number.
fn next_test_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    // Above every fixed port still baked into server_yaml literals (18020-18391)
    // and below the macOS/Linux ephemeral ranges.
    static NEXT: AtomicU16 = AtomicU16::new(19000);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

fn start_server(args: &[&str]) -> std::process::Child {
    let mut cmd = Command::new(lite_server_bin());
    cmd.arg("serve")
        .current_dir(project_root())
        // Nobody reads the child's stdout: a pipe would fill its 64KB buffer
        // and block the server on write. null also keeps the orphaned-server
        // footprint minimal when cleanup never runs.
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        // Self-terminate when the test binary (our parent) dies — including
        // SIGKILL, which atexit/ServerGuard cannot catch. The server watches
        // its parent pid and runs its normal graceful shutdown (reaping python
        // workers) once reparented to init. Test-only flag; production never
        // sets it, so the watchdog is not even spawned there.
        .env("LITESERVER_DIE_WITH_PARENT", "1");

    for arg in args {
        cmd.arg(arg);
    }
    // Test servers do I/O only (real work lives in the Python workers) —
    // 2 tokio threads each instead of the default CPU count. With dozens of
    // dedicated servers under full-suite parallelism this removes pure
    // scheduling waste (a startup-time flake factor).
    if !args.contains(&"--threads") {
        cmd.args(["--threads", "2"]);
    }

    #[cfg(unix)]
    unsafe {
        cmd.pre_exec(|| {
            libc::setpgid(0, 0);
            Ok(())
        });
    }

    cmd.spawn().expect("Failed to start server")
}

fn stop_server(mut child: std::process::Child) {
    #[cfg(unix)]
    {
        let pid = child.id() as i32;
        let _ = unsafe { libc::kill(-pid, libc::SIGKILL) };
    }
    #[cfg(not(unix))]
    {
        let _ = child.kill();
    }
    let _ = child.wait();
}

/// RAII guard that kills a dedicated server on drop — including when the test
/// panics before an explicit `stop_server()`. Without it, a failing
/// dedicated-server test orphans the server, which inherits the test's stderr
/// pipe (the `2>&1 | tail` of the runner) so the pipe never sees EOF and the
/// whole test command hangs.
struct ServerGuard(Option<std::process::Child>);

impl ServerGuard {
    fn start(args: &[&str]) -> Self {
        ServerGuard(Some(start_server(args)))
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(child) = self.0.take() {
            stop_server(child);
        }
    }
}

/// Create a self-contained model_repo in a temp directory with test_model.
fn create_test_model_repo() -> std::path::PathBuf {
    let tmp = std::env::temp_dir().join(format!("lite-server-test-{}", std::process::id()));
    let model_dir = tmp.join("test_model/1");
    std::fs::create_dir_all(&model_dir).unwrap();

    std::fs::write(
        model_dir.join("model.py"),
        r#"from lite_server import LitAPI

from lite_server.exceptions import BadRequestError


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        value = request.get("input", 0)
        if value is None:
            raise BadRequestError(
                "input must not be null", code="invalid_input", param="input")
        return value

    def predict(self, x):
        return {"output": x * 2}

    def encode_response(self, output):
        return output
"#,
    )
    .unwrap();

    std::fs::write(
        model_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();

    // bin_model: unary binary passthrough (P1) — encode_response declares a
    // non-JSON media_type and returns raw bytes; the server must forward them
    // verbatim instead of JSON-parsing (which collapses non-JSON to `{}`).
    let bin_dir = tmp.join("bin_model/1");
    std::fs::create_dir_all(&bin_dir).unwrap();
    std::fs::write(
        bin_dir.join("model.py"),
        r#"from lite_server import LitAPI
from lite_server.response import Response


class BinAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request

    def predict(self, x):
        return bytes(range(256))

    def encode_response(self, output):
        return Response(content=output, media_type="application/octet-stream")
"#,
    )
    .unwrap();
    std::fs::write(
        bin_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();

    // ord_model: multi-key JSON response. The unary JSON path re-encodes via
    // serde_json (BTreeMap → sorted keys); byte-identity of that output is
    // pinned by test_unary_json_response_byte_identical so the P1 passthrough
    // change cannot silently alter the JSON path.
    let ord_dir = tmp.join("ord_model/1");
    std::fs::create_dir_all(&ord_dir).unwrap();
    std::fs::write(
        ord_dir.join("model.py"),
        r#"from lite_server import LitAPI


class OrdAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request

    def predict(self, x):
        return {"z": 1, "a": 2}

    def encode_response(self, output):
        return output
"#,
    )
    .unwrap();
    std::fs::write(
        ord_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();

    // raw_echo_model: accepts raw bytes requests (application/octet-stream)
    // and echoes the byte length back as JSON — end-to-end proof that the
    // Content-Type dispatch reaches the worker with raw bytes intact (D9).
    let raw_echo_dir = tmp.join("raw_echo_model/1");
    std::fs::create_dir_all(&raw_echo_dir).unwrap();
    std::fs::write(
        raw_echo_dir.join("model.py"),
        r#"from lite_server import LitAPI


class RawEchoAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        if isinstance(request, bytes):
            return {"raw_len": len(request)}
        return request  # JSON path

    def predict(self, x):
        return x

    def encode_response(self, output):
        return output
"#,
    )
    .unwrap();
    std::fs::write(
        raw_echo_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();

    // batch_model: max_batch_size=2 so concurrent requests aggregate into
    // one BatchRequest.  predict() records the aggregated batch size in each
    // item's body; encode_response() gives the "bad" item its own 400 status
    // and X-Item header.
    let batch_dir = tmp.join("batch_model/1");
    std::fs::create_dir_all(&batch_dir).unwrap();
    std::fs::write(
        batch_dir.join("model.py"),
        r#"from lite_server import LitAPI
from lite_server.response import Response


class BatchAPI(LitAPI):
    def setup(self, device):
        pass

    def batch(self, inputs):
        return inputs

    def predict(self, batched):
        n = len(batched)
        return [dict(item, batch_size=n) for item in batched]

    def unbatch(self, output):
        return output

    def encode_response(self, output):
        if output.get("kind") == "bad":
            return Response(
                content={"error": "bad item"},
                status_code=400,
                headers={"X-Item": "bad"},
            )
        return output
"#,
    )
    .unwrap();
    std::fs::write(
        batch_dir.join("config.yaml"),
        "max_batch_size: 2\nbatch_timeout: 0.1\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();

    // policy_model: declares rate_limit + cors policies in config.yaml;
    // the Rust HTTP layer enforces them per model version.
    let policy_dir = tmp.join("policy_model/1");
    std::fs::create_dir_all(&policy_dir).unwrap();
    std::fs::write(
        policy_dir.join("model.py"),
        r#"from lite_server import LitAPI


class PolicyAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x * 2}

    def encode_response(self, output):
        return output
"#,
    )
    .unwrap();
    std::fs::write(
        policy_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\npolicies:\n  rate_limit: { requests_per_minute: 3, burst: 3 }\n  cors:\n    allow_origins: [\"https://app.example.com\"]\n    allow_methods: [\"POST\"]\n    allow_headers: [\"content-type\"]\n",
    )
    .unwrap();

    // route_model: declares @route custom routes (phase 2). /status, /pets/{id}
    // (path params), /echo (POST body). The shadow @route.post("/infer") hits a
    // reserved leaf and must be skipped at ingest so it never shadows inference.
    let route_dir = tmp.join("route_model/1");
    std::fs::create_dir_all(&route_dir).unwrap();
    std::fs::write(
        route_dir.join("model.py"),
        r#"from lite_server import LitAPI, route


class RouteAPI(LitAPI):
    def setup(self, device):
        self.loaded = True

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x * 2}

    def encode_response(self, output):
        return output

    @route.get("/status")
    def status(self, ctx):
        return {"model_loaded": self.loaded, "method": ctx.meta.method, "version": "v1"}

    @route.get("/pets/{pet_id}")
    def get_pet(self, ctx):
        return {"pet_id": ctx.state["path_params"]["pet_id"]}

    @route.post("/echo")
    def echo(self, ctx):
        return {"echo": ctx.request, "method": ctx.meta.method}

    @route.post("/infer")  # reserved leaf → skipped at ingest (warn)
    def shadow(self, ctx):
        return {"shadowed": True}

    @route.get("/livez")  # root probe lives at /livez — no model-namespace collision (B3)
    def livez(self, ctx):
        return {"alive": True}

    @route.get("/models")
    def models(self, ctx):
        # ctx.server: live registry of the hosting server (phase 2b)
        return {"loaded": ctx.server.registry.list_loaded()}

    @route.post("/call_test_model")
    async def call_test_model(self, ctx):
        # cross-model inference through ctx.server (phase 2b)
        out = await ctx.server.inference.infer("test_model", {"input": 7})
        return {"test_model_out": out}

    @route.post("/call_self")
    async def call_self(self, ctx):
        # self-inference must raise ValueError (deadlock guard, phase 2b)
        out = await ctx.server.inference.infer("route_model", {"input": 7})
        return {"self_out": out}

    @route.get("/ticks")
    def ticks(self, ctx):
        # StreamingResponse: SSE framing by default media type (phase 3)
        from lite_server.response import StreamingResponse

        async def gen():
            for n in (1, 2, 3):
                yield {"n": n}

        return StreamingResponse(content=gen(), headers={"X-Route": "ticks"})

    @route.get("/download")
    def download(self, ctx):
        # non-SSE media type: chunk bytes pass through verbatim (phase 3)
        from lite_server.response import StreamingResponse

        return StreamingResponse(
            content=iter([b"chunk1-", b"chunk2"]),
            media_type="application/octet-stream",
        )
"#,
    )
    .unwrap();
    std::fs::write(
        route_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();

    // route_model v2: same route paths, different handler behavior — used to
    // verify versioned /v2/models/:m/versions/:v/<tail> dispatch isolation.
    let route_v2_dir = tmp.join("route_model/2");
    std::fs::create_dir_all(&route_v2_dir).unwrap();
    std::fs::write(
        route_v2_dir.join("model.py"),
        r#"from lite_server import LitAPI, route


class RouteAPI(LitAPI):
    def setup(self, device):
        self.loaded = True

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x * 2}

    def encode_response(self, output):
        return output

    @route.get("/status")
    def status(self, ctx):
        return {"model_loaded": self.loaded, "method": ctx.meta.method, "version": "v2"}
"#,
    )
    .unwrap();
    std::fs::write(
        route_v2_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();

    // decoupled_model: implements predict_decoupled (P9-1) — pushes N chunks
    // (N = input) then closes. Used by the gRPC DecoupledInfer integration test.
    let decoupled_dir = tmp.join("decoupled_model/1");
    std::fs::create_dir_all(&decoupled_dir).unwrap();
    std::fs::write(
        decoupled_dir.join("model.py"),
        r#"from lite_server import LitAPI


class DecoupledAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    async def predict_decoupled(self, data, sender):
        # Push N chunks (N = decoded input) then close. The channel may stay
        # open past this method's return; here we close inline.
        for i in range(data):
            await sender.send({"index": i})
        await sender.close()

    def encode_response(self, output):
        return output
"#,
    )
    .unwrap();
    std::fs::write(
        decoupled_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();

    // warmup_model (P-WARM): same logic as test_model (doubles input) but with
    // warmup enabled — M7 多样本：sample1 ×2 + sample2 ×1 = 3 dummy inferences
    // must complete before the version becomes Ready. Proves warmup runs the
    // real predict path (multi-sample loop) and still serves traffic afterward.
    let warmup_dir = tmp.join("warmup_model/1");
    std::fs::create_dir_all(warmup_dir.join("warmup")).unwrap();
    std::fs::write(
        warmup_dir.join("model.py"),
        r#"from lite_server import LitAPI


class WarmupAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x * 2}

    def encode_response(self, output):
        return output
"#,
    )
    .unwrap();
    std::fs::write(
        warmup_dir.join("warmup").join("input.json"),
        "{\"input\": 5}\n",
    )
    .unwrap();
    // M7 多样本：第二个样本文件（不同输入形状），iterations 缺省 1。
    std::fs::write(
        warmup_dir.join("warmup").join("input_batch.json"),
        "{\"input\": [5, 6, 7]}\n",
    )
    .unwrap();
    std::fs::write(
        warmup_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\npolicies:\n  warmup:\n    enabled: true\n    samples:\n      - input_ref: warmup/input.json\n        iterations: 2\n      - input_ref: warmup/input_batch.json\n",
    )
    .unwrap();

    // warmup_fail_model (P-WARM / D33): warmup enabled but dummy_input_ref
    // points to a missing file → run_warmup fails reading it → the version is
    // marked Failed with last_failure, never serving.
    let warmup_fail_dir = tmp.join("warmup_fail_model/1");
    std::fs::create_dir_all(&warmup_fail_dir).unwrap();
    std::fs::write(
        warmup_fail_dir.join("model.py"),
        r#"from lite_server import LitAPI


class WarmupFailAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x * 2}

    def encode_response(self, output):
        return output
"#,
    )
    .unwrap();
    std::fs::write(
        warmup_fail_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\npolicies:\n  warmup:\n    enabled: true\n    samples:\n      - input_ref: does_not_exist.json\n",
    )
    .unwrap();

    // ws_probe_model (B2): reports how the worker dispatched the first WS
    // frame — raw bytes vs parsed JSON — plus the content-type it saw in
    // meta.headers, so tests can pin the E3/E4 dispatch and CT normalization.
    let ws_probe_dir = tmp.join("ws_probe_model/1");
    std::fs::create_dir_all(&ws_probe_dir).unwrap();
    std::fs::write(
        ws_probe_dir.join("model.py"),
        r#"from lite_server import LitAPI


def _report(data, ctx):
    ct = ctx.meta.headers.get("content-type", "") if ctx is not None else ""
    if isinstance(data, bytes):
        return {"kind": "bytes", "len": len(data), "ct": ct}
    return {"kind": "json", "ct": ct}


class ProbeHandler:
    def on_open(self, initial_data, ctx=None):
        return _report(initial_data, ctx)

    def on_chunk(self, chunk):
        return None

    def on_close(self):
        pass


class WsProbeAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output

    def bidi_stream(self):
        return ProbeHandler()

    async def predict_decoupled(self, data, sender, ctx=None):
        await sender.send(_report(data, ctx))
        await sender.close()
"#,
    )
    .unwrap();
    std::fs::write(
        ws_probe_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();

    tmp
}

/// Return a cached model_repo path (created once, reused across tests).
fn test_model_repo() -> &'static std::path::PathBuf {
    static REPO: OnceLock<std::path::PathBuf> = OnceLock::new();
    REPO.get_or_init(create_test_model_repo)
}

async fn wait_for_server(port: u16, timeout_secs: u64) {
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{}/health", port);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);

    while tokio::time::Instant::now() < deadline {
        if let Ok(resp) = client.get(&url).timeout(Duration::from_secs(1)).send().await {
            if resp.status() == 200 {
                return;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("Server did not start within {} seconds", timeout_secs);
}

/// Poll until a model is ready. Returns false on timeout.
async fn wait_model_ready(base: &str, model: &str, timeout_secs: u64) -> bool {
    let client = reqwest::Client::new();
    let url = format!("{}/v2/models/{}/ready", base, model);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);

    while tokio::time::Instant::now() < deadline {
        if let Ok(resp) = client.get(&url).timeout(Duration::from_secs(1)).send().await {
            if let Ok(body) = resp.json::<Value>().await {
                if body["ready"].as_bool() == Some(true) {
                    return true;
                }
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Load a model and wait until ready.
async fn load_model(base: &str, model: &str, version: &str) {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/repository/models/{}/versions/{}/load", base, model, version))
        .send()
        .await
        .expect("load request failed");
    assert_eq!(resp.status(), 200, "load failed: {:?}", resp.text().await);
    assert!(
        wait_model_ready(base, model, 30).await,
        "model {} did not become ready",
        model
    );
}

/// Unload a model.
async fn unload_model(base: &str, model: &str, version: &str) {
    let client = reqwest::Client::new();
    let _ = client
        .post(format!("{}/v2/repository/models/{}/versions/{}/unload", base, model, version))
        .send()
        .await;
}

// ---------------------------------------------------------------------------
// P4-2 graceful-shutdown helpers (unix)
// ---------------------------------------------------------------------------

/// A model whose `predict` sleeps `sleep_secs` — used to keep an RPC in-flight
/// across a SIGTERM so drain / abort behavior is observable. `slow_model/1`
/// doubles the input like `test_model`.
#[cfg(unix)]
fn create_slow_model_repo(sleep_secs: u64) -> std::path::PathBuf {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-slow-{}-{}",
        std::process::id(),
        sleep_secs
    ));
    let model_dir = tmp.join("slow_model/1");
    std::fs::create_dir_all(&model_dir).unwrap();
    std::fs::write(
        model_dir.join("model.py"),
        format!(
            r#"from lite_server import LitAPI
import time


class SlowAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        time.sleep({secs})
        return {{"output": x * 2}}

    def encode_response(self, output):
        return output
"#,
            secs = sleep_secs
        ),
    )
    .unwrap();
    std::fs::write(
        model_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
    tmp
}

/// A model with a `bidi_stream` handler (P-FLOW bidi cancel test): `on_chunk`
/// sleeps 5s so a client disconnect lands mid-processing — the reply chunk
/// produced after the sleep is what makes the server's dead-response-stream
/// `tx.send` fail and its cleanup run; `on_close` writes `marker` so the test
/// can observe that the server's StreamCancel reached the worker.
#[cfg(unix)]
fn create_bidi_model_repo(marker: &std::path::Path) -> std::path::PathBuf {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-bidi-{}",
        std::process::id()
    ));
    let model_dir = tmp.join("bidi_model/1");
    std::fs::create_dir_all(&model_dir).unwrap();
    std::fs::write(
        model_dir.join("model.py"),
        format!(
            r#"from lite_server import LitAPI
import time


class BidiHandler:
    def on_open(self, initial_data):
        return {{"opened": True}}

    def on_chunk(self, chunk):
        time.sleep(5)
        return {{"echo": chunk}}

    def on_close(self):
        with open("{marker}", "w") as f:
            f.write("closed")


class BidiAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request

    def predict(self, x):
        return {{"output": x}}

    def encode_response(self, output):
        return output

    def bidi_stream(self):
        return BidiHandler()
"#,
            marker = marker.to_string_lossy()
        ),
    )
    .unwrap();
    std::fs::write(
        model_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
    tmp
}

/// SIGTERM the server's MAIN process only (not its process group) so the Python
/// worker stays alive long enough for an in-flight RPC to complete during drain.
#[cfg(unix)]
fn send_sigterm(child: &std::process::Child) {
    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }
}

/// Poll until the child exits, or `timeout_secs` elapse. Returns true if it
/// exited on its own.
#[cfg(unix)]
async fn wait_for_exit(child: &mut std::process::Child, timeout_secs: u64) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return true,
            _ => {
                if tokio::time::Instant::now() >= deadline {
                    return false;
                }
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Connect a tonic channel to the server's TCP gRPC port.
#[cfg(unix)]
async fn grpc_tcp_channel(grpc_port: u16) -> tonic::transport::Channel {
    tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{}", grpc_port))
        .expect("grpc endpoint")
        .connect()
        .await
        .expect("grpc connect")
}

// ---------------------------------------------------------------------------
// Shared server fixture — one server for most tests
// ---------------------------------------------------------------------------

static SHARED_SERVER: Mutex<Option<Child>> = Mutex::new(None);

const SHARED_PORT: u16 = 18010;

/// Kill any stale lite-server-core process listening on the given port.
fn kill_stale_on_port(port: u16) {
    let output = Command::new("lsof")
        .args(["-ti", &format!(":{}", port)])
        .output();
    if let Ok(out) = output {
        let pids = String::from_utf8_lossy(&out.stdout);
        for pid_str in pids.lines() {
            if let Ok(pid) = pid_str.trim().parse::<i32>() {
                // Verify it's our binary before killing
                let ps = Command::new("ps")
                    .args(["-p", &pid.to_string(), "-o", "comm="])
                    .output();
                if let Ok(ps_out) = ps {
                    let comm = String::from_utf8_lossy(&ps_out.stdout);
                    if comm.contains("lite-server") {
                        unsafe { libc::kill(pid, libc::SIGKILL); }
                    }
                }
            }
        }
    }
}

/// Start the shared server once. Safe to call from async context.
async fn ensure_shared_server() {
    // Scope the mutex guard to a block so it is dropped before we await —
    // holding a std MutexGuard across an await risks deadlock and trips
    // clippy::await_holding_lock.
    let already_started = {
        let mut guard = SHARED_SERVER.lock().unwrap();
        if guard.is_some() {
            true
        } else {
            kill_stale_on_port(SHARED_PORT);
            let repo = test_model_repo();
            let child = start_server(&[
                "--port", &SHARED_PORT.to_string(),
                "--model-repo", &repo.to_string_lossy(),
                "--no-metrics",
                "--no-grpc",
                "--log-level", "warn",
            ]);
            *guard = Some(child);
            register_shared_cleanup();
            false
        }
    };
    wait_for_server(SHARED_PORT, if already_started { 10 } else { 15 }).await;
}

/// Kill the shared server (and its whole process group, Python workers
/// included) when the test binary exits. Statics never drop, so without this
/// hook the server is orphaned (PPID=1) and keeps holding the inherited
/// stderr pipe — background/CI runners then never see EOF and look hung.
/// kill_stale_on_port remains the fallback for SIGKILLed test binaries.
fn register_shared_cleanup() {
    #[cfg(unix)]
    {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            extern "C" fn cleanup() {
                // A poisoned mutex means a panic raced with us; skip and let
                // the next run's kill_stale_on_port handle leftovers.
                if let Ok(guard) = SHARED_SERVER.lock() {
                    if let Some(ref child) = *guard {
                        unsafe { libc::kill(-(child.id() as i32), libc::SIGKILL) };
                    }
                }
            }
            unsafe { libc::atexit(cleanup) };
        });
    }
    // Non-unix: no atexit/process-group kill — acceptable leak, these tests
    // already depend on unix tooling (lsof in kill_stale_on_port).
}

async fn shared_base() -> String {
    ensure_shared_server().await;
    format!("http://127.0.0.1:{}", SHARED_PORT)
}

// ---------------------------------------------------------------------------
// G-test shared server (streaming lifecycle G1-G5/Q2) — one server for the
// five dedicated-server tests in this file. Five separate servers (each with
// Python workers) saturated the late-scheduled test wave and starved
// unrelated tests' startups; one shared server with all G models removes
// that peak without touching test parallelism.
// ---------------------------------------------------------------------------

static G_SERVER: Mutex<Option<Child>> = Mutex::new(None);
const G_PORT: u16 = 18011;
const G_METRICS_PORT: u16 = 18012;

const G_EOF_PY: &str = r#"import time
from lite_server import LitAPI


class EofAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 1)

    def predict(self, x):
        return {"output": x}

    def stream_predict(self, request):
        for i in range(30):
            yield {"index": i}
            time.sleep(0.2)

    def encode_response(self, output):
        return output
"#;

const G_SLOW_PY: &str = r#"import time
from lite_server import LitAPI


class SlowStreamAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 1)

    def predict(self, x):
        return {"output": x}

    def stream_predict(self, request):
        for i in range(6):
            yield {"index": i}
            time.sleep(0.5)

    def encode_response(self, output):
        return output
"#;

const G_PRE_PY: &str = r#"from lite_server import LitAPI


class PreAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("text", "")

    def predict(self, x):
        return {"pre": x}

    def encode_response(self, output):
        return output
"#;

const G_TAIL_PY: &str = r#"from lite_server import LitAPI


class TailAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("pre", "")

    def predict(self, x):
        return {"tokens": x}

    def stream_predict(self, request):
        for w in request.split():
            yield {"token": w}

    def encode_response(self, output):
        return output
"#;

const G_ENS_YAML: &str = r#"ensemble:
  steps:
    - name: pre
      model: ens_pre
      version: "1"
      inputs:
        text: "$request.text"
    - name: tail
      model: ens_tail
      version: "1"
      stream: true
      inputs:
        pre: "$pre.pre"
"#;

const G_GRACE_PY: &str = r#"import asyncio
from lite_server import LitAPI


class GraceAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request

    def predict(self, x):
        return x

    async def predict_decoupled(self, data, sender):
        async def _push():
            for i in range(600):
                if sender.closing:
                    await sender.send({"final": True})
                    await sender.close()
                    return
                await sender.send({"index": i})
                await asyncio.sleep(0.5)
        asyncio.create_task(_push())

    def encode_response(self, output):
        return output
"#;

fn create_g_test_repo() -> &'static std::path::PathBuf {
    static REPO: OnceLock<std::path::PathBuf> = OnceLock::new();
    REPO.get_or_init(|| {
        let tmp = std::env::temp_dir().join(format!("lite-server-gtest-{}", std::process::id()));
        let write_model = |name: &str, py: &str, cfg: &str| {
            let dir = tmp.join(name).join("1");
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("model.py"), py).unwrap();
            std::fs::write(dir.join("config.yaml"), cfg).unwrap();
        };
        write_model("eof_model", G_EOF_PY,
            "max_batch_size: 1
batch_timeout: 0.0
stream: true
accelerator: cpu
devices: 1
workers_per_device: 1
");
        write_model("stream_counted", QUICK_STREAM_PY,
            "max_batch_size: 1
batch_timeout: 0.0
stream: true
accelerator: cpu
devices: 1
workers_per_device: 2
max_requests: 2
");
        write_model("stream_legacy", QUICK_STREAM_PY,
            "max_batch_size: 1
batch_timeout: 0.0
stream: true
accelerator: cpu
devices: 1
workers_per_device: 2
max_requests: 2
count_streams_toward_max_requests: false
");
        write_model("ens_pre", G_PRE_PY,
            "max_batch_size: 1
batch_timeout: 0.0
stream: false
accelerator: cpu
devices: 1
workers_per_device: 1
");
        write_model("ens_tail", G_TAIL_PY,
            "max_batch_size: 1
batch_timeout: 0.0
stream: true
accelerator: cpu
devices: 1
workers_per_device: 2
max_requests: 2
");
        write_model("cap_model", G_SLOW_PY,
            "max_batch_size: 1
batch_timeout: 0.0
stream: true
accelerator: cpu
devices: 1
workers_per_device: 2
max_concurrent_streams: 2
");
        write_model("grace_model", G_GRACE_PY,
            "max_batch_size: 1
batch_timeout: 0.0
stream: true
accelerator: cpu
devices: 1
workers_per_device: 1
max_requests: 1
recycle_stream_drain_timeout_secs: 1
recycle_stream_grace_ms: 3000
");
        // Ensemble model: config.yaml only (no model.py).
        let ens_dir = tmp.join("ens_budget_model").join("1");
        std::fs::create_dir_all(&ens_dir).unwrap();
        std::fs::write(ens_dir.join("config.yaml"), G_ENS_YAML).unwrap();
        tmp
    })
}

async fn ensure_g_server() {
    let already_started = {
        let mut guard = G_SERVER.lock().unwrap();
        if guard.is_some() {
            true
        } else {
            kill_stale_on_port(G_PORT);
            kill_stale_on_port(G_METRICS_PORT);
            let repo = create_g_test_repo();
            let child = start_server(&[
                "--port", &G_PORT.to_string(),
                "--metrics-port", &G_METRICS_PORT.to_string(),
                "--model-repo", &repo.to_string_lossy(),
                "--no-grpc",
                "--log-level", "warn",
            ]);
            *guard = Some(child);
            false
        }
    };
    wait_for_server(G_PORT, if already_started { 10 } else { 60 }).await;
}

async fn g_base() -> String {
    ensure_g_server().await;
    format!("http://127.0.0.1:{}", G_PORT)
}

fn g_metrics_base() -> String {
    format!("http://127.0.0.1:{}", G_METRICS_PORT)
}

// ---------------------------------------------------------------------------
// Health & Info (shared server, no state mutation)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_health_endpoint() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{}/health", base)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    // Structured JSON since phase 3 (no plain-text "ok" compat).
    let body: Value = resp.json().await.unwrap();
    assert!(body["status"].is_string());
    assert!(body["models"].is_array());
}

#[tokio::test]
#[serial]
async fn test_health_probes() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    // livez: checks nothing, always 200
    let resp = client.get(format!("{}/livez", base)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "alive");

    // startupz: nothing Pending/Loading → 200
    let resp = client.get(format!("{}/startupz", base)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "started");

    load_model(&base, MODEL, "1").await;

    // readyz: a serving model exists → 200 and the model is listed
    let resp = client.get(format!("{}/readyz", base)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ready");
    let models: Vec<&str> = body["models"].as_array().unwrap()
        .iter().filter_map(|m| m.as_str()).collect();
    assert!(models.contains(&MODEL), "readyz models: {:?}", models);

    // /health: per-version status (snake_case serde) + loaded_at epoch secs,
    // grouped under the model since §4.5.
    let resp = client.get(format!("{}/health", base)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ready");
    let entry = body["models"].as_array().unwrap().iter()
        .find(|m| m["name"] == MODEL)
        .and_then(|m| m["versions"].as_array().unwrap().iter()
            .find(|e| e["version"] == "1"))
        .expect("test_model/1 must appear in /health");
    assert_eq!(entry["status"], "ready");
    assert!(entry["loaded_at"].as_u64().is_some(),
        "loaded_at must be epoch seconds: {:?}", entry);

    unload_model(&base, MODEL, "1").await;
}

/// With zero loaded models: readyz 503, startupz/livez still green,
/// /health reports not_ready with an empty model list.
#[tokio::test]
async fn test_readyz_503_when_no_models() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let repo = test_model_repo();
    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-metrics",
        "--no-grpc",
        "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    let client = reqwest::Client::new();

    let resp = client.get(format!("{}/readyz", base)).send().await.unwrap();
    assert_eq!(resp.status(), 503);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "not_ready");

    let resp = client.get(format!("{}/startupz", base)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let resp = client.get(format!("{}/livez", base)).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client.get(format!("{}/health", base)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "not_ready");
    assert_eq!(body["models"].as_array().unwrap().len(), 0);
}

/// Model-level health: field renamed model→name, workers carry `ejected`;
/// /v2/models status is snake_case serde rather than Debug formatting.
#[tokio::test]
#[serial]
async fn test_model_health_fields() {
    let base = shared_base().await;
    let client = reqwest::Client::new();
    load_model(&base, MODEL, "1").await;

    let resp = client
        .get(format!("{}/v2/models/{}/health", base, MODEL))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["name"], MODEL);
    assert!(body.get("model").is_none(), "field must be renamed to name");
    let workers = body["workers"].as_array().unwrap();
    assert!(!workers.is_empty());
    assert_eq!(workers[0]["ejected"], false);

    let resp = client.get(format!("{}/v2/models", base)).send().await.unwrap();
    let body: Value = resp.json().await.unwrap();
    let entry = body["models"].as_array().unwrap().iter()
        .find(|m| m["name"] == MODEL)
        .expect("test_model must appear in /v2/models");
    assert_eq!(entry["status"], "ready");

    unload_model(&base, MODEL, "1").await;
}

/// route_model declares @route.get("/livez"). The root probe lives at /livez,
/// so a model-namespace route with the same leaf does not shadow it and must
/// be served (B3: probes removed from SYSTEM_ROUTE_LEAVES).
#[tokio::test]
#[serial]
async fn test_custom_route_probe_leaf_served() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, ROUTE_MODEL, "1").await;

    let resp = client
        .get(format!("{}/v2/models/{}/livez", base, ROUTE_MODEL))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200, "probe leaf is not reserved in the model namespace");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["alive"], true);

    unload_model(&base, ROUTE_MODEL, "1").await;
}

#[tokio::test]
#[serial]
async fn test_info_endpoint() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{}/info", base)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["server"], "lite-server");
}

// ---------------------------------------------------------------------------
// Model lifecycle (sequential — each test loads/unloads test_model)
// ---------------------------------------------------------------------------

const MODEL: &str = "test_model";
const ROUTE_MODEL: &str = "route_model";

#[tokio::test]
#[serial]
async fn test_model_load_ready_infer_unload() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    // Load
    load_model(&base, MODEL, "1").await;

    // Ready check
    let resp = client
        .get(format!("{}/v2/models/{}/ready", base, MODEL))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ready"], true);

    // List models
    let resp = client.get(format!("{}/v2/models", base)).send().await.unwrap();
    let body: Value = resp.json().await.unwrap();
    let models: Vec<&str> = body["models"].as_array().unwrap()
        .iter().filter_map(|m| m["name"].as_str()).collect();
    assert!(models.contains(&MODEL), "model not in list: {:?}", models);

    // Infer
    let resp = client
        .post(format!("{}/v2/models/{}/infer", base, MODEL))
        .json(&json!({"input": 5}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["output"], 10);

    // Unload
    unload_model(&base, MODEL, "1").await;

    // Verify gone
    let resp = client
        .get(format!("{}/v2/models/{}/ready", base, MODEL))
        .send().await.unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ready"], false);
}

/// P-WARM (§4.3): a model with warmup enabled loads to Ready only after the
/// dummy inferences complete, then serves real traffic normally. A Ready
/// outcome under `warmup.enabled: true` is itself proof the warmup succeeded
/// — a failed dummy inference would have marked the version Failed.
#[tokio::test]
#[serial]
async fn test_warmup_enabled_model_loads_and_serves() {
    let base = shared_base().await;
    let client = reqwest::Client::new();
    load_model(&base, "warmup_model", "1").await;

    // Warmup completed → serving.
    let resp = client
        .get(format!("{}/v2/models/warmup_model/ready", base))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ready"], true);

    // Real traffic still works after warmup (input doubled).
    let resp = client
        .post(format!("{}/v2/models/warmup_model/infer", base))
        .json(&json!({"input": 7}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["output"], 14);

    unload_model(&base, "warmup_model", "1").await;
}

/// P-WARM / D33: warmup failure (missing dummy input file) marks the version
/// Failed with `last_failure`, never serving. The load returns an error and
/// the model stays not-ready.
#[tokio::test]
#[serial]
async fn test_warmup_failure_marks_version_failed() {
    let base = shared_base().await;
    // pool_max_idle_per_host(0): the 5s never-ready wait below idles the
    // pooled connection right into the server's 5s h1 keepalive reap → the
    // next request can race the FIN and eat a RST. Dial fresh per request.
    // Any test reusing one client across a >=5s idle window needs this too.
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .build()
        .unwrap();

    // Load returns an error (WorkerCrashed) — do not use load_model (asserts 200).
    let resp = client
        .post(format!("{}/v2/repository/models/warmup_fail_model/versions/1/load", base))
        .send().await.unwrap();
    assert!(
        !resp.status().is_success(),
        "warmup-failing load must error, got {}",
        resp.status()
    );

    // Give the load a moment to settle into Failed, then confirm it never
    // becomes Ready.
    assert!(
        !wait_model_ready(&base, "warmup_fail_model", 5).await,
        "warmup-failing model must never become ready"
    );

    // /health exposes the Failed state + last_failure reason.
    let resp = client.get(format!("{}/health", base)).send().await.unwrap();
    let body: Value = resp.json().await.unwrap();
    let entry = body["models"].as_array().unwrap().iter()
        .find(|m| m["name"] == "warmup_fail_model")
        .and_then(|m| m["versions"].as_array().unwrap().iter()
            .find(|e| e["version"] == "1"))
        .expect("warmup_fail_model/1 must be registered (Failed) in /health");
    assert_eq!(entry["status"], "failed");
    let reason = entry["last_failure"].as_str().expect("last_failure must be set");
    assert!(
        reason.contains("does_not_exist.json") || reason.contains("dummy input"),
        "last_failure should mention the dummy input: {}",
        reason
    );

    // Clean up the Failed version (its workers were spawned before warmup).
    unload_model(&base, "warmup_fail_model", "1").await;
}

#[tokio::test]
#[serial]
async fn test_model_list_versions() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, MODEL, "1").await;

    let resp = client
        .get(format!("{}/v2/models/{}/versions", base, MODEL))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let versions = body["versions"].as_array().unwrap();
    assert!(versions.iter().any(|v| v["version"] == "1"));

    unload_model(&base, MODEL, "1").await;
}

#[tokio::test]
#[serial]
async fn test_model_infer_versioned() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, MODEL, "1").await;

    let resp = client
        .post(format!("{}/v2/models/{}/versions/1/infer", base, MODEL))
        .json(&json!({"input": 7}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["output"], 14);

    unload_model(&base, MODEL, "1").await;
}

// ---------------------------------------------------------------------------
// Custom routes (@route, phase 2)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_custom_route_status() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, ROUTE_MODEL, "1").await;

    let resp = client
        .get(format!("{}/v2/models/{}/status", base, ROUTE_MODEL))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["model_loaded"], true);
    assert_eq!(body["method"], "GET");

    unload_model(&base, ROUTE_MODEL, "1").await;
}

#[tokio::test]
#[serial]
async fn test_custom_route_path_params() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, ROUTE_MODEL, "1").await;

    let resp = client
        .get(format!("{}/v2/models/{}/pets/123", base, ROUTE_MODEL))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["pet_id"], "123");

    unload_model(&base, ROUTE_MODEL, "1").await;
}

#[tokio::test]
#[serial]
async fn test_custom_route_post_body_and_method() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, ROUTE_MODEL, "1").await;

    let resp = client
        .post(format!("{}/v2/models/{}/echo", base, ROUTE_MODEL))
        .json(&json!({"hello": "world"}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["echo"], json!({"hello": "world"}));
    assert_eq!(body["method"], "POST");

    unload_model(&base, ROUTE_MODEL, "1").await;
}

#[tokio::test]
#[serial]
async fn test_custom_route_reserved_leaf_does_not_shadow_inference() {
    // route_model declares @route.post("/infer") — a reserved leaf. It is
    // skipped at ingest, so /infer must still run real inference, not the
    // custom handler.
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, ROUTE_MODEL, "1").await;

    let resp = client
        .post(format!("{}/v2/models/{}/infer", base, ROUTE_MODEL))
        .json(&json!({"input": 5}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["output"], 10);
    assert!(body.get("shadowed").is_none(), "custom /infer must not shadow");

    unload_model(&base, ROUTE_MODEL, "1").await;
}

#[tokio::test]
#[serial]
async fn test_custom_route_unknown_tail_404() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, ROUTE_MODEL, "1").await;

    let resp = client
        .get(format!("{}/v2/models/{}/no-such-route", base, ROUTE_MODEL))
        .send().await.unwrap();
    assert_eq!(resp.status(), 404);

    unload_model(&base, ROUTE_MODEL, "1").await;
}

#[tokio::test]
#[serial]
async fn test_custom_route_wrong_method_405() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, ROUTE_MODEL, "1").await;

    // /status is GET-only → POST must return 405 (not 404).
    let resp = client
        .post(format!("{}/v2/models/{}/status", base, ROUTE_MODEL))
        .send().await.unwrap();
    assert_eq!(resp.status(), 405);

    unload_model(&base, ROUTE_MODEL, "1").await;
}

#[tokio::test]
#[serial]
async fn test_custom_route_multi_version_isolation() {
    // v1 and v2 declare /status with different handlers. Versioned paths must
    // dispatch to each version's own handler (RouteTable keyed by version).
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, ROUTE_MODEL, "1").await;
    load_model(&base, ROUTE_MODEL, "2").await;

    let resp = client
        .get(format!("{}/v2/models/{}/versions/1/status", base, ROUTE_MODEL))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["version"], "v1");

    let resp = client
        .get(format!("{}/v2/models/{}/versions/2/status", base, ROUTE_MODEL))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["version"], "v2");

    unload_model(&base, ROUTE_MODEL, "1").await;
    unload_model(&base, ROUTE_MODEL, "2").await;
}

#[tokio::test]
#[serial]
async fn test_custom_route_ctx_server_registry() {
    // ctx.server.registry.list_loaded() queries the hosting server live
    // (phase 2b: worker → loopback HTTP GET /v2/models).
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, ROUTE_MODEL, "1").await;

    let resp = client
        .get(format!("{}/v2/models/{}/models", base, ROUTE_MODEL))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let names: Vec<&str> = body["loaded"]
        .as_array().expect("loaded must be an array")
        .iter().filter_map(|m| m["name"].as_str()).collect();
    assert!(names.contains(&ROUTE_MODEL), "loaded={:?}", names);

    unload_model(&base, ROUTE_MODEL, "1").await;
}

#[tokio::test]
#[serial]
async fn test_custom_route_ctx_server_cross_model_infer() {
    // ctx.server.inference.infer() runs inference on a *different* model via
    // loopback HTTP POST /v2/models/:m/infer (phase 2b).
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, MODEL, "1").await;
    load_model(&base, ROUTE_MODEL, "1").await;

    let resp = client
        .post(format!("{}/v2/models/{}/call_test_model", base, ROUTE_MODEL))
        .json(&json!({}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["test_model_out"]["output"], 14);

    unload_model(&base, ROUTE_MODEL, "1").await;
    unload_model(&base, MODEL, "1").await;
}

#[tokio::test]
#[serial]
async fn test_custom_route_ctx_server_self_infer_rejected() {
    // infer() back into the handler's own model+version must raise ValueError
    // (deadlock guard) → handler failure → structured 500.
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, ROUTE_MODEL, "1").await;

    let resp = client
        .post(format!("{}/v2/models/{}/call_self", base, ROUTE_MODEL))
        .json(&json!({}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 500);

    unload_model(&base, ROUTE_MODEL, "1").await;
}

#[tokio::test]
#[serial]
async fn test_custom_route_streaming() {
    // StreamingResponse from a @route handler (phase 3): the default media
    // type frames each chunk as one SSE event; other media types pass chunk
    // bytes through verbatim.
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, ROUTE_MODEL, "1").await;

    let resp = client
        .get(format!("{}/v2/models/{}/ticks", base, ROUTE_MODEL))
        .send().await.unwrap();
    let ticks_status = resp.status();
    let ticks_headers = resp.headers().clone();
    let body = resp.text().await.unwrap();
    assert_eq!(ticks_status, 200, "GET /ticks failed: {body}");
    assert_eq!(
        ticks_headers.get("content-type").unwrap(),
        "text/event-stream"
    );
    assert_eq!(ticks_headers.get("x-route").unwrap(), "ticks");
    assert_eq!(
        body,
        // P1: orjson 紧凑输出(无空格)
        "data: {\"n\":1}\n\ndata: {\"n\":2}\n\ndata: {\"n\":3}\n\n"
    );

    let resp = client
        .get(format!("{}/v2/models/{}/download", base, ROUTE_MODEL))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "application/octet-stream"
    );
    let body = resp.text().await.unwrap();
    assert_eq!(body, "chunk1-chunk2");

    unload_model(&base, ROUTE_MODEL, "1").await;
}

#[tokio::test]
#[serial]
async fn test_model_repository_index() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    // Load a model first so the index is non-empty
    load_model(&base, MODEL, "1").await;

    let resp = client
        .post(format!("{}/v2/repository/index", base))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    // Response can be array or object with models key
    let models = body.as_array()
        .or_else(|| body["models"].as_array())
        .expect("expected array or {models: [...]}");
    assert!(!models.is_empty(), "repository index should not be empty");

    unload_model(&base, MODEL, "1").await;
}

// ---------------------------------------------------------------------------
// Batch aggregation: per-item status codes / headers must stay independent
// ---------------------------------------------------------------------------

const BATCH_MODEL: &str = "batch_model";
const DECOUPLED_MODEL: &str = "decoupled_model";

#[tokio::test]
#[serial]
async fn test_batch_aggregation_per_item_status_and_headers() {
    let base = shared_base().await;
    let client = reqwest::Client::new();
    load_model(&base, BATCH_MODEL, "1").await;

    // Fire two concurrent requests — with max_batch_size=2 and
    // batch_timeout=0.1 they aggregate into ONE BatchRequest.
    let bad = client
        .post(format!("{}/v2/models/{}/infer", base, BATCH_MODEL))
        .json(&json!({"kind": "bad"}))
        .send();
    let ok = client
        .post(format!("{}/v2/models/{}/infer", base, BATCH_MODEL))
        .json(&json!({"kind": "ok"}))
        .send();
    let (bad_resp, ok_resp) = tokio::join!(bad, ok);
    let bad_resp = bad_resp.unwrap();
    let ok_resp = ok_resp.unwrap();

    // Per-item status codes travel independently: the bad item is a 400,
    // the ok item stays a 200 (both would have been 200 before the fix).
    assert_eq!(bad_resp.status(), 400);
    assert_eq!(ok_resp.status(), 200);

    // Per-item headers likewise: only the bad item carries X-Item.
    assert_eq!(bad_resp.headers().get("x-item").unwrap(), "bad");
    assert!(ok_resp.headers().get("x-item").is_none());

    // batch_size=2 proves the two requests really did aggregate into one
    // BatchRequest rather than two Single requests.
    let ok_body: Value = ok_resp.json().await.unwrap();
    assert_eq!(ok_body["batch_size"], 2);
    assert_eq!(ok_body["kind"], "ok");

    let bad_body: Value = bad_resp.json().await.unwrap();
    assert_eq!(bad_body["error"], "bad item");

    unload_model(&base, BATCH_MODEL, "1").await;
}

// ---------------------------------------------------------------------------
// HTTP response compression (P1-4)
// ---------------------------------------------------------------------------

/// With `server.compression: true`: JSON responses are gzipped when the
/// client advertises `accept-encoding: gzip`; SSE streams are excluded
/// (compression would break per-event flush); WS upgrade is unaffected.
#[tokio::test]
#[serial]
async fn test_http_response_compression() {
    use futures::{SinkExt, StreamExt};

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let repo = test_model_repo();
    let tmp_dir =
        std::env::temp_dir().join(format!("lite-server-http-gzip-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 0.0.0.0\n  http_port: {}\n  grpc_port: {}\n  metrics_port: 18092\n  log_level: warn\n  compression: true\nmetrics:\n  enabled: false\ngrpc:\n  enabled: false\nmodel_repository:\n  path: {}\n",
            http_port,
            grpc_port,
            repo.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, MODEL, "1").await;

    // no_gzip: reqwest must not transparently decompress — we inspect the raw
    // response headers/body.
    let client = reqwest::Client::builder().no_gzip().build().unwrap();

    // 1. JSON response is gzip-compressed when the client accepts it.
    let resp = client
        .get(format!("{}/health", base))
        .header("accept-encoding", "gzip")
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-encoding").and_then(|v| v.to_str().ok()),
        Some("gzip"),
        "JSON response must be gzip-compressed"
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(&body[..2], &[0x1f, 0x8b], "body must be gzip-framed");

    // 2. SSE stream is excluded from compression and still delivers frames.
    let mut resp = client
        .post(format!("{}/v2/models/{}/events", base, MODEL))
        .header("accept-encoding", "gzip")
        .json(&json!({"input": 3}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let content_type = resp.headers().get("content-type").unwrap().to_str().unwrap().to_string();
    assert!(content_type.contains("text/event-stream"), "expected SSE content-type, got: {}", content_type);
    assert!(
        resp.headers().get("content-encoding").is_none(),
        "SSE responses must not be compressed"
    );
    // The SSE stream outlives the test (server-side idle timeout ~300s), so
    // read only until the first data frame arrives, then drop the response.
    let mut body = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !body.contains("data:") && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), resp.chunk()).await {
            Ok(Ok(Some(bytes))) => body.push_str(&String::from_utf8_lossy(&bytes)),
            _ => break,
        }
    }
    assert!(body.contains("data:"), "SSE response should contain data frames: {}", body);

    // 3. WS handshake and streaming are unaffected.
    let ws_url = format!("ws://127.0.0.1:{}/v2/models/{}/stream", http_port, MODEL);
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.expect("WS connect failed");
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&json!({"input": 4})).unwrap(),
    )).await.unwrap();
    let msg = ws.next().await.expect("WS must yield a message").unwrap();
    assert!(msg.is_text() || msg.is_binary(), "WS must deliver a data frame");
    let _ = ws.close(None).await;

    unload_model(&base, MODEL, "1").await;
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

// ---------------------------------------------------------------------------
// gRPC Unix domain socket (P4-1)
// ---------------------------------------------------------------------------
//
// `grpc.host: unix:/path` makes the gRPC server bind a UDS instead of TCP.
// Verified end-to-end: a tonic client connects over the socket, an Infer RPC
// drives the worker, the grpc.health.v1 Health RPC answers, and the socket
// file carries the configured permission bits.

#[cfg(unix)]
fn unique_uds_path(label: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    format!(
        "/tmp/lite-server-grpc-uds-{}-{}-{}.sock",
        std::process::id(),
        label,
        n
    )
}

/// Connect a tonic channel to a UDS path, retrying briefly until the server
/// has bound the socket (the HTTP port being up is a hint, not a guarantee).
#[cfg(unix)]
async fn uds_grpc_channel(sock_path: &str) -> tonic::transport::Channel {
    use hyper_util::rt::TokioIo;
    use tower::service_fn;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let path = sock_path.to_string();
        let endpoint = tonic::transport::Endpoint::try_from("http://localhost")
            .expect("static placeholder URI is valid");
        match endpoint
            .connect_with_connector(service_fn(move |_: http::Uri| {
                let path = path.clone();
                async move {
                    Ok::<_, std::io::Error>(TokioIo::new(
                        tokio::net::UnixStream::connect(&path).await?,
                    ))
                }
            }))
            .await
        {
            Ok(channel) => return channel,
            Err(_) if tokio::time::Instant::now() < deadline => {
                sleep(Duration::from_millis(200)).await;
                continue;
            }
            Err(e) => panic!("failed to connect gRPC over UDS {}: {}", sock_path, e),
        }
    }
}

/// Assert a socket file's permission bits (lower 9) match `mode`.
#[cfg(unix)]
fn assert_socket_mode(path: &str, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    let meta = std::fs::metadata(path).unwrap_or_else(|e| panic!("stat {}: {}", path, e));
    assert_eq!(
        meta.permissions().mode() & 0o777,
        mode,
        "socket {} perms mismatch",
        path
    );
}

/// gRPC `grpc.host: unix:/path` drives Infer + Health over the socket, and the
/// default inference-socket permission is 0o666.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_grpc_uds_infer_health_default_mode() {
    use std::collections::HashMap;
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;

    let http_port = next_test_port();
    let grpc_port = next_test_port(); // parsed but unused for bind (UDS)
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let repo = test_model_repo();
    let tmp_dir =
        std::env::temp_dir().join(format!("lite-server-grpc-uds-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let sock = unique_uds_path("default");
    let _ = std::fs::remove_file(&sock);
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {}\n  grpc_port: {}\n  metrics_port: 18096\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\n  host: unix:{}\nmodel_repository:\n  path: {}\n",
            http_port, grpc_port, sock, repo.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, MODEL, "1").await;

    let channel = uds_grpc_channel(&sock).await;

    // Health: overall service "" → SERVING once the model is ready.
    let mut health =
        tonic_health::pb::health_client::HealthClient::new(channel.clone());
    let hresp = health
        .check(tonic_health::pb::HealthCheckRequest { service: String::new() })
        .await
        .expect("Health.check over UDS must succeed")
        .into_inner();
    assert_eq!(
        hresp.status,
        tonic_health::ServingStatus::Serving as i32,
        "overall health should be SERVING once a model is loaded"
    );

    // Infer: test_model doubles the input (21 -> 42).
    let mut infer = LiteServerClient::new(channel);
    let resp = infer
        .infer(InferRequest {
            model_name: MODEL.to_string(),
            version: "1".to_string(),
            data: br#"{"input":21}"#.to_vec().into(),
            headers: HashMap::new(),

        ..Default::default()
})
        .await
        .expect("Infer over UDS must succeed")
        .into_inner();
    let body = String::from_utf8_lossy(&resp.data);
    assert!(
        body.contains("42"),
        "test_model doubles the input (21 -> 42); got: {}",
        body
    );

    // Default inference-socket permission.
    assert_socket_mode(&sock, 0o666);

    unload_model(&base, MODEL, "1").await;
    let _ = std::fs::remove_file(&sock);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// A custom `grpc.socket_mode` is applied to the UDS (432 = 0o660).
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_grpc_uds_custom_socket_mode() {
    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let repo = test_model_repo();
    let tmp_dir =
        std::env::temp_dir().join(format!("lite-server-grpc-uds-mode-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let sock = unique_uds_path("mode");
    let _ = std::fs::remove_file(&sock);
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {}\n  grpc_port: {}\n  metrics_port: 18099\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\n  host: unix:{}\n  socket_mode: 432\nmodel_repository:\n  path: {}\n",
            http_port, grpc_port, sock, repo.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 60).await;

    // Connecting + a Health round-trip proves the socket is live; no model is
    // loaded here, so the overall status is NOT_SERVING (we only require Ok).
    let channel = uds_grpc_channel(&sock).await;
    let mut health =
        tonic_health::pb::health_client::HealthClient::new(channel);
    health
        .check(tonic_health::pb::HealthCheckRequest { service: String::new() })
        .await
        .expect("Health.check over UDS must succeed");

    assert_socket_mode(&sock, 0o660); // 432 decimal = 0o660

    let _ = std::fs::remove_file(&sock);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

// ---------------------------------------------------------------------------
// gRPC graceful shutdown (P4-2)
// ---------------------------------------------------------------------------

/// An in-flight gRPC Infer started before SIGTERM must complete (tonic
/// `serve_with_shutdown` drains in-flight RPCs), then the server self-exits.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_grpc_graceful_shutdown_drains_inflight() {
    use std::collections::HashMap;
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let repo = create_slow_model_repo(2);
    let tmp_dir =
        std::env::temp_dir().join(format!("lite-server-p42-drain-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18212\n  graceful_timeout: 10.0\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();
    let mut child = start_server(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "slow_model", "1").await;

    // Fire a gRPC Infer (2s predict), then SIGTERM mid-flight. The server must
    // drain the in-flight RPC so the client still gets 42.
    let channel = grpc_tcp_channel(grpc_port).await;
    let infer_handle = tokio::spawn(async move {
        let mut client = LiteServerClient::new(channel);
        client
            .infer(InferRequest {
                model_name: "slow_model".to_string(),
                version: "1".to_string(),
                data: br#"{"input":21}"#.to_vec().into(),
                headers: HashMap::new(),

            ..Default::default()
})
            .await
    });
    sleep(Duration::from_millis(400)).await; // let predict start
    send_sigterm(&child);

    let got = tokio::time::timeout(Duration::from_secs(20), infer_handle)
        .await
        .expect("infer task did not finish");
    let resp = got
        .expect("infer task panicked")
        .expect("drained in-flight Infer must complete, not error")
        .into_inner();
    let body = String::from_utf8_lossy(&resp.data);
    assert!(
        body.contains("42"),
        "drained in-flight Infer must complete (21 -> 42); got: {}",
        body
    );

    let exited = wait_for_exit(&mut child, 20).await;
    if !exited {
        let _ = child.kill();
        let _ = child.wait();
    }
    assert!(exited, "server must self-exit after draining in-flight gRPC");

    let _ = std::fs::remove_dir_all(&tmp_dir);
    let _ = std::fs::remove_dir_all(&repo);
}

/// P4-2（对账 C）超时→abort（HTTP + gRPC 双侧）：graceful_timeout(2s) <
/// RPC 时长(30s) 时 drain 窗口耗尽即强制 abort——server 远早于 30s 退出，
/// 不会傻等慢 RPC 完成。
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_graceful_shutdown_aborts_inflight_after_timeout() {
    for use_grpc in [false, true] {
        let http_port = next_test_port();
        let grpc_port = next_test_port();
        kill_stale_on_port(http_port);
        kill_stale_on_port(grpc_port);
        let repo = create_slow_model_repo(30);
        // 端到端退出时间 = server abort(graceful_timeout=2s) + 队列 drain
        // 兜底(unload_grace 沿用 server.timeout=5s) + worker SIGKILL 升级
        // (worker_kill_timeout)——把模型的 kill 超时收窄到 3s,全程 ≪ 30s。
        let model_cfg = repo.join("slow_model/1/config.yaml");
        let mut cfg_text = std::fs::read_to_string(&model_cfg).unwrap();
        cfg_text.push_str("worker_kill_timeout: 3\n");
        std::fs::write(&model_cfg, cfg_text).unwrap();
        let tmp_dir = std::env::temp_dir().join(format!(
            "lite-server-p42-abort-{}-{}",
            std::process::id(),
            use_grpc
        ));
        std::fs::create_dir_all(&tmp_dir).unwrap();
        let server_yaml = tmp_dir.join("server.yaml");
        std::fs::write(
            &server_yaml,
            format!(
                "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18213\n  graceful_timeout: 2.0\n  shutdown_stream_grace_ms: 0\n  timeout: 5.0\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {repo}\n",
                http_port = http_port,
                grpc_port = grpc_port,
                repo = repo.to_string_lossy()
            ),
        )
        .unwrap();
        let mut child = start_server(&["--config", &server_yaml.to_string_lossy()]);
        wait_for_server(http_port, 60).await;
        let base = format!("http://127.0.0.1:{}", http_port);
        load_model(&base, "slow_model", "1").await;

        // 30s 慢推理在途（结果不重要——abort 后必然失败/被断）。
        let inflight = if use_grpc {
            let channel = grpc_tcp_channel(grpc_port).await;
            tokio::spawn(async move {
                use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
                use lite_server::proto::liteserver::InferRequest;
                use std::collections::HashMap;
                let mut client = LiteServerClient::new(channel);
                let _ = client
                    .infer(InferRequest {
                        model_name: "slow_model".to_string(),
                        version: "1".to_string(),
                        data: br#"{"input":1}"#.to_vec().into(),
                        headers: HashMap::new(),
                        ..Default::default()
                    })
                    .await;
            })
        } else {
            let url = format!("{}/v2/models/slow_model/infer", base);
            tokio::spawn(async move {
                let _ = reqwest::Client::new()
                    .post(url)
                    .body(r#"{"input":1}"#)
                    .send()
                    .await;
            })
        };
        sleep(Duration::from_secs(1)).await; // RPC 已在 worker 内执行

        let sigterm_at = std::time::Instant::now();
        send_sigterm(&child);
        let exited = wait_for_exit(&mut child, 15).await;
        let elapsed = sigterm_at.elapsed();
        if !exited {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = inflight.await;
        assert!(
            exited,
            "{}: graceful_timeout=2s 时 server 必须 abort 退出(30s 慢 RPC 在途)",
            if use_grpc { "gRPC" } else { "HTTP" }
        );
        assert!(
            elapsed < Duration::from_secs(15),
            "{}: abort 须远早于 30s RPC 完成点退出,实际 {elapsed:?}",
            if use_grpc { "gRPC" } else { "HTTP" }
        );

        let _ = std::fs::remove_dir_all(&tmp_dir);
        let _ = std::fs::remove_dir_all(&repo);
    }
}

/// Once SIGTERM starts graceful shutdown, the gRPC server stops accepting NEW
/// RPCs (tonic `serve_with_shutdown` sends GOAWAY / stops the listener): an
/// in-flight Infer still drains to completion, but a NEW Infer fired after the
/// signal is rejected. (A request that outlives its own per-request deadline or
/// the worker unload-grace is cut separately — not asserted here, where timing
/// against those coupled timeouts is racy.)
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_grpc_shutdown_rejects_new_rpcs() {
    use std::collections::HashMap;
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let repo = create_slow_model_repo(5); // keeps the server in drain mode
    let tmp_dir =
        std::env::temp_dir().join(format!("lite-server-p42-reject-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18222\n  timeout: 30.0\n  graceful_timeout: 12.0\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();
    let mut child = start_server(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "slow_model", "1").await;

    let channel = grpc_tcp_channel(grpc_port).await;
    // In-flight Infer #1 (5s predict) keeps the server in drain mode.
    let chan1 = channel.clone();
    let infer1 = tokio::spawn(async move {
        let mut client = LiteServerClient::new(chan1);
        client
            .infer(InferRequest {
                model_name: "slow_model".to_string(),
                version: "1".to_string(),
                data: br#"{"input":21}"#.to_vec().into(),
                headers: HashMap::new(),

            ..Default::default()
})
            .await
    });
    sleep(Duration::from_millis(400)).await; // let predict start
    send_sigterm(&child);
    sleep(Duration::from_millis(2500)).await; // drain mode + GOAWAY propagated

    // NEW Infer #2 after the signal must be rejected (no new streams accepted).
    let mut client2 = LiteServerClient::new(channel);
    let infer2 = tokio::time::timeout(
        Duration::from_secs(6),
        client2.infer(InferRequest {
            model_name: "slow_model".to_string(),
            version: "1".to_string(),
            data: br#"{"input":99}"#.to_vec().into(),
            headers: HashMap::new(),

        ..Default::default()
}),
    )
    .await;
    if let Ok(Ok(_)) = infer2 {
        panic!("new gRPC RPC after SIGTERM must be rejected, got Ok")
    }
    // Err or timeout — both mean rejected / not served

    // In-flight Infer #1 still drains to completion.
    let resp = tokio::time::timeout(Duration::from_secs(20), infer1)
        .await
        .expect("infer1 did not finish")
        .expect("infer1 panicked")
        .expect("in-flight Infer must drain, not error")
        .into_inner();
    assert!(
        String::from_utf8_lossy(&resp.data).contains("42"),
        "drained in-flight Infer must complete (21 -> 42)"
    );

    let exited = wait_for_exit(&mut child, 20).await;
    if !exited {
        let _ = child.kill();
        let _ = child.wait();
    }
    assert!(exited, "server must self-exit after drain");

    let _ = std::fs::remove_dir_all(&tmp_dir);
    let _ = std::fs::remove_dir_all(&repo);
}

/// C3 (P4-2): once SIGTERM arrives, /readyz stops returning 200 within a short
/// window — either 503 (draining flag, on a keep-alive connection) or
/// connection-refused (listener stopped) — so the LB摘流.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_shutdown_health_goes_unavailable() {
    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let repo = test_model_repo();
    let tmp_dir =
        std::env::temp_dir().join(format!("lite-server-p42-drainflag-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18232\n  graceful_timeout: 8.0\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();
    let mut child = start_server(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, MODEL, "1").await;

    let client = reqwest::Client::new();
    assert_eq!(
        client.get(format!("{}/readyz", base)).send().await.unwrap().status(),
        200
    );

    send_sigterm(&child);

    // readyz must stop returning 200 within the drain window — 503 (draining
    // flag) or connection-refused (listener stopped) both count as摘流.
    let mut unavailable = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    while tokio::time::Instant::now() < deadline {
        match client.get(format!("{}/readyz", base)).send().await {
            Ok(r) if r.status() == 200 => {}
            _ => {
                unavailable = true;
                break;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(unavailable, "readyz must become unavailable after SIGTERM");

    let exited = wait_for_exit(&mut child, 15).await;
    if !exited {
        let _ = child.kill();
        let _ = child.wait();
    }
    assert!(exited, "server must self-exit after drain");

    let _ = std::fs::remove_dir_all(&tmp_dir);
}

// ---------------------------------------------------------------------------
// SSE streaming
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_sse_streaming() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, MODEL, "1").await;

    // SSE endpoint expects POST with streaming response
    let resp = client
        .post(format!("{}/v2/models/{}/events", base, MODEL))
        .json(&json!({"input": 3}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let content_type = resp.headers().get("content-type").unwrap().to_str().unwrap().to_string();
    assert!(content_type.contains("text/event-stream"), "expected SSE content-type, got: {}", content_type);

    // Bound the drain: the SSE response must terminate (close the body) once
    // the worker sends [DONE]. If the server keeps the stream open (cleanup
    // blocking response closure), this fails the test fast instead of hanging
    // the whole suite and orphaning the shared server.
    let body = tokio::time::timeout(Duration::from_secs(15), resp.text())
        .await
        .expect("SSE response body did not close within 15s")
        .unwrap();
    // SSE format: "data: {...}\n\n"
    assert!(body.contains("data:"), "SSE response should contain data frames: {}", body);

    unload_model(&base, MODEL, "1").await;
}

// ---------------------------------------------------------------------------
// SSE decoupled streaming (HTTP decoupled plan PR-1)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_http_sse_decoupled_pushes_chunks_then_done() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, DECOUPLED_MODEL, "1").await;

    // POST to the decoupled SSE endpoint. decoupled_model with input=3
    // pushes 3 chunks ({"index": 0..2}) then [DONE].
    let resp = client
        .post(format!("{}/v2/models/{}/decoupled", base, DECOUPLED_MODEL))
        .json(&json!({"input": 3}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let content_type = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        content_type.contains("text/event-stream"),
        "expected SSE content-type, got: {}",
        content_type
    );

    let body = tokio::time::timeout(Duration::from_secs(15), resp.text())
        .await
        .expect("SSE decoupled response body did not close within 15s")
        .unwrap();

    // Parse SSE lines: "data: <json>\n\n". Each chunk is a "data:" line.
    let data_lines: Vec<&str> = body
        .lines()
        .filter(|l| l.starts_with("data: "))
        .collect();
    // 3 data chunks + [DONE] = 4 data lines
    assert_eq!(
        data_lines.len(),
        4,
        "expected 4 data lines (3 chunks + [DONE]); got body: {body}"
    );

    // First 3 are chunks with ordered indices.
    for (i, line) in data_lines.iter().take(3).enumerate() {
        let json_str = line.strip_prefix("data: ").unwrap();
        let v: Value = serde_json::from_str(json_str).expect("chunk data is JSON");
        assert_eq!(v["index"], i, "chunks must arrive in order");
    }
    // Last line is [DONE].
    assert_eq!(
        data_lines[3], "data: [DONE]",
        "terminal event must be [DONE]; got: {}",
        data_lines[3]
    );

    unload_model(&base, DECOUPLED_MODEL, "1").await;
}

// ---------------------------------------------------------------------------
// WebSocket streaming
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_websocket_streaming() {
    use tokio_tungstenite::connect_async;
    use futures::SinkExt;
    use futures::StreamExt;

    let base = shared_base().await;

    load_model(&base, MODEL, "1").await;

    let ws_url = format!("ws://127.0.0.1:{}/v2/models/{}/stream", SHARED_PORT, MODEL);
    let (mut ws, _) = connect_async(&ws_url).await.expect("WS connect failed");

    // Send inference request as text (JSON)
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&json!({"input": 4})).unwrap(),
    )).await.expect("WS send failed");

    // Collect messages until done or timeout
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut got_response = false;

    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), ws.next()).await {
            Ok(Some(Ok(msg))) => {
                match msg {
                    tokio_tungstenite::tungstenite::Message::Text(text) => {
                        let body: Value = serde_json::from_str(&text).unwrap_or(json!({}));
                        if body.get("done").is_some() || body.get("error").is_some() {
                            got_response = true;
                            break;
                        }
                        if body.get("output").is_some() {
                            got_response = true;
                        }
                    }
                    tokio_tungstenite::tungstenite::Message::Binary(_) => {
                        got_response = true;
                    }
                    _ => {}
                }
            }
            _ => break,
        }
    }

    assert!(got_response, "WS did not receive a response");
    let _ = ws.close(None).await;

    unload_model(&base, MODEL, "1").await;
}

/// WS decoupled streaming: first Text frame payload → worker pushes N
/// Binary chunks + {"done":true} (model-driven lifetime).
#[tokio::test]
#[serial]
async fn test_http_ws_decoupled_pushes_chunks_then_done() {
    use tokio_tungstenite::connect_async;
    use futures::SinkExt;
    use futures::StreamExt;

    let base = shared_base().await;

    load_model(&base, DECOUPLED_MODEL, "1").await;

    let ws_url = format!(
        "ws://127.0.0.1:{}/v2/models/{}/decoupled-stream",
        SHARED_PORT, DECOUPLED_MODEL
    );
    let (mut ws, _) = connect_async(&ws_url).await.expect("WS connect failed");

    // First frame = request payload (input=3 → worker pushes 3 chunks).
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&json!({"input": 3})).unwrap(),
    ))
    .await
    .expect("WS send failed");

    // Collect messages until the server closes the socket after Done.
    let mut msgs: Vec<String> = Vec::new();
    while let Ok(Some(Ok(msg))) =
        tokio::time::timeout(Duration::from_secs(10), ws.next()).await
    {
        match msg {
            tokio_tungstenite::tungstenite::Message::Binary(b) => {
                msgs.push(format!("bin:{}", String::from_utf8_lossy(&b)));
            }
            tokio_tungstenite::tungstenite::Message::Text(t) => {
                msgs.push(format!("txt:{}", t));
            }
            _ => {}
        }
    }

    // 3 Binary chunks + terminal {"done":true}
    assert_eq!(msgs.len(), 4, "expected 4 messages; got: {msgs:?}");
    for i in 0..3 {
        assert!(
            msgs[i].starts_with("bin:"),
            "message {i} must be Binary chunk; got: {msgs:?}"
        );
        let json_str = &msgs[i][4..]; // strip "bin:" prefix
        let v: Value = serde_json::from_str(json_str).expect("chunk data is JSON");
        assert_eq!(v["index"], i, "chunks must arrive in order");
    }
    assert_eq!(msgs[3], "txt:{\"done\":true}", "terminal frame must be Done");

    unload_model(&base, DECOUPLED_MODEL, "1").await;
}

/// WS bidi: client sends first Text frame + 2 Binary frames; the bidi reader
/// forwards them to the worker as chunks, and the stream completes normally.
#[tokio::test]
#[serial]
async fn test_ws_bidi_binary_forwarding() {
    use futures::StreamExt;
    use futures::SinkExt;
    use tokio_tungstenite::connect_async;

    let base = shared_base().await;
    load_model(&base, MODEL, "1").await;

    let ws_url = format!("ws://127.0.0.1:{}/v2/models/{}/stream", SHARED_PORT, MODEL);
    let (mut ws, _) = connect_async(&ws_url).await.expect("WS connect failed");

    // First frame: JSON payload (legacy).
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&json!({"input": 1})).unwrap(),
    )).await.expect("WS send first frame failed");

    // Two additional Binary frames (bidi).
    ws.send(tokio_tungstenite::tungstenite::Message::Binary(vec![1, 2, 3]))
        .await.expect("WS send binary 1 failed");
    ws.send(tokio_tungstenite::tungstenite::Message::Binary(vec![4, 5, 6]))
        .await.expect("WS send binary 2 failed");

    // Collect messages until done/error or timeout.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut got_terminal = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), ws.next()).await {
            Ok(Some(Ok(msg))) => {
                if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
                    let body: Value = serde_json::from_str(&text).unwrap_or(json!({}));
                    if body.get("done").is_some() || body.get("error").is_some() {
                        got_terminal = true;
                        break;
                    }
                }
            }
            _ => break,
        }
    }
    assert!(got_terminal, "WS bidi: expected terminal frame after binary chunks");
    let _ = ws.close(None).await;

    unload_model(&base, MODEL, "1").await;
}

/// WS bidi: app-level `{"type":"close"}` gracefully ends the input side;
/// the output side continues and the terminal Done frame still arrives.
#[tokio::test]
#[serial]
async fn test_ws_bidi_app_level_close() {
    use futures::StreamExt;
    use futures::SinkExt;
    use tokio_tungstenite::connect_async;

    let base = shared_base().await;
    load_model(&base, MODEL, "1").await;

    let ws_url = format!("ws://127.0.0.1:{}/v2/models/{}/stream", SHARED_PORT, MODEL);
    let (mut ws, _) = connect_async(&ws_url).await.expect("WS connect failed");

    // First frame: JSON payload.
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&json!({"input": 2})).unwrap(),
    )).await.expect("WS send first frame failed");

    // App-level close frame.
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"type":"close"}"#.to_string(),
    )).await.expect("WS send close frame failed");

    // Collect messages until done/error or timeout.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut got_terminal = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), ws.next()).await {
            Ok(Some(Ok(msg))) => {
                if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
                    let body: Value = serde_json::from_str(&text).unwrap_or(json!({}));
                    if body.get("done").is_some() || body.get("error").is_some() {
                        got_terminal = true;
                        break;
                    }
                }
            }
            _ => break,
        }
    }
    assert!(got_terminal, "WS bidi close: expected terminal frame after app-level close");
    let _ = ws.close(None).await;

    unload_model(&base, MODEL, "1").await;
}

/// WS bidi: an unknown Text control frame after the first frame triggers a
/// protocol error — the server sends `{"error":"unknown control frame"}` and
/// closes the connection.
#[tokio::test]
#[serial]
async fn test_ws_bidi_unknown_control_frame() {
    use futures::StreamExt;
    use futures::SinkExt;
    use tokio_tungstenite::connect_async;

    let base = shared_base().await;
    load_model(&base, MODEL, "1").await;

    let ws_url = format!("ws://127.0.0.1:{}/v2/models/{}/stream", SHARED_PORT, MODEL);
    let (mut ws, _) = connect_async(&ws_url).await.expect("WS connect failed");

    // First frame: JSON payload.
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&json!({"input": 3})).unwrap(),
    )).await.expect("WS send first frame failed");

    // Unknown control frame.
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        r#"{"type":"unknown_cmd"}"#.to_string(),
    )).await.expect("WS send unknown frame failed");

    // Collect until error or timeout.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut got_error = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), ws.next()).await {
            Ok(Some(Ok(msg))) => {
                if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
                    let body: Value = serde_json::from_str(&text).unwrap_or(json!({}));
                    if body.get("error").is_some() {
                        got_error = true;
                        break;
                    }
                }
            }
            _ => break,
        }
    }
    assert!(got_error, "WS bidi: expected error frame after unknown control frame");
    let _ = ws.close(None).await;

    unload_model(&base, MODEL, "1").await;
}

// ---------------------------------------------------------------------------
// B2 (tensor-bytes-consistency): WS first-frame dispatch (E3/E4/E5)
// ---------------------------------------------------------------------------

/// Connect a WS client, optionally setting Content-Type on the upgrade
/// request (non-browser clients can; browsers cannot — which is why the
/// frame type is the dispatch signal).
#[cfg(unix)]
async fn ws_connect_with_ct(
    url: &str,
    ct: Option<&str>,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = url.into_client_request().expect("WS request build failed");
    if let Some(ct) = ct {
        req.headers_mut()
            .insert("content-type", ct.parse().unwrap());
    }
    let (ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .expect("WS connect failed");
    ws
}

/// Send a Binary first frame and read the probe model's report back as JSON.
/// Bidi (`/stream`) reports in a Text frame; decoupled (`/decoupled-stream`)
/// reports in a Binary chunk — accept either.
#[cfg(unix)]
async fn ws_binary_first_frame_probe(ws_url: &str, ct: Option<&str>) -> Value {
    use futures::{SinkExt, StreamExt};
    let mut ws = ws_connect_with_ct(ws_url, ct).await;
    ws.send(tokio_tungstenite::tungstenite::Message::Binary(
        b"\x00\x01\x02".to_vec(),
    ))
    .await
    .expect("WS send Binary first frame failed");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let msg = tokio::time::timeout_at(deadline, ws.next())
            .await
            .expect("timed out waiting for probe report")
            .expect("WS closed before probe report")
            .expect("WS error before probe report");
        let text = match msg {
            tokio_tungstenite::tungstenite::Message::Text(t) => t,
            tokio_tungstenite::tungstenite::Message::Binary(b) => {
                String::from_utf8_lossy(&b).to_string()
            }
            _ => continue,
        };
        if let Ok(v) = serde_json::from_str::<Value>(&text) {
            if v.get("kind").is_some() {
                let _ = ws.close(None).await;
                return v;
            }
        }
    }
}

/// B2 §7.2 (regression): a Text first frame that is not valid JSON is
/// rejected with {"error":"invalid JSON"} then close — the legacy path is
/// byte-identical to 0.8.2.
#[tokio::test]
#[serial]
async fn test_ws_first_frame_text_invalid_json_rejected() {
    use futures::{SinkExt, StreamExt};
    let base = shared_base().await;
    load_model(&base, "ws_probe_model", "1").await;

    let ws_url = format!("ws://127.0.0.1:{}/v2/models/ws_probe_model/stream", SHARED_PORT);
    let mut ws = ws_connect_with_ct(&ws_url, None).await;
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        "this is not json".to_string(),
    ))
    .await
    .expect("WS send failed");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut got_error = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                let body: Value = serde_json::from_str(&text).unwrap_or(json!({}));
                if body.get("error").is_some() {
                    assert_eq!(
                        body["error"], "invalid JSON",
                        "Text first frame must keep the legacy rejection"
                    );
                    got_error = true;
                    break;
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }
    assert!(got_error, "expected invalid-JSON error frame");
    let _ = ws.close(None).await;
    unload_model(&base, "ws_probe_model", "1").await;
}

/// B2 §7.2 (E4): Binary first frame + missing upgrade CT → raw bytes to the
/// worker with content-type injected as application/octet-stream.
#[tokio::test]
#[serial]
async fn test_ws_first_frame_binary_missing_ct_injected() {
    let base = shared_base().await;
    load_model(&base, "ws_probe_model", "1").await;

    let ws_url = format!("ws://127.0.0.1:{}/v2/models/ws_probe_model/stream", SHARED_PORT);
    let v = ws_binary_first_frame_probe(&ws_url, None).await;
    assert_eq!(v["kind"], "bytes", "Binary first frame must dispatch raw");
    assert_eq!(v["len"], 3, "all 3 bytes must arrive");
    assert_eq!(
        v["ct"], "application/octet-stream",
        "missing CT must be injected as octet-stream"
    );

    unload_model(&base, "ws_probe_model", "1").await;
}

/// B2 §7.2 (E4): Binary first frame + non-JSON CT → CT preserved as payload
/// metadata for the model.
#[tokio::test]
#[serial]
async fn test_ws_first_frame_binary_non_json_ct_preserved() {
    let base = shared_base().await;
    load_model(&base, "ws_probe_model", "1").await;

    let ws_url = format!("ws://127.0.0.1:{}/v2/models/ws_probe_model/stream", SHARED_PORT);
    let v = ws_binary_first_frame_probe(&ws_url, Some("image/png")).await;
    assert_eq!(v["kind"], "bytes");
    assert_eq!(
        v["ct"], "image/png",
        "non-JSON CT must pass through to the worker"
    );

    unload_model(&base, "ws_probe_model", "1").await;
}

/// B2 §7.2 (E4/E5 pin): Binary first frame + JSON CT is contradictory —
/// frame type wins and the CT is rewritten to octet-stream (0.8.2 parsed
/// such frames as JSON; this is the documented behavior change).
#[tokio::test]
#[serial]
async fn test_ws_first_frame_binary_json_ct_rewritten() {
    let base = shared_base().await;
    load_model(&base, "ws_probe_model", "1").await;

    let ws_url = format!("ws://127.0.0.1:{}/v2/models/ws_probe_model/stream", SHARED_PORT);
    // Binary frame carrying non-UTF8 bytes + a JSON upgrade CT: 0.8.2 lossy-
    // decoded and rejected it; now the frame type wins and it stays raw.
    let v = ws_binary_first_frame_probe(&ws_url, Some("application/json")).await;
    assert_eq!(v["kind"], "bytes", "frame type must win over a JSON CT");
    assert_eq!(
        v["ct"], "application/octet-stream",
        "JSON CT must be rewritten to octet-stream"
    );

    unload_model(&base, "ws_probe_model", "1").await;
}

/// B2 §7.2: decoupled WS shares handle_ws_stream — the missing-CT injection
/// must behave identically on /decoupled-stream.
#[tokio::test]
#[serial]
async fn test_ws_decoupled_first_frame_binary_missing_ct_injected() {
    let base = shared_base().await;
    load_model(&base, "ws_probe_model", "1").await;

    let ws_url = format!(
        "ws://127.0.0.1:{}/v2/models/ws_probe_model/decoupled-stream",
        SHARED_PORT
    );
    let v = ws_binary_first_frame_probe(&ws_url, None).await;
    assert_eq!(v["kind"], "bytes", "decoupled: Binary first frame must dispatch raw");
    assert_eq!(v["len"], 3);
    assert_eq!(v["ct"], "application/octet-stream");

    unload_model(&base, "ws_probe_model", "1").await;
}

/// B2 §7.2: decoupled WS — JSON CT rewrite parity with /stream.
#[tokio::test]
#[serial]
async fn test_ws_decoupled_first_frame_binary_json_ct_rewritten() {
    let base = shared_base().await;
    load_model(&base, "ws_probe_model", "1").await;

    let ws_url = format!(
        "ws://127.0.0.1:{}/v2/models/ws_probe_model/decoupled-stream",
        SHARED_PORT
    );
    let v = ws_binary_first_frame_probe(&ws_url, Some("application/json")).await;
    assert_eq!(v["kind"], "bytes");
    assert_eq!(v["ct"], "application/octet-stream");

    unload_model(&base, "ws_probe_model", "1").await;
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_metrics_endpoint() {
    let port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(port);
    kill_stale_on_port(metrics_port);
    let repo = test_model_repo();
    let server = start_server(&[
        "--port", &port.to_string(),
        "--metrics-port", &metrics_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc",
        "--log-level", "warn",
    ]);

    wait_for_server(port, 15).await;

    let client = reqwest::Client::new();

    // Load a model so metrics have content
    load_model(&format!("http://127.0.0.1:{}", port), "test_model", "1").await;

    let resp = client
        .get(format!("http://127.0.0.1:{}/metrics", metrics_port))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("liteserver"), "metrics body: {}", body);

    stop_server(server);
}

// ---------------------------------------------------------------------------
// API Response Standardization — error body + observability headers
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_error_response_format() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    // POST to a nonexistent model — should get structured error
    let resp = client
        .post(format!("{}/v2/models/nonexistent_model/infer", base))
        .json(&json!({"input": 1}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);

    // Verify response headers
    assert!(resp.headers().get("x-request-id").is_some(),
        "x-request-id must be present on error responses");
    assert!(resp.headers().get("x-processing-time-ms").is_some(),
        "x-processing-time-ms must be present on error responses");

    // Verify error body: type, message, code, param
    let body: Value = resp.json().await.unwrap();
    let err = &body["error"];
    assert!(err.is_object(), "error should be an object, got: {:?}", err);
    assert_eq!(err["type"], "not_found_error");
    assert!(!err["message"].as_str().unwrap().is_empty());
    assert_eq!(err["code"], "model_not_found");
    assert!(err["param"].is_null(),
        "param should be null but was {}", err["param"]);
}

#[tokio::test]
#[serial]
async fn test_observability_headers_on_success() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/health", base))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(resp.headers().get("x-request-id").is_some(),
        "x-request-id must be present on success responses");
    assert!(resp.headers().get("x-processing-time-ms").is_some(),
        "x-processing-time-ms must be present on success responses");
    // x-processing-time-ms should be a non-negative integer
    let ms = resp.headers().get("x-processing-time-ms").unwrap().to_str().unwrap();
    assert!(ms.parse::<u64>().is_ok(),
        "x-processing-time-ms should be a u64, got: {}", ms);
}

#[tokio::test]
#[serial]
async fn test_x_client_request_id_propagation() {
    let base = shared_base().await;
    let client = reqwest::Client::new();
    let trace_id = "integration-test-001";

    let resp = client
        .get(format!("{}/health", base))
        .header("x-client-request-id", trace_id)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("x-request-id").unwrap().to_str().unwrap(),
        trace_id,
        "x-request-id should echo x-client-request-id"
    );
}

#[tokio::test]
#[serial]
async fn test_infer_error_response_has_code_and_param() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    // Load model first so the model route is active
    load_model(&base, MODEL, "1").await;

    // test_model raises BadRequestError(code="invalid_input", param="input")
    // for null input — verifies the full Python → Rust → client chain.
    let resp = client
        .post(format!("{}/v2/models/{}/infer", base, MODEL))
        .json(&json!({"input": null}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    assert!(resp.headers().get("x-request-id").is_some(),
        "x-request-id must be present");
    assert!(resp.headers().get("x-processing-time-ms").is_some(),
        "x-processing-time-ms must be present");

    let body: Value = resp.json().await.unwrap();
    let err = &body["error"];
    assert_eq!(err["type"], "invalid_request_error");
    assert_eq!(err["message"], "input must not be null");
    assert_eq!(err["code"], "invalid_input");
    assert_eq!(err["param"], "input");

    unload_model(&base, MODEL, "1").await;
}

#[tokio::test]
#[serial]
async fn test_unknown_route_standardized_404() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/no-such-route", base))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
    assert!(resp.headers().get("x-request-id").is_some());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["type"], "not_found_error");
    assert_eq!(body["error"]["code"], "route_not_found");
    assert!(body["error"]["param"].is_null());
}

#[tokio::test]
#[serial]
async fn test_method_not_allowed_standardized_405() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    // /health only supports GET
    let resp = client
        .post(format!("{}/health", base))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 405);
    assert!(resp.headers().get("x-request-id").is_some());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["type"], "method_not_allowed");
    assert_eq!(body["error"]["code"], "method_not_allowed");
    assert!(body["error"]["param"].is_null());
}

#[tokio::test]
#[serial]
async fn test_malformed_json_standardized_400() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    // Route exists; the ApiJson extractor rejects before any model lookup.
    let resp = client
        .post(format!("{}/v2/models/any_model/infer", base))
        .header("content-type", "application/json")
        .body("{not valid json")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    assert!(resp.headers().get("x-request-id").is_some());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["type"], "invalid_request_error");
    assert_eq!(body["error"]["code"], "invalid_request_body");
    assert!(body["error"]["param"].is_null());
}

// ---------------------------------------------------------------------------
// SSE streaming must carry CORS headers — covers A2
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_sse_rate_limit_returns_429() {
    // Dedicated server: the rate-limit bucket is shared across tests on the
    // shared server (key model:/predict, burst=3), so a fresh process gives a
    // deterministic starting bucket.
    let port = next_test_port();
    kill_stale_on_port(port);
    let repo = test_model_repo();
    let _server = ServerGuard::start(&[
        "--port",
        &port.to_string(),
        "--model-repo",
        &repo.to_string_lossy(),
        "--no-metrics",
        "--no-grpc",
        "--log-level",
        "warn",
    ]);
    wait_for_server(port, 15).await;
    let base = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::new();
    load_model(&base, POLICY_MODEL, "1").await;

    // policy_model: RateLimit(rpm=3, burst=3). First 3 SSE requests allowed,
    // 4th rejected with 429 + sane Retry-After.
    for i in 0..3 {
        let resp = client
            .post(format!("{}/v2/models/{}/events", base, POLICY_MODEL))
            .json(&json!({"input": i}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "SSE request {} should be allowed", i);
    }
    let resp = client
        .post(format!("{}/v2/models/{}/events", base, POLICY_MODEL))
        .json(&json!({"input": 9}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 429);
    let retry = resp
        .headers()
        .get("retry-after")
        .expect("429 must carry Retry-After");
    let secs: u64 = retry.to_str().unwrap().parse().unwrap();
    assert!(
        (1..=60).contains(&secs),
        "Retry-After {} out of sane range",
        secs
    );

    // _server guard kills the process group on drop (incl. panic path).
}

// ---------------------------------------------------------------------------
// SSE streaming must carry CORS headers — covers A2
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_sse_response_carries_cors_headers() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    load_model(&base, POLICY_MODEL, "1").await;

    // SSE success path must carry CORS headers (cors_middleware attaches ACAO
    // when the request Origin is allowed, including the stream-start response).
    let resp = client
        .post(format!("{}/v2/models/{}/events", base, POLICY_MODEL))
        .header("origin", "https://app.example.com")
        .json(&json!({"input": 3}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("access-control-allow-origin")
            .unwrap()
            .to_str()
            .unwrap(),
        "https://app.example.com"
    );
    // Drain the SSE body — must terminate after [DONE]; bounded so a regression
    // (stream kept open) fails the test instead of hanging the suite.
    let _ = tokio::time::timeout(Duration::from_secs(15), resp.text())
        .await
        .expect("SSE response body did not close within 15s")
        .unwrap();

    unload_model(&base, POLICY_MODEL, "1").await;
}

// ---------------------------------------------------------------------------
// OPTIONS preflight (CORS) on inference routes — covers A1
// ---------------------------------------------------------------------------

const POLICY_MODEL: &str = "policy_model";

#[tokio::test]
#[serial]
async fn test_inference_options_preflight_all_routes() {
    let base = shared_base().await;
    let client = reqwest::Client::new();

    // policy_model declares Cors(allow_origins=["https://app.example.com"]).
    load_model(&base, POLICY_MODEL, "1").await;

    // All four inference routes (incl. the two versioned ones) must answer
    // a CORS preflight (OPTIONS + Origin + ACRM) with 204 + ACAO. P-CORS moved
    // preflight into cors_middleware (short-circuits before routing).
    let routes = [
        format!("/v2/models/{}/infer", POLICY_MODEL),
        format!("/v2/models/{}/versions/1/infer", POLICY_MODEL),
        format!("/v2/models/{}/events", POLICY_MODEL),
        format!("/v2/models/{}/versions/1/events", POLICY_MODEL),
    ];
    for path in &routes {
        let resp = client
            .request(reqwest::Method::OPTIONS, format!("{}{}", base, path))
            .header("origin", "https://app.example.com")
            .header("access-control-request-method", "POST")
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            204,
            "preflight {} should be 204, got {}",
            path,
            resp.status()
        );
        let acao = resp
            .headers()
            .get("access-control-allow-origin")
            .unwrap_or_else(|| panic!("ACAO missing on preflight {}", path));
        assert_eq!(acao.to_str().unwrap(), "https://app.example.com");
    }

    // A model WITHOUT a Cors declaration answers OPTIONS with 405.
    load_model(&base, MODEL, "1").await;
    let resp = client
        .request(
            reqwest::Method::OPTIONS,
            format!("{}/v2/models/{}/infer", base, MODEL),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 405);

    unload_model(&base, POLICY_MODEL, "1").await;
    unload_model(&base, MODEL, "1").await;
}

// ---------------------------------------------------------------------------
// Inference CORS on success + rate-limit error — covers A8
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_infer_cors_success_and_rate_limit_error() {
    // Dedicated server: policy_model's rate-limit bucket (burst=3) is shared
    // across tests on the shared server, so a fresh process gives a clean bucket.
    let port = next_test_port();
    kill_stale_on_port(port);
    let repo = test_model_repo();
    let _server = ServerGuard::start(&[
        "--port",
        &port.to_string(),
        "--model-repo",
        &repo.to_string_lossy(),
        "--no-metrics",
        "--no-grpc",
        "--log-level",
        "warn",
    ]);
    wait_for_server(port, 15).await;
    let base = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::new();
    load_model(&base, POLICY_MODEL, "1").await;

    // Success: 200 + ACAO (cors_middleware attaches ACAO for an allowed Origin
    // on unary infer too).
    let resp = client
        .post(format!("{}/v2/models/{}/infer", base, POLICY_MODEL))
        .header("origin", "https://app.example.com")
        .json(&json!({"input": 5}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("access-control-allow-origin")
            .unwrap()
            .to_str()
            .unwrap(),
        "https://app.example.com"
    );
    assert_eq!(resp.json::<Value>().await.unwrap()["output"], 10);

    // 2nd + 3rd requests stay within burst=3.
    for _ in 0..2 {
        let resp = client
            .post(format!("{}/v2/models/{}/infer", base, POLICY_MODEL))
            .json(&json!({"input": 1}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    // 4th: rate-limited → 429 + Retry-After. The error response must still
    // carry ACAO (cors_middleware attaches it on the response regardless of
    // downstream status), and Retry-After must be a sane 1..=60 seconds
    // (C1 Rust defense against rpm<=0 → u64::MAX).
    let resp = client
        .post(format!("{}/v2/models/{}/infer", base, POLICY_MODEL))
        .header("origin", "https://app.example.com")
        .json(&json!({"input": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 429);
    let retry = resp
        .headers()
        .get("retry-after")
        .expect("429 must carry Retry-After");
    let secs: u64 = retry.to_str().unwrap().parse().unwrap();
    assert!((1..=60).contains(&secs), "Retry-After {} out of range", secs);
    assert_eq!(
        resp.headers()
            .get("access-control-allow-origin")
            .unwrap()
            .to_str()
            .unwrap(),
        "https://app.example.com"
    );
}

// ---------------------------------------------------------------------------
// Hot reload (separate server with temp dir)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
#[ignore] // flaky on CI: filesystem watcher timeout on macOS runners
async fn test_hot_reload() {
    let original = r#"from lite_server import LitAPI


class TestAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x * 2}

    def encode_response(self, output):
        return output
"#;

    let tmp_dir = std::env::temp_dir().join(format!("lite-server-hotreload-{}", std::process::id()));
    let repo_src = test_model_repo().clone();
    let repo_dst = tmp_dir.join("model_repo");

    tokio::fs::create_dir_all(&repo_dst).await.unwrap();
    copy_dir_recursive(&repo_src, &repo_dst).await.unwrap();

    // Enable hot reload in config
    let config_yaml = repo_dst.join("test_model/1/config.yaml");
    tokio::fs::write(&config_yaml, "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\nhot_reload: true\nhot_reload_interval: 1.0\n").await.unwrap();

    let model_py = repo_dst.join("test_model/1/model.py");
    tokio::fs::write(&model_py, original).await.unwrap();

    let port = next_test_port();
    kill_stale_on_port(port);
    let server = start_server(&[
        "--port", &port.to_string(),
        "--model-repo", &repo_dst.to_string_lossy(),
        "--no-metrics",
        "--no-grpc",
        "--log-level", "warn",
    ]);

    wait_for_server(port, 15).await;
    let base = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::new();

    // Load and verify initial behavior (x * 2)
    load_model(&base, "test_model", "1").await;
    let resp = client
        .post(format!("{}/v2/models/test_model/infer", base))
        .json(&json!({"input": 5}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["output"], 10);

    // Modify the model (x * 2 → x * 3)
    let modified = original.replace("x * 2", "x * 3");
    tokio::fs::write(&model_py, modified).await.unwrap();

    // Poll until hot reload picks up the change (no fixed sleep)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut reloaded = false;
    while tokio::time::Instant::now() < deadline {
        let resp = client
            .post(format!("{}/v2/models/test_model/infer", base))
            .json(&json!({"input": 5}))
            .send().await.unwrap();
        if resp.status() == 200 {
            let body: Value = resp.json().await.unwrap();
            if body["output"] == 15 {
                reloaded = true;
                break;
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
    assert!(reloaded, "hot reload did not pick up model change within 30s");

    stop_server(server);
    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
}

// ---------------------------------------------------------------------------
// P3: on_file_changed hook refreshes in-process (no worker restart)
// ---------------------------------------------------------------------------

/// The model reads its multiplier from mult.txt at setup AND in its
/// on_file_changed hook. Changing mult.txt must change inference output
/// while the response's boot id stays constant — proof the worker process
/// was NOT restarted (a restart would re-import the module → new boot id,
/// and would also produce the new output, so output alone can't
/// distinguish the paths).
#[tokio::test]
#[serial]
#[ignore] // flaky on CI: filesystem watcher timeout on macOS runners
async fn test_hot_reload_on_file_changed_hook_avoids_restart() {
    let model_py = r#"import os
import uuid

from lite_server import LitAPI

BOOT_ID = str(uuid.uuid4())


class TestAPI(LitAPI):
    def setup(self, device):
        self.mult = self._read_mult()

    def _read_mult(self):
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "mult.txt")) as f:
            return int(f.read().strip())

    def on_file_changed(self, changed_files):
        if any(f.endswith("mult.txt") for f in changed_files):
            self.mult = self._read_mult()
            return "handled"
        return None

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x * self.mult, "boot": BOOT_ID}

    def encode_response(self, output):
        return output
"#;

    let tmp_dir = std::env::temp_dir().join(format!("lite-server-fchanged-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp_dir);
    let version_dir = tmp_dir.join("test_model").join("1");
    std::fs::create_dir_all(&version_dir).unwrap();
    std::fs::write(version_dir.join("model.py"), model_py).unwrap();
    std::fs::write(version_dir.join("mult.txt"), "2\n").unwrap();
    std::fs::write(
        version_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\nhot_reload: true\nhot_reload_patterns: [\"*.txt\"]\n",
    ).unwrap();

    let port = next_test_port();
    kill_stale_on_port(port);
    let server = start_server(&[
        "--port", &port.to_string(),
        "--model-repo", &tmp_dir.to_string_lossy(),
        "--no-metrics",
        "--no-grpc",
        "--log-level", "warn",
    ]);

    wait_for_server(port, 15).await;
    let base = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::new();

    load_model(&base, "test_model", "1").await;
    let resp = client
        .post(format!("{}/v2/models/test_model/infer", base))
        .json(&json!({"input": 5}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["output"], 10);
    let boot_before = body["boot"].as_str().unwrap().to_string();

    // Change only the data file — the hook must pick it up in-process.
    tokio::fs::write(version_dir.join("mult.txt"), "3\n").await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut refreshed = false;
    while tokio::time::Instant::now() < deadline {
        let resp = client
            .post(format!("{}/v2/models/test_model/infer", base))
            .json(&json!({"input": 5}))
            .send().await.unwrap();
        if resp.status() == 200 {
            let body: Value = resp.json().await.unwrap();
            if body["output"] == 15 {
                assert_eq!(
                    body["boot"].as_str().unwrap(),
                    boot_before,
                    "output changed but boot id changed too — worker was restarted \
                     instead of refreshed in-process via on_file_changed"
                );
                refreshed = true;
                break;
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
    assert!(refreshed, "on_file_changed hook did not pick up mult.txt change within 30s");

    stop_server(server);
    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
}

// ---------------------------------------------------------------------------
// §4.1: max_requests rolling recycle must target the triggering worker only
// ---------------------------------------------------------------------------

/// v1 hits max_requests while v2 is the active version: the per-worker
/// rolling recycle must respawn exactly one worker of v1 (one
/// `liteserver_worker_respawns_total{reason="rolling_recycle"}` tick) and
/// leave v1's sibling worker, the whole v2 version (loaded_at unchanged),
/// and the active pointer untouched.
#[tokio::test]
async fn test_max_requests_rolling_recycle_targets_triggering_worker() {
    let model_py = r#"from lite_server import LitAPI


class ReloadAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x * 2}

    def encode_response(self, output):
        return output
"#;

    let tmp_dir = std::env::temp_dir().join(format!("lite-server-reloadver-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp_dir);
    let base_cfg = "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\n";
    for (v, cfg) in [
        ("1", format!("{}workers_per_device: 2\nmax_requests: 1\n", base_cfg)),
        ("2", format!("{}workers_per_device: 1\n", base_cfg)),
    ] {
        let dir = tmp_dir.join("reload_model").join(v);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("model.py"), model_py).unwrap();
        std::fs::write(dir.join("config.yaml"), cfg).unwrap();
    }

    let port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(port);
    kill_stale_on_port(metrics_port);
    let _server = ServerGuard::start(&[
        "--port", &port.to_string(),
        "--metrics-port", &metrics_port.to_string(),
        "--model-repo", &tmp_dir.to_string_lossy(),
        "--no-grpc",
        "--log-level", "warn",
    ]);
    wait_for_server(port, 60).await;
    let base = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::new();

    // /health (§4.5): per-version entries nested under their model.
    let health_entry = |body: &Value, v: &str| {
        body["models"].as_array().unwrap().iter()
            .find(|m| m["name"] == "reload_model")
            .and_then(|m| m["versions"].as_array().unwrap().iter()
                .find(|e| e["version"] == v))
            .cloned()
    };
    let get_health = || async {
        let resp = client.get(format!("{}/health", base)).send().await.unwrap();
        resp.json::<Value>().await.unwrap()
    };
    // Rolling-recycle count per version from the metrics endpoint (the
    // series is absent until the first recycle).
    let rolling_recycles = |body: &str, v: &str| -> f64 {
        let needle = format!(
            "liteserver_worker_respawns_total{{model=\"reload_model\",reason=\"rolling_recycle\",version=\"{v}\"}}"
        );
        body.lines()
            .find(|l| l.starts_with(&needle))
            .and_then(|l| l.rsplit_once(' '))
            .and_then(|(_, n)| n.parse().ok())
            .unwrap_or(0.0)
    };
    let get_metrics = || async {
        let resp = client
            .get(format!("http://127.0.0.1:{}/metrics", metrics_port))
            .send().await.unwrap();
        resp.text().await.unwrap()
    };

    // Load both versions explicitly (default control_mode loads nothing at
    // startup). Loading v1 auto-activates it (no active version yet).
    load_model(&base, "reload_model", "1").await;
    load_model(&base, "reload_model", "2").await;

    // load_model waits on the bare /ready (active version); poll /health until
    // v2 itself reports ready.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut v2 = None;
    while tokio::time::Instant::now() < deadline {
        let body = get_health().await;
        v2 = health_entry(&body, "2").filter(|e| e["status"] == "ready");
        if v2.is_some() {
            break;
        }
        sleep(Duration::from_millis(200)).await;
    }
    let v2 = v2.expect("v2 did not become ready");
    let v2_loaded_at = v2["loaded_at"].clone();
    // L3: without this presence check a missing loaded_at would make the
    // poll-loop equality below pass vacuously (Null == Null).
    assert!(v2_loaded_at.as_u64().is_some(), "v2 loaded_at: {:?}", v2_loaded_at);
    let body = get_health().await;
    let v1_loaded_at = health_entry(&body, "1").expect("v1 loaded")["loaded_at"].clone();
    assert!(v1_loaded_at.as_u64().is_some(), "v1 loaded_at: {:?}", v1_loaded_at);

    // Make v2 the active version.
    let resp = client
        .post(format!("{}/v2/models/reload_model/versions/2/activate", base))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);

    // Hit v1's per-worker max_requests via the versioned path.
    let resp = client
        .post(format!("{}/v2/models/reload_model/versions/1/infer", base))
        .json(&json!({"input": 21}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);

    // Poll: exactly one rolling recycle for v1 (one worker only — its
    // sibling keeps serving), none for v2, and NEITHER version is reloaded
    // (loaded_at unchanged — a rolling recycle is not a version reload).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut recycled = false;
    while tokio::time::Instant::now() < deadline {
        let body = get_health().await;
        let v2 = health_entry(&body, "2").expect("v2 must stay registered");
        assert_eq!(v2["status"], "ready", "active v2 must be untouched: {:?}", v2);
        assert_eq!(v2["loaded_at"], v2_loaded_at, "active v2 must not be reloaded");
        let v1 = health_entry(&body, "1").expect("v1 must stay registered — rolling recycle is not an unload");
        assert_eq!(v1["loaded_at"], v1_loaded_at, "v1 must not be reloaded");
        let m = get_metrics().await;
        assert_eq!(rolling_recycles(&m, "2"), 0.0, "v2 must not recycle");
        if rolling_recycles(&m, "1") == 1.0 {
            recycled = true;
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(recycled, "v1 did not roll a worker within 60s of hitting max_requests");

    // Exactly one slot recycled, and v1 returns to ready.
    let m = get_metrics().await;
    assert_eq!(
        rolling_recycles(&m, "1"),
        1.0,
        "exactly one worker recycles for a single crossing"
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut v1_back = false;
    while tokio::time::Instant::now() < deadline {
        let body = get_health().await;
        if health_entry(&body, "1").is_some_and(|v1| v1["status"] == "ready") {
            v1_back = true;
            break;
        }
        sleep(Duration::from_millis(200)).await;
    }
    assert!(v1_back, "v1 did not return to ready after recycle");

    // The recycled standby version must not steal the active pointer (§4.3).
    let resp = client
        .get(format!("{}/v2/models/reload_model/versions", base))
        .send().await.unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["active_version"], "2", "active must stay v2 after v1 recycle");

    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
}

/// A rolling recycle must not drop traffic: with two workers and a small
/// max_requests, a sequential burst is fully served (200s) while workers
/// recycle underneath.
#[tokio::test]
async fn should_keep_serving_during_rolling_recycle() {
    let model_py = r#"from lite_server import LitAPI


class RollingAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x * 2}

    def encode_response(self, output):
        return output
"#;

    let tmp_dir = std::env::temp_dir().join(format!("lite-server-rolling-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp_dir);
    let dir = tmp_dir.join("rolling_model").join("1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("model.py"), model_py).unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 2\nmax_requests: 3\n",
    )
    .unwrap();

    let port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(port);
    kill_stale_on_port(metrics_port);
    let _server = ServerGuard::start(&[
        "--port", &port.to_string(),
        "--metrics-port", &metrics_port.to_string(),
        "--model-repo", &tmp_dir.to_string_lossy(),
        "--no-grpc",
        "--log-level", "warn",
    ]);
    wait_for_server(port, 60).await;
    let base = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::new();
    load_model(&base, "rolling_model", "1").await;

    // Sequential burst: 8 requests against two slots with max_requests=3 —
    // both slots cross their budget mid-burst.
    for i in 0..8 {
        let resp = client
            .post(format!("{}/v2/models/rolling_model/infer", base))
            .json(&json!({"input": i}))
            .send().await.unwrap();
        assert_eq!(resp.status(), 200, "request {i} must be served during recycle");
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["output"], json!(i * 2));
    }

    // At least one worker must have been replaced by a rolling recycle.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut recycled = false;
    while tokio::time::Instant::now() < deadline {
        let resp = client
            .get(format!("http://127.0.0.1:{}/metrics", metrics_port))
            .send().await.unwrap();
        let body = resp.text().await.unwrap();
        let count = body.lines()
            .find(|l| l.starts_with(
                "liteserver_worker_respawns_total{model=\"rolling_model\",reason=\"rolling_recycle\",version=\"1\"}"
            ))
            .and_then(|l| l.rsplit_once(' '))
            .and_then(|(_, n)| n.parse::<f64>().ok())
            .unwrap_or(0.0);
        if count >= 1.0 {
            recycled = true;
            break;
        }
        sleep(Duration::from_millis(200)).await;
    }
    assert!(recycled, "at least one worker must be recycled during the burst");

    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
}

/// H1 regression: with the status coordinator running (health_check_interval
/// > 0), a rolling recycle must NEVER flip the version to Degraded — every
/// request during the recycle window is served (200), because the recycle
/// claims the slot without ejecting it. setup() sleeps 2s so the recycle
/// window spans many coordinator (0.2s) ticks; a 0.5s predict keeps several
/// request admissions inside that window.
#[tokio::test]
async fn should_stay_ready_during_rolling_recycle_with_health_checks() {
    let model_py = r#"import time
from lite_server import LitAPI


class ReadyAPI(LitAPI):
    def setup(self, device):
        time.sleep(2)

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        time.sleep(0.5)
        return {"output": x * 2}

    def encode_response(self, output):
        return output
"#;

    let tmp_dir = std::env::temp_dir().join(format!("lite-server-ready-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp_dir);
    let dir = tmp_dir.join("ready_model").join("1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("model.py"), model_py).unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 2\nmax_requests: 3\nhealth_check_interval: 0.2\n",
    )
    .unwrap();

    let port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(port);
    kill_stale_on_port(metrics_port);
    let _server = ServerGuard::start(&[
        "--port", &port.to_string(),
        "--metrics-port", &metrics_port.to_string(),
        "--model-repo", &tmp_dir.to_string_lossy(),
        "--no-grpc",
        "--log-level", "warn",
    ]);
    wait_for_server(port, 60).await;
    let base = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::new();
    load_model(&base, "ready_model", "1").await;

    // Sequential burst: 8 slow requests (≈4s) against two slots with
    // max_requests=3 — both slots cross their budget mid-burst, and the
    // 0.2s coordinator ticks many times inside each recycle window.
    for i in 0..8 {
        let resp = client
            .post(format!("{}/v2/models/ready_model/infer", base))
            .json(&json!({"input": i}))
            .send().await.unwrap();
        assert_eq!(
            resp.status(),
            200,
            "request {i} must be served: a rolling recycle must not degrade the version"
        );
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["output"], json!(i * 2));
    }

    // Sanity: the burst really did trigger at least one rolling recycle.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut recycled = false;
    while tokio::time::Instant::now() < deadline {
        let resp = client
            .get(format!("http://127.0.0.1:{}/metrics", metrics_port))
            .send().await.unwrap();
        let body = resp.text().await.unwrap();
        let count = body.lines()
            .find(|l| l.starts_with(
                "liteserver_worker_respawns_total{model=\"ready_model\",reason=\"rolling_recycle\",version=\"1\"}"
            ))
            .and_then(|l| l.rsplit_once(' '))
            .and_then(|(_, n)| n.parse::<f64>().ok())
            .unwrap_or(0.0);
        if count >= 1.0 {
            recycled = true;
            break;
        }
        sleep(Duration::from_millis(200)).await;
    }
    assert!(recycled, "the burst must have triggered a rolling recycle");

    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
}

/// H2 regression (path a): the old worker dies on its own after the recycle
/// was claimed but before the respawn runs. The graceful respawn must NOT
/// queue a stop for the dead peer — ZMQ PAIR would replay it to the
/// replacement on reconnect and kill it silently.
///
/// The kill window is created deterministically WITHOUT in-flight tricks
/// (a worker slot executes batches serially, so a quick request can never
/// cross the budget while a slow one is still in flight): the respawn
/// listener is serial and global — a blocker model's slow-starting recycle
/// (8s setup) occupies it while replay_model's recycle signal sits queued,
/// leaving ample time to SIGKILL the already-claimed worker.
#[tokio::test]
async fn should_not_replay_stop_to_replacement_when_old_worker_dies_mid_recycle() {
    let blocker_py = r#"import time
from lite_server import LitAPI


class BlockerAPI(LitAPI):
    def setup(self, device):
        time.sleep(8)

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output
"#;
    let replay_py = r#"from lite_server import LitAPI


class ReplayAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output
"#;

    let tmp_dir = std::env::temp_dir().join(format!("lite-server-replay-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp_dir);
    let blocker_dir = tmp_dir.join("blocker_model").join("1");
    let replay_dir = tmp_dir.join("replay_model").join("1");
    std::fs::create_dir_all(&blocker_dir).unwrap();
    std::fs::create_dir_all(&replay_dir).unwrap();
    std::fs::write(blocker_dir.join("model.py"), blocker_py).unwrap();
    std::fs::write(
        blocker_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\nmax_requests: 1\n",
    )
    .unwrap();
    std::fs::write(replay_dir.join("model.py"), replay_py).unwrap();
    std::fs::write(
        replay_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 2\nmax_requests: 1\n",
    )
    .unwrap();

    let port = next_test_port();
    kill_stale_on_port(port);
    // Clean leftover workers from a previous failed run — replay_model's
    // worker 0 is found by pgrep below, and a stale match would be killed
    // instead of the real one.
    let _ = std::process::Command::new("pkill")
        .args(["-9", "-f", "lite_server.worker.inference --model-name replay_model"])
        .status();
    let _server = ServerGuard::start(&[
        "--port", &port.to_string(),
        "--model-repo", &tmp_dir.to_string_lossy(),
        "--no-grpc",
        "--log-level", "warn",
    ]);
    // 60s: dedicated-server startup is the suite's bottleneck under full
    // parallel load (30s proved flaky there).
    wait_for_server(port, 60).await;
    let base = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::new();
    load_model(&base, "blocker_model", "1").await;
    load_model(&base, "replay_model", "1").await;

    // replay_model's worker 0 python process, found by its command line
    // (unique to this test's model name; /health exposes no pids).
    let worker_pid = || {
        let out = std::process::Command::new("pgrep")
            .args([
                "-f",
                "lite_server.worker.inference --model-name replay_model .* --worker-id 0 ",
            ])
            .output()
            .expect("pgrep runs");
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|l| l.trim().parse::<u32>().ok())
            .next()
    };
    let infer = |model: &str, pin_worker0: bool| {
        let client = client.clone();
        let base = base.clone();
        let model = model.to_string();
        async move {
            let mut req = client
                .post(format!("{}/v2/models/{}/infer", base, model))
                .timeout(Duration::from_secs(30))
                .json(&json!({"input": 1}));
            if pin_worker0 {
                req = req.header("x-lite-worker-id", "0");
            }
            req.send().await.map(|r| r.status().as_u16())
        }
    };

    // 1. Occupy the serial respawn listener: blocker_model crosses its
    //    budget and its replacement takes ~8s to start.
    let status = infer("blocker_model", false).await.expect("blocker answered");
    assert_eq!(status, 200);

    // 2. replay_model slot 0 crosses its budget; its recycle signal queues
    //    behind blocker_model's respawn.
    let status = infer("replay_model", true).await.expect("replay answered");
    assert_eq!(status, 200, "the crossing request itself is served");

    // 3. Kill the claimed worker while its recycle signal is still queued
    //    (H2 path a: the old worker dies on its own before the respawn).
    let old_pid = worker_pid().expect("replay_model worker 0 python process found");
    let killed = std::process::Command::new("kill")
        .args(["-9", &old_pid.to_string()])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    assert!(killed, "must SIGKILL the claimed worker 0 (pid {old_pid})");

    // 4. The replacement must come up with a fresh pid (only after the
    //    listener finishes blocker_model's ~8s respawn)…
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut new_pid = None;
    while tokio::time::Instant::now() < deadline {
        match worker_pid() {
            Some(pid) if pid != old_pid => {
                new_pid = Some(pid);
                break;
            }
            _ => sleep(Duration::from_millis(200)).await,
        }
    }
    let new_pid = new_pid.expect("replacement worker 0 came up");

    // …and it must SURVIVE: a replayed stop would kill it within seconds of
    // its connect. Observe well past the startup window, then serve slot 0.
    sleep(Duration::from_secs(5)).await;
    assert_eq!(
        worker_pid(),
        Some(new_pid),
        "replacement must not be killed by a replayed stale stop"
    );
    let status = infer("replay_model", true).await.expect("slot 0 answered");
    assert_eq!(status, 200, "the recycled slot serves its replacement");

    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
}

// ---------------------------------------------------------------------------
// §4.2: max_loaded_versions LRU eviction
// ---------------------------------------------------------------------------

/// With max_loaded_versions=2 and v1 active, loading a third version must
/// evict the least-recently-used non-active version (v2) while the active
/// version stays untouched.
#[tokio::test]
async fn test_max_loaded_versions_lru_eviction() {
    let model_py = r#"from lite_server import LitAPI


class LruAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x * 2}

    def encode_response(self, output):
        return output
"#;

    let tmp_dir = std::env::temp_dir().join(format!("lite-server-lru-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp_dir);
    let cfg = "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n";
    for v in ["1", "2", "3"] {
        let dir = tmp_dir.join("lru_model").join(v);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("model.py"), model_py).unwrap();
        std::fs::write(dir.join("config.yaml"), cfg).unwrap();
    }
    let port = next_test_port();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 0.0.0.0\n  http_port: {}\n  grpc_port: 18991\n  metrics_port: 18992\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: false\nmodel_repository:\n  path: {}\norchestration:\n  control_mode: explicit\n  models:\n    - name: lru_model\n      load_policy: explicit\n      max_loaded_versions: 2\n",
            port,
            tmp_dir.to_string_lossy()
        ),
    )
    .unwrap();

    kill_stale_on_port(port);
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(port, 60).await;
    let base = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::new();

    let health_versions = |body: &Value| -> Vec<String> {
        body["models"].as_array().unwrap().iter()
            .filter(|m| m["name"] == "lru_model")
            .flat_map(|m| m["versions"].as_array().unwrap().iter()
                .map(|e| e["version"].as_str().unwrap().to_string())
                .collect::<Vec<_>>())
            .collect()
    };

    // v1 loads first and becomes active (no active version yet); v2 loads
    // next without taking over active.
    load_model(&base, "lru_model", "1").await;
    load_model(&base, "lru_model", "2").await;

    // Loading v3 exceeds max_loaded_versions=2 → v2 (the only non-active
    // version) is evicted; active v1 must survive.
    load_model(&base, "lru_model", "3").await;

    let resp = client.get(format!("{}/health", base)).send().await.unwrap();
    let body: Value = resp.json().await.unwrap();
    let mut versions = health_versions(&body);
    versions.sort();
    assert_eq!(versions, vec!["1".to_string(), "3".to_string()],
        "v2 must be evicted, active v1 and new v3 remain: {:?}", body["models"]);

    // Active v1 still serves.
    let resp = client
        .post(format!("{}/v2/models/lru_model/infer", base))
        .json(&json!({"input": 21}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["output"], 42);

    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
}

// ---------------------------------------------------------------------------
// control_mode: auto — model poller
// ---------------------------------------------------------------------------

const POLL_MODEL_PY: &str = r#"from lite_server import LitAPI


class PollAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x * 2}

    def encode_response(self, output):
        return output
"#;

const POLL_MODEL_CFG: &str =
    "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n";

fn write_poll_model_version(repo: &std::path::Path, version: &str) {
    let dir = repo.join("poll_model").join(version);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("model.py"), POLL_MODEL_PY).unwrap();
    std::fs::write(dir.join("config.yaml"), POLL_MODEL_CFG).unwrap();
}

fn write_auto_poll_server_yaml(tmp_dir: &std::path::Path, port: u16, grpc_port: u16, metrics_port: u16) -> std::path::PathBuf {
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 0.0.0.0\n  http_port: {}\n  grpc_port: {}\n  metrics_port: {}\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: false\nmodel_repository:\n  path: {}\norchestration:\n  control_mode: auto\n  poll_interval: 1\n  load_models:\n    - poll_model\n",
            port,
            grpc_port,
            metrics_port,
            tmp_dir.to_string_lossy()
        ),
    )
    .unwrap();
    server_yaml
}

/// Auto-mode server.yaml with a per-model strategy entry. Unlike
/// `write_auto_poll_server_yaml` (no strategy → legacy load-all default),
/// this exercises the documented load_policy paths under reconcile.
fn write_auto_poll_strategy_yaml(
    tmp_dir: &std::path::Path,
    port: u16,
    grpc_port: u16,
    metrics_port: u16,
    load_policy: &str,
    versions_to_load: &[&str],
) -> std::path::PathBuf {
    let versions_yaml = versions_to_load
        .iter()
        .map(|v| format!("\"{}\"", v))
        .collect::<Vec<_>>()
        .join(", ");
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 0.0.0.0\n  http_port: {}\n  grpc_port: {}\n  metrics_port: {}\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: false\nmodel_repository:\n  path: {}\norchestration:\n  control_mode: auto\n  poll_interval: 1\n  load_models:\n    - poll_model\n  models:\n    - name: poll_model\n      load_policy: {}\n      versions_to_load: [{}]\n",
            port,
            grpc_port,
            metrics_port,
            tmp_dir.to_string_lossy(),
            load_policy,
            versions_yaml
        ),
    )
    .unwrap();
    server_yaml
}

/// Versions of poll_model listed in a /health response body.
fn poll_model_versions(body: &Value) -> Vec<String> {
    body["models"].as_array().unwrap().iter()
        .filter(|m| m["name"] == "poll_model")
        .flat_map(|m| m["versions"].as_array().unwrap().iter()
            .map(|e| e["version"].as_str().unwrap().to_string())
            .collect::<Vec<_>>())
        .collect()
}

/// control_mode=auto: a version directory created after startup is
/// discovered by the poller and loaded without any API call.
#[tokio::test]
async fn test_auto_poll_discovers_new_version() {
    let tmp_dir = std::env::temp_dir().join(format!("lite-server-autopoll-disc-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp_dir);
    write_poll_model_version(&tmp_dir, "1");

    let port = next_test_port();
    let server_yaml = write_auto_poll_server_yaml(&tmp_dir, port, 18111, 18112);
    kill_stale_on_port(port);
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(port, 60).await;
    let base = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::new();

    // v1 is loaded at startup (initial reconcile).
    assert!(
        wait_model_ready(&base, "poll_model", 30).await,
        "v1 did not become ready at startup"
    );

    // Create v2 after startup; the poller must discover and load it.
    write_poll_model_version(&tmp_dir, "2");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut found = false;
    while tokio::time::Instant::now() < deadline {
        if let Ok(resp) = client.get(format!("{}/health", base)).send().await {
            let body: Value = resp.json().await.unwrap();
            if poll_model_versions(&body).iter().any(|v| v == "2") {
                found = true;
                break;
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
    assert!(found, "poller did not discover and load v2 within 30s");

    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
}

/// control_mode=auto: a version directory removed from disk is unloaded by
/// the poller (declarative semantics — disk state is the source of truth).
#[tokio::test]
async fn test_auto_poll_unloads_removed_version() {
    let tmp_dir = std::env::temp_dir().join(format!("lite-server-autopoll-unload-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp_dir);
    write_poll_model_version(&tmp_dir, "1");
    write_poll_model_version(&tmp_dir, "2");

    let port = next_test_port();
    let server_yaml = write_auto_poll_server_yaml(&tmp_dir, port, 18113, 18114);
    kill_stale_on_port(port);
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(port, 60).await;
    let base = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::new();

    // Both versions load at startup (no strategy → all disk versions).
    assert!(
        wait_model_ready(&base, "poll_model", 30).await,
        "startup versions did not become ready"
    );
    let resp = client.get(format!("{}/health", base)).send().await.unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        poll_model_versions(&body).len(),
        2,
        "both versions should be loaded at startup: {:?}",
        body["models"]
    );

    // Remove v2 from disk; the poller must unload it.
    std::fs::remove_dir_all(tmp_dir.join("poll_model").join("2")).unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut removed = false;
    while tokio::time::Instant::now() < deadline {
        if let Ok(resp) = client.get(format!("{}/health", base)).send().await {
            let body: Value = resp.json().await.unwrap();
            let versions = poll_model_versions(&body);
            if versions.len() == 1 && versions[0] == "1" {
                removed = true;
                break;
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
    assert!(removed, "poller did not unload removed v2 within 30s");

    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
}

/// control_mode=auto + load_policy=latest: when a newer version directory
/// appears, reconcile loads it AND unloads the superseded version (the
/// target set is the single latest version).
#[tokio::test]
async fn test_auto_poll_latest_unloads_superseded_version() {
    let tmp_dir = std::env::temp_dir().join(format!("lite-server-autopoll-latest-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp_dir);
    write_poll_model_version(&tmp_dir, "1");

    let port = next_test_port();
    let server_yaml = write_auto_poll_strategy_yaml(&tmp_dir, port, 18115, 18116, "latest", &[]);
    kill_stale_on_port(port);
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(port, 60).await;
    let base = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::new();

    // v1 is the latest on disk at startup.
    assert!(
        wait_model_ready(&base, "poll_model", 30).await,
        "v1 did not become ready at startup"
    );

    // A newer version appears; reconcile must load v2 and unload v1.
    write_poll_model_version(&tmp_dir, "2");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut swapped = false;
    while tokio::time::Instant::now() < deadline {
        if let Ok(resp) = client.get(format!("{}/health", base)).send().await {
            let body: Value = resp.json().await.unwrap();
            if poll_model_versions(&body) == vec!["2".to_string()] {
                swapped = true;
                break;
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
    assert!(
        swapped,
        "latest policy did not swap to v2 (load v2 + unload v1) within 30s"
    );

    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
}

/// control_mode=auto + load_policy=explicit: a version directory that is not
/// in versions_to_load must NOT be loaded, no matter how many poll ticks pass.
#[tokio::test]
async fn test_auto_poll_explicit_ignores_unlisted_version() {
    let tmp_dir = std::env::temp_dir().join(format!("lite-server-autopoll-expl-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp_dir);
    write_poll_model_version(&tmp_dir, "1");

    let port = next_test_port();
    let server_yaml = write_auto_poll_strategy_yaml(&tmp_dir, port, 18117, 18118, "explicit", &["1"]);
    kill_stale_on_port(port);
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(port, 60).await;
    let base = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::new();

    assert!(
        wait_model_ready(&base, "poll_model", 30).await,
        "v1 did not become ready at startup"
    );

    // v2 is not in versions_to_load; it must stay unloaded across several
    // poll ticks (poll_interval = 1s).
    write_poll_model_version(&tmp_dir, "2");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        let resp = client.get(format!("{}/health", base)).send().await.unwrap();
        let body: Value = resp.json().await.unwrap();
        assert_eq!(
            poll_model_versions(&body),
            vec!["1".to_string()],
            "explicit policy must not load unlisted v2: {:?}",
            body["models"]
        );
        sleep(Duration::from_millis(500)).await;
    }

    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
}

// ---------------------------------------------------------------------------
// §4.3: weighted / canary routing
// ---------------------------------------------------------------------------

/// Two versions of canary_model — v1 computes x*2, v2 computes x*3, so
/// responses identify the serving version (§4.3 weights / §4.4 canary pin).
fn write_canary_model_repo(tmp_dir: &std::path::Path) {
    let model_py = |factor: i32| format!(r#"from lite_server import LitAPI


class CanaryAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {{"output": x * {}}}

    def encode_response(self, output):
        return output
"#, factor);

    let _ = std::fs::remove_dir_all(tmp_dir);
    let cfg = "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n";
    for (v, factor) in [("1", 2), ("2", 3)] {
        let dir = tmp_dir.join("canary_model").join(v);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("model.py"), model_py(factor)).unwrap();
        std::fs::write(dir.join("config.yaml"), cfg).unwrap();
    }
}

/// Weights drive bare-request distribution; x-lite-version pins explicitly
/// (requires `features.canary_override: true`, P5-2); activate is a hard
/// cutover.
#[tokio::test]
async fn test_weighted_routing_canary() {
    let tmp_dir = std::env::temp_dir().join(format!("lite-server-canary-{}", std::process::id()));
    write_canary_model_repo(&tmp_dir);

    let port = next_test_port();
    kill_stale_on_port(port);
    let cfg_dir = std::env::temp_dir().join(format!("lite-server-canary-cfg-{}", std::process::id()));
    std::fs::create_dir_all(&cfg_dir).unwrap();
    let server_yaml = cfg_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {}\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: false\nmodel_repository:\n  path: {}\nfeatures:\n  canary_override: true\n",
            port,
            tmp_dir.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(port, 60).await;
    let base = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::new();

    load_model(&base, "canary_model", "1").await;
    load_model(&base, "canary_model", "2").await;

    let infer_output = |headers: &[(&str, &str)]| {
        let base = base.clone();
        let client = client.clone();
        let headers: Vec<(String, String)> =
            headers.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        async move {
            let mut req = client
                .post(format!("{}/v2/models/canary_model/infer", base))
                .json(&json!({"input": 1}));
            for (k, v) in headers {
                req = req.header(k, v);
            }
            let resp = req.send().await.unwrap();
            assert_eq!(resp.status(), 200);
            resp.json::<Value>().await.unwrap()["output"].as_i64().unwrap()
        }
    };
    let put_weights = |body: &str| {
        let base = base.clone();
        let client = client.clone();
        let body = body.to_string();
        async move {
            let resp = client
                .put(format!("{}/v2/models/canary_model/routing", base))
                .header("content-type", "application/json")
                .body(body)
                .send().await.unwrap();
            assert_eq!(resp.status(), 200);
        }
    };

    // 100/0: all traffic to v1.
    put_weights(r#"{"weights":{"1":100,"2":0}}"#).await;
    for _ in 0..20 {
        assert_eq!(infer_output(&[]).await, 2, "100/0 must serve only v1");
    }

    // 90/10: roughly proportional split (n=100, expect ~10 v2, wide bounds).
    put_weights(r#"{"weights":{"1":90,"2":10}}"#).await;
    let mut v2_count = 0;
    for _ in 0..100 {
        if infer_output(&[]).await == 3 {
            v2_count += 1;
        }
    }
    assert!((2..=25).contains(&v2_count), "v2 served {} / 100, expected ~10", v2_count);

    // Header pin beats weights: 0/100 with x-lite-version: 1 → v1.
    put_weights(r#"{"weights":{"2":100}}"#).await;
    assert_eq!(infer_output(&[]).await, 3, "100% v2 after zeroing v1");
    assert_eq!(
        infer_output(&[("x-lite-version", "1")]).await,
        2,
        "x-lite-version header must pin to v1"
    );

    // activate(v1) is a hard cutover: weights become 100/0.
    let resp = client
        .post(format!("{}/v2/models/canary_model/versions/1/activate", base))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    for _ in 0..20 {
        assert_eq!(infer_output(&[]).await, 2, "activate must hard-switch to v1");
    }

    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
}

/// Streaming variant of the canary fixture — v1 yields token "one", v2 yields
/// token "two", so the first SSE frame identifies the serving version.
fn write_canary_stream_model_repo(tmp_dir: &std::path::Path) {
    let model_py = |token: &str| format!(r#"from lite_server import LitAPI


class CanaryStreamAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request

    async def stream_predict(self, request, ctx):
        yield {{"token": "{token}"}}
"#, token = token);

    let _ = std::fs::remove_dir_all(tmp_dir);
    let cfg = "max_batch_size: 1\nbatch_timeout: 0.0\nstream: true\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n";
    for (v, token) in [("1", "one"), ("2", "two")] {
        let dir = tmp_dir.join("canary_stream_model").join(v);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("model.py"), model_py(token)).unwrap();
        std::fs::write(dir.join("config.yaml"), cfg).unwrap();
    }
}

/// §4.3 weighted routing applies to bare streaming URLs too — SSE/WS/h2 bidi
/// share `resolve_version` with unary (explicit version > pin > weighted >
/// active). `test_weighted_routing_canary` only covers /infer; this closes
/// the SSE gap (weighted split + pin + versioned-path skip).
#[tokio::test]
async fn test_weighted_routing_canary_sse() {
    let tmp_dir = std::env::temp_dir()
        .join(format!("lite-server-canary-sse-{}", std::process::id()));
    write_canary_stream_model_repo(&tmp_dir);

    let port = next_test_port();
    kill_stale_on_port(port);
    let cfg_dir =
        std::env::temp_dir().join(format!("lite-server-canary-sse-cfg-{}", std::process::id()));
    std::fs::create_dir_all(&cfg_dir).unwrap();
    let server_yaml = cfg_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {}\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: false\nmodel_repository:\n  path: {}\nfeatures:\n  canary_override: true\n",
            port,
            tmp_dir.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(port, 60).await;
    let base = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::new();

    load_model(&base, "canary_stream_model", "1").await;
    load_model(&base, "canary_stream_model", "2").await;

    // One bare /events request; the worker yields a single token then [DONE],
    // so the SSE body closes quickly. Returns 1/2 per the served version.
    let sse_version = |headers: &[(&str, &str)]| {
        let base = base.clone();
        let client = client.clone();
        let headers: Vec<(String, String)> =
            headers.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        async move {
            let mut req = client
                .post(format!("{}/v2/models/canary_stream_model/events", base))
                .json(&json!({"input": 1}));
            for (k, v) in headers {
                req = req.header(k, v);
            }
            let resp = req.send().await.unwrap();
            assert_eq!(resp.status(), 200);
            let body = tokio::time::timeout(Duration::from_secs(15), resp.text())
                .await
                .expect("SSE body did not close within 15s")
                .unwrap();
            if body.contains(r#""token":"one""#) {
                1
            } else if body.contains(r#""token":"two""#) {
                2
            } else {
                panic!("unexpected SSE body: {body}");
            }
        }
    };
    let put_weights = |body: &str| {
        let base = base.clone();
        let client = client.clone();
        let body = body.to_string();
        async move {
            let resp = client
                .put(format!("{}/v2/models/canary_stream_model/routing", base))
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
        }
    };

    // 100/0: all traffic to v1.
    put_weights(r#"{"weights":{"1":100,"2":0}}"#).await;
    for _ in 0..20 {
        assert_eq!(sse_version(&[]).await, 1, "100/0 must serve only v1");
    }

    // 90/10: roughly proportional split (n=100, expect ~10 v2, wide bounds).
    put_weights(r#"{"weights":{"1":90,"2":10}}"#).await;
    let mut v2_count = 0;
    for _ in 0..100 {
        if sse_version(&[]).await == 2 {
            v2_count += 1;
        }
    }
    assert!(
        (2..=25).contains(&v2_count),
        "v2 served {} / 100, expected ~10",
        v2_count
    );

    // Header pin beats weights: 0/100 with x-lite-version: 1 → v1.
    put_weights(r#"{"weights":{"2":100}}"#).await;
    assert_eq!(sse_version(&[]).await, 2, "100% v2 after zeroing v1");
    assert_eq!(
        sse_version(&[("x-lite-version", "1")]).await,
        1,
        "x-lite-version header must pin to v1"
    );

    // Versioned URL skips pin and weights: /versions/1/events under 0/100.
    let resp = client
        .post(format!("{base}/v2/models/canary_stream_model/versions/1/events"))
        .json(&json!({"input": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = tokio::time::timeout(Duration::from_secs(15), resp.text())
        .await
        .expect("SSE body did not close within 15s")
        .unwrap();
    assert!(
        body.contains(r#""token":"one""#),
        "versioned path must serve the explicit v1: {body}"
    );

    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
}

/// Bidi variant of the canary fixture — v1's `on_chunk` returns token "one",
/// v2 returns token "two", so the echo frame identifies the serving version.
fn write_canary_bidi_model_repo(tmp_dir: &std::path::Path) {
    let model_py = |token: &str| format!(r#"from lite_server import LitAPI


class BidiHandler:
    def on_open(self, initial_data):
        return {{"opened": True}}

    def on_chunk(self, chunk):
        return {{"token": "{token}"}}

    def on_close(self):
        pass


class CanaryBidiAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request

    def predict(self, x):
        return {{"output": x}}

    def encode_response(self, output):
        return output

    def bidi_stream(self):
        return BidiHandler()
"#, token = token);

    let _ = std::fs::remove_dir_all(tmp_dir);
    let cfg = "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n";
    for (v, token) in [("1", "one"), ("2", "two")] {
        let dir = tmp_dir.join("canary_bidi_model").join(v);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("model.py"), model_py(token)).unwrap();
        std::fs::write(dir.join("config.yaml"), cfg).unwrap();
    }
}

/// §4.3 weighted routing applies to bare h2 bidi URLs too — h2 bidi shares
/// `resolve_version` with SSE/WS/unary (bidi.rs). This closes the last
/// transport gap: weighted split + pin + versioned-path skip, over real h2
/// full-duplex sessions (harness = test_h2_bidi_prior_knowledge_full_duplex).
#[tokio::test]
async fn test_weighted_routing_canary_h2_bidi() {
    use lite_server::proto::liteserver as pb;
    use lite_server::streaming::lpm;

    let tmp_dir =
        std::env::temp_dir().join(format!("lite-server-canary-bidi-{}", std::process::id()));
    write_canary_bidi_model_repo(&tmp_dir);

    let port = next_test_port();
    kill_stale_on_port(port);
    let cfg_dir =
        std::env::temp_dir().join(format!("lite-server-canary-bidi-cfg-{}", std::process::id()));
    std::fs::create_dir_all(&cfg_dir).unwrap();
    let server_yaml = cfg_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {}\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: false\nmodel_repository:\n  path: {}\nfeatures:\n  canary_override: true\n",
            port,
            tmp_dir.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(port, 60).await;
    let base = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap();

    load_model(&base, "canary_bidi_model", "1").await;
    load_model(&base, "canary_bidi_model", "2").await;

    // One full-duplex h2 bidi session: Open → on_open frame → Data chunk →
    // echo frame (token identifies the served version) → Close → terminal
    // Close frame. Returns 1/2 per the served version.
    let bidi_version = |path: String, headers: &[(&str, &str)]| {
        let base = base.clone();
        let client = client.clone();
        let headers: Vec<(String, String)> =
            headers.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        async move {
            async fn read_frame(
                stream: &mut (impl futures::Stream<Item = Result<bytes::Bytes, reqwest::Error>>
                              + Unpin),
                buf: &mut bytes::BytesMut,
            ) -> pb::BidiChunk {
                use futures::StreamExt;
                let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
                loop {
                    if let Ok(Some(c)) = lpm::try_decode_frame(buf) {
                        return c;
                    }
                    let next = tokio::time::timeout_at(deadline, stream.next())
                        .await
                        .expect("timed out waiting for LPM frame")
                        .expect("response stream ended before frame");
                    buf.extend_from_slice(&next.expect("response stream error"));
                }
            }

            let (body_tx, body_rx) =
                tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(8);
            let body_rx = std::sync::Arc::new(std::sync::Mutex::new(body_rx));
            let body_stream = futures::stream::poll_fn(move |cx| {
                body_rx.lock().unwrap().poll_recv(cx)
            });
            // Full-duplex bootstrap: the server awaits the first LPM frame
            // before committing the 200 — queue BidiOpen up front.
            body_tx
                .send(Ok(lpm::encode_frame(&pb::BidiChunk {
                    stream_id: String::new(),
                    payload: Some(pb::bidi_chunk::Payload::Open(pb::BidiOpen {
                        initial_data: bytes::Bytes::from_static(b"{}"),
                        ..Default::default()
                    })),
                })))
                .await
                .unwrap();
            let mut req = client
                .post(format!("{base}{path}"))
                .header("content-type", "application/x-lite-bidi")
                .body(reqwest::Body::wrap_stream(body_stream));
            for (k, v) in headers {
                req = req.header(k, v);
            }
            let resp = req.send().await.unwrap();
            assert_eq!(resp.status(), 200, "h2 bidi must accept the session");

            let mut resp_stream = resp.bytes_stream();
            let mut buf = bytes::BytesMut::new();

            // 1. The queued Open → on_open Data frame.
            let f = read_frame(&mut resp_stream, &mut buf).await;
            assert!(
                matches!(f.payload, Some(pb::bidi_chunk::Payload::Data(_))),
                "expected on_open Data frame, got {:?}",
                f.payload
            );

            // 2. Data sent mid-stream → echo Data frame carrying the token.
            body_tx
                .send(Ok(lpm::encode_frame(&pb::BidiChunk {
                    stream_id: String::new(),
                    payload: Some(pb::bidi_chunk::Payload::Data(pb::BidiData {
                        data: bytes::Bytes::from_static(br#"{"chunk": 1}"#),
                    })),
                })))
                .await
                .unwrap();
            let f = read_frame(&mut resp_stream, &mut buf).await;
            let served = match f.payload {
                Some(pb::bidi_chunk::Payload::Data(d)) => {
                    let body = String::from_utf8_lossy(&d.data);
                    if body.contains(r#""token":"one""#) {
                        1
                    } else if body.contains(r#""token":"two""#) {
                        2
                    } else {
                        panic!("unexpected echo frame: {body}");
                    }
                }
                other => panic!("expected echo Data frame, got {other:?}"),
            };

            // 3. Close → terminal Close frame.
            body_tx
                .send(Ok(lpm::encode_frame(&pb::BidiChunk {
                    stream_id: String::new(),
                    payload: Some(pb::bidi_chunk::Payload::Close(pb::BidiClose {})),
                })))
                .await
                .unwrap();
            let f = read_frame(&mut resp_stream, &mut buf).await;
            assert!(
                matches!(f.payload, Some(pb::bidi_chunk::Payload::Close(_))),
                "expected terminal Close frame, got {:?}",
                f.payload
            );
            served
        }
    };
    let put_weights = |body: &str| {
        let base = base.clone();
        let client = reqwest::Client::new();
        let body = body.to_string();
        async move {
            let resp = client
                .put(format!("{}/v2/models/canary_bidi_model/routing", base))
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
        }
    };
    let bare = "/v2/models/canary_bidi_model/bidi".to_string();

    // 100/0: all traffic to v1.
    put_weights(r#"{"weights":{"1":100,"2":0}}"#).await;
    for _ in 0..20 {
        assert_eq!(bidi_version(bare.clone(), &[]).await, 1, "100/0 must serve only v1");
    }

    // 90/10: roughly proportional split (n=100, expect ~10 v2, wide bounds).
    put_weights(r#"{"weights":{"1":90,"2":10}}"#).await;
    let mut v2_count = 0;
    for _ in 0..100 {
        if bidi_version(bare.clone(), &[]).await == 2 {
            v2_count += 1;
        }
    }
    assert!(
        (2..=25).contains(&v2_count),
        "v2 served {} / 100, expected ~10",
        v2_count
    );

    // Header pin beats weights: 0/100 with x-lite-version: 1 → v1.
    put_weights(r#"{"weights":{"2":100}}"#).await;
    assert_eq!(bidi_version(bare.clone(), &[]).await, 2, "100% v2 after zeroing v1");
    assert_eq!(
        bidi_version(bare.clone(), &[("x-lite-version", "1")]).await,
        1,
        "x-lite-version header must pin to v1"
    );

    // Versioned URL skips pin and weights: /versions/1/bidi under 0/100.
    assert_eq!(
        bidi_version("/v2/models/canary_bidi_model/versions/1/bidi".to_string(), &[]).await,
        1,
        "versioned path must serve the explicit v1"
    );

    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
}

/// P5-2 (蓝图 §4.4): gRPC x-lite-version pin parity——metadata 优先、fallback
/// proto headers map；非法 pin → InvalidArgument；pin 版本不存在 → NotFound。
#[tokio::test]
async fn test_grpc_canary_pin_parity() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;
    use std::collections::HashMap;

    async fn grpc_infer(
        infer: &mut LiteServerClient<tonic::transport::Channel>,
        pin_metadata: Option<&'static str>,
        pin_proto_header: Option<&'static str>,
    ) -> Result<i64, tonic::Status> {
        let mut req = tonic::Request::new(InferRequest {
            model_name: "canary_model".to_string(),
            version: String::new(),
            data: br#"{"input":1}"#.to_vec().into(),
            headers: pin_proto_header
                .map(|p| HashMap::from([("x-lite-version".to_string(), p.to_string())]))
                .unwrap_or_default(),

        ..Default::default()
});
        if let Some(p) = pin_metadata {
            req.metadata_mut().insert("x-lite-version", p.parse().unwrap());
        }
        let resp = infer.infer(req).await?.into_inner();
        let body: Value = serde_json::from_slice(&resp.data).unwrap();
        Ok(body["output"].as_i64().unwrap())
    }

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let tmp_dir = std::env::temp_dir().join(format!("lite-server-grpc-canary-{}", std::process::id()));
    write_canary_model_repo(&tmp_dir);
    let cfg_dir = std::env::temp_dir().join(format!("lite-server-grpc-canary-cfg-{}", std::process::id()));
    std::fs::create_dir_all(&cfg_dir).unwrap();
    let server_yaml = cfg_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {}\n  grpc_port: {}\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {}\nfeatures:\n  canary_override: true\n",
            http_port,
            grpc_port,
            tmp_dir.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "canary_model", "1").await;
    load_model(&base, "canary_model", "2").await;

    // Weights 0/100: bare infer goes to v2 — only a pin can land on v1.
    let resp = reqwest::Client::new()
        .put(format!("{}/v2/models/canary_model/routing", base))
        .header("content-type", "application/json")
        .body(r#"{"weights":{"2":100}}"#)
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{}", grpc_port))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut infer = LiteServerClient::new(channel);

    // metadata pin → v1 despite 0/100 weights.
    assert_eq!(grpc_infer(&mut infer, Some("1"), None).await.unwrap(), 2,
        "metadata pin must route to v1");
    // proto headers map fallback → v1.
    assert_eq!(grpc_infer(&mut infer, None, Some("1")).await.unwrap(), 2,
        "proto headers map pin must route to v1");
    // metadata beats proto headers when both are present.
    assert_eq!(grpc_infer(&mut infer, Some("1"), Some("2")).await.unwrap(), 2,
        "metadata pin must win over proto headers pin");
    // no pin → weighted routing (v2).
    assert_eq!(grpc_infer(&mut infer, None, None).await.unwrap(), 3,
        "no pin → weights route to v2");
    // pin to an unregistered version → NotFound.
    let err = grpc_infer(&mut infer, Some("9"), None).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound, "unknown pin version → NotFound");
    // invalid pin → InvalidArgument.
    let err = grpc_infer(&mut infer, Some("a b"), None).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "invalid pin → InvalidArgument");

    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
}

/// P5-2 (蓝图 §4.4, D16): 默认配置（features 段缺省 → canary_override=false）
/// 下 x-lite-version 被整体忽略——pin 不影响权重路由，非法/不存在的 pin 也不报错。
#[tokio::test]
async fn test_canary_override_default_off_ignores_pin() {
    let port = next_test_port();
    kill_stale_on_port(port);
    let tmp_dir = std::env::temp_dir().join(format!("lite-server-canary-off-{}", std::process::id()));
    write_canary_model_repo(&tmp_dir);
    let cfg_dir = std::env::temp_dir().join(format!("lite-server-canary-off-cfg-{}", std::process::id()));
    std::fs::create_dir_all(&cfg_dir).unwrap();
    let server_yaml = cfg_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {}\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: false\nmodel_repository:\n  path: {}\n",
            port,
            tmp_dir.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(port, 60).await;
    let base = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::new();
    load_model(&base, "canary_model", "1").await;
    load_model(&base, "canary_model", "2").await;

    let resp = client
        .put(format!("{}/v2/models/canary_model/routing", base))
        .header("content-type", "application/json")
        .body(r#"{"weights":{"2":100}}"#)
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let infer_output = |pin: Option<&str>| {
        let base = base.clone();
        let client = client.clone();
        let pin = pin.map(str::to_string);
        async move {
            let mut req = client
                .post(format!("{}/v2/models/canary_model/infer", base))
                .json(&json!({"input": 1}));
            if let Some(p) = pin {
                req = req.header("x-lite-version", p);
            }
            let resp = req.send().await.unwrap();
            assert_eq!(resp.status(), 200, "pin must not affect status when switch is off");
            resp.json::<Value>().await.unwrap()["output"].as_i64().unwrap()
        }
    };

    assert_eq!(infer_output(Some("1")).await, 3, "switch off → pin ignored, weights (v2) serve");
    assert_eq!(infer_output(Some("9")).await, 3, "switch off → unknown pin ignored, no 404");
    assert_eq!(infer_output(None).await, 3);

    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    tokio::fs::create_dir_all(dst).await?;
    let mut entries = tokio::fs::read_dir(src).await?;
    while let Some(entry) = entries.next_entry().await? {
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        let file_type = entry.file_type().await?;
        if file_type.is_dir() {
            Box::pin(copy_dir_recursive(&src_path, &dst_path)).await?;
        } else {
            tokio::fs::copy(&src_path, &dst_path).await?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// gRPC unary infer routes through the InferenceQueue (#1)
// ---------------------------------------------------------------------------

/// Two concurrent gRPC `Infer` RPCs against batch_model (max_batch_size=2,
/// batch_timeout=0.1) must aggregate into ONE BatchRequest, so each response
/// carries batch_size=2. Before #1, gRPC sent each request directly to a worker
/// via client.send(), bypassing the queue, so both would report batch_size=1.
#[tokio::test]
#[serial]
async fn test_grpc_infer_aggregates_into_batch() {
    use bytes::Bytes;
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;
    use std::collections::HashMap;

    // Dedicated server WITH gRPC enabled (the shared server runs --no-grpc).
    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let repo = test_model_repo();
    let _server = ServerGuard::start(&[
        "--port",
        &http_port.to_string(),
        "--grpc-port",
        &grpc_port.to_string(),
        "--model-repo",
        &repo.to_string_lossy(),
        "--no-metrics",
        "--log-level",
        "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, BATCH_MODEL, "1").await;

    let client = LiteServerClient::connect(format!("http://127.0.0.1:{}", grpc_port))
        .await
        .expect("gRPC client must connect");

    let payload = Bytes::from(serde_json::to_vec(&json!({"input": 1})).unwrap());
    let mk_req = || InferRequest {
        model_name: BATCH_MODEL.to_string(),
        version: "1".to_string(),
        data: payload.clone(),
        headers: HashMap::new(),

    ..Default::default()
};
    // infer() takes &mut self, so clone the client for concurrent calls.
    let mut c1 = client.clone();
    let mut c2 = client.clone();
    let (a, b) = tokio::join!(c1.infer(mk_req()), c2.infer(mk_req()));
    let ra = a.expect("infer A must succeed").into_inner();
    let rb = b.expect("infer B must succeed").into_inner();

    let ja: Value = serde_json::from_slice(&ra.data).expect("infer A data is JSON");
    let jb: Value = serde_json::from_slice(&rb.data).expect("infer B data is JSON");
    assert_eq!(ja["batch_size"], 2, "gRPC infer A must be batched into size 2");
    assert_eq!(jb["batch_size"], 2, "gRPC infer B must be batched into size 2");

    unload_model(&base, BATCH_MODEL, "1").await;
}

// ---------------------------------------------------------------------------
// P9-1: gRPC DecoupledInfer (1:N, model-controlled channel lifetime)
// ---------------------------------------------------------------------------

/// `DecoupledInfer` against `decoupled_model` (predict_decoupled pushes N
/// chunks then closes): the client receives N `is_final=false` data frames
/// followed by one terminal `is_final=true` frame, in order.
#[tokio::test]
#[serial]
async fn test_grpc_decoupled_infer_pushes_chunks_then_final() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::DecoupledInferRequest;
    use std::collections::HashMap;

    // Dedicated server WITH gRPC (shared server runs --no-grpc).
    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let repo = test_model_repo();
    let _server = ServerGuard::start(&[
        "--port",
        &http_port.to_string(),
        "--grpc-port",
        &grpc_port.to_string(),
        "--model-repo",
        &repo.to_string_lossy(),
        "--no-metrics",
        "--log-level",
        "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, DECOUPLED_MODEL, "1").await;

    let mut client = LiteServerClient::connect(format!("http://127.0.0.1:{}", grpc_port))
        .await
        .expect("gRPC client must connect");

    let payload = bytes::Bytes::from(serde_json::to_vec(&json!({"input": 3})).unwrap());
    let req = DecoupledInferRequest {
        model_name: DECOUPLED_MODEL.to_string(),
        version: "1".to_string(),
        data: payload,
        headers: HashMap::new(),
        ..Default::default()
    };
    let resp = client
        .decoupled_infer(req)
        .await
        .expect("DecoupledInfer must open");
    let mut stream = resp.into_inner();

    let mut frames = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(15), stream.message()).await {
            Ok(Ok(Some(frame))) => frames.push(frame),
            Ok(Ok(None)) => break,
            Ok(Err(status)) => panic!("DecoupledInfer stream error: {:?}", status),
            Err(_) => panic!("DecoupledInfer stream did not close within 15s"),
        }
    }
    // 3 data chunks (is_final=false) + 1 terminal (is_final=true).
    assert_eq!(frames.len(), 4, "expected 4 frames: {:?}", frames);
    for (i, frame) in frames.iter().take(3).enumerate() {
        assert!(!frame.is_final, "chunk {} must be non-final", i);
        let v: Value = serde_json::from_slice(&frame.data).expect("chunk data is JSON");
        assert_eq!(v["index"], i, "chunks must arrive in order");
    }
    assert!(frames[3].is_final, "last frame must be terminal is_final=true");

    unload_model(&base, DECOUPLED_MODEL, "1").await;
}

/// `DecoupledInfer` against a model WITHOUT `predict_decoupled` (test_model)
/// returns `FailedPrecondition` (worker emits a structured not_implemented
/// error; Rust maps error_type→FailedPrecondition).
#[tokio::test]
#[serial]
async fn test_grpc_decoupled_infer_not_implemented_is_failed_precondition() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::DecoupledInferRequest;
    use std::collections::HashMap;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let repo = test_model_repo();
    let _server = ServerGuard::start(&[
        "--port",
        &http_port.to_string(),
        "--grpc-port",
        &grpc_port.to_string(),
        "--model-repo",
        &repo.to_string_lossy(),
        "--no-metrics",
        "--log-level",
        "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, MODEL, "1").await;

    let mut client = LiteServerClient::connect(format!("http://127.0.0.1:{}", grpc_port))
        .await
        .expect("gRPC client must connect");

    let payload = bytes::Bytes::from(serde_json::to_vec(&json!({"input": 2})).unwrap());
    let req = DecoupledInferRequest {
        model_name: MODEL.to_string(),
        version: "1".to_string(),
        data: payload,
        headers: HashMap::new(),
        ..Default::default()
    };
    let resp = client.decoupled_infer(req).await;
    // The stream opens (server returns Ok), then the first frame is the
    // worker's StreamError → surfaced as a tonic Status on the stream.
    let mut stream = resp.expect("open succeeds").into_inner();
    let err = loop {
        match tokio::time::timeout(Duration::from_secs(15), stream.message()).await {
            Ok(Ok(Some(_))) => continue,
            Ok(Ok(None)) => panic!("stream ended with no error"),
            Ok(Err(status)) => break status,
            Err(_) => panic!("timed out waiting for error frame"),
        }
    };
    assert_eq!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "decoupled on a model without predict_decoupled → FailedPrecondition, got {:?}",
        err
    );

    unload_model(&base, MODEL, "1").await;
}

// ---------------------------------------------------------------------------
// P9-1（对账 C）：Rust 侧断连回收 + idle 超时 e2e
// ---------------------------------------------------------------------------

/// 慢速 decoupled 模型 fixture：每 chunk 间隔 100ms，共 50 个（约 5s）。
fn write_slow_decoupled_repo(repo: &std::path::Path) {
    let dir = repo.join("slow_decoupled/1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("model.py"),
        r#"from lite_server import LitAPI
import asyncio


class SlowDecoupledAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 50)

    async def predict_decoupled(self, data, sender):
        for i in range(data):
            await asyncio.sleep(0.1)
            await sender.send({"index": i})
        await sender.close()

    def encode_response(self, output):
        return output
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
}

/// Rust client 中途断开 → worker sender 失效 + 通道回收（对账 C）。可观察
/// 断言：断连后 worker 不被孤儿推送卡死——同一模型的后续 decoupled 请求
/// 正常完成。
#[tokio::test]
#[serial]
async fn test_grpc_decoupled_client_disconnect_reclaims_stream() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::DecoupledInferRequest;
    use std::collections::HashMap;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);

    let repo = std::env::temp_dir()
        .join(format!("lite-server-p9disc-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    write_slow_decoupled_repo(&repo);

    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--grpc-port", &grpc_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-metrics",
        "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "slow_decoupled", "1").await;

    let mut client = LiteServerClient::connect(format!("http://127.0.0.1:{}", grpc_port))
        .await
        .expect("gRPC client must connect");

    // 第一条流：读 2 帧后断开（drop stream = client disconnect）。
    let resp = client
        .decoupled_infer(DecoupledInferRequest {
            model_name: "slow_decoupled".to_string(),
            version: "1".to_string(),
            data: bytes::Bytes::from(serde_json::to_vec(&json!({"input": 50})).unwrap()),
            headers: HashMap::new(),
            ..Default::default()
        })
        .await
        .expect("open first stream");
    let mut stream = resp.into_inner();
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(5), stream.message())
            .await
            .expect("frame")
            .expect("ok")
            .expect("some");
    }
    drop(stream);
    // 给断连→cancel→回收留传播时间。
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 第二条流（input=3）：worker 未被断连流的孤儿推送卡死 → 正常完成。
    let resp = client
        .decoupled_infer(DecoupledInferRequest {
            model_name: "slow_decoupled".to_string(),
            version: "1".to_string(),
            data: bytes::Bytes::from(serde_json::to_vec(&json!({"input": 3})).unwrap()),
            headers: HashMap::new(),
            ..Default::default()
        })
        .await
        .expect("open second stream");
    let mut stream = resp.into_inner();
    let mut frames = 0;
    loop {
        match tokio::time::timeout(Duration::from_secs(15), stream.message()).await {
            Ok(Ok(Some(frame))) => {
                frames += 1;
                if frame.is_final {
                    break;
                }
            }
            Ok(Ok(None)) => break,
            other => panic!("second stream after disconnect poisoned: {other:?}"),
        }
    }
    assert_eq!(frames, 4, "second stream must complete 3 chunks + final");

    unload_model(&base, "slow_decoupled", "1").await;
    let _ = std::fs::remove_dir_all(&repo);
}

/// idle 超时 → 服务端关闭（对账 C）：模型 30s 不推送任何 chunk，
/// decoupled_idle_timeout_secs=1 → 流在远小于 30s 内被回收结束。
#[tokio::test]
#[serial]
async fn test_grpc_decoupled_idle_timeout_closes_stream() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::DecoupledInferRequest;
    use std::collections::HashMap;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);

    let repo = std::env::temp_dir()
        .join(format!("lite-server-p9idle-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    // 永不推送的模型：sleep 30s 后才 close。
    let dir = repo.join("never_push/1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("model.py"),
        r#"from lite_server import LitAPI
import asyncio


class NeverPushAPI(LitAPI):
    def setup(self, device):
        pass

    async def predict_decoupled(self, data, sender):
        await asyncio.sleep(30)
        await sender.close()
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();

    let tmp_dir = std::env::temp_dir()
        .join(format!("lite-server-p9idle-yaml-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18374\n  timeout: 30.0\n  decoupled_idle_timeout_secs: 1.0\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();

    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "never_push", "1").await;

    let mut client = LiteServerClient::connect(format!("http://127.0.0.1:{}", grpc_port))
        .await
        .expect("gRPC client must connect");

    let start = std::time::Instant::now();
    let resp = client
        .decoupled_infer(DecoupledInferRequest {
            model_name: "never_push".to_string(),
            version: "1".to_string(),
            data: bytes::Bytes::from(serde_json::to_vec(&json!({})).unwrap()),
            headers: HashMap::new(),
            ..Default::default()
        })
        .await
        .expect("open stream");
    let mut stream = resp.into_inner();
    let mut saw_final = false;
    loop {
        match tokio::time::timeout(Duration::from_secs(15), stream.message()).await {
            Ok(Ok(Some(frame))) => {
                if frame.is_final {
                    saw_final = true;
                }
            }
            Ok(Ok(None)) => break,
            Ok(Err(_)) => break, // idle 回收可表现为错误终态或流结束
            Err(_) => panic!("stream not reclaimed within 15s (idle=1s)"),
        }
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(15),
        "idle timeout must reclaim the stuck stream promptly, took {elapsed:?}"
    );
    assert!(!saw_final, "idle-reclaimed stream must not report a normal final frame");

    unload_model(&base, "never_push", "1").await;
    let _ = std::fs::remove_dir_all(&repo);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

// ---------------------------------------------------------------------------
// P2-1: gRPC 请求指标 + GIE 指标语义
// ---------------------------------------------------------------------------

/// gRPC Infer 完成（成功 + 失败）记请求指标：/metrics 暴露
/// liteserver_requests_total（D5: 无 protocol label，与 HTTP 共享计数），
/// 并暴露 GIE 语义 gauge（默认 namespace `liteserver`）。
#[tokio::test]
#[serial]
async fn test_grpc_infer_records_request_metrics() {
    use bytes::Bytes;
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;
    use std::collections::HashMap;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    kill_stale_on_port(metrics_port);
    let repo = test_model_repo();
    let _server = ServerGuard::start(&[
        "--port",
        &http_port.to_string(),
        "--grpc-port",
        &grpc_port.to_string(),
        "--metrics-port",
        &metrics_port.to_string(),
        "--model-repo",
        &repo.to_string_lossy(),
        "--log-level",
        "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, MODEL, "1").await;

    let mut client = LiteServerClient::connect(format!("http://127.0.0.1:{}", grpc_port))
        .await
        .expect("gRPC client must connect");

    // 成功 → 2xx
    let ok = client
        .infer(InferRequest {
            model_name: MODEL.to_string(),
            version: "1".to_string(),
            data: Bytes::from(serde_json::to_vec(&json!({"input": 1})).unwrap()),
            headers: HashMap::new(),

        ..Default::default()
})
        .await;
    assert!(ok.is_ok(), "gRPC infer must succeed: {:?}", ok.err());

    // 模型不存在 → NotFound (4xx)
    let err = client
        .infer(InferRequest {
            model_name: "no_such_model".to_string(),
            version: String::new(),
            data: Bytes::from_static(b"{}"),
            headers: HashMap::new(),

        ..Default::default()
})
        .await
        .expect_err("unknown model must fail");
    assert_eq!(err.code(), tonic::Code::NotFound);

    let body = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{}/metrics", metrics_port))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        body.contains("liteserver_requests_total{model=\"test_model\",status=\"2xx\",version=\"1\"}"),
        "2xx series missing: {}", body
    );
    // A2 (leak-gap-audit-0821): a never-registered model's reject records
    // under the constant ~unknown~ label — nothing ever unloads such a pair,
    // so raw-label recording is a permanent series per probe (enumeration
    // cardinality attack). Registered models keep their real labels.
    assert!(
        body.contains("liteserver_requests_total{model=\"~unknown~\",status=\"4xx\",version=\"\"}"),
        "4xx series under the constant unknown-model label missing: {}", body
    );
    assert!(
        !body.contains("liteserver_requests_total{model=\"no_such_model\",status=\"4xx\",version=\"\"}"),
        "a never-registered model name must not create its own series: {}", body
    );
    // GIE/EPP 语义 gauge（默认 namespace `liteserver`）：TotalQueuedRequests
    // 映射既有 queue depth；KVCacheUtilization 无 KV 概念上报 N/A (NaN)。
    assert!(body.contains("liteserver:total_queued_requests"),
        "GIE TotalQueuedRequests gauge missing: {}", body);
    assert!(body.contains("liteserver:kv_cache_utilization NaN"),
        "GIE KVCacheUtilization must be NaN (N/A): {}", body);

    unload_model(&base, MODEL, "1").await;
}

// ---------------------------------------------------------------------------
// P2-2: gRPC x-request-id + x-processing-time-ms 回显
// ---------------------------------------------------------------------------

/// gRPC 客户端传 `x-client-request-id` metadata → 响应 metadata 回显
/// `x-request-id`（同值）+ `x-processing-time-ms`（蓝图 §4.1 P2-2）。
/// 错误路径（模型不存在）同样回显——覆盖 interceptor→RequestContext→handler
/// 出口注入的全链路（unit test 直调 handler 不经 interceptor）。
#[tokio::test]
#[serial]
async fn test_grpc_infer_echoes_request_id() {
    use bytes::Bytes;
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;
    use std::collections::HashMap;
    use tonic::Request;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    kill_stale_on_port(metrics_port);
    let repo = test_model_repo();
    let _server = ServerGuard::start(&[
        "--port",
        &http_port.to_string(),
        "--grpc-port",
        &grpc_port.to_string(),
        "--metrics-port",
        &metrics_port.to_string(),
        "--model-repo",
        &repo.to_string_lossy(),
        "--log-level",
        "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, MODEL, "1").await;

    let mut client = LiteServerClient::connect(format!("http://127.0.0.1:{}", grpc_port))
        .await
        .expect("gRPC client must connect");

    // 成功路径回显
    let mut req = Request::new(InferRequest {
        model_name: MODEL.to_string(),
        version: "1".to_string(),
        data: Bytes::from(serde_json::to_vec(&json!({"input": 1})).unwrap()),
        headers: HashMap::new(),

    ..Default::default()
});
    req.metadata_mut()
        .insert("x-client-request-id", "p22-foo".parse().unwrap());
    let resp = client
        .infer(req)
        .await
        .expect("gRPC infer must succeed");
    let md = resp.metadata();
    assert_eq!(
        md.get("x-request-id").and_then(|v| v.to_str().ok()),
        Some("p22-foo"),
        "x-request-id must echo the client id: {:?}",
        md
    );
    assert!(
        md.get("x-processing-time-ms").is_some(),
        "x-processing-time-ms must be present"
    );

    // 错误路径（模型不存在）同样回显
    let mut err_req = Request::new(InferRequest {
        model_name: "no_such_model".to_string(),
        version: String::new(),
        data: Bytes::from_static(b"{}"),
        headers: HashMap::new(),

    ..Default::default()
});
    err_req
        .metadata_mut()
        .insert("x-client-request-id", "p22-bar".parse().unwrap());
    let err = client
        .infer(err_req)
        .await
        .expect_err("unknown model must fail");
    assert_eq!(err.code(), tonic::Code::NotFound);
    assert_eq!(
        err.metadata().get("x-request-id").and_then(|v| v.to_str().ok()),
        Some("p22-bar"),
        "x-request-id must echo on the error path too: {:?}",
        err.metadata()
    );
    assert!(
        err.metadata().get("x-processing-time-ms").is_some(),
        "x-processing-time-ms must be present on the error path"
    );

    unload_model(&base, MODEL, "1").await;
}

// ---------------------------------------------------------------------------
// P3-1: gRPC 限流（ResourceExhausted 专给限流，落 4xx）
// ---------------------------------------------------------------------------

/// policy_model 声明 rate_limit rpm=3 burst=3。gRPC 连续 4 次 infer：前 3 次
/// 放行，第 4 次 ResourceExhausted + retry-after（蓝图 §4.1 P3-1 / §4.0.9）。
#[tokio::test]
#[serial]
async fn test_grpc_infer_rate_limit_returns_resource_exhausted() {
    use bytes::Bytes;
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;
    use std::collections::HashMap;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    kill_stale_on_port(metrics_port);
    let repo = test_model_repo();
    let _server = ServerGuard::start(&[
        "--port",
        &http_port.to_string(),
        "--grpc-port",
        &grpc_port.to_string(),
        "--metrics-port",
        &metrics_port.to_string(),
        "--model-repo",
        &repo.to_string_lossy(),
        "--log-level",
        "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "policy_model", "1").await;
    assert!(
        wait_model_ready(&base, "policy_model", 20).await,
        "policy_model must become ready"
    );

    let mut client = LiteServerClient::connect(format!("http://127.0.0.1:{}", grpc_port))
        .await
        .expect("gRPC client must connect");

    let mk = || InferRequest {
        model_name: "policy_model".to_string(),
        version: "1".to_string(),
        data: Bytes::from(serde_json::to_vec(&json!({"input": 1})).unwrap()),
        headers: HashMap::new(),

    ..Default::default()
};

    // burst=3：前 3 次放行。
    for i in 0..3 {
        let r = client.infer(mk()).await;
        assert!(r.is_ok(), "request {} must be allowed: {:?}", i + 1, r.err());
    }
    // 第 4 次 ResourceExhausted + retry-after。
    let err = client
        .infer(mk())
        .await
        .expect_err("4th request must be rate-limited");
    assert_eq!(
        err.code(),
        tonic::Code::ResourceExhausted,
        "over-limit must be ResourceExhausted (not Unavailable): {:?}",
        err
    );
    assert!(
        err.metadata().get("retry-after").is_some(),
        "retry-after metadata must be present: {:?}",
        err.metadata()
    );

    unload_model(&base, "policy_model", "1").await;
}

// ---------------------------------------------------------------------------
// gRPC health checking (grpc.health.v1, phase 3)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_grpc_health_service() {
    use tonic_health::pb::health_check_response::ServingStatus;
    use tonic_health::pb::health_client::HealthClient;
    use tonic_health::pb::HealthCheckRequest;

    // Dedicated server WITH gRPC enabled (the shared server runs --no-grpc).
    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let repo = test_model_repo();
    let _server = ServerGuard::start(&[
        "--port",
        &http_port.to_string(),
        "--grpc-port",
        &grpc_port.to_string(),
        "--model-repo",
        &repo.to_string_lossy(),
        "--no-metrics",
        "--log-level",
        "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);

    let channel = tonic::transport::Endpoint::new(format!("http://127.0.0.1:{}", grpc_port))
        .expect("valid gRPC address")
        .connect()
        .await
        .expect("health channel must connect");
    let mut client = HealthClient::new(channel);

    // No models loaded: overall NOT_SERVING.
    let resp = client
        .check(HealthCheckRequest { service: String::new() })
        .await.unwrap().into_inner();
    assert_eq!(resp.status(), ServingStatus::NotServing);

    load_model(&base, MODEL, "1").await;

    // Overall and per-model services go SERVING (load syncs before the
    // admin call returns, so no polling needed).
    let resp = client
        .check(HealthCheckRequest { service: String::new() })
        .await.unwrap().into_inner();
    assert_eq!(resp.status(), ServingStatus::Serving);
    let resp = client
        .check(HealthCheckRequest { service: MODEL.to_string() })
        .await.unwrap().into_inner();
    assert_eq!(resp.status(), ServingStatus::Serving);

    // Per-version service "{model}/{version}" (§4.5).
    let resp = client
        .check(HealthCheckRequest { service: format!("{}/1", MODEL) })
        .await.unwrap().into_inner();
    assert_eq!(resp.status(), ServingStatus::Serving);

    // Unknown service → NotFound per the health-check spec.
    let err = client
        .check(HealthCheckRequest { service: "no_such_model".to_string() })
        .await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);

    unload_model(&base, MODEL, "1").await;

    // After unload the per-model service is cleared and the overall
    // service flips back to NOT_SERVING.
    let err = client
        .check(HealthCheckRequest { service: MODEL.to_string() })
        .await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
    // The per-version service is cleared too.
    let err = client
        .check(HealthCheckRequest { service: format!("{}/1", MODEL) })
        .await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
    let resp = client
        .check(HealthCheckRequest { service: String::new() })
        .await.unwrap().into_inner();
    assert_eq!(resp.status(), ServingStatus::NotServing);
}

// ---------------------------------------------------------------------------
// gRPC response compression (P1-3)
// ---------------------------------------------------------------------------

/// With `grpc.response_compression: true` and a gzip-capable client, unary
/// Infer responses must be gzip-compressed (`grpc-encoding: gzip`).
#[tokio::test]
#[serial]
async fn test_grpc_response_compression_gzip() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;
    use std::collections::HashMap;
    use tonic::codec::CompressionEncoding;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let repo = test_model_repo();
    let tmp_dir =
        std::env::temp_dir().join(format!("lite-server-grpc-gzip-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 0.0.0.0\n  http_port: {}\n  grpc_port: {}\n  metrics_port: 18082\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\n  response_compression: true\nmodel_repository:\n  path: {}\n",
            http_port,
            grpc_port,
            repo.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, MODEL, "1").await;

    let channel = tonic::transport::Endpoint::new(format!("http://127.0.0.1:{}", grpc_port))
        .expect("valid gRPC address")
        .connect()
        .await
        .expect("gRPC channel must connect");
    let mut client = LiteServerClient::new(channel).accept_compressed(CompressionEncoding::Gzip);

    let resp = client
        .infer(InferRequest {
            model_name: MODEL.to_string(),
            version: "1".to_string(),
            data: bytes::Bytes::from(serde_json::to_vec(&json!({"input": 1})).unwrap()),
            headers: HashMap::new(),

        ..Default::default()
})
        .await
        .expect("infer must succeed");

    assert_eq!(
        resp.metadata().get("grpc-encoding").and_then(|v| v.to_str().ok()),
        Some("gzip"),
        "response must be gzip-compressed when enabled and the client accepts it"
    );

    unload_model(&base, MODEL, "1").await;
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

// ---------------------------------------------------------------------------
// P1-2 keepalive 装配冒烟(对账 C):配置→builder→serve 全链真实生效
// ---------------------------------------------------------------------------

/// 装配冒烟:配 http2_keepalive_interval/timeout 后 server 正常装配并服务,
/// 且空闲超过一个 ping 周期后连接仍健康(keepalive 帧路径真实走过)。
#[tokio::test]
async fn test_grpc_http2_keepalive_assembly_smoke() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;
    use std::collections::HashMap;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let repo = test_model_repo();
    let tmp_dir =
        std::env::temp_dir().join(format!("lite-server-grpc-keepalive-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 0.0.0.0\n  http_port: {}\n  grpc_port: {}\n  metrics_port: 18083\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\n  http2_keepalive_interval_secs: 1\n  http2_keepalive_timeout_secs: 2\nmodel_repository:\n  path: {}\n",
            http_port,
            grpc_port,
            repo.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, MODEL, "1").await;

    let channel = tonic::transport::Endpoint::new(format!("http://127.0.0.1:{}", grpc_port))
        .expect("valid gRPC address")
        .connect()
        .await
        .expect("gRPC channel must connect with keepalive configured");
    let mut client = LiteServerClient::new(channel);

    // 空闲超过一个 ping 周期(1s),让 server 的 keepalive PING 真实发出。
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    let resp = client
        .infer(InferRequest {
            model_name: MODEL.to_string(),
            version: "1".to_string(),
            data: bytes::Bytes::from(serde_json::to_vec(&json!({"input": 1})).unwrap()),
            headers: HashMap::new(),
            ..Default::default()
        })
        .await;
    assert!(
        resp.is_ok(),
        "keepalive 装配后 infer 必须正常(空闲>1 个 ping 周期后): {:?}",
        resp.err()
    );

    unload_model(&base, MODEL, "1").await;
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// Write a sub-model whose predict() sleeps 1.5s, returning {"out": x}.
fn write_slow_submodel(repo: &std::path::Path, name: &str) {
    let dir = repo.join(format!("{}/1", name));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("model.py"),
        r#"import time
from lite_server import LitAPI


class SlowAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request

    def predict(self, x):
        time.sleep(1.5)
        return {"out": x.get("x", 0) if isinstance(x, dict) else x}

    def encode_response(self, output):
        return output
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
}

/// A 3-step serial ensemble (a→b→c), each step sleeping 1.5s, runs ~4.5s total.
/// With `--timeout 2.0` the outer total-timeout (#3) must abort it at ~2.0s
/// with an error — whereas the per-step timeout (also 2.0s) never fires
/// because no single step exceeds it. Before #3 there was no outer bound, so
/// the chain completed successfully (~4.5s, HTTP 200).
#[tokio::test]
#[serial]
async fn test_ensemble_total_timeout_bounds_serial_layers() {
    let repo = std::env::temp_dir()
        .join(format!("lite-server-ensemble-timeout-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    write_slow_submodel(&repo, "a");
    write_slow_submodel(&repo, "b");
    write_slow_submodel(&repo, "c");

    let ens_dir = repo.join("slow_ensemble/1");
    std::fs::create_dir_all(&ens_dir).unwrap();
    std::fs::write(
        ens_dir.join("config.yaml"),
        r#"
ensemble:
  steps:
    - name: a
      model: a
      version: "1"
      inputs:
        x: "$request.x"
    - name: b
      model: b
      version: "1"
      inputs:
        x: "$a.out"
    - name: c
      model: c
      version: "1"
      inputs:
        x: "$b.out"
"#,
    )
    .unwrap();

    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let _server = ServerGuard::start(&[
        "--port",
        &http_port.to_string(),
        "--timeout",
        "2.0",
        "--model-repo",
        &repo.to_string_lossy(),
        "--no-grpc",
        "--no-metrics",
        "--log-level",
        "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);

    // Preload sub-models so execute_step skips auto-load; the timeout must
    // come from serial step accumulation, not worker spawn latency.
    load_model(&base, "a", "1").await;
    load_model(&base, "b", "1").await;
    load_model(&base, "c", "1").await;
    load_model(&base, "slow_ensemble", "1").await;

    let client = reqwest::Client::new();
    let start = std::time::Instant::now();
    let resp = client
        .post(format!("{}/v2/models/slow_ensemble/infer", base))
        .json(&json!({"x": 1}))
        .send()
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert!(
        !resp.status().is_success(),
        "ensemble must error on total timeout, got status {}",
        resp.status()
    );
    assert!(
        elapsed < std::time::Duration::from_secs(3),
        "ensemble must abort within the total budget (~2s), took {:?}",
        elapsed
    );

    let _ = std::fs::remove_dir_all(&repo);
}

// ---------------------------------------------------------------------------
// P-ENSEMBLE-GRPC: gRPC unary infer dispatches ensemble models through the DAG
// executor (mirrors HTTP do_infer), instead of falling through to the worker
// queue (ensemble models have no workers).
// ---------------------------------------------------------------------------

/// Write a fast sub-model whose `predict` returns immediately. `predict_body`
/// is the indented body of `def predict(self, x):` (must return a dict with an
/// `out` key so the ensemble DAG can chain `$<step>.out`).
fn write_submodel(repo: &std::path::Path, name: &str, predict_body: &str) {
    let dir = repo.join(format!("{}/1", name));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("model.py"),
        format!(
            r#"from lite_server import LitAPI


class SubAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request

    def predict(self, x):
{body}

    def encode_response(self, output):
        return output
"#,
            body = predict_body,
        ),
    )
    .unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
}

/// gRPC Infer on an ensemble model walks the DAG: a parallel recall layer
/// (`recall_a` ∥ `recall_b`, both echoing request.x) feeds a serial `merge`
/// step (sums the two outputs). request {x:5} → {out:10}. Exercises both the
/// parallel layer and serial dependency edges over gRPC, matching HTTP.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_grpc_ensemble_infer_walks_dag() {
    use std::collections::HashMap;
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);

    let repo = std::env::temp_dir()
        .join(format!("lite-server-grpc-ensemble-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    // echo sub-model: echoes request.x as {"out": x}.
    write_submodel(
        &repo,
        "echo",
        r#"        return {"out": x.get("x", 0) if isinstance(x, dict) else x}"#,
    );
    // summer sub-model: sums two inputs as {"out": a + b}.
    write_submodel(
        &repo,
        "summer",
        "        a = x.get(\"a\", 0)\n        b = x.get(\"b\", 0)\n        return {\"out\": a + b}",
    );
    // Ensemble DAG: recall_a ∥ recall_b (parallel layer) → merge (serial).
    let ens_dir = repo.join("ensemble_model/1");
    std::fs::create_dir_all(&ens_dir).unwrap();
    std::fs::write(
        ens_dir.join("config.yaml"),
        r#"
ensemble:
  steps:
    - name: recall_a
      model: echo
      version: "1"
      inputs:
        x: "$request.x"
    - name: recall_b
      model: echo
      version: "1"
      inputs:
        x: "$request.x"
    - name: merge
      model: summer
      version: "1"
      inputs:
        a: "$recall_a.out"
        b: "$recall_b.out"
"#,
    )
    .unwrap();

    let tmp_dir = std::env::temp_dir()
        .join(format!("lite-server-grpc-ensemble-yaml-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18352\n  timeout: 30.0\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();

    let _guard = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "echo", "1").await;
    load_model(&base, "summer", "1").await;
    load_model(&base, "ensemble_model", "1").await;

    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);
    let resp = client
        .infer(InferRequest {
            model_name: "ensemble_model".to_string(),
            version: "1".to_string(),
            data: bytes::Bytes::from(serde_json::to_vec(&json!({"x": 5})).unwrap()),
            headers: HashMap::new(),

        ..Default::default()
})
        .await
        .expect("ensemble gRPC Infer must walk the DAG, not error")
        .into_inner();

    let got: Value =
        serde_json::from_slice(&resp.data).expect("ensemble response must be JSON");
    assert_eq!(
        got["out"].as_i64(),
        Some(10),
        "parallel recall (5,5) → merge sum = 10; got: {}",
        got
    );

    let _ = std::fs::remove_dir_all(&repo);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// A malformed ensemble DAG (cycle) surfaces as a gRPC error Status, not OK and
/// not a panic — the cycle is detected when `execute_ensemble` parses the config
/// (load succeeds; the error fires at infer time) and maps through D4.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_grpc_ensemble_dag_error_maps_to_status() {
    use std::collections::HashMap;
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);

    let repo = std::env::temp_dir()
        .join(format!("lite-server-grpc-ensemble-cycle-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    write_submodel(
        &repo,
        "echo",
        r#"        return {"out": x.get("x", 0) if isinstance(x, dict) else x}"#,
    );
    // Cyclic DAG: a → b → a. validate_dag rejects it at infer time.
    let ens_dir = repo.join("ensemble_cycle/1");
    std::fs::create_dir_all(&ens_dir).unwrap();
    std::fs::write(
        ens_dir.join("config.yaml"),
        r#"
ensemble:
  steps:
    - name: a
      model: echo
      version: "1"
      inputs:
        x: "$b.out"
    - name: b
      model: echo
      version: "1"
      inputs:
        x: "$a.out"
"#,
    )
    .unwrap();

    let tmp_dir = std::env::temp_dir()
        .join(format!("lite-server-grpc-ensemble-cycle-yaml-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18362\n  timeout: 30.0\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();

    let _guard = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    // §4.4 note ③ (batch 0): config validation fires at LOAD time — a cyclic
    // DAG fails load (invalid_configuration 400), the model never registers.
    // (Previously the cycle was only detected when the config was parsed at
    // infer time; batch 0 moves DAG validation onto the load path, P0/P6.)
    let client = reqwest::Client::new();
    let load_resp = client
        .post(format!(
            "{}/v2/repository/models/ensemble_cycle/versions/1/load",
            base
        ))
        .send()
        .await
        .expect("load request");
    assert_eq!(
        load_resp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "cyclic DAG must fail at load time (§4.4 note ③)"
    );

    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);
    let result = client
        .infer(InferRequest {
            model_name: "ensemble_cycle".to_string(),
            version: "1".to_string(),
            data: bytes::Bytes::from(serde_json::to_vec(&json!({"x": 1})).unwrap()),
            headers: HashMap::new(),

        ..Default::default()
})
        .await;

    assert!(
        result.is_err(),
        "an unloaded cyclic ensemble must surface as a gRPC error, not OK; got: {:?}",
        result.map(|r| String::from_utf8_lossy(&r.into_inner().data).to_string())
    );

    let _ = std::fs::remove_dir_all(&repo);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

// ---------------------------------------------------------------------------
// P7-1: endpoint-class access control. Admin class in key mode requires a key
// on BOTH protocols; a missing/wrong key is rejected (401 / Unauthenticated).
// Health stays public; inference stays public (default). No workers needed —
// only GetInfo / list_models / health are exercised.
// ---------------------------------------------------------------------------
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_access_control_admin_key_mode_both_protocols() {
    use lite_server::proto::liteserver::admin_client::AdminClient;
    use lite_server::proto::liteserver::GetInfoRequest;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);

    // Empty model repo: GetInfo / list_models / health work without workers.
    let repo = std::env::temp_dir()
        .join(format!("lite-server-p71-empty-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    std::fs::create_dir_all(&repo).unwrap();

    let tmp_dir = std::env::temp_dir()
        .join(format!("lite-server-p71-yaml-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18372\n  timeout: 30.0\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\naccess_control:\n  admin:\n    http:\n      mode: key\n      key: x-api-key\n      value: secret\n    grpc:\n      mode: key\n      key: x-token\n      value: secret\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();

    let _guard = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    let base = format!("http://127.0.0.1:{}", http_port);

    // gRPC Admin (header "x-token"): no key → Unauthenticated, key → OK.
    let channel = grpc_tcp_channel(grpc_port).await;
    let mut admin = AdminClient::new(channel);
    let denied = admin.get_info(GetInfoRequest {}).await;
    assert!(denied.is_err(), "gRPC Admin without key must be rejected");
    assert_eq!(
        denied.err().unwrap().code(),
        tonic::Code::Unauthenticated,
        "expected Unauthenticated"
    );
    let mut req = tonic::Request::new(GetInfoRequest {});
    req.metadata_mut()
        .insert("x-token", "secret".parse().unwrap());
    let resp = admin
        .get_info(req)
        .await
        .expect("gRPC Admin with correct key must succeed")
        .into_inner();
    assert_eq!(resp.server, "lite-server");

    // HTTP admin (list_models, header "x-api-key"): no key → 401, key → 200.
    let client = reqwest::Client::new();
    let no_key = client.get(format!("{}/v2/models", base)).send().await.unwrap();
    assert_eq!(no_key.status(), 401, "HTTP admin without key must be 401");
    let with_key = client
        .get(format!("{}/v2/models", base))
        .header("x-api-key", "secret")
        .send()
        .await
        .unwrap();
    assert_eq!(with_key.status(), 200, "HTTP admin with key must be 200");

    // A wrong key is rejected (constant-time compare still denies).
    let wrong = client
        .get(format!("{}/v2/models", base))
        .header("x-api-key", "wrong")
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), 401, "HTTP admin with wrong key must be 401");

    // Health stays public even with admin key configured.
    let health = client.get(format!("{}/health", base)).send().await.unwrap();
    assert_eq!(health.status(), 200, "health must remain public");

    let _ = std::fs::remove_dir_all(&repo);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// P6-2(对账 C):gRPC Admin Load/Unload happy-path 状态流转(真实 worker)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_grpc_admin_load_unload_happy_path() {
    use lite_server::proto::liteserver::admin_client::AdminClient;
    use lite_server::proto::liteserver::{
        ListModelsRequest, LoadModelRequest, ModelReadyRequest, UnloadModelRequest,
    };

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let repo = test_model_repo();
    let tmp_dir =
        std::env::temp_dir().join(format!("lite-server-p62-yaml-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18373\n  timeout: 30.0\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();

    let _guard = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;

    let channel = grpc_tcp_channel(grpc_port).await;
    let mut admin = AdminClient::new(channel);

    // Load(真实 Python worker 拉起)→ ModelReady 轮询转就绪。
    let load = admin
        .load_model(LoadModelRequest { model_name: MODEL.to_string(), version: "1".to_string() })
        .await
        .expect("LoadModel happy path");
    assert!(load.into_inner().success);

    let mut ready = false;
    for _ in 0..100 {
        let r = admin
            .model_ready(ModelReadyRequest { model_name: MODEL.to_string(), version: Some("1".to_string()) })
            .await
            .expect("ModelReady")
            .into_inner();
        if r.ready {
            ready = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }
    assert!(ready, "model did not become ready after LoadModel");

    let models = admin
        .list_models(ListModelsRequest {})
        .await
        .expect("ListModels")
        .into_inner();
    assert!(
        models.models.iter().any(|m| m.name == MODEL && m.version == "1"),
        "loaded model must appear in ListModels: {:?}",
        models.models
    );

    // Unload → ListModels 不再含该模型。
    let unload = admin
        .unload_model(UnloadModelRequest { model_name: MODEL.to_string(), version: Some("1".to_string()) })
        .await
        .expect("UnloadModel happy path");
    assert!(unload.into_inner().success);
    let models = admin
        .list_models(ListModelsRequest {})
        .await
        .expect("ListModels after unload")
        .into_inner();
    assert!(
        !models.models.iter().any(|m| m.name == MODEL),
        "unloaded model must disappear from ListModels"
    );

    let _ = std::fs::remove_dir_all(&tmp_dir);
}

// P7-2: grpc.admin_bind splits Admin onto a second server. With admin_bind set
// to a UDS, Admin RPCs are reachable ONLY via the UDS (and the socket is
// owner-only 0o600); the main TCP port returns UNIMPLEMENTED for Admin (the
// service is not registered there). health stays on both ports.
// ---------------------------------------------------------------------------
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_admin_bind_splits_admin_onto_udf() {
    use lite_server::proto::liteserver::admin_client::AdminClient;
    use lite_server::proto::liteserver::GetInfoRequest;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let admin_sock = std::env::temp_dir()
        .join(format!("lite-server-p72-admin-{}.sock", std::process::id()));
    let admin_sock_str = admin_sock.to_string_lossy().into_owned();
    let _ = std::fs::remove_file(&admin_sock);

    // Empty model repo — only Admin GetInfo / health are exercised.
    let repo = std::env::temp_dir()
        .join(format!("lite-server-p72-empty-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    std::fs::create_dir_all(&repo).unwrap();

    let tmp_dir = std::env::temp_dir()
        .join(format!("lite-server-p72-yaml-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18392\n  timeout: 30.0\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\n  admin_bind: unix:{sock}\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            sock = admin_sock_str,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();

    let _guard = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;

    // Main TCP gRPC port: Admin RPC → UNIMPLEMENTED (Admin is NOT registered on
    // the main port once admin_bind splits it off). health stays on the main port.
    let main_channel = grpc_tcp_channel(grpc_port).await;
    let mut main_admin = AdminClient::new(main_channel.clone());
    let main_admin_res = main_admin.get_info(GetInfoRequest {}).await;
    assert!(main_admin_res.is_err(), "Admin on main port must be rejected");
    assert_eq!(
        main_admin_res.err().unwrap().code(),
        tonic::Code::Unimplemented,
        "Admin RPC on main port → UNIMPLEMENTED (service not registered)"
    );
    let mut main_health =
        tonic_health::pb::health_client::HealthClient::new(main_channel);
    let health = main_health
        .check(tonic_health::pb::HealthCheckRequest { service: String::new() })
        .await;
    assert!(health.is_ok(), "health must stay on the main port: {:?}", health.err());

    // Admin UDS: Admin RPC → OK, and the socket is owner-only 0o600.
    let admin_channel = uds_grpc_channel(&admin_sock_str).await;
    let mut uds_admin = AdminClient::new(admin_channel);
    let resp = uds_admin
        .get_info(GetInfoRequest {})
        .await
        .expect("Admin RPC via admin_bind UDS must succeed")
        .into_inner();
    assert_eq!(resp.server, "lite-server");
    assert_socket_mode(&admin_sock_str, 0o600);

    let _ = std::fs::remove_file(&admin_sock);
    let _ = std::fs::remove_dir_all(&repo);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

// ---------------------------------------------------------------------------
// Parent-death self-cleanup — robust to the test binary being SIGKILLed.
// atexit/ServerGuard cannot catch SIGKILL, so the spawned server (and its
// python workers) must self-terminate when reparented to init. Gated by
// LITESERVER_DIE_WITH_PARENT, which start_server sets for every test server.
// ---------------------------------------------------------------------------

/// Find the pid of a server's python worker for `model`, polling until it
/// appears or `timeout_secs` elapse. macOS/Linux `pgrep -P` lists children.
#[cfg(unix)]
async fn wait_for_worker_pid(server_pid: i32, model: &str, timeout_secs: u64) -> Option<i32> {
    let pattern = format!("lite_server.worker.inference --model-name {}", model);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        let out = Command::new("pgrep")
            .args(["-P", &server_pid.to_string(), "-f", &pattern])
            .output()
            .ok()?;
        if let Some(pid) = String::from_utf8_lossy(&out.stdout)
            .lines()
            .next()
            .and_then(|l| l.trim().parse::<i32>().ok())
        {
            return Some(pid);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        sleep(Duration::from_millis(200)).await;
    }
}

/// Poll a pid until it no longer exists (kill(pid,0) != 0) or `timeout_secs`.
#[cfg(unix)]
async fn wait_for_pid_gone(pid: i32, timeout_secs: u64) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if unsafe { libc::kill(pid, 0) } != 0 {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        sleep(Duration::from_millis(100)).await;
    }
}

/// A server whose parent is killed must self-terminate (SIGKILL can't be caught
/// by atexit/Drop — only a surviving child watching its parent can clean up).
#[cfg(unix)]
#[tokio::test]
async fn die_with_parent_self_terminates_when_parent_killed() {
    use std::io::BufRead;
    let port = next_test_port();
    kill_stale_on_port(port);
    let repo = test_model_repo().to_string_lossy().to_string();
    // Spawn the server under a disposable `sh` wrapper so we can SIGKILL its
    // PARENT. `exec sleep` keeps the wrapper alive under a stable pid; the
    // server's own output is discarded so the wrapper's stdout carries only
    // `echo $!` (the server pid).
    let mut wrapper = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "{bin} serve --port {port} --model-repo {repo} --no-metrics --no-grpc --log-level warn >/dev/null 2>&1 & echo $!; exec sleep 60",
            bin = lite_server_bin().display(),
        ))
        .env("LITESERVER_DIE_WITH_PARENT", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("wrapper spawn");
    let wrapper_pid = wrapper.id() as i32;
    let mut line = String::new();
    {
        let stdout = wrapper.stdout.take().expect("wrapper stdout");
        let mut reader = std::io::BufReader::new(stdout);
        reader.read_line(&mut line).expect("read server pid");
    }
    let server_pid: i32 = line.trim().parse().expect("server pid");
    wait_for_server(port, 20).await;

    // SIGKILL the wrapper = the server's parent.
    unsafe {
        libc::kill(wrapper_pid, libc::SIGKILL);
    }
    let _ = wrapper.wait();

    let gone = wait_for_pid_gone(server_pid, 6).await;
    if !gone {
        unsafe {
            libc::kill(server_pid, libc::SIGKILL);
        }
    }
    assert!(
        gone,
        "server did not self-terminate within 6s after its parent was SIGKILLed"
    );
}

/// A python worker whose server (parent) is SIGKILLed must self-terminate via
/// its own parent-death watch — the server's watchdog can't run when the server
/// itself is SIGKILLed.
#[cfg(unix)]
#[tokio::test]
async fn worker_self_terminates_when_server_killed() {
    let port = next_test_port();
    kill_stale_on_port(port);
    let repo = test_model_repo();
    let mut server = start_server(&[
        "--port",
        &port.to_string(),
        "--model-repo",
        &repo.to_string_lossy(),
        "--no-metrics",
        "--no-grpc",
        "--log-level",
        "warn",
    ]);
    let server_pid = server.id() as i32;
    let base = format!("http://127.0.0.1:{}", port);
    wait_for_server(port, 20).await;
    load_model(&base, MODEL, "1").await;

    let worker_pid = wait_for_worker_pid(server_pid, MODEL, 10)
        .await
        .expect("worker process not found");

    // SIGKILL the server directly (not graceful) — its watchdog can't run.
    unsafe {
        libc::kill(server_pid, libc::SIGKILL);
    }
    let _ = server.wait();

    let gone = wait_for_pid_gone(worker_pid, 6).await;
    if !gone {
        unsafe {
            libc::kill(worker_pid, libc::SIGKILL);
        }
    }
    assert!(
        gone,
        "worker did not self-terminate within 6s after its server was SIGKILLed"
    );
}

/// Send SIGINT to the server's main process (mirrors send_sigterm).
#[cfg(unix)]
fn send_sigint(child: &std::process::Child) {
    unsafe {
        libc::kill(child.id() as i32, libc::SIGINT);
    }
}

/// A model whose setup() sleeps far longer than any test window, so the
/// server stays inside the model-loading phase of startup until signaled.
#[cfg(unix)]
fn slow_setup_model_repo(tag: &str) -> std::path::PathBuf {
    let repo = std::env::temp_dir().join(format!("lite-server-{tag}-{}", std::process::id()));
    let model_dir = repo.join("slow_m").join("1");
    std::fs::create_dir_all(&model_dir).unwrap();
    std::fs::write(
        model_dir.join("model.py"),
        "import time\nfrom lite_server import LitAPI\nclass A(LitAPI):\n  def setup(self, d): time.sleep(120)\n  def decode_request(self, r): return r\n  def predict(self, x): return x\n  def encode_response(self, o): return o\n",
    )
    .unwrap();
    std::fs::write(
        model_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
    repo
}

/// Boot config that loads every repo model (control_mode "all") on three
/// ephemeral ports.
#[cfg(unix)]
fn startup_signal_config(tag: &str, repo: &std::path::Path) -> (String, u16) {
    let http_port = next_test_port();
    let grpc_port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    kill_stale_on_port(metrics_port);
    let cfg_dir =
        std::env::temp_dir().join(format!("lite-server-{tag}-cfg-{}", std::process::id()));
    std::fs::create_dir_all(&cfg_dir).unwrap();
    let cfg = cfg_dir.join("server.yaml");
    std::fs::write(
        &cfg,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: {metrics_port}\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\norchestration:\n  control_mode: all\nmodel_repository:\n  path: {repo}\n",
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();
    (cfg.to_string_lossy().to_string(), http_port)
}

/// SIGINT while models are still loading must shut the server down cleanly.
/// Before the main select! in run() nothing listened for signals: worker
/// spawn + setup/warmup can take minutes (or wedge), and on the binary path
/// Ctrl+C fell to the default action — an unclean kill with no worker
/// reaping.
#[cfg(unix)]
#[tokio::test]
async fn sigint_during_model_load_exits_cleanly() {
    let repo = slow_setup_model_repo("sigint-startup");
    let (cfg_arg, _http_port) = startup_signal_config("sigint-startup", &repo);
    let mut server = start_server(&["--config", &cfg_arg]);
    let server_pid = server.id() as i32;

    // Wait until the worker process exists: its setup() then sleeps 120s, so
    // the server is deterministically inside load_initial_models.
    wait_for_worker_pid(server_pid, "slow_m", 30)
        .await
        .expect("worker process not found — server never entered model load");

    send_sigint(&server);
    let exited = wait_for_exit(&mut server, 20).await;
    if !exited {
        stop_server(server);
        panic!("SIGINT during startup must terminate the server within 20s");
    }
    let status = server.wait().unwrap();
    assert!(
        status.success(),
        "startup SIGINT must be a graceful exit, got {status}"
    );
}

/// Same startup signal, but through the Python-embedded path
/// (`lite_server.serve`) — where a startup Ctrl+C was fully swallowed:
/// CPython's SIGINT handler only sets a flag for KeyboardInterrupt, and the
/// main thread is parked in thread.join() inside serve(), so the exception
/// can never fire until startup completes on its own. Skipped when the Rust
/// extension is not built (`maturin develop`), e.g. a bare cargo checkout.
#[cfg(unix)]
#[tokio::test]
async fn sigint_during_model_load_exits_cleanly_python_embedded() {
    let probe = Command::new("python3")
        .args(["-c", "from lite_server import serve; assert serve is not None"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    match probe {
        Ok(s) if s.success() => {}
        _ => {
            eprintln!("skipping: lite_server extension not built (maturin develop)");
            return;
        }
    }

    let repo = slow_setup_model_repo("sigint-startup-py");
    let (cfg_arg, _http_port) = startup_signal_config("sigint-startup-py", &repo);
    let mut cmd = Command::new("python3");
    cmd.arg("-c")
        .arg(format!(
            "from lite_server import serve; serve(config={cfg_arg:?})"
        ))
        .current_dir(project_root())
        .env("PYTHONPATH", project_root().join("python"))
        .env("LITESERVER_DIE_WITH_PARENT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    unsafe {
        cmd.pre_exec(|| {
            libc::setpgid(0, 0);
            Ok(())
        });
    }
    let mut server = cmd.spawn().expect("failed to spawn embedded server");
    let server_pid = server.id() as i32;

    wait_for_worker_pid(server_pid, "slow_m", 30)
        .await
        .expect("worker process not found — server never entered model load");

    send_sigint(&server);
    let exited = wait_for_exit(&mut server, 20).await;
    if !exited {
        stop_server(server);
        panic!(
            "SIGINT during startup must terminate an embedded server within 20s \
             (CPython defers KeyboardInterrupt until serve() returns)"
        );
    }
}

/// R4: in PRODUCTION (no LITESERVER_DIE_WITH_PARENT), a worker must still
/// self-terminate when its server is SIGKILLed. The worker's parent-death watch
/// must be on by default — not gated by the test-only env flag — or a hard
/// server crash (SIGKILL / abort, where kill_on_drop can't fire) orphans every
/// worker. Identical to `worker_self_terminates_when_server_killed` except the
/// env flag is NOT set.
#[cfg(unix)]
#[tokio::test]
async fn worker_self_terminates_in_prod_no_env_flag() {
    let port = next_test_port();
    kill_stale_on_port(port);
    let repo = test_model_repo();

    // Inline command: identical to `start_server` EXCEPT no
    // LITESERVER_DIE_WITH_PARENT — this is the production condition.
    let mut cmd = Command::new(lite_server_bin());
    cmd.arg("serve")
        .arg("--port")
        .arg(port.to_string())
        .arg("--model-repo")
        .arg(repo)
        .arg("--no-metrics")
        .arg("--no-grpc")
        .arg("--log-level")
        .arg("warn")
        .current_dir(project_root())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    #[cfg(unix)]
    unsafe {
        cmd.pre_exec(|| {
            libc::setpgid(0, 0);
            Ok(())
        });
    }
    let mut server = cmd.spawn().expect("Failed to start server");
    let server_pid = server.id() as i32;
    let base = format!("http://127.0.0.1:{}", port);

    wait_for_server(port, 20).await;
    load_model(&base, MODEL, "1").await;

    let worker_pid = wait_for_worker_pid(server_pid, MODEL, 10)
        .await
        .expect("worker process not found");

    // SIGKILL the server directly (not graceful) — its watchdog can't run, and
    // kill_on_drop can't fire under SIGKILL. Only a worker-side parent watch
    // can clean up.
    unsafe {
        libc::kill(server_pid, libc::SIGKILL);
    }
    let _ = server.wait();

    let gone = wait_for_pid_gone(worker_pid, 8).await;
    if !gone {
        unsafe {
            libc::kill(worker_pid, libc::SIGKILL);
        }
    }
    assert!(
        gone,
        "worker did not self-terminate in prod (no env flag) within 8s after its server was SIGKILLed"
    );
}

// ---------------------------------------------------------------------------
// P8-1: sequence_id cross-request worker affinity (sticky routing)
// ---------------------------------------------------------------------------

/// Parse `liteserver_worker_inference_total{...,worker_id="N"} <value>` for one
/// worker out of a /metrics scrape. Returns 0 when the series is absent.
fn worker_inference_total(body: &str, model: &str, worker_id: u32) -> u64 {
    let needle_model = format!("model=\"{model}\"");
    let needle_worker = format!("worker_id=\"{worker_id}\"");
    for line in body.lines() {
        if line.starts_with("liteserver_worker_inference_total{")
            && line.contains(&needle_model)
            && line.contains(&needle_worker)
        {
            return line
                .rsplit_once(' ')
                .map(|(_, v)| v.trim().parse().unwrap_or(0))
                .unwrap_or(0);
        }
    }
    0
}

/// P8-1: many CONCURRENT requests carrying the SAME `sequence_id` all land on
/// ONE worker (rendezvous hashing is deterministic even before the registry
/// records), while concurrent requests WITHOUT a sequence_id balance across
/// both workers via least-loaded. B2 load-threshold fallback is disabled via
/// config so pure stickiness is observable.
#[tokio::test]
#[serial]
async fn test_sequence_id_stickiness_pins_concurrent_requests_to_one_worker() {
    let http_port = next_test_port();
    let grpc_port = next_test_port();
    let metrics_port = next_test_port();
    for p in [http_port, grpc_port, metrics_port] {
        kill_stale_on_port(p);
    }

    // 2-worker model so stickiness has somewhere to pin to.
    let repo = std::env::temp_dir().join(format!("lite-server-p81-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    let model_dir = repo.join("sticky_model/1");
    std::fs::create_dir_all(&model_dir).unwrap();
    std::fs::write(
        model_dir.join("model.py"),
        r#"from lite_server import LitAPI


class StickyAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        return {"output": x * 2}

    def encode_response(self, output):
        return output
"#,
    )
    .unwrap();
    std::fs::write(
        model_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 2\n",
    )
    .unwrap();

    // Disable B2 load-threshold fallback so a pinned worker that gets 2+ ahead
    // does not trigger power-of-two — pure stickiness is what we assert here.
    let cfg_path =
        std::env::temp_dir().join(format!("lite-server-p81-cfg-{}.yaml", std::process::id()));
    std::fs::write(
        &cfg_path,
        "server:\n  balance_abs_threshold: 0\n  balance_rel_threshold: 0.0\n",
    )
    .unwrap();

    let _server = ServerGuard::start(&[
        "--port",
        &http_port.to_string(),
        "--grpc-port",
        &grpc_port.to_string(),
        "--metrics-port",
        &metrics_port.to_string(),
        "--model-repo",
        &repo.to_string_lossy(),
        "--config",
        &cfg_path.to_string_lossy(),
        "--log-level",
        "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "sticky_model", "1").await;

    let client = reqwest::Client::new();
    let infer_url = format!("{base}/v2/models/sticky_model/infer");

    // 8 CONCURRENT requests with the SAME sequence_id → all on one worker.
    let sticky: Vec<_> = (0..8)
        .map(|i| {
            let c = client.clone();
            let url = infer_url.clone();
            tokio::spawn(async move {
                c.post(&url)
                    .header("x-sequence-id", "seq-A")
                    .json(&json!({"input": i}))
                    .send()
                    .await
                    .unwrap()
                    .status()
            })
        })
        .collect();
    for h in sticky {
        assert_eq!(h.await.unwrap(), 200, "sticky infer must succeed");
    }

    let body = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{}/metrics", metrics_port))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let w0 = worker_inference_total(&body, "sticky_model", 0);
    let w1 = worker_inference_total(&body, "sticky_model", 1);
    assert_eq!(w0 + w1, 8, "all 8 sticky requests must be counted");
    assert!(
        (w0 == 8 && w1 == 0) || (w0 == 0 && w1 == 8),
        "same sequence_id must pin to a single worker, got w0={w0} w1={w1}"
    );

    // 8 CONCURRENT requests WITHOUT sequence_id → least-loaded balances across
    // both workers (each gains some). Proves non-affinity routing is untouched.
    let plain: Vec<_> = (0..8)
        .map(|i| {
            let c = client.clone();
            let url = infer_url.clone();
            tokio::spawn(async move {
                c.post(&url)
                    .json(&json!({"input": i}))
                    .send()
                    .await
                    .unwrap()
                    .status()
            })
        })
        .collect();
    for h in plain {
        assert_eq!(h.await.unwrap(), 200, "plain infer must succeed");
    }
    let body2 = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{}/metrics", metrics_port))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let added0 = worker_inference_total(&body2, "sticky_model", 0) - w0;
    let added1 = worker_inference_total(&body2, "sticky_model", 1) - w1;
    assert_eq!(added0 + added1, 8, "all 8 plain requests must be counted");
    assert!(
        added0 > 0 && added1 > 0,
        "non-affinity must balance across workers, got +{added0}/+{added1}"
    );

    unload_model(&base, "sticky_model", "1").await;
    let _ = std::fs::remove_dir_all(&repo);
    let _ = std::fs::remove_file(&cfg_path);
}

// ---------------------------------------------------------------------------
// Port allocator — collision-free ports for dedicated-server tests
// ---------------------------------------------------------------------------
//
// `kill_stale_on_port` does a blind `libc::kill(pid, SIGKILL)` on any
// `lite-server` process holding the port. Two tests reusing a fixed port and
// running concurrently (a `#[serial]` test is excluded only from OTHER serial
// tests, not from non-serial ones) cross-SIGKILL each other's live server —
// which under default-thread parallelism surfaces as a mid-test server death
// and, depending on timing, a hang the runner SIGKILLs with no FAILED line.
// These tests pin the invariant that prevents that: `next_test_port()` never
// hands the same port to two callers, concurrent or not.

#[test]
fn next_test_port_is_unique_single_threaded() {
    let mut seen = std::collections::HashSet::new();
    for _ in 0..256 {
        assert!(seen.insert(next_test_port()), "allocator reused a port");
    }
}

#[test]
fn next_test_port_is_unique_under_concurrency() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;

    let seen = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
    let collisions = Arc::new(AtomicUsize::new(0));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let (seen, collisions) = (seen.clone(), collisions.clone());
            thread::spawn(move || {
                for _ in 0..64 {
                    if !seen.lock().unwrap().insert(next_test_port()) {
                        collisions.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(
        collisions.load(Ordering::Relaxed),
        0,
        "concurrent port allocation collided — sibling tests could share a port"
    );
}

// ---------------------------------------------------------------------------
// P-DEADLINE — per-request deadline propagation (蓝图 §4.0.10 / §4.4)
// ---------------------------------------------------------------------------

/// Slow model repo with configurable worker count. A unary server-side timeout
/// does NOT cancel the worker (it keeps running its predict), so a single-worker
/// model serializes slow requests behind timed-out ones. Multiple workers let
/// concurrent deadline cases run independently.
fn create_slow_model_repo_workers(sleep_secs: u64, workers: u32) -> std::path::PathBuf {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-sloww-{}-{}-{}",
        std::process::id(),
        sleep_secs,
        workers
    ));
    let model_dir = tmp.join("slow_model/1");
    std::fs::create_dir_all(&model_dir).unwrap();
    std::fs::write(
        model_dir.join("model.py"),
        format!(
            r#"from lite_server import LitAPI
import time


class SlowAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
        time.sleep({secs})
        return {{"output": x * 2}}

    def encode_response(self, output):
        return output
"#,
            secs = sleep_secs
        ),
    )
    .unwrap();
    std::fs::write(
        model_dir.join("config.yaml"),
        format!(
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: {workers}\n",
            workers = workers
        ),
    )
    .unwrap();
    tmp
}

// A slow model (2s predict, 4 workers) + a short `server.timeout` exercises the
// three unary cases end-to-end. The 4 workers let the three concurrent requests
// run independently (a unary server timeout does not cancel the still-running
// worker, so a 1-worker model would serialize them):
//   1. no client header  → server.timeout fallback fires (504).
//   2. x-lite-timeout    → client deadline fires, tighter than the fallback.
//   3. x-lite-timeout    → a generous client deadline lets the slow request
//                          complete (200), proving the path is not broken.

#[tokio::test]
#[serial]
async fn test_p_deadline_http_client_and_fallback() {
    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let repo = create_slow_model_repo_workers(2, 4);
    let tmp_dir = std::env::temp_dir().join(format!("lite-server-pdeadline-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18212\n  timeout: 1.0\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: false\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "slow_model", "1").await;

    let client = reqwest::Client::new();
    let url = format!("{}/v2/models/slow_model/versions/1/infer", base);

    // 1. No header → server.timeout=1.0 fallback fires (504) well before the
    //    2s predict finishes.
    let t0 = std::time::Instant::now();
    let resp = client
        .post(&url)
        .json(&json!({"input": 1}))
        .send()
        .await
        .unwrap();
    let elapsed_fallback = t0.elapsed();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::GATEWAY_TIMEOUT,
        "fallback to server.timeout must 504"
    );
    assert!(
        elapsed_fallback < Duration::from_secs(3),
        "fallback should fire near the 1s server.timeout, took {elapsed_fallback:?}"
    );

    // 2. x-lite-timeout=0.5 → client deadline (tighter than the 1s fallback)
    //    fires, faster than the fallback case.
    let t0 = std::time::Instant::now();
    let resp = client
        .post(&url)
        .header("x-lite-timeout", "0.5")
        .json(&json!({"input": 1}))
        .send()
        .await
        .unwrap();
    let elapsed_client = t0.elapsed();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::GATEWAY_TIMEOUT,
        "client x-lite-timeout must 504"
    );
    assert!(
        elapsed_client < elapsed_fallback,
        "client deadline (0.5s) should fire before the 1s fallback; client={elapsed_client:?} fallback={elapsed_fallback:?}"
    );

    // 3. x-lite-timeout=10 → generous client deadline lets the 2s predict
    //    complete (200), proving the deadline path does not break normal flow.
    let resp = client
        .post(&url)
        .header("x-lite-timeout", "10")
        .json(&json!({"input": 21}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "generous deadline must let it complete");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["output"], 42, "slow predict result must come back");

    unload_model(&base, "slow_model", "1").await;
    let _ = std::fs::remove_dir_all(&tmp_dir);
    let _ = std::fs::remove_dir_all(&repo);
}

/// gRPC unary deadline: a short `server.timeout` (no client grpc-timeout, no
/// client-side timeout) makes the SERVER return DEADLINE_EXCEEDED — exercising
/// the gRPC unary deadline wrap. (A client-sent `grpc-timeout` is also enforced
/// client-side by tonic as `Cancelled`, so it cannot isolate the server wrap
/// here; grpc-timeout parsing is covered by unit tests instead.)
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_p_deadline_grpc_server_timeout() {
    use std::collections::HashMap;
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let repo = create_slow_model_repo(5);
    let tmp_dir = std::env::temp_dir().join(format!(
        "lite-server-pdeadline-grpc-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18212\n  timeout: 1.0\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "slow_model", "1").await;

    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);
    // No client timeout and no grpc-timeout: the client waits, so the server's
    // own 1s deadline wrap must surface DEADLINE_EXCEEDED (5s predict > 1s).
    let req = tonic::Request::new(InferRequest {
        model_name: "slow_model".to_string(),
        version: "1".to_string(),
        data: br#"{"input":21}"#.to_vec().into(),
        headers: HashMap::new(),
        ..Default::default()
    });
    let status = client
        .infer(req)
        .await
        .expect_err("must error — worker outlives the server timeout");
    assert_eq!(
        status.code(),
        tonic::Code::DeadlineExceeded,
        "server timeout must yield DEADLINE_EXCEEDED, got: {status}"
    );

    unload_model(&base, "slow_model", "1").await;
    let _ = std::fs::remove_dir_all(&tmp_dir);
    let _ = std::fs::remove_dir_all(&repo);
}

/// AUDIT (P1, P-DEADLINE): `batch_infer_impl` resolves the deadline
/// (grpc/mod.rs:470-471) and writes it into `meta.deadline_unix_ns` (482), but —
/// unlike unary `infer_impl` (307-323, which wraps `response_rx` in
/// `tokio::time::timeout(remaining(...))`) — its response wait is the bare
/// `client.send(internal_req).await` (grpc/mod.rs:530-533). So a BatchInfer
/// whose worker outlives `server.timeout` is NOT bounded server-side; the server
/// waits for the full worker run and returns Ok. This test sends a BatchInfer to
/// the 5s slow model under `server.timeout: 1.0` and asserts the server enforces
/// its own deadline (DEADLINE_EXCEEDED). It FAILS on the current code (returns Ok
/// after the worker completes).
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_p_deadline_grpc_batch_infer_must_enforce_server_timeout() {
    use std::collections::HashMap;
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::BatchInferRequest;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let repo = create_slow_model_repo(5);
    let tmp_dir = std::env::temp_dir().join(format!(
        "lite-server-pdeadline-grpc-batch-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18213\n  timeout: 1.0\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "slow_model", "1").await;

    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);
    // No client timeout / grpc-timeout: only the server's 1s deadline can bound
    // the call. The 5s predict outlives it, so a wrapped wait must surface
    // DEADLINE_EXCEEDED. Today batch_infer does not wrap → this returns Ok.
    let req = tonic::Request::new(BatchInferRequest {
        model_name: "slow_model".to_string(),
        version: "1".to_string(),
        items: vec![br#"{"input":21}"#.to_vec().into()],
        headers: HashMap::new(),
    });
    let status = client
        .batch_infer(req)
        .await
        .expect_err("batch_infer must be bounded by server.timeout like unary infer");
    assert_eq!(
        status.code(),
        tonic::Code::DeadlineExceeded,
        "batch_infer should surface DEADLINE_EXCEEDED under server.timeout, got: {status}"
    );

    unload_model(&base, "slow_model", "1").await;
    let _ = std::fs::remove_dir_all(&tmp_dir);
    let _ = std::fs::remove_dir_all(&repo);
}

/// AUDIT (P1, P-FLOW §4.0.9): `bidi_stream_impl`'s cleanup block never sent
/// `build_stream_cancel` to the worker, unlike its siblings `stream_infer`
/// (grpc/mod.rs:791-792) and `decoupled_infer` (:1038-1039). A bidi client
/// dropping the RPC mid-session therefore orphaned the worker-side session
/// (compute slot held until model unload / worker shutdown). This test opens
/// a bidi stream, hard-drops the client while the fixture's `on_chunk` is
/// mid-sleep, and asserts the cancel reaches the worker: the fixture's
/// `on_close` (invoked exactly once per session, on close OR cancel) writes a
/// marker file. FAILS on unfixed code (no cancel → no `on_close` → no marker).
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_p_flow_grpc_bidi_disconnect_propagates_cancel() {
    use lite_server::proto::liteserver as pb;
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let tmp_dir = std::env::temp_dir().join(format!(
        "lite-server-bidi-cancel-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let marker = tmp_dir.join("cancel.marker");
    let repo = create_bidi_model_repo(&marker);
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18214\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "bidi_model", "1").await;

    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);
    let (req_tx, req_rx) = tokio::sync::mpsc::channel::<pb::BidiChunk>(8);
    // Pre-queue BidiOpen BEFORE the RPC: the server's handler awaits the first
    // message before returning its Response, so the client future only resolves
    // once an open is already flowing (else both sides wait on each other).
    req_tx
        .send(pb::BidiChunk {
            stream_id: String::new(),
            payload: Some(pb::bidi_chunk::Payload::Open(pb::BidiOpen {
                model_name: "bidi_model".to_string(),
                version: "1".to_string(),
                initial_data: bytes::Bytes::from_static(b"{}"),
                sequence_id: None,
                ..Default::default()
            })),
        })
        .await
        .expect("queue BidiOpen failed");
    let mut resp = tokio::time::timeout(
        Duration::from_secs(10),
        client.bidi_stream(tonic::Request::new(
            tokio_stream::wrappers::ReceiverStream::new(req_rx),
        )),
    )
    .await
    .expect("bidi_stream RPC did not resolve within 10s")
    .expect("bidi_stream RPC failed")
    .into_inner();
    let first = tokio::time::timeout(Duration::from_secs(5), resp.message())
        .await
        .expect("timed out waiting for bidi on_open reply")
        .expect("bidi stream errored before on_open reply")
        .expect("bidi stream closed before on_open reply");
    assert!(
        matches!(first.payload, Some(pb::bidi_chunk::Payload::Data(_))),
        "expected on_open data chunk, got: {:?}",
        first.payload
    );

    // Send one chunk; the fixture's on_chunk sleeps 5s, so the worker session
    // is still busy when the client vanishes below.
    req_tx
        .send(pb::BidiChunk {
            stream_id: String::new(),
            payload: Some(pb::bidi_chunk::Payload::Data(pb::BidiData {
                data: bytes::Bytes::from_static(br#"{"input": 1}"#),
            })),
        })
        .await
        .expect("send BidiData failed");
    sleep(Duration::from_millis(500)).await; // ensure on_chunk is mid-sleep

    // Hard disconnect: dropping both halves makes tonic RST_STREAM the RPC.
    drop(req_tx);
    drop(resp);
    drop(client);

    // After on_chunk's sleep completes, the worker emits the reply chunk; the
    // server's forward on the dead response stream fails, its cleanup must
    // send StreamCancel, and the worker then runs on_close → marker file.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline && !marker.exists() {
        sleep(Duration::from_millis(200)).await;
    }
    assert!(
        marker.exists(),
        "worker never received StreamCancel: bidi on_close marker missing \
         (P-FLOW cancel-propagation gap on the bidi path)"
    );

    unload_model(&base, "bidi_model", "1").await;
    let _ = std::fs::remove_dir_all(&tmp_dir);
    let _ = std::fs::remove_dir_all(&repo);
}

/// D4 half-close (蓝图 §4 HTTP bidi): when a gRPC bidi client ends its request
/// stream without sending an explicit `BidiClose`, the server must send
/// `StreamRequest::Close` to the worker so the input side is gracefully
/// terminated (rather than hanging until the forwarder's cleanup Cancel).
/// The fixture's `on_close` writes a marker — the test verifies it appears.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_d4_grpc_bidi_half_close_sends_close_to_worker() {
    use lite_server::proto::liteserver as pb;
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let tmp_dir = std::env::temp_dir().join(format!(
        "lite-server-bidi-halfclose-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let marker = tmp_dir.join("halfclose.marker");
    let model_dir = tmp_dir.join("bidi_hc/1");
    std::fs::create_dir_all(&model_dir).unwrap();
    std::fs::write(
        model_dir.join("model.py"),
        format!(
            r#"from lite_server import LitAPI


class BidiHandler:
    def on_open(self, initial_data):
        return {{"opened": True}}

    def on_chunk(self, chunk):
        return {{"echo": chunk}}

    def on_close(self):
        with open("{marker}", "w") as f:
            f.write("closed")


class BidiAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request

    def predict(self, x):
        return {{"output": x}}

    def encode_response(self, output):
        return output

    def bidi_stream(self):
        return BidiHandler()
"#,
            marker = marker.to_string_lossy()
        ),
    )
    .unwrap();
    std::fs::write(
        model_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18214\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {}\n",
            // Repository ROOT (contains <model>/<version>/); model_dir is the
            // version dir itself — parent() alone would point one level too deep.
            tmp_dir.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "bidi_hc", "1").await;

    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);
    let (req_tx, req_rx) = tokio::sync::mpsc::channel::<pb::BidiChunk>(8);
    req_tx
        .send(pb::BidiChunk {
            stream_id: String::new(),
            payload: Some(pb::bidi_chunk::Payload::Open(pb::BidiOpen {
                model_name: "bidi_hc".to_string(),
                version: "1".to_string(),
                initial_data: bytes::Bytes::from_static(b"{}"),
                sequence_id: None,
                ..Default::default()
            })),
        })
        .await
        .expect("queue BidiOpen failed");
    let mut resp = tokio::time::timeout(
        Duration::from_secs(10),
        client.bidi_stream(tonic::Request::new(
            tokio_stream::wrappers::ReceiverStream::new(req_rx),
        )),
    )
    .await
    .expect("bidi_stream RPC did not resolve within 10s")
    .expect("bidi_stream RPC failed")
    .into_inner();

    // Read on_open response.
    let first = tokio::time::timeout(Duration::from_secs(5), resp.message())
        .await
        .expect("timed out waiting for on_open reply")
        .expect("bidi stream errored before on_open reply")
        .expect("bidi stream closed before on_open reply");
    assert!(
        matches!(first.payload, Some(pb::bidi_chunk::Payload::Data(_))),
        "expected on_open data chunk"
    );

    // Send one Data chunk so the worker has received input.
    req_tx
        .send(pb::BidiChunk {
            stream_id: String::new(),
            payload: Some(pb::bidi_chunk::Payload::Data(pb::BidiData {
                data: bytes::Bytes::from_static(br#"{"chunk": 1}"#),
            })),
        })
        .await
        .expect("send BidiData failed");

    // Half-close: drop ONLY the sender. The receiver stays alive so the gRPC
    // stream signals end-of-stream (not RST_STREAM), and the server's
    // incoming_task must send Close for the worker.
    drop(req_tx);

    // The worker should receive Close and call on_close → marker written.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline && !marker.exists() {
        sleep(Duration::from_millis(200)).await;
    }
    assert!(
        marker.exists(),
        "D4 half-close: worker never received Close (on_close marker missing after client half-close)"
    );

    // Drain remaining responses (on_chunk echo + close frame).
    while let Ok(Ok(Some(_chunk))) =
        tokio::time::timeout(Duration::from_secs(3), resp.message()).await
    {
        // consume
    }
    drop(resp);
    drop(client);

    unload_model(&base, "bidi_hc", "1").await;
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// §6.9-6: real h2 end-to-end — dedicated server (h2c prior-knowledge via
/// hyper auto-detect) + reqwest prior-knowledge client, full-duplex roundtrip
/// through a `bidi_stream` echo model: Open → on_open Data; mid-stream Data →
/// echo Data; Close → terminal Close frame + body EOF.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_h2_bidi_prior_knowledge_full_duplex() {
    use lite_server::proto::liteserver as pb;
    use lite_server::streaming::lpm;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let tmp_dir = std::env::temp_dir().join(format!(
        "lite-server-h2bidi-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let model_dir = tmp_dir.join("h2bidi/1");
    std::fs::create_dir_all(&model_dir).unwrap();
    std::fs::write(
        model_dir.join("model.py"),
        r#"from lite_server import LitAPI


class BidiHandler:
    def on_open(self, initial_data):
        return {"opened": True}

    def on_chunk(self, chunk):
        return {"echo": chunk}

    def on_close(self):
        pass


class BidiAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output

    def bidi_stream(self):
        return BidiHandler()
"#,
    )
    .unwrap();
    std::fs::write(
        model_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18216\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {}\n",
            tmp_dir.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "h2bidi", "1").await;

    // h2 prior-knowledge client with a live, streamed request body.
    // (reqwest's wrap_stream needs a Sync stream; poll the tokio mpsc
    // receiver through a mutex — poll_recv never blocks.)
    let (body_tx, body_rx) =
        tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(8);
    let body_rx = std::sync::Arc::new(std::sync::Mutex::new(body_rx));
    let body_stream = futures::stream::poll_fn(move |cx| {
        body_rx.lock().unwrap().poll_recv(cx)
    });
    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap();
    // Full-duplex bootstrap (plan §8): the client MUST start streaming the
    // body immediately — the server awaits the first LPM frame (bounded by
    // server.timeout) before committing the 200, so queue BidiOpen up front.
    body_tx
        .send(Ok(lpm::encode_frame(&pb::BidiChunk {
            stream_id: String::new(),
            payload: Some(pb::bidi_chunk::Payload::Open(pb::BidiOpen {
                initial_data: bytes::Bytes::from_static(b"{}"),
                ..Default::default()
            })),
        })))
        .await
        .unwrap();
    let resp = client
        .post(format!("{}/v2/models/h2bidi/bidi", base))
        .header("content-type", "application/x-lite-bidi")
        .body(reqwest::Body::wrap_stream(body_stream))
        .send()
        .await
        .expect("h2 bidi POST failed");
    assert_eq!(resp.status(), 200, "h2 bidi must accept the session");
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "application/x-lite-bidi"
    );
    assert_eq!(resp.version(), reqwest::Version::HTTP_2, "must be real h2");

    // Read one LPM frame from the response stream (10s budget).
    async fn read_frame(
        stream: &mut (impl futures::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Unpin),
        buf: &mut bytes::BytesMut,
    ) -> pb::BidiChunk {
        use futures::StreamExt;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(Some(c)) = lpm::try_decode_frame(buf) {
                return c;
            }
            let next = tokio::time::timeout_at(deadline, stream.next())
                .await
                .expect("timed out waiting for LPM frame")
                .expect("response stream ended before frame");
            buf.extend_from_slice(&next.expect("response stream error"));
        }
    }

    let mut resp_stream = resp.bytes_stream();
    let mut buf = bytes::BytesMut::new();

    // 1. The queued Open → on_open Data frame.
    let f = read_frame(&mut resp_stream, &mut buf).await;
    match f.payload {
        Some(pb::bidi_chunk::Payload::Data(d)) => {
            assert!(String::from_utf8_lossy(&d.data).contains("opened"));
        }
        other => panic!("expected on_open Data frame, got {other:?}"),
    }
    assert!(f.stream_id.starts_with("http-bidi-"));

    // 2. Full-duplex: Data sent mid-stream → echo Data frame.
    body_tx
        .send(Ok(lpm::encode_frame(&pb::BidiChunk {
            stream_id: String::new(),
            payload: Some(pb::bidi_chunk::Payload::Data(pb::BidiData {
                data: bytes::Bytes::from_static(br#"{"chunk": 1}"#),
            })),
        })))
        .await
        .unwrap();
    let f = read_frame(&mut resp_stream, &mut buf).await;
    match f.payload {
        Some(pb::bidi_chunk::Payload::Data(d)) => {
            assert!(String::from_utf8_lossy(&d.data).contains("echo"));
        }
        other => panic!("expected echo Data frame, got {other:?}"),
    }

    // 3. Close → terminal Close frame, then body EOF.
    body_tx
        .send(Ok(lpm::encode_frame(&pb::BidiChunk {
            stream_id: String::new(),
            payload: Some(pb::bidi_chunk::Payload::Close(pb::BidiClose {})),
        })))
        .await
        .unwrap();
    let f = read_frame(&mut resp_stream, &mut buf).await;
    assert!(
        matches!(f.payload, Some(pb::bidi_chunk::Payload::Close(_))),
        "expected terminal Close frame, got {:?}",
        f.payload
    );
    {
        use futures::StreamExt;
        while let Some(item) = tokio::time::timeout(Duration::from_secs(5), resp_stream.next())
            .await
            .expect("timed out waiting for response EOF")
        {
            buf.extend_from_slice(&item.expect("response stream error"));
        }
        assert!(
            lpm::try_decode_frame(&mut buf).ok().flatten().is_none(),
            "no frames after terminal Close"
        );
    }

    unload_model(&base, "h2bidi", "1").await;
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

// ---------------------------------------------------------------------------
// B1 (tensor-bytes-consistency): h2 bidi initial_data Content-Type dispatch
// ---------------------------------------------------------------------------

/// Shared fixture for the B1 dispatch tests: a bidi model whose `on_open`
/// reports how the worker dispatched the initial_data (raw bytes vs parsed
/// JSON), so the test can assert the Rust/Python dispatch agreement.
#[cfg(unix)]
async fn start_h2_bidi_ct_server(tag: &str) -> (String, ServerGuard, std::path::PathBuf) {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let repo = std::env::temp_dir().join(format!(
        "lite-server-h2bidi-ct-{tag}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&repo);
    let model_dir = repo.join("h2bidi/1");
    std::fs::create_dir_all(&model_dir).unwrap();
    std::fs::write(
        model_dir.join("model.py"),
        r#"from lite_server import LitAPI


class BidiHandler:
    def on_open(self, initial_data):
        if isinstance(initial_data, bytes):
            return {"kind": "bytes", "len": len(initial_data)}
        return {"kind": "json", "value": initial_data}

    def on_chunk(self, chunk):
        return {"echo": chunk}

    def on_close(self):
        pass


class BidiAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request

    def predict(self, x):
        return {"output": x}

    def encode_response(self, output):
        return output

    def bidi_stream(self):
        return BidiHandler()
"#,
    )
    .unwrap();
    std::fs::write(
        model_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
    let server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc", "--no-metrics", "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "h2bidi", "1").await;
    (base, server, repo)
}

/// POST a single LPM BidiOpen frame with a static body (frame + END_STREAM)
/// and the given Content-Type; returns the raw response.
#[cfg(unix)]
async fn h2_bidi_open(base: &str, ct: &str, initial_data: &'static [u8]) -> reqwest::Response {
    use lite_server::proto::liteserver as pb;
    use lite_server::streaming::lpm;
    let frame = lpm::encode_frame(&pb::BidiChunk {
        stream_id: String::new(),
        payload: Some(pb::bidi_chunk::Payload::Open(pb::BidiOpen {
            initial_data: bytes::Bytes::from_static(initial_data),
            ..Default::default()
        })),
    });
    reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap()
        .post(format!("{}/v2/models/h2bidi/bidi", base))
        .header("content-type", ct)
        .body(frame)
        .send()
        .await
        .expect("h2 bidi POST failed")
}

/// Read the first Data frame payload from an accepted bidi response.
#[cfg(unix)]
async fn h2_bidi_first_data(resp: reqwest::Response) -> Vec<u8> {
    use futures::StreamExt;
    use lite_server::proto::liteserver as pb;
    use lite_server::streaming::lpm;
    let mut stream = resp.bytes_stream();
    let mut buf = bytes::BytesMut::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(Some(c)) = lpm::try_decode_frame(&mut buf) {
            match c.payload {
                Some(pb::bidi_chunk::Payload::Data(d)) => return d.data.to_vec(),
                other => panic!("expected on_open Data frame, got {other:?}"),
            }
        }
        let next = tokio::time::timeout_at(deadline, stream.next())
            .await
            .expect("timed out waiting for LPM frame")
            .expect("response stream ended before frame");
        buf.extend_from_slice(&next.expect("response stream error"));
    }
}

/// B1 §7.1: JSON Content-Type + malformed initial_data → 400 before the
/// worker stream opens (a committed 200 would prove the worker opened).
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_h2_bidi_initial_data_invalid_json_ct_400() {
    let (base, _server, repo) = start_h2_bidi_ct_server("badjson").await;

    let resp = h2_bidi_open(&base, "application/json", b"{not-json").await;
    assert_eq!(
        resp.status(),
        400,
        "malformed JSON initial_data must be rejected at the edge"
    );
    let body: Value = resp.json().await.unwrap();
    let msg = body["error"]["message"].as_str().unwrap();
    assert!(
        msg.contains("invalid JSON in BidiOpen initial_data"),
        "error must name the initial_data validation, got: {msg}"
    );

    let _ = std::fs::remove_dir_all(&repo);
}

/// B1 §7.1 (E1 pin): the framing Content-Type is treated as absent (JSON
/// default), so malformed JSON under application/x-lite-bidi must ALSO 400 —
/// without the E1 mirror rule this would slip through as "raw".
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_h2_bidi_initial_data_framing_ct_invalid_json_400() {
    let (base, _server, repo) = start_h2_bidi_ct_server("framing").await;

    let resp = h2_bidi_open(&base, "application/x-lite-bidi", b"\xff\xfe garbage").await;
    assert_eq!(
        resp.status(),
        400,
        "framing CT must keep JSON-default dispatch (E1), got {}",
        resp.status()
    );

    let _ = std::fs::remove_dir_all(&repo);
}

/// B1 §7.1: application/octet-stream + arbitrary bytes → no validation, the
/// worker receives raw bytes (Python raw branch).
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_h2_bidi_initial_data_octet_stream_raw_passthrough() {
    let (base, _server, repo) = start_h2_bidi_ct_server("raw").await;

    let resp = h2_bidi_open(&base, "application/octet-stream", b"\x00\x01\x02").await;
    assert_eq!(resp.status(), 200, "raw CT must skip JSON validation");
    let data = h2_bidi_first_data(resp).await;
    let v: Value = serde_json::from_slice(&data).unwrap();
    assert_eq!(
        v["kind"], "bytes",
        "worker must receive raw bytes, got: {v}"
    );
    assert_eq!(v["len"], 3, "all 3 bytes must arrive, got: {v}");

    unload_model(&base, "h2bidi", "1").await;
    let _ = std::fs::remove_dir_all(&repo);
}

/// B1 §7.1 (E2 pin): empty initial_data with a JSON Content-Type is legal —
/// Python maps it to {} — and must not be rejected by the Rust validation.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_h2_bidi_initial_data_empty_skips_validation() {
    let (base, _server, repo) = start_h2_bidi_ct_server("empty").await;

    let resp = h2_bidi_open(&base, "application/json", b"").await;
    assert_eq!(
        resp.status(),
        200,
        "empty initial_data must skip validation (E2), got {}",
        resp.status()
    );
    let data = h2_bidi_first_data(resp).await;
    let v: Value = serde_json::from_slice(&data).unwrap();
    assert_eq!(
        v["kind"], "json",
        "Python must map empty payload to {{}} (json branch), got: {v}"
    );
    assert_eq!(v["value"], json!({}), "empty payload maps to empty object");

    unload_model(&base, "h2bidi", "1").await;
    let _ = std::fs::remove_dir_all(&repo);
}

/// WS6 (P-DEADLINE bidi parity): a bidi stream whose client never sends the
/// opening message must be recovered after `server.timeout`, instead of
/// hanging the handler unbounded. The server's `bidi_stream` reads the first
/// message bounded by the resolved deadline; with `server.timeout: 2.0` and no
/// client `grpc-timeout`, the wait is bounded to ~2s and the RPC fails with
/// `DeadlineExceeded`.
#[tokio::test]
#[serial]
async fn test_grpc_bidi_first_message_timeout() {
    use lite_server::proto::liteserver as pb;
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let tmp_dir = std::env::temp_dir().join(format!(
        "lite-server-bidi-fmto-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let marker = tmp_dir.join("fmto.marker");
    let repo = create_bidi_model_repo(&marker);
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18215\n  log_level: warn\n  timeout: 2.0\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "bidi_model", "1").await;

    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);
    // Withhold the opening message: the client never sends BidiOpen, so the
    // server's handler blocks on `stream.message()` until the deadline fires.
    let (_withhold_tx, req_rx) = tokio::sync::mpsc::channel::<pb::BidiChunk>(8);
    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        client.bidi_stream(tonic::Request::new(
            tokio_stream::wrappers::ReceiverStream::new(req_rx),
        )),
    )
    .await
    .expect("bidi_stream RPC did not resolve within 10s");
    let elapsed = started.elapsed();
    let status = outcome.expect_err(
        "bidi_stream must fail with DeadlineExceeded when the opening message is withheld",
    );
    assert_eq!(
        status.code(),
        tonic::Code::DeadlineExceeded,
        "expected DeadlineExceeded, got: {status}"
    );
    assert!(
        elapsed >= Duration::from_millis(1500) && elapsed <= Duration::from_secs(6),
        "first-message timeout should fire ~2s after the RPC, got {elapsed:?}"
    );

    let _ = std::fs::remove_dir_all(&tmp_dir);
    let _ = std::fs::remove_dir_all(&repo);
}

/// WS6 (P-DEADLINE WS parity): a WebSocket client that upgrades but never sends
/// the first message must be closed after `server.timeout`, not held open
/// forever. With `server.timeout: 2.0`, the server's first-message recv is
/// bounded by the idle budget and the socket is closed.
#[tokio::test]
#[serial]
async fn test_ws_first_message_timeout() {
    use futures::StreamExt;
    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    let tmp_dir = std::env::temp_dir().join(format!(
        "lite-server-ws-fmto-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let repo = create_test_model_repo();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18216\n  log_level: warn\n  timeout: 2.0\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "test_model", "1").await;

    let ws_url = format!("ws://127.0.0.1:{}/v2/models/test_model/stream", http_port);
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .expect("WS connect failed");
    // Withhold the first message: the server must close the socket after the
    // ~2s first-message idle budget fires.
    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(Duration::from_secs(8), ws.next()).await;
    let elapsed = started.elapsed();
    // ws.next() resolves (None / Close / Error) once the server closes.
    assert!(
        outcome.is_ok(),
        "server should close the idle WS within 8s (first-message timeout)"
    );
    assert!(
        elapsed >= Duration::from_millis(1500) && elapsed <= Duration::from_secs(6),
        "first-message timeout should fire ~2s after upgrade, got {elapsed:?}"
    );
}

/// WS4 (features.* gating): with the route-gated features off, the corresponding
/// routes are unmounted (404 via route_fallback), while always-on routes stay up.
#[tokio::test]
#[serial]
async fn test_feature_toggles_unmount_routes() {
    let http_port = next_test_port();
    let grpc_port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    kill_stale_on_port(metrics_port);
    let repo = create_test_model_repo();
    let tmp_dir = std::env::temp_dir().join(format!(
        "lite-server-feat-off-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: {metrics_port}\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {repo}\nfeatures:\n  timeline: false\n  alerts: false\n  version_compare: false\n  streaming: false\n  sse: false\n  websocket_streaming: false\n",
            http_port = http_port,
            grpc_port = grpc_port,
            metrics_port = metrics_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "test_model", "1").await;

    let client = reqwest::Client::new();
    // Sanity: an always-on route still answers (server is up and routing).
    assert_eq!(
        client.get(format!("{}/health", base)).send().await.unwrap().status(),
        200
    );
    // timeline / alerts / version_compare unmounted → 404.
    assert_eq!(client.get(format!("{}/metrics/timeline", base)).send().await.unwrap().status(), 404);
    assert_eq!(
        client.get(format!("{}/metrics/timeline/test_model", base)).send().await.unwrap().status(),
        404
    );
    assert_eq!(client.get(format!("{}/metrics/alerts", base)).send().await.unwrap().status(), 404);
    assert_eq!(
        client.get(format!("{}/v2/models/test_model/compare", base)).send().await.unwrap().status(),
        404
    );
    // streaming off → SSE + WS routes unmounted → 404.
    assert_eq!(
        client
            .post(format!("{}/v2/models/test_model/events", base))
            .json(&json!({"input": 1}))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(
        client.get(format!("{}/v2/models/test_model/stream", base)).send().await.unwrap().status(),
        404
    );
}

/// WS4 (features.grpc_streaming): with grpc_streaming off, the three streaming
/// RPCs return UNIMPLEMENTED before admission. stream_infer is exercised here;
/// batch_infer (unary) is ungated and unaffected.
#[tokio::test]
#[serial]
async fn test_grpc_streaming_disabled_returns_unimplemented() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::StreamInferRequest;
    use std::collections::HashMap;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    kill_stale_on_port(metrics_port);
    let repo = test_model_repo();
    let tmp_dir = std::env::temp_dir().join(format!(
        "lite-server-grpcs-off-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: {metrics_port}\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {repo}\nfeatures:\n  grpc_streaming: false\n",
            http_port = http_port,
            grpc_port = grpc_port,
            metrics_port = metrics_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, MODEL, "1").await;

    let mut client = LiteServerClient::connect(format!("http://127.0.0.1:{}", grpc_port))
        .await
        .expect("gRPC client must connect");

    let req = StreamInferRequest {
        model_name: MODEL.to_string(),
        version: "1".to_string(),
        data: bytes::Bytes::from(serde_json::to_vec(&json!({"input": 2})).unwrap()),
        headers: HashMap::new(),
        ..Default::default()
    };
    let status = client
        .stream_infer(req)
        .await
        .expect_err("stream_infer must return Unimplemented when grpc_streaming=false");
    assert_eq!(
        status.code(),
        tonic::Code::Unimplemented,
        "expected Unimplemented, got: {status}"
    );
}

/// WS2 (cache_registry): on graceful shutdown the registry snapshot is written;
/// on restart it is consumed. Boot a server with `cache_registry: true`, load +
/// activate a version, SIGTERM, assert the snapshot file pins the version, then
/// restart and confirm the server boots cleanly (restore ran).
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_cache_registry_snapshots_and_restores() {
    let http_port = next_test_port();
    let grpc_port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    kill_stale_on_port(metrics_port);

    let repo_dir =
        std::env::temp_dir().join(format!("lite-server-cache-int-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo_dir);
    let model_dir = repo_dir.join("cm").join("1");
    std::fs::create_dir_all(&model_dir).unwrap();
    std::fs::write(
        model_dir.join("model.py"),
        "from lite_server import LitAPI\nclass A(LitAPI):\n  def setup(self, d): pass\n  def decode_request(self, r): return r\n  def predict(self, x): return {\"out\": x}\n  def encode_response(self, o): return o\n",
    )
    .unwrap();
    std::fs::write(
        model_dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();

    let tmp_dir = std::env::temp_dir().join(format!("lite-server-cache-cfg-{}", std::process::id()));
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::create_dir_all(&tmp_dir).unwrap();
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: {metrics_port}\n  log_level: warn\n  cache_registry: true\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            metrics_port = metrics_port,
            repo = repo_dir.to_string_lossy()
        ),
    )
    .unwrap();
    let cfg_arg = server_yaml.to_string_lossy().to_string();

    // --- Boot #1: load + activate, then graceful SIGTERM. ---
    let mut child = start_server(&["--config", &cfg_arg]);
    wait_for_server(http_port, 30).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "cm", "1").await;
    // Explicitly activate so an active-version pin is recorded before shutdown.
    let activate = reqwest::Client::new()
        .post(format!("{}/v2/models/cm/versions/1/activate", base))
        .send()
        .await
        .expect("activate request failed");
    assert_eq!(activate.status(), 200, "activate failed");

    send_sigterm(&child);
    let exited = wait_for_exit(&mut child, 30).await;
    assert!(exited, "server did not exit on SIGTERM");

    // --- The snapshot file must exist and pin cm → 1. ---
    let snap_path = repo_dir.join(".lite-server-registry.json");
    let snap: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&snap_path).expect("snapshot file written"))
            .expect("snapshot is valid JSON");
    assert_eq!(
        snap["active_versions"]["cm"].as_str(),
        Some("1"),
        "snapshot must pin the active version: {snap}"
    );
    assert!(
        snap["models"]["cm"].is_object(),
        "snapshot must include the model strategy: {snap}"
    );

    // --- Boot #2 (restart): restore consumes the snapshot; server boots clean. ---
    let child2 = start_server(&["--config", &cfg_arg]);
    wait_for_server(http_port, 30).await;
    let health = reqwest::Client::new()
        .get(format!("{}/health", base))
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), 200, "server must boot after restore");

    stop_server(child2);
    let _ = std::fs::remove_dir_all(&repo_dir);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// WS2 regression (cache_registry): with `cache_registry` on, a pinned
/// (previously active) version that fails to load on restart suppresses
/// reconcile's auto-activate fallback. Boot #1 pins cm → v1; v1's model.py is
/// broken before boot #2. Observed defect: v2 loads fine but is never
/// activated, so the model cannot serve — while a cache_registry-less restart
/// auto-activates v2 and recovers by itself.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_cache_registry_stale_pin_blocks_auto_activation() {
    let http_port = next_test_port();
    let grpc_port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    kill_stale_on_port(metrics_port);

    // Model cm with two healthy versions: v1 (to be pinned) and v2.
    let repo_dir =
        std::env::temp_dir().join(format!("lite-server-cache-pin-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo_dir);
    let healthy_model = "from lite_server import LitAPI\nclass A(LitAPI):\n  def setup(self, d): pass\n  def decode_request(self, r): return r\n  def predict(self, x): return {\"out\": x}\n  def encode_response(self, o): return o\n";
    let model_config = "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n";
    for v in ["1", "2"] {
        let model_dir = repo_dir.join("cm").join(v);
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(model_dir.join("model.py"), healthy_model).unwrap();
        std::fs::write(model_dir.join("config.yaml"), model_config).unwrap();
    }

    let tmp_dir =
        std::env::temp_dir().join(format!("lite-server-cache-pin-cfg-{}", std::process::id()));
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::create_dir_all(&tmp_dir).unwrap();
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: {metrics_port}\n  log_level: warn\n  cache_registry: true\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\norchestration:\n  control_mode: all\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            metrics_port = metrics_port,
            repo = repo_dir.to_string_lossy()
        ),
    )
    .unwrap();
    let cfg_arg = server_yaml.to_string_lossy().to_string();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap();
    let base = format!("http://127.0.0.1:{}", http_port);

    // Poll /v2/models/cm until reconcile has loaded the given versions as Ready.
    async fn wait_versions(
        client: &reqwest::Client,
        base: &str,
        ready: &[&str],
        timeout_secs: u64,
    ) -> serde_json::Value {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
        loop {
            let resp = client
                .get(format!("{}/v2/models/cm/versions", base))
                .send()
                .await
                .unwrap();
            if resp.status() == 200 {
                let v: serde_json::Value = resp.json().await.unwrap();
                let versions = v["versions"].as_array().cloned().unwrap_or_default();
                let ready_ok = ready.iter().all(|want| {
                    versions
                        .iter()
                        .any(|x| x["version"] == *want && x["status"] == "ready")
                });
                if ready_ok {
                    return v;
                }
            }
            if std::time::Instant::now() > deadline {
                return client
                    .get(format!("{}/v2/models/cm/versions", base))
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    // --- Boot #1: both versions healthy; activate v1; SIGTERM → snapshot pins cm → 1. ---
    let mut child = start_server(&["--config", &cfg_arg]);
    wait_for_server(http_port, 30).await;
    wait_versions(&client, &base, &["1", "2"], 30).await;
    let activate = client
        .post(format!("{}/v2/models/cm/versions/1/activate", base))
        .send()
        .await
        .expect("activate request failed");
    assert_eq!(activate.status(), 200, "activate failed");
    send_sigterm(&child);
    let exited = wait_for_exit(&mut child, 30).await;
    assert!(exited, "server did not exit on SIGTERM");

    // --- Break v1: the pinned version must fail to load on restart. ---
    std::fs::write(
        repo_dir.join("cm").join("1").join("model.py"),
        "def setup(:", // SyntaxError → the worker fails at import.
    )
    .unwrap();

    // --- Boot #2: restore pins cm → 1; v1 load fails; v2 loads fine. ---
    let child2 = start_server(&["--config", &cfg_arg]);
    wait_for_server(http_port, 30).await;
    let info = wait_versions(&client, &base, &["2"], 30).await;

    // The seeded pin points at the broken v1, but reconcile's auto-activate
    // fallback must still promote the healthy v2 — otherwise the model is
    // stuck unusable after a restart (the defect this test pins down).
    let v1 = info["versions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["version"] == "1")
        .expect("v1 registered after the failed load");
    assert_eq!(v1["status"], "failed", "v1 must be broken after the failed load");
    assert_eq!(
        info["active_version"].as_str(),
        Some("2"),
        "auto-activate fallback must promote the healthy v2 (defect: stale pin blocks it)"
    );
    let infer = client
        .post(format!("{}/v2/models/cm/infer", base))
        .json(&json!({"input": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        infer.status(),
        200,
        "model must serve via the auto-activated healthy v2 (defect: routes to broken pin)"
    );
    stop_server(child2);

    // --- Contrast: the same repo without cache_registry auto-activates v2. ---
    let hp2 = next_test_port();
    let gp2 = next_test_port();
    let mp2 = next_test_port();
    kill_stale_on_port(hp2);
    kill_stale_on_port(gp2);
    kill_stale_on_port(mp2);
    let plain_yaml = tmp_dir.join("server-plain.yaml");
    std::fs::write(
        &plain_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {hp2}\n  grpc_port: {gp2}\n  metrics_port: {mp2}\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\norchestration:\n  control_mode: all\nmodel_repository:\n  path: {repo}\n",
            hp2 = hp2,
            gp2 = gp2,
            mp2 = mp2,
            repo = repo_dir.to_string_lossy()
        ),
    )
    .unwrap();
    let base2 = format!("http://127.0.0.1:{}", hp2);
    let child3 = start_server(&["--config", &plain_yaml.to_string_lossy()]);
    wait_for_server(hp2, 30).await;
    let info3 = wait_versions(&client, &base2, &["2"], 30).await;
    assert_eq!(
        info3["active_version"].as_str(),
        Some("2"),
        "without cache_registry the restart must auto-activate the healthy v2"
    );
    let infer3 = client
        .post(format!("{}/v2/models/cm/infer", base2))
        .json(&json!({"input": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(infer3.status(), 200, "recovered model must serve");
    stop_server(child3);

    let _ = std::fs::remove_dir_all(&repo_dir);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

// ===========================================================================
// Multi-platform / multi-version audit regression tests (2026-08-02)
// ===========================================================================

/// A gRPC port already taken at startup must fail fast with a startup error,
/// NOT wedge the server. With a model already loaded, `run()` unwinds on the
/// EADDRINUSE and the tokio Runtime drop deadlocks against the in-flight ZMQ
/// blocking task (the worker handshake poll): the process then lives on
/// forever with no error output and health never serving.
///
/// Reproduced manually: hog port 8001 + a loaded model → the server logs
/// "Starting gRPC", then neither exits nor serves; `sample` shows
/// lite-server-main stuck in `Runtime::drop → BlockingPool::shutdown` while a
/// tokio-rt-worker sits in `WorkerZmqClient::new → zmq_poll`.
#[tokio::test]
#[serial]
async fn test_grpc_port_conflict_fails_fast_not_wedge() {
    let http_port = next_test_port();
    let grpc_port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    kill_stale_on_port(metrics_port);

    // Occupy the gRPC port — what a second lite-server (or any other service)
    // does at startup.
    let hog = std::net::TcpListener::bind(("127.0.0.1", grpc_port)).unwrap();

    let repo = test_model_repo();
    let tmp_dir =
        std::env::temp_dir().join(format!("lite-server-grpc-conflict-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    // Orchestration auto-loads the model BEFORE the gRPC bind error unwinds
    // run(), which is what leaves the blocking ZMQ handshake task in flight.
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: {metrics_port}\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\norchestration:\n  control_mode: explicit\n  load_models: [test_model]\n  models:\n    - name: test_model\n      load_policy: all\nmodel_repository:\n  path: {}\n",
            repo.to_string_lossy()
        ),
    )
    .unwrap();

    let mut server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);

    // The server must exit with a startup error inside the deadline. Today it
    // wedges: the process stays alive forever, never exits, never serves.
    let child = server.0.as_mut().unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(50);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(
                !status.success(),
                "server must exit non-zero on a gRPC port conflict; got {:?}",
                status
            );
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "server wedged on a gRPC port conflict: neither exited with an error \
                 nor started serving — the Runtime drop deadlocked on the ZMQ \
                 blocking task (port-conflict startup must fail fast)"
            );
        }
        sleep(Duration::from_millis(500)).await;
    }

    drop(hog);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// Worker spawn must not depend on a `python` alias on PATH: Debian/Ubuntu
/// installs and many container base images ship only `python3` (no
/// `python-is-python3`). The worker command is hardcoded to `python`
/// (worker/process.rs::new_worker_command), so models fail to load there.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_worker_spawn_works_with_python3_only_path() {
    use std::os::unix::fs::PermissionsExt;

    let http_port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(metrics_port);

    // A PATH that has `python3` but no `python`. We expose the host's python3
    // through a wrapper script rather than a symlink: CPython locates its venv
    // `pyvenv.cfg` from argv[0]'s directory, so a symlink placed in this temp
    // bin_dir would skip the venv (argv[0] = bin_dir/python3 → no pyvenv.cfg
    // nearby) and the worker would import-fail on `google.protobuf` for reasons
    // unrelated to the interpreter-name fallback under test. The wrapper execs
    // the real venv interpreter, so argv[0] points at it and site-packages load.
    let bin_dir =
        std::env::temp_dir().join(format!("lite-server-py3bin-{}", std::process::id()));
    std::fs::create_dir_all(&bin_dir).unwrap();
    let out = std::process::Command::new("python3")
        .arg("-c")
        .arg("import sys; print(sys.executable)")
        .output()
        .unwrap();
    assert!(out.status.success(), "python3 must exist for this test");
    let real_python = String::from_utf8(out.stdout).unwrap();
    let real_python = real_python.trim();
    let wrapper = bin_dir.join("python3");
    std::fs::write(&wrapper, format!("#!/bin/sh\nexec {} \"$@\"\n", real_python)).unwrap();
    let mut perms = std::fs::metadata(&wrapper).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&wrapper, perms).unwrap();
    assert!(
        !bin_dir.join("python").exists(),
        "test setup broken: PATH dir must not provide a `python` alias"
    );

    let repo = test_model_repo();
    let tmp_dir =
        std::env::temp_dir().join(format!("lite-server-py3only-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: 18098\n  metrics_port: {metrics_port}\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: false\norchestration:\n  control_mode: explicit\n  load_models: [test_model]\n  models:\n    - name: test_model\n      load_policy: all\nmodel_repository:\n  path: {}\n",
            repo.to_string_lossy()
        ),
    )
    .unwrap();

    let mut cmd = Command::new(lite_server_bin());
    cmd.arg("serve")
        .arg("--config")
        .arg(&server_yaml)
        .current_dir(project_root())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .env("LITESERVER_DIE_WITH_PARENT", "1")
        .env("PATH", &bin_dir)
        // Worker imports resolve via the crate's python dir, exactly like
        // find_python_module_path sets PYTHONPATH in production.
        .env("PYTHONPATH", project_root().join("python"));
    #[cfg(unix)]
    unsafe {
        cmd.pre_exec(|| {
            libc::setpgid(0, 0);
            Ok(())
        });
    }
    let child = cmd.spawn().expect("failed to spawn server");
    let _guard = ServerGuard(Some(child));

    let base = format!("http://127.0.0.1:{http_port}");
    wait_for_server(http_port, 60).await;
    assert!(
        wait_model_ready(&base, MODEL, 20).await,
        "model must become ready with only `python3` on PATH — the worker \
         spawn must not hardcode the `python` alias (Debian/Ubuntu ship no \
         python-is-python3)"
    );

    let _ = std::fs::remove_dir_all(&bin_dir);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// `--log-level warning` — the spelling the benchmark harness uses
/// (benchmarks/scripts/run_liteserver.py) — must not break the server's own
/// log filter. tracing's EnvFilter accepts `warn`, not `warning`: the
/// lite_server directive is dropped with an "ignoring ... error parsing level
/// filter" line and the server's WARN logs go silent.
#[tokio::test]
#[serial]
async fn test_log_level_warning_is_accepted() {
    let http_port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(metrics_port);

    let tmp_dir =
        std::env::temp_dir().join(format!("lite-server-loglvl-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: 18097\n  metrics_port: {metrics_port}\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: false\nmodel_repository:\n  path: {}\n",
            tmp_dir.to_string_lossy()
        ),
    )
    .unwrap();

    let mut cmd = Command::new(lite_server_bin());
    cmd.arg("serve")
        .arg("--config")
        .arg(&server_yaml)
        .arg("--log-level")
        .arg("warning")
        .current_dir(project_root())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .env("LITESERVER_DIE_WITH_PARENT", "1");
    #[cfg(unix)]
    unsafe {
        cmd.pre_exec(|| {
            libc::setpgid(0, 0);
            Ok(())
        });
    }
    let mut child = cmd.spawn().expect("failed to spawn server");

    // The filter parse happens at logging init, well inside this window.
    sleep(Duration::from_secs(6)).await;

    let _ = child.kill();
    let _ = child.wait();
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stderr.take() {
        use std::io::Read;
        let _ = pipe.read_to_string(&mut stderr);
    }

    assert!(
        !stderr.contains("error parsing level filter"),
        "`--log-level warning` must be accepted by the server log filter; \
         stderr contained: {}",
        stderr.lines().next().unwrap_or("(empty)")
    );

    let _ = std::fs::remove_dir_all(&tmp_dir);
}

// F4/F6 — Strategy 1 e2e: after a health-check kill + respawn, the worker's
// bound ZMQ PAIR socket is REUSED (respawn does not create a new client).
//
// Decisive assertion routes through `get_zmq_clients` (a custom @route): under
// the pre-fix code the respawn replaces zmq_clients[key][0] with a fresh
// client whose `bind` collides (EEXIST) on the stable endpoint and dies, so
// the custom route errors; under Strategy 1 the slot keeps the original
// client whose bound socket accepts the new worker's reconnect, so the route
// succeeds. Unary /infer is routed via the inference_queue snapshot and would
// pass under both codes (the snapshot's client stays bound), so it is only a
// sanity check here — NOT decisive on its own.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_worker_respawn_after_health_check_kill_reuses_client() {
    let http_port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(metrics_port);
    let repo = test_model_repo();

    let child = start_server(&[
        "--port", &http_port.to_string(),
        "--metrics-port", &metrics_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc",
        "--log-level", "warn",
        // Fast health-check cadence so a frozen worker is killed within
        // ~1s — mirrors the in-crate kill-escalation test
        // (inference_queue.rs:2961).
        "--health-check-interval", "0.5",
        "--health-check-timeout", "0.1",
        "--health-check-kill-threshold", "2",
        "--ejection-error-threshold", "1",
        "--ejection-max-percent", "100",
    ]);
    let server_pid = child.id() as i32;
    let _guard = ServerGuard(Some(child));

    let base = format!("http://127.0.0.1:{http_port}");
    wait_for_server(http_port, 60).await;
    load_model(&base, "route_model", "1").await;

    let client = reqwest::Client::new();

    // Freeze the worker. SIGSTOP (not SIGKILL) keeps the process alive so the
    // server's monitor does NOT observe a natural exit — the health-check
    // probe path must drive the kill + respawn via RespawnSignal, which is the
    // exact path under test.
    let original_pid = wait_for_worker_pid(server_pid, "route_model", 10)
        .await
        .expect("route_model worker must spawn");
    let _ = unsafe { libc::kill(original_pid, libc::SIGSTOP) };

    // Wait for the server to kill the frozen worker and respawn a replacement:
    // the respawns counter bumps (after mark_ready) and the original pid dies.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let body = match client
            .get(format!("http://127.0.0.1:{metrics_port}/metrics"))
            .send().await
        {
            Ok(r) => r.text().await.unwrap_or_default(),
            Err(_) => String::new(),
        };
        let respawned = body.contains("liteserver_worker_respawns_total")
            && body.contains("reason=\"health_check\"");
        let gone = unsafe { libc::kill(original_pid, 0) } != 0;
        if respawned && gone {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "health-check kill+respawn did not occur within 30s \
                 (respawned={respawned}, original_pid_gone={gone})"
            );
        }
        sleep(Duration::from_millis(200)).await;
    }

    // A replacement worker with a different pid must come up.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let new_pid = loop {
        if let Some(p) = wait_for_worker_pid(server_pid, "route_model", 1).await {
            if p != original_pid {
                break p;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("replacement route_model worker pid did not appear");
        }
        sleep(Duration::from_millis(200)).await;
    };
    assert_ne!(new_pid, original_pid);
    // Let the reused client + new connection settle.
    sleep(Duration::from_millis(300)).await;

    // DECISIVE — custom route dispatches via get_zmq_clients (the zmq_clients
    // map), not the inference_queue snapshot:
    //   pre-fix   → slot holds the colliding/dead replacement client → non-200
    //   Strategy 1 → slot holds the reused original client → 200
    let resp = client
        .get(format!("{}/v2/models/route_model/status", base))
        .timeout(Duration::from_secs(5))
        .send().await
        .expect("status route request failed");
    assert_eq!(
        resp.status(),
        200,
        "custom route (get_zmq_clients path) failed after respawn — \
         the worker client was not reused"
    );
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["model_loaded"], true);

    // SANITY — unary inference (inference_queue snapshot path).
    let resp = client
        .post(format!("{}/v2/models/route_model/infer", base))
        .json(&json!({"input": 5}))
        .timeout(Duration::from_secs(5))
        .send().await
        .expect("infer request failed");
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["output"], 10);
}

// ---------------------------------------------------------------------------
// P1: unary binary passthrough (media_type non-JSON → data verbatim)
// ---------------------------------------------------------------------------

/// A model that declares media_type=application/octet-stream and returns raw
/// bytes must have those bytes forwarded verbatim. Before P1 the unary Ok
/// path JSON-parsed every response and collapsed non-JSON payloads to `{}`.
#[tokio::test]
#[serial]
async fn test_unary_binary_media_type_passthrough() {
    let base = shared_base().await;
    let client = reqwest::Client::new();
    load_model(&base, "bin_model", "1").await;

    let resp = client
        .post(format!("{}/v2/models/bin_model/infer", base))
        .json(&json!({"input": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let ct = resp
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap_or("").to_string())
        .unwrap_or_default();
    assert!(
        ct.starts_with("application/octet-stream"),
        "content-type should be application/octet-stream, got {ct:?}"
    );

    let body = resp.bytes().await.unwrap();
    let expected: Vec<u8> = (0u8..=255).collect();
    assert_eq!(
        body.as_ref(),
        expected.as_slice(),
        "binary payload must arrive byte-identical (0x00..0xFF)"
    );
}

/// P1 guard: the JSON path (media_type empty) must stay byte-identical,
/// including serde_json's sorted-key re-encode. Pins ord_model's response so
/// the passthrough change cannot silently alter the JSON path.
#[tokio::test]
#[serial]
async fn test_unary_json_response_byte_identical() {
    let base = shared_base().await;
    let client = reqwest::Client::new();
    load_model(&base, "ord_model", "1").await;

    let resp = client
        .post(format!("{}/v2/models/ord_model/infer", base))
        .json(&json!({"input": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    assert_eq!(text, r#"{"a":2,"z":1}"#, "JSON path must stay byte-identical");
}

// ---------------------------------------------------------------------------
// tensor/bytes request (0.8.3) — Content-Type dispatch, error semantics
// ---------------------------------------------------------------------------

/// End-to-end: POST raw bytes (application/octet-stream) → worker receives
/// raw bytes → echoes length back. Proves the Content-Type dispatch chain
/// (ApiBody → do_infer → ZMQ → Python worker) is byte-native end-to-end.
#[tokio::test]
#[serial]
async fn test_unary_raw_bytes_request_passthrough() {
    let base = shared_base().await;
    let client = reqwest::Client::new();
    load_model(&base, "raw_echo_model", "1").await;

    let raw_body: Vec<u8> = (0u8..=255).collect();
    let resp = client
        .post(format!("{}/v2/models/raw_echo_model/infer", base))
        .header("content-type", "application/octet-stream")
        .body(raw_body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "raw bytes request must succeed");

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["raw_len"], 256,
        "worker must receive all 256 raw bytes unchanged");
    unload_model(&base, "raw_echo_model", "1").await;
}

/// Backward compat: missing Content-Type → default JSON. Uses the same
/// raw_echo_model but omits the content-type header.
#[tokio::test]
#[serial]
async fn test_unary_missing_content_type_defaults_json() {
    let base = shared_base().await;
    let client = reqwest::Client::new();
    load_model(&base, "raw_echo_model", "1").await;

    let resp = client
        .post(format!("{}/v2/models/raw_echo_model/infer", base))
        .json(&json!({"input": 42}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "JSON request without content-type must succeed");
    unload_model(&base, "raw_echo_model", "1").await;
}

/// D6: Content-Encoding on any inference route → 415.
#[tokio::test]
#[serial]
async fn test_unary_415_content_encoding() {
    let base = shared_base().await;
    let client = reqwest::Client::new();
    load_model(&base, "raw_echo_model", "1").await;

    let resp = client
        .post(format!("{}/v2/models/raw_echo_model/infer", base))
        .header("content-type", "application/json")
        .header("content-encoding", "gzip")
        .body(r#"{"input": 1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 415, "Content-Encoding must return 415");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "unsupported_media_type");
    assert!(body["error"]["message"].as_str().unwrap().contains("Content-Encoding"));
    unload_model(&base, "raw_echo_model", "1").await;
}

/// 8 MB JSON payload (> old 2 MB default) → 200 with new 64 MiB default.
#[tokio::test]
#[serial]
async fn test_unary_large_json_payload_8mb() {
    let base = shared_base().await;
    let client = reqwest::Client::new();
    load_model(&base, "raw_echo_model", "1").await;

    // Build an 8 MB JSON object.
    let mut large = String::from(r#"{"data":["#);
    // Each entry is ~8 bytes: "X,". We need ~1M entries for 8 MB.
    let entries = 1_000_000usize;
    large.reserve(entries * 8 + 10);
    for i in 0..entries {
        if i > 0 {
            large.push(',');
        }
        large.push_str(&i.to_string());
    }
    large.push_str("]}");

    let resp = client
        .post(format!("{}/v2/models/raw_echo_model/infer", base))
        .header("content-type", "application/json")
        .body(large)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200,
        "8 MB JSON must succeed with new 64 MiB default body limit");
    unload_model(&base, "raw_echo_model", "1").await;
}

/// D11: metrics endpoint must expose `lite_server_http_request_body_bytes`
/// with `content_type="json"` and `content_type="raw"` labels.
#[tokio::test]
#[serial]
async fn test_unary_metrics_body_bytes() {
    let base = shared_base().await;
    let client = reqwest::Client::new();
    load_model(&base, "raw_echo_model", "1").await;

    // Send one JSON request and one raw-bytes request.
    let _ = client
        .post(format!("{}/v2/models/raw_echo_model/infer", base))
        .json(&json!({"input": 1}))
        .send()
        .await
        .unwrap();

    let raw_body: Vec<u8> = (0u8..=255).collect();
    let _ = client
        .post(format!("{}/v2/models/raw_echo_model/infer", base))
        .header("content-type", "application/octet-stream")
        .body(raw_body)
        .send()
        .await
        .unwrap();

    // Scrape metrics endpoint.
    let metrics_resp = client
        .get(format!("{}/metrics", base))
        .send()
        .await
        .unwrap();
    assert_eq!(metrics_resp.status(), 200);
    let metrics_text = metrics_resp.text().await.unwrap();

    // Histogram must be present.
    assert!(
        metrics_text.contains("lite_server_http_request_body_bytes"),
        "metrics must expose lite_server_http_request_body_bytes histogram\n{}",
        metrics_text.lines().take(5).collect::<Vec<_>>().join("\n")
    );
    // Both content_type labels must appear.
    assert!(
        metrics_text.contains(r#"content_type="json""#),
        "metrics must include json label"
    );
    assert!(
        metrics_text.contains(r#"content_type="raw""#),
        "metrics must include raw label"
    );
    unload_model(&base, "raw_echo_model", "1").await;
}

/// §6.3 test_ensemble_accepts_json (P0): HTTP ensemble + JSON walks the DAG
/// (parallel recall_a ∥ recall_b → serial merge; {x:5} → {out:10}). The
/// ensemble branch materializes Value from the validated JSON bytes — the
/// DAG must behave exactly as before the Content-Type dispatch change.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_ensemble_accepts_json() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);

    let repo = std::env::temp_dir()
        .join(format!("lite-server-http-ensemble-ct-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    write_submodel(
        &repo,
        "echo",
        r#"        return {"out": x.get("x", 0) if isinstance(x, dict) else x}"#,
    );
    write_submodel(
        &repo,
        "summer",
        "        a = x.get(\"a\", 0)\n        b = x.get(\"b\", 0)\n        return {\"out\": a + b}",
    );
    let ens_dir = repo.join("ensemble_model/1");
    std::fs::create_dir_all(&ens_dir).unwrap();
    std::fs::write(
        ens_dir.join("config.yaml"),
        r#"
ensemble:
  steps:
    - name: recall_a
      model: echo
      version: "1"
      inputs:
        x: "$request.x"
    - name: recall_b
      model: echo
      version: "1"
      inputs:
        x: "$request.x"
    - name: merge
      model: summer
      version: "1"
      inputs:
        a: "$recall_a.out"
        b: "$recall_b.out"
"#,
    )
    .unwrap();

    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc", "--no-metrics", "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "echo", "1").await;
    load_model(&base, "summer", "1").await;
    load_model(&base, "ensemble_model", "1").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ensemble_model/infer", base))
        .json(&json!({"x": 5}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "ensemble + JSON must succeed");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["out"], 10, "DAG merge must produce out=10, got {body}");

    let _ = std::fs::remove_dir_all(&repo);
}

/// §6.3 test_ensemble_binary_root_field_access_400 (P0 → B3): HTTP ensemble
/// + non-JSON Content-Type is now accepted as binary root input (B3, E6 —
/// D7 relaxed). However, field-level access (`$request.x`) on binary data is
/// rejected because bytes carry no field semantics (E7).
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_ensemble_binary_root_field_access_400() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);

    let repo = std::env::temp_dir()
        .join(format!("lite-server-http-ensemble-raw-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    write_submodel(&repo, "echo", r#"        return {"out": x}"#);
    let ens_dir = repo.join("ensemble_model/1");
    std::fs::create_dir_all(&ens_dir).unwrap();
    std::fs::write(
        ens_dir.join("config.yaml"),
        r#"
ensemble:
  steps:
    - name: only
      model: echo
      version: "1"
      inputs:
        x: "$request.x"
"#,
    )
    .unwrap();

    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc", "--no-metrics", "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "echo", "1").await;
    load_model(&base, "ensemble_model", "1").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ensemble_model/infer", base))
        .header("content-type", "application/octet-stream")
        .body(vec![0u8, 1, 2, 3])
        .send()
        .await
        .unwrap();
    // B3: binary root input is accepted (D7 relaxed), but field access
    // ($request.x) on binary → 400 (E7: no field semantics on bytes).
    assert_eq!(resp.status(), 400, "field access on binary root must be 400");
    let body: Value = resp.json().await.unwrap();
    let msg = body["error"]["message"].as_str().unwrap();
    assert!(
        msg.contains("field"),
        "error must mention field extraction from binary, got: {msg}"
    );

    let _ = std::fs::remove_dir_all(&repo);
}

/// AUDIT (B3 egress, inference.rs): a final-step binary output whose
/// media_type is not a valid HTTP header value must NOT panic the request
/// handler. The unary binary passthrough this egress mirrors
/// (inference.rs:291-302) builds the response with
/// `.body(...).map_err(AppError::Internal)`; the B3 ensemble egress calls
/// `.unwrap()` on the same builder, so a worker-controlled media_type
/// containing CR/LF turns into a handler panic and the connection drops
/// without any response. Correct behavior (unary parity): a 500 response.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_audit_ensemble_binary_egress_invalid_media_type() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);

    let repo = std::env::temp_dir()
        .join(format!("lite-server-http-ensemble-badmt-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    // Submodel declares a non-JSON media_type containing CR/LF (invalid in an
    // HTTP header value). Content is raw bytes, so the step output becomes
    // EnsembleValue::Binary and reaches the B3 HTTP egress.
    let dir = repo.join("badmt/1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("model.py"),
        r#"from lite_server import LitAPI
from lite_server.response import Response


class BadMtAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request

    def predict(self, x):
        return b"payload"

    def encode_response(self, output):
        return Response(content=output, media_type="text/plain\r\nX-Injected: 1")
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
    let ens_dir = repo.join("ensemble_model/1");
    std::fs::create_dir_all(&ens_dir).unwrap();
    std::fs::write(
        ens_dir.join("config.yaml"),
        r#"
ensemble:
  steps:
    - name: only
      model: badmt
      version: "1"
      inputs:
        x: "$request.x"
"#,
    )
    .unwrap();

    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc", "--no-metrics", "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "badmt", "1").await;
    load_model(&base, "ensemble_model", "1").await;

    let client = reqwest::Client::new();
    let result = client
        .post(format!("{}/v2/models/ensemble_model/infer", base))
        .json(&json!({"x": 1}))
        .send()
        .await;
    let resp = result.expect(
        "handler must not panic on invalid media_type — a response must come back",
    );
    assert_eq!(
        resp.status(),
        500,
        "invalid media_type must map to 500 (unary passthrough parity), got {}",
        resp.status()
    );

    let _ = std::fs::remove_dir_all(&repo);
}

/// B3 §7.3 (Option A full chain): image bytes → ensemble root (Binary) →
/// first layer receives raw bytes → JSON features → final layer → JSON out.
/// Pins root→first-layer binary passthrough AND mid-DAG Json flow together.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_ensemble_binary_root_to_json_chain() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);

    let repo = std::env::temp_dir()
        .join(format!("lite-server-ensemble-binchain-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    // vision: reports whether it received raw bytes and how many.
    write_submodel(
        &repo,
        "vision",
        "        return {\"features\": [len(x)] if isinstance(x, (bytes, bytearray)) else [], \"was_bytes\": isinstance(x, (bytes, bytearray))}",
    );
    // classifier: consumes the JSON features.
    write_submodel(
        &repo,
        "classifier",
        "        return {\"label\": \"cat\", \"wb\": x.get(\"wb\"), \"n\": len(x.get(\"feats\", []))}",
    );
    let ens_dir = repo.join("ensemble_model/1");
    std::fs::create_dir_all(&ens_dir).unwrap();
    std::fs::write(
        ens_dir.join("config.yaml"),
        r#"
ensemble:
  steps:
    - name: vision
      model: vision
      version: "1"
      inputs:
        img: "$request"
    - name: classifier
      model: classifier
      version: "1"
      inputs:
        feats: "$vision.features"
        wb: "$vision.was_bytes"
"#,
    )
    .unwrap();

    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc", "--no-metrics", "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "vision", "1").await;
    load_model(&base, "classifier", "1").await;
    load_model(&base, "ensemble_model", "1").await;

    let image: Vec<u8> = (0u8..=255).collect();
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ensemble_model/infer", base))
        .header("content-type", "application/octet-stream")
        .body(image.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "binary root chain must succeed");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["label"], "cat");
    assert_eq!(
        body["wb"], true,
        "first layer must have received raw bytes, got: {body}"
    );
    assert_eq!(body["n"], 1, "features list must flow through, got: {body}");

    let _ = std::fs::remove_dir_all(&repo);
}

/// Write a sub-model whose encode_response declares a non-JSON media_type
/// and returns raw bytes 0x00..=0xFF (ensemble step output becomes Binary).
#[cfg(unix)]
fn write_binary_submodel(repo: &std::path::Path, name: &str) {
    let dir = repo.join(format!("{}/1", name));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("model.py"),
        r#"from lite_server import LitAPI
from lite_server.response import Response


class BinOutAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request

    def predict(self, x):
        return bytes(range(256))

    def encode_response(self, output):
        return Response(content=output, media_type="application/octet-stream")
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
}

/// B3 §7.3 (E7 boundary): a step's binary output referenced by a downstream
/// step → 400 (binary must not flow between internal DAG steps; Option A
/// scope). Regression pin for the Option-B slope.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_ensemble_step_binary_output_referenced_downstream_400() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);

    let repo = std::env::temp_dir()
        .join(format!("lite-server-ensemble-stepbin-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    write_binary_submodel(&repo, "binout");
    write_submodel(&repo, "echo", r#"        return {"out": x}"#);
    let ens_dir = repo.join("ensemble_model/1");
    std::fs::create_dir_all(&ens_dir).unwrap();
    std::fs::write(
        ens_dir.join("config.yaml"),
        r#"
ensemble:
  steps:
    - name: produce
      model: binout
      version: "1"
      inputs:
        x: "$request.x"
    - name: consume
      model: echo
      version: "1"
      inputs:
        x: "$produce"
"#,
    )
    .unwrap();

    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc", "--no-metrics", "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "binout", "1").await;
    load_model(&base, "echo", "1").await;
    load_model(&base, "ensemble_model", "1").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ensemble_model/infer", base))
        .json(&json!({"x": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "referencing a binary step output downstream must be 400, got {}",
        resp.status()
    );
    let body: Value = resp.json().await.unwrap();
    let msg = body["error"]["message"].as_str().unwrap();
    assert!(
        msg.contains("binary step output"),
        "error must name the binary-flow boundary, got: {msg}"
    );

    let _ = std::fs::remove_dir_all(&repo);
}

/// B3 §7.3: final-step binary output → HTTP response passes the bytes and
/// content-type through verbatim (mirrors the unary passthrough).
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_ensemble_final_binary_output_http_passthrough() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);

    let repo = std::env::temp_dir()
        .join(format!("lite-server-ensemble-finalbin-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    write_binary_submodel(&repo, "binout");
    let ens_dir = repo.join("ensemble_model/1");
    std::fs::create_dir_all(&ens_dir).unwrap();
    std::fs::write(
        ens_dir.join("config.yaml"),
        r#"
ensemble:
  steps:
    - name: only
      model: binout
      version: "1"
      inputs:
        x: "$request.x"
"#,
    )
    .unwrap();

    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc", "--no-metrics", "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "binout", "1").await;
    load_model(&base, "ensemble_model", "1").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/ensemble_model/infer", base))
        .json(&json!({"x": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap_or("").to_string())
        .unwrap_or_default();
    assert!(
        ct.starts_with("application/octet-stream"),
        "final binary output must pass its content-type through, got {ct:?}"
    );
    let body = resp.bytes().await.unwrap();
    let expected: Vec<u8> = (0u8..=255).collect();
    assert_eq!(
        body.as_ref(),
        expected.as_slice(),
        "final binary output must arrive byte-identical"
    );

    let _ = std::fs::remove_dir_all(&repo);
}

/// B3 §7.3 (infer.rs:96 regression pin): gRPC ensemble with malformed-JSON
/// `data` falls back to Binary (opaque bytes reach the first layer) instead
/// of the old silent `Value::Null` swallow.
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_grpc_ensemble_malformed_json_becomes_binary() {
    use std::collections::HashMap;
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);

    let repo = std::env::temp_dir()
        .join(format!("lite-server-grpc-ensemble-bin-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    // probe: reports whether it received raw bytes and how many.
    write_submodel(
        &repo,
        "probe",
        "        return {\"was_bytes\": isinstance(x, (bytes, bytearray)), \"len\": len(x) if isinstance(x, (bytes, bytearray)) else -1}",
    );
    let ens_dir = repo.join("ensemble_model/1");
    std::fs::create_dir_all(&ens_dir).unwrap();
    std::fs::write(
        ens_dir.join("config.yaml"),
        r#"
ensemble:
  steps:
    - name: only
      model: probe
      version: "1"
      inputs:
        data: "$request"
"#,
    )
    .unwrap();

    let tmp_dir = std::env::temp_dir()
        .join(format!("lite-server-grpc-ensemble-bin-yaml-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18353\n  timeout: 30.0\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();

    let _guard = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 30).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "probe", "1").await;
    load_model(&base, "ensemble_model", "1").await;

    let payload = b"\xff\xfe not json";
    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);
    let resp = client
        .infer(InferRequest {
            model_name: "ensemble_model".to_string(),
            version: "1".to_string(),
            data: bytes::Bytes::from_static(payload),
            headers: HashMap::new(),
            ..Default::default()
        })
        .await
        .expect("malformed JSON gRPC ensemble input must not error")
        .into_inner();

    let got: Value = serde_json::from_slice(&resp.data).expect("response must be JSON");
    assert_eq!(
        got["was_bytes"], true,
        "malformed JSON must reach the first layer as raw bytes, got: {got}"
    );
    assert_eq!(
        got["len"].as_u64(),
        Some(payload.len() as u64),
        "all raw bytes must arrive, got: {got}"
    );

    let _ = std::fs::remove_dir_all(&repo);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// §6.3 test_sse_raw_bytes_input (P0): SSE + application/octet-stream →
/// the worker receives raw bytes (D3 zero-round-trip applies to the SSE
/// path too — audit B confirmed sse_infer_impl never reads JSON fields).
/// (§6.3 test_sse_json_input is already covered by test_sse_streaming.)
#[tokio::test]
#[serial]
async fn test_sse_raw_bytes_input() {
    let base = shared_base().await;
    let client = reqwest::Client::new();
    load_model(&base, "raw_echo_model", "1").await;

    let raw_body: Vec<u8> = (0u8..=255).collect();
    let resp = client
        .post(format!("{}/v2/models/raw_echo_model/events", base))
        .header("content-type", "application/octet-stream")
        .body(raw_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "SSE raw bytes request must succeed");
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(ct.contains("text/event-stream"), "expected SSE, got: {ct}");

    let body = tokio::time::timeout(Duration::from_secs(15), resp.text())
        .await
        .expect("SSE body did not close within 15s")
        .unwrap();
    assert!(
        body.contains("\"raw_len\": 256") || body.contains("\"raw_len\":256"),
        "worker must have received all 256 raw bytes; SSE body: {body}"
    );
    unload_model(&base, "raw_echo_model", "1").await;
}

/// §6.3 test_unary_413_body_too_large (P0 status assertion; P1 structured
/// fields): a body over server.max_request_body_bytes → 413 with max_size,
/// and actual_size when Content-Length is known (B3 fix, e2e lock).
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_unary_413_body_too_large() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);

    let tmp_dir = std::env::temp_dir()
        .join(format!("lite-server-413-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: 0\n  metrics_port: 0\n  log_level: warn\n  max_request_body_bytes: 16\nmetrics:\n  enabled: false\ngrpc:\n  enabled: false\nmodel_repository:\n  path: {}\n",
            tmp_dir.to_string_lossy()
        ),
    )
    .unwrap();
    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 60).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{}/v2/models/whatever/infer", http_port))
        .header("content-type", "application/octet-stream")
        .body(vec![b'x'; 64])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413, "body over the 16-byte limit must be 413");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "payload_too_large");
    assert_eq!(body["error"]["max_size"], 16);
    assert_eq!(
        body["error"]["actual_size"], 64,
        "actual_size must be reported when Content-Length is known"
    );

    let _ = std::fs::remove_dir_all(&tmp_dir);
}

// ---------------------------------------------------------------------------
// Triton Binary Tensor Data Extension(阶段 1,批次 1)
// ---------------------------------------------------------------------------

/// kserve-master `test_infer_type.py:262` wire 夹具移植(与
/// src/http/handlers/triton_binary_tests.rs 同一常量):546B JSON 头 +
/// 14B 二进制尾,Σ binary_data_size = 8 + 6 = 14。
const KSERVE_E2E_HEAD: &str = r#"{"id":"4be4e82f-5500-420a-a5c5-ac86841e271b","model_name":"test_model","inputs":[{"name":"input1","shape":[3],"datatype":"INT32","parameters":{"test-str":"dummy"},"data":[1,2,3]},{"name":"input2","shape":[1],"datatype":"BYTES","parameters":{"test-int":2,"binary_data_size":8}},{"name":"input3","shape":[3],"datatype":"FP16","parameters":{"binary_data_size":6}}],"outputs":[{"name":"output-0","parameters":{"test-str":"dummy","test-bool":true,"test-int":100}},{"name":"output-1","parameters":{"test-str":"dummy","test-bool":true,"test-int":100}}]}"#;
const KSERVE_E2E_TAIL: &[u8] = b"\x04\x00\x00\x00test\xcd<f@fB";

/// 阶段 1:暴露 ctx.binary_data 切分结果的 worker(Python 侧切分验收)。
fn write_triton_binary_split_model(repo: &std::path::Path, name: &str) {
    let dir = repo.join(format!("{name}/1"));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("model.py"),
        r#"from lite_server import LitAPI


class TritonSplitAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request, ctx):
        return {"binary_data": {k: list(v) for k, v in (ctx.binary_data or {}).items()}}

    def predict(self, x):
        return x

    def encode_response(self, output):
        return output
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
}

/// 阶段 1:按声明顺序拼接二进制尾并直通(byte-identical 验收,§7 验收 1)。
/// worker 契约(D4)下 ctx.request = 解析后的 JSON 头 dict,头部字节保真由
/// extractor 单测 /echo-bytes 锁定;本 worker 锁定**二进制尾**端到端逐字节。
fn write_triton_binary_echo_model(repo: &std::path::Path, name: &str) {
    let dir = repo.join(format!("{name}/1"));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("model.py"),
        r#"from lite_server import LitAPI
from lite_server.response import Response


class TritonEchoAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request, ctx):
        return ctx.binary_data  # {name: memoryview},dict 保序 = 声明顺序

    def predict(self, x):
        return b"".join(bytes(v) for v in x.values())

    def encode_response(self, output):
        return Response(content=output, media_type="application/octet-stream")
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
}

/// §6.1 test_unary_triton_binary_e2e (P0):多 tensor 二进制请求 → worker
/// 侧 ctx.binary_data 按声明顺序切分正确(JSON 头 dict 进 ctx.request)。
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_unary_triton_binary_e2e() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let repo = std::env::temp_dir()
        .join(format!("lite-server-triton-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    write_triton_binary_split_model(&repo, "triton_split");

    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc", "--no-metrics", "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{http_port}");
    load_model(&base, "triton_split", "1").await;

    let mut body = KSERVE_E2E_HEAD.as_bytes().to_vec();
    body.extend_from_slice(KSERVE_E2E_TAIL);
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/v2/models/triton_split/infer"))
        .header("content-type", "application/octet-stream")
        .header("inference-header-content-length", "546")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "e2e 多 tensor 二进制必须成功");
    let v: Value = resp.json().await.unwrap();
    assert_eq!(
        v["binary_data"]["input2"],
        json!([4, 0, 0, 0, 116, 101, 115, 116]),
        "input2 = 4B 长度前缀 + \"test\""
    );
    assert_eq!(
        v["binary_data"]["input3"],
        json!([205, 60, 102, 64, 102, 66]),
        "input3 = 3×FP16 LE"
    );
    assert!(v["binary_data"].get("input1").is_none(), "JSON data 的 input 不进 binary_data");

    let _ = std::fs::remove_dir_all(&repo);
}

/// §6.1 test_unary_triton_binary_byte_identical (P0):端到端字节不变——
/// 二进制尾按声明顺序拼接后逐字节等于原 wire tail(头部字节保真由
/// extractor 单测 /echo-bytes 锁定)。
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_unary_triton_binary_byte_identical() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let repo = std::env::temp_dir()
        .join(format!("lite-server-triton-bytes-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    write_triton_binary_echo_model(&repo, "triton_echo");

    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc", "--no-metrics", "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{http_port}");
    load_model(&base, "triton_echo", "1").await;

    let mut body = KSERVE_E2E_HEAD.as_bytes().to_vec();
    body.extend_from_slice(KSERVE_E2E_TAIL);
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/v2/models/triton_echo/infer"))
        .header("content-type", "application/octet-stream")
        .header("inference-header-content-length", "546")
        .body(body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let echoed = resp.bytes().await.unwrap();
    assert_eq!(
        echoed.as_ref(),
        KSERVE_E2E_TAIL,
        "二进制尾(input2=8B + input3=6B,声明顺序)端到端逐字节不变"
    );

    let _ = std::fs::remove_dir_all(&repo);
}

/// §6.1 test_unary_triton_binary_metrics (P1):content_type="triton_binary"
/// count +1。
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_unary_triton_binary_metrics() {
    let http_port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(metrics_port);
    let repo = std::env::temp_dir()
        .join(format!("lite-server-triton-metrics-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    write_triton_binary_echo_model(&repo, "triton_echo");

    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--metrics-port", &metrics_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc", "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{http_port}");
    load_model(&base, "triton_echo", "1").await;

    let mut body = KSERVE_E2E_HEAD.as_bytes().to_vec();
    body.extend_from_slice(KSERVE_E2E_TAIL);
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/v2/models/triton_echo/infer"))
        .header("content-type", "application/octet-stream")
        .header("inference-header-content-length", "546")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let scrape = client
        .get(format!("http://127.0.0.1:{metrics_port}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let count = body_bytes_count(&scrape, "triton_binary");
    assert_eq!(count, 1, "triton_binary count 必须 +1,metrics:\n{scrape}");

    let _ = std::fs::remove_dir_all(&repo);
}

/// §6.1 test_unary_triton_binary_400_mismatch (P1):Σ 不匹配 e2e → 400 +
/// 结构化错误体。
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_unary_triton_binary_400_mismatch() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let repo = std::env::temp_dir()
        .join(format!("lite-server-triton-400-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    write_triton_binary_echo_model(&repo, "triton_echo");

    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc", "--no-metrics", "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{http_port}");
    load_model(&base, "triton_echo", "1").await;

    // Σ = 14,但 tail 只给 10 字节
    let mut body = KSERVE_E2E_HEAD.as_bytes().to_vec();
    body.extend_from_slice(&[0u8; 10]);
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/v2/models/triton_echo/infer"))
        .header("content-type", "application/octet-stream")
        .header("inference-header-content-length", "546")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "Σ 不匹配必须 400");
    let v: Value = resp.json().await.unwrap();
    assert!(
        v["error"].is_string(),
        "Triton 客户端(IHCL header → T1 Kserve)的错误体必须扁平(C9,批次 2): {v}"
    );

    let _ = std::fs::remove_dir_all(&repo);
}

/// §6.6 test_kserve_flat_worker_error_e2e:信封 JSON + worker 错误 → 扁平
/// 错误体(经协议层分派)。
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_kserve_flat_worker_error_e2e() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let repo = std::env::temp_dir()
        .join(format!("lite-server-kserve-flat-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    let dir = repo.join("boom/1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("model.py"),
        r#"from lite_server import LitAPI
from lite_server.exceptions import BadRequestError


class BoomAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request

    def predict(self, x):
        raise BadRequestError("invalid input value")
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();

    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc", "--no-metrics", "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{http_port}");
    load_model(&base, "boom", "1").await;

    let client = reqwest::Client::new();
    // 信封请求(T2 命中)→ Kserve 模式 → worker 错误扁平
    let resp = client
        .post(format!("{base}/v2/models/boom/infer"))
        .json(&json!({"inputs": [{"name": "a", "shape": [1], "datatype": "FP32", "data": [1.0]}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "worker 错误必须 400");
    let v: Value = resp.json().await.unwrap();
    assert!(v["error"].is_string(), "信封请求的错误体必须扁平,got: {v}");

    // 非信封自由 JSON(同 worker)→ OpenAI 形状(回归)
    let resp = client
        .post(format!("{base}/v2/models/boom/infer"))
        .json(&json!({"prompt": "hello"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let v: Value = resp.json().await.unwrap();
    assert!(v["error"].is_object(), "非信封请求保持 OpenAI 形状,got: {v}");

    let _ = std::fs::remove_dir_all(&repo);
}

/// §6.2 test_binary_data_output_e2e:请求带 binary_data_output flag →
/// 响应 JSON 头 + 二进制尾 + Inference-Header-Content-Length;客户端重组
/// 数值一致。
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_binary_data_output_e2e() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let repo = std::env::temp_dir()
        .join(format!("lite-server-bdo-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    // worker 返回 KServe 信封(FP32 输出)
    let dir = repo.join("env_model/1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("model.py"),
        r#"from lite_server import LitAPI


class EnvAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request

    def predict(self, x):
        return {"outputs": [{"name": "out", "shape": [2], "datatype": "FP32", "data": [1.0, 2.0]}]}

    def encode_response(self, output):
        return output
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();

    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc", "--no-metrics", "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{http_port}");
    load_model(&base, "env_model", "1").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/v2/models/env_model/infer"))
        .json(&json!({
            "id": "r1",
            "inputs": [{"name": "a", "shape": [1], "datatype": "FP32", "data": [1.0]}],
            "parameters": {"binary_data_output": true},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "application/octet-stream"
    );
    let head_len: usize = resp
        .headers()
        .get("inference-header-content-length")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len() - head_len, 8, "二进制尾 = 2 × FP32(4B)");
    // 客户端重组:JSON 头 + 二进制尾 → 数值一致
    let head: Value = serde_json::from_slice(&body[..head_len]).unwrap();
    assert_eq!(head["id"], "r1", "id 回显");
    assert_eq!(head["outputs"][0]["parameters"]["binary_data_size"], 8);
    let mut vals = Vec::new();
    for chunk in body[head_len..].chunks_exact(4) {
        let mut b = [0u8; 4];
        b.copy_from_slice(chunk);
        vals.push(f32::from_le_bytes(b));
    }
    assert_eq!(vals, vec![1.0, 2.0], "客户端重组数值一致");

    // 无 flag → 既有 passthrough(JSON 信封原样)
    let resp = client
        .post(format!("{base}/v2/models/env_model/infer"))
        .json(&json!({
            "inputs": [{"name": "a", "shape": [1], "datatype": "FP32", "data": [1.0]}],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(resp.headers().get("content-type").unwrap().to_str().unwrap().contains("json"));

    let _ = std::fs::remove_dir_all(&repo);
}

/// §6.1 test_triton_binary_ensemble_rejects (P0,C4):ensemble + TritonBinary
/// → 400,信息明确(容器格式须等 §9.6 Option B)。
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_triton_binary_ensemble_rejects() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let repo = std::env::temp_dir()
        .join(format!("lite-server-triton-ensemble-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    write_submodel(&repo, "echo", r#"        return {"out": x}"#);
    let ens_dir = repo.join("ensemble_model/1");
    std::fs::create_dir_all(&ens_dir).unwrap();
    std::fs::write(
        ens_dir.join("config.yaml"),
        r#"
ensemble:
  steps:
    - name: only
      model: echo
      version: "1"
      inputs:
        x: "$request.x"
"#,
    )
    .unwrap();

    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc", "--no-metrics", "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{http_port}");
    load_model(&base, "echo", "1").await;
    load_model(&base, "ensemble_model", "1").await;

    let mut body = KSERVE_E2E_HEAD.as_bytes().to_vec();
    body.extend_from_slice(KSERVE_E2E_TAIL);
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/v2/models/ensemble_model/infer"))
        .header("content-type", "application/octet-stream")
        .header("inference-header-content-length", "546")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "ensemble 必须显式拒绝 TritonBinary(C4)");
    let v: Value = resp.json().await.unwrap();
    // C9(批次 2):Triton 客户端(IHCL → T1 Kserve)错误体扁平
    let msg = v["error"].as_str().unwrap_or_default();
    assert!(
        msg.contains("Triton Binary"),
        "拒绝信息必须明确容器格式,got: {msg}"
    );

    let _ = std::fs::remove_dir_all(&repo);
}

/// 从 /metrics 抓取 `lite_server_http_request_body_bytes_count{content_type=...}`
/// 的计数值;系列缺失时返回 0。
fn body_bytes_count(body: &str, content_type: &str) -> u64 {
    let needle = format!("content_type=\"{content_type}\"");
    for line in body.lines() {
        if line.starts_with("lite_server_http_request_body_bytes_count{")
            && line.contains(&needle)
        {
            return line
                .rsplit_once(' ')
                .map(|(_, v)| v.trim().parse().unwrap_or(0))
                .unwrap_or(0);
        }
    }
    0
}

// ---------------------------------------------------------------------------
// KServe V2 管理面(阶段 3,批次 3):/v2 元数据 / health / 模型元数据 / bare load
// ---------------------------------------------------------------------------

/// §6.3 test_v2_server_metadata (P0):/v2 → name/version/extensions 含
/// binary_tensor_data。
#[tokio::test]
#[serial]
async fn test_v2_server_metadata() {
    let base = shared_base().await;
    let client = reqwest::Client::new();
    let resp = client.get(format!("{base}/v2")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["name"], "lite-server");
    assert!(v["version"].as_str().is_some_and(|s| !s.is_empty()));
    let exts = v["extensions"].as_array().unwrap();
    assert!(
        exts.iter().any(|e| e == "binary_tensor_data"),
        "extensions 必须声明 binary_tensor_data: {v}"
    );
}

async fn fetch_json(
    client: &reqwest::Client,
    base: &str,
    path: &str,
) -> (reqwest::StatusCode, Value) {
    let resp = client.get(format!("{base}{path}")).send().await.unwrap();
    (resp.status(), resp.json().await.unwrap())
}

/// §6.3 test_v2_health_paths (P0):/v2/health/live、/v2/health/ready 与
/// /livez /readyz **语义一致**(同一 handler 别名路由)——livez 恒 200;
/// readyz 的 status 与 body 与 /readyz 逐项相同(无模型加载时同为 503)。
#[tokio::test]
#[serial]
async fn test_v2_health_paths() {
    let base = shared_base().await;
    let client = reqwest::Client::new();
    for path in ["/livez", "/v2/health/live"] {
        let resp = client.get(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(resp.status(), 200, "{path} must be 200");
        let v: Value = resp.json().await.unwrap();
        assert_eq!(v["status"], "alive", "{path} semantics == livez");
    }
    // 别名语义一致:状态码与 body 均相同(共享同一 handler)
    let (s1, b1) = fetch_json(&client, &base, "/readyz").await;
    let (s2, b2) = fetch_json(&client, &base, "/v2/health/ready").await;
    assert_eq!(s1, s2, "/readyz 与 /v2/health/ready 状态码必须一致");
    assert_eq!(b1, b2, "/readyz 与 /v2/health/ready body 必须一致");
}

/// §6.3 test_v2_model_metadata_spec_shape + versioned (P0/P1):规范形状
/// name/platform/inputs/outputs 必填,无 state 字段。
#[tokio::test]
#[serial]
async fn test_v2_model_metadata_spec_shape() {
    let base = shared_base().await;
    load_model(&base, MODEL, "1").await;
    let client = reqwest::Client::new();

    for path in [
        format!("/v2/models/{MODEL}"),
        format!("/v2/models/{MODEL}/versions/1"),
    ] {
        let resp = client.get(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(resp.status(), 200, "{path} must be 200");
        let v: Value = resp.json().await.unwrap();
        assert_eq!(v["name"], MODEL);
        assert!(
            v["platform"].as_str().is_some_and(|p| !p.is_empty()),
            "platform 必填(J1 兜底 custom 不空串): {v}"
        );
        assert!(v["inputs"].is_array(), "inputs 必填(可空数组): {v}");
        assert!(v["outputs"].is_array(), "outputs 必填(可空数组): {v}");
        assert!(
            v.get("state").is_none(),
            "state 不是 KServe 规范字段(C2),不得返回: {v}"
        );
    }
    // 不存在的模型 → 404
    let resp = client
        .get(format!("{base}/v2/models/definitely-not-a-model"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    unload_model(&base, MODEL, "1").await;
}

/// §6.3 test_v2_bare_load_alias (P0,G14):bare load → 加载 active 版本,
/// 幂等 200;无 active → 明确错误;响应 KServe 形状 {"name","load":true}(J2)。
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_v2_bare_load_alias() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let repo = test_model_repo();
    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc", "--no-metrics", "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{http_port}");
    let client = reqwest::Client::new();

    // 无 active → 明确错误(404)
    let resp = client
        .post(format!("{base}/v2/repository/models/{MODEL}/load"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "无 active 版本必须明确报错");

    // 加载 + 激活后 → 幂等 200(重复调用仍 200,KServe load 语义 C10)
    load_model(&base, MODEL, "1").await;
    for _ in 0..2 {
        let resp = client
            .post(format!("{base}/v2/repository/models/{MODEL}/load"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "bare load 必须幂等 200");
        let v: Value = resp.json().await.unwrap();
        assert_eq!(v["name"], MODEL);
        assert_eq!(v["load"], true, "KServe 响应形状(J2): {v}");
    }
}

/// §6.3 test_new_routes_fallback_regression (P1,G17):新路由注册后原
/// fallback 路径行为锁定——/v2/models/:m 现为 200(原 404)。
#[tokio::test]
#[serial]
async fn test_new_routes_fallback_regression() {
    let base = shared_base().await;
    load_model(&base, MODEL, "1").await;
    let client = reqwest::Client::new();
    // 裸模型路径不再是 404(G17:注册前落 route_fallback)
    let resp = client
        .get(format!("{base}/v2/models/{MODEL}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "模型元数据路径必须已注册(G17)");
    // 不存在模型的裸路径 → 404(路由注册了,但模型不存在)
    let resp = client
        .get(format!("{base}/v2/models/no-such-model"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    unload_model(&base, MODEL, "1").await;
}

// ---------------------------------------------------------------------------
// Triton Generate extension(阶段 4,批次 4,D9)
// ---------------------------------------------------------------------------

/// §6.4 test_generate_unary_passthrough (P0):/generate 与 /infer 同请求 →
/// 响应 byte-identical(别名语义,J3:unary 即 infer 别名,无 gate)。
#[tokio::test]
#[serial]
async fn test_generate_unary_passthrough() {
    let base = shared_base().await;
    load_model(&base, MODEL, "1").await;
    let client = reqwest::Client::new();
    let body = json!({"input": 5});

    let infer = client
        .post(format!("{base}/v2/models/{MODEL}/infer"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let generate = client
        .post(format!("{base}/v2/models/{MODEL}/generate"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(generate.status(), 200);
    assert_eq!(
        generate.bytes().await.unwrap(),
        infer.bytes().await.unwrap(),
        "/generate 必须与 /infer 响应 byte-identical(别名语义)"
    );

    unload_model(&base, MODEL, "1").await;
}

/// §6.4 test_generate_stream_sse_envelope (P0):每 SSE 事件 = data: <JSON>;
/// 结束即连接关闭(Generate 风格无 [DONE]——D9/Triton 行为)。
#[tokio::test]
#[serial]
async fn test_generate_stream_sse_envelope() {
    let base = shared_base().await;
    load_model(&base, MODEL, "1").await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/v2/models/{MODEL}/generate_stream"))
        .json(&json!({"input": 5}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "generate_stream 必须可开流");
    let ct = resp.headers().get("content-type").unwrap().to_str().unwrap().to_string();
    assert!(ct.contains("text/event-stream"), "expected SSE, got {ct}");
    let body = tokio::time::timeout(Duration::from_secs(15), resp.text())
        .await
        .expect("generate_stream 响应必须在超时内关闭")
        .unwrap();
    assert!(body.contains("data:"), "SSE 事件必须存在: {body}");
    assert!(
        !body.contains("[DONE]"),
        "Generate 风格结束即连接关闭,无 [DONE] 标记: {body}"
    );

    unload_model(&base, MODEL, "1").await;
}

/// §6.4 test_generate_stream_binary_flag_400 (P1):generate_stream + binary
/// flag → 400(SSE 文本不能携带二进制,D10;与 /events 同一检查)。
#[tokio::test]
#[serial]
async fn test_generate_stream_binary_flag_400() {
    let base = shared_base().await;
    load_model(&base, MODEL, "1").await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/v2/models/{MODEL}/generate_stream"))
        .json(&json!({
            "inputs": [{"name": "a", "shape": [1], "datatype": "FP32", "data": [1.0]}],
            "parameters": {"binary_data_output": true},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "SSE 不能携带二进制输出(D10)");
    let v: Value = resp.json().await.unwrap();
    assert!(
        v["error"].is_object() || v["error"].is_string(),
        "结构化错误体必须存在: {v}"
    );

    unload_model(&base, MODEL, "1").await;
}

/// §6.4 test_generate_stream_nonstream_worker + test_generate_stream_unary_untouched
/// (P1/P0):非流式 worker 的 generate_stream 与 /events 同路径同行为(J4);
/// 非流式 /infer 响应 byte-identical 回归。
#[tokio::test]
#[serial]
async fn test_generate_stream_nonstream_worker_and_unary_untouched() {
    let base = shared_base().await;
    load_model(&base, MODEL, "1").await;
    let client = reqwest::Client::new();
    let body = json!({"input": 3});

    // 非流式 worker:generate_stream 与 /events 行为一致(都 200 + SSE 帧)
    let ev = client
        .post(format!("{base}/v2/models/{MODEL}/events"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let gen = client
        .post(format!("{base}/v2/models/{MODEL}/generate_stream"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(ev.status(), 200, "/events 基线");
    assert_eq!(gen.status(), 200, "非流式 worker 的 generate_stream 与 /events 同路径");

    // 非流式 /infer 回归
    let infer = client
        .post(format!("{base}/v2/models/{MODEL}/infer"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(infer.status(), 200);
    let v: Value = infer.json().await.unwrap();
    assert_eq!(v["output"], 6, "非流式 /infer 必须不受 generate 影响");

    unload_model(&base, MODEL, "1").await;
}

/// §6.4 test_generate_stream_legacy_events_untouched (P0):/events 自有格式
/// 回归——data: chunk + data: [DONE](与 generate_stream 并存,帧格式不同)。
#[tokio::test]
#[serial]
async fn test_generate_stream_legacy_events_untouched() {
    let base = shared_base().await;
    load_model(&base, MODEL, "1").await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/v2/models/{MODEL}/events"))
        .json(&json!({"input": 3}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = tokio::time::timeout(Duration::from_secs(15), resp.text())
        .await
        .expect("events 响应必须在超时内关闭")
        .unwrap();
    assert!(body.contains("data:"), "/events 帧必须存在: {body}");
    assert!(body.contains("[DONE]"), "/events 自有格式保持 [DONE] 终止: {body}");

    unload_model(&base, MODEL, "1").await;
}

/// §6.4 test_generate_stream_triton_binary_request (P1):流式 + 多 tensor
/// 二进制请求组合——规范核对裁定:允许(Triton generate 请求 schema 模型
/// 自定义,server 维持透传哲学,worker 语义归 worker)。
#[tokio::test]
#[serial]
async fn test_generate_stream_triton_binary_request() {
    let base = shared_base().await;
    load_model(&base, MODEL, "1").await;
    let client = reqwest::Client::new();
    let head = r#"{"id":"g1","inputs":[{"name":"a","shape":[2],"datatype":"FP32","parameters":{"binary_data_size":8}}]}"#;
    let mut body_bytes = head.as_bytes().to_vec();
    body_bytes.extend_from_slice(&[0u8, 1, 2, 3, 4, 5, 6, 7]);
    let resp = client
        .post(format!("{base}/v2/models/{MODEL}/generate_stream"))
        .header("content-type", "application/octet-stream")
        .header("inference-header-content-length", head.len().to_string())
        .body(body_bytes)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "TritonBinary 请求组合必须透传(透传哲学)");

    unload_model(&base, MODEL, "1").await;
}

/// §6.4 test_generate_stream_mid_stream_error (P1):流中途错误 → 后续
/// data: 携带 error JSON(Triton 行为:HTTP 状态码由首个 SSE 响应固定,
/// 客户端须逐事件检查——该 caveat 已写入文档)。
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_generate_stream_mid_stream_error() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let repo = std::env::temp_dir()
        .join(format!("lite-server-gen-err-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    let dir = repo.join("gen_err/1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("model.py"),
        r#"from lite_server import LitAPI


class GenErrAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request

    async def stream_predict(self, request, ctx):
        yield {"token": "hello"}
        raise ValueError("mid-stream boom")
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: true\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();

    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc", "--no-metrics", "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{http_port}");
    load_model(&base, "gen_err", "1").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/v2/models/gen_err/generate_stream"))
        .json(&json!({"prompt": "hi"}))
        .send()
        .await
        .unwrap();
    // HTTP 状态码由首个 SSE 响应固定(200);错误在后续事件内(Triton 行为)
    assert_eq!(resp.status(), 200, "流中途错误不改变已固定的 HTTP 状态码");
    let body = tokio::time::timeout(Duration::from_secs(15), resp.text())
        .await
        .expect("generate_stream 响应必须在超时内关闭")
        .unwrap();
    assert!(body.contains("data: {\"token\":\"hello\"}"), "首个 chunk 正常: {body}");
    assert!(
        body.contains("\"error\""),
        "流中途错误必须携带在后续 data: 事件内(Triton 行为): {body}"
    );

    let _ = std::fs::remove_dir_all(&repo);
}

// ---------------------------------------------------------------------------
// openai-compact(阶段 6,批次 5,J5/J6/C19):/v1 5 端点
// ---------------------------------------------------------------------------

/// worker:OpenAI 兼容翻译层(helpers/openai.py)——chat/completions/
/// embeddings 按请求体特征分支;stream_predict 逐 chunk。
fn write_openai_compat_model(repo: &std::path::Path, name: &str, stream: bool) {
    let dir = repo.join(format!("{name}/1"));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("model.py"),
        r#"from lite_server import LitAPI


class OpenAICompatAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request

    def predict(self, x):
        from lite_server.helpers.openai import (
            build_chat_response, build_completions_response,
            build_embeddings_response,
        )
        if "messages" in x:
            return build_chat_response(
                "echo:" + x["messages"][-1]["content"],
                model=x.get("model", "m"),
            )
        if "prompt" in x:
            return build_completions_response(
                "echo:" + str(x["prompt"]), model=x.get("model", "m"),
            )
        return build_embeddings_response([0.1, 0.2], model=x.get("model", "m"))

    async def stream_predict(self, x, ctx):
        from lite_server.helpers.openai import build_chat_chunk
        for tok in ["hel", "lo"]:
            yield build_chat_chunk(tok, model=x.get("model", "m"))
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        format!(
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: {stream}\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n"
        ),
    )
    .unwrap();
}

/// §6.7 test_chat_completions_unary (P0):/v1/chat/completions 非流式 →
/// chat 形状 JSON(choices/message)。
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_chat_completions_unary() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let repo = std::env::temp_dir()
        .join(format!("lite-server-oai-unary-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    write_openai_compat_model(&repo, "chat", false);

    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc", "--no-metrics", "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{http_port}");
    load_model(&base, "chat", "1").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/v1/chat/completions"))
        .json(&json!({
            "model": "chat",
            "messages": [{"role": "user", "content": "hi"}],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["object"], "chat.completion");
    assert_eq!(v["model"], "chat");
    assert_eq!(v["choices"][0]["message"]["content"], "echo:hi");
    assert_eq!(v["choices"][0]["message"]["role"], "assistant");

    let _ = std::fs::remove_dir_all(&repo);
}

/// openai-compact 专属鉴权门(2026-08-09 方案):`openai_compact.auth` 只锁
/// /v1 5 端点——缺 key → 401(OpenAI 形状),Bearer 通过;错 key 拒绝;
/// SSE 臂同样过门;v2 infer 与 /health 不带 key 照常 200(侧效为零)。
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_v1_gate_scoped_to_openai_compact() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let repo = std::env::temp_dir()
        .join(format!("lite-server-oai-gate-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    write_openai_compat_model(&repo, "chat", false);

    let tmp_dir = std::env::temp_dir()
        .join(format!("lite-server-oai-gate-cfg-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 0.0.0.0\n  http_port: {}\n  grpc_port: {}\n  metrics_port: 18094\n  log_level: warn\ngrpc:\n  enabled: false\nmetrics:\n  enabled: false\nmodel_repository:\n  path: {}\nopenai_compact:\n  auth:\n    mode: key\n    key: authorization\n    value: sk-integration-secret\n",
            http_port,
            next_test_port(),
            repo.to_string_lossy()
        ),
    )
    .unwrap();

    let _server = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{http_port}");
    load_model(&base, "chat", "1").await;

    let client = reqwest::Client::new();

    // 1. /v1/chat/completions:缺 key → 401 OpenAI 形状;Bearer → 200 echo。
    let resp = client
        .post(format!("{base}/v1/chat/completions"))
        .json(&json!({
            "model": "chat",
            "messages": [{"role": "user", "content": "hi"}],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["error"]["type"], "authentication_error");
    assert!(
        v["error"]["message"].as_str().unwrap().contains("missing API key"),
        "缺 header 文案:{}",
        v["error"]["message"]
    );

    let resp = client
        .post(format!("{base}/v1/chat/completions"))
        .header("authorization", "Bearer sk-integration-secret")
        .json(&json!({
            "model": "chat",
            "messages": [{"role": "user", "content": "hi"}],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["choices"][0]["message"]["content"], "echo:hi");

    // 2. 错 key → 401;stream:true 同样被门拦截(worker 之前)。
    let resp = client
        .post(format!("{base}/v1/chat/completions"))
        .header("authorization", "Bearer wrong")
        .json(&json!({
            "model": "chat",
            "messages": [{"role": "user", "content": "hi"}],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "错 key 必须拒绝");

    let resp = client
        .post(format!("{base}/v1/chat/completions"))
        .json(&json!({
            "model": "chat",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": true,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "SSE 臂同样过门(worker 前拦截)");

    // 3. /v1/models 缺 key → 401(OpenAI SDK 会先打它;列表端点无 model
    // 上下文,per-model policies.auth 覆盖不到);带 key → 200 列表。
    let resp = client.get(format!("{base}/v1/models")).send().await.unwrap();
    assert_eq!(resp.status(), 401, "/v1/models 也要 key");
    let resp = client
        .get(format!("{base}/v1/models"))
        .header("authorization", "Bearer sk-integration-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["object"], "list");

    // 4. 边界:非 /v1 端点零影响——v2 infer 与 /health 不带 key 照常 200。
    let resp = client
        .post(format!("{base}/v2/models/chat/infer"))
        .json(&json!({"input": 5}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "v2 infer 不受 openai_compact.auth 影响");
    let resp = client.get(format!("{base}/health")).send().await.unwrap();
    assert_eq!(resp.status(), 200, "health 不受影响");

    let _ = std::fs::remove_dir_all(&repo);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// §6.7 test_chat_completions_sse (P0):stream: true → data: {json} 逐 chunk
/// + data: [DONE];连接正常终止。
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_chat_completions_sse() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let repo = std::env::temp_dir()
        .join(format!("lite-server-oai-sse-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    write_openai_compat_model(&repo, "chat", true);

    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc", "--no-metrics", "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{http_port}");
    load_model(&base, "chat", "1").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/v1/chat/completions"))
        .json(&json!({
            "model": "chat",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": true,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp.headers().get("content-type").unwrap().to_str().unwrap().to_string();
    assert!(ct.contains("text/event-stream"), "expected SSE, got {ct}");
    let body = tokio::time::timeout(Duration::from_secs(15), resp.text())
        .await
        .expect("SSE 响应必须在超时内关闭")
        .unwrap();
    assert!(body.contains("data: {\"id\""), "逐 chunk 帧必须存在: {body}");
    assert!(body.contains("\"chat.completion.chunk\""), "chunk 对象形状: {body}");
    assert!(body.contains("data: [DONE]"), "OpenAI SSE 以 [DONE] 终止: {body}");

    let _ = std::fs::remove_dir_all(&repo);
}

/// §6.7 test_completions_echo + test_embeddings (P1):/v1/completions 与
/// /v1/embeddings 透传 worker(翻译层在 worker 侧,J6)。
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_completions_and_embeddings() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let repo = std::env::temp_dir()
        .join(format!("lite-server-oai-others-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    write_openai_compat_model(&repo, "chat", false);

    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc", "--no-metrics", "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{http_port}");
    load_model(&base, "chat", "1").await;

    let client = reqwest::Client::new();
    // completions
    let resp = client
        .post(format!("{base}/v1/completions"))
        .json(&json!({"model": "chat", "prompt": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["object"], "text_completion");
    assert_eq!(v["choices"][0]["text"], "echo:hi");
    // embeddings
    let resp = client
        .post(format!("{base}/v1/embeddings"))
        .json(&json!({"model": "chat", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["object"], "list");
    assert_eq!(v["data"][0]["embedding"], json!([0.1, 0.2]));

    let _ = std::fs::remove_dir_all(&repo);
}

/// §6.7 test_v1_models_list + test_v1_models_retrieve (P1):/v1/models 列表 =
/// 注册模型名;/v1/models/{model} 单模型对象;不存在 → 404 OpenAI 形状。
#[tokio::test]
#[serial]
async fn test_v1_models_list_and_retrieve() {
    let base = shared_base().await;
    load_model(&base, MODEL, "1").await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{base}/v1/models")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["object"], "list");
    let ids: Vec<&str> = v["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|m| m["id"].as_str())
        .collect();
    assert!(ids.contains(&MODEL), "/v1/models 必须含 {MODEL}: {v}");

    let resp = client
        .get(format!("{base}/v1/models/{MODEL}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["id"], MODEL);
    assert_eq!(v["object"], "model");

    // 不存在 → 404 OpenAI 形状(经协议层分派)
    let resp = client
        .get(format!("{base}/v1/models/no-such-model"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    let v: Value = resp.json().await.unwrap();
    assert!(v["error"].is_object(), "OpenAI 错误形状: {v}");

    unload_model(&base, MODEL, "1").await;
}

/// §6.7 test_v1_404_model_not_found (P0):不存在模型 → 404 + OpenAI 错误形状。
#[tokio::test]
#[serial]
async fn test_v1_404_model_not_found() {
    let base = shared_base().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/v1/chat/completions"))
        .json(&json!({
            "model": "definitely-not-a-model",
            "messages": [{"role": "user", "content": "hi"}],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "不存在的模型必须 404");
    let v: Value = resp.json().await.unwrap();
    assert!(v["error"].is_object(), "OpenAI 错误形状: {v}");
}

/// §6.7 test_v1_mid_stream_error (P1):流中途错误 → 后续 data: 携带 error
/// JSON(OpenAI SSE 惯例)。
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_v1_mid_stream_error() {
    let http_port = next_test_port();
    kill_stale_on_port(http_port);
    let repo = std::env::temp_dir()
        .join(format!("lite-server-oai-err-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    let dir = repo.join("chat/1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("model.py"),
        r#"from lite_server import LitAPI
from lite_server.helpers.openai import build_chat_chunk


class ErrAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request

    async def stream_predict(self, x, ctx):
        yield build_chat_chunk("hel", model="chat")
        raise ValueError("mid-stream boom")
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: true\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();

    let _server = ServerGuard::start(&[
        "--port", &http_port.to_string(),
        "--model-repo", &repo.to_string_lossy(),
        "--no-grpc", "--no-metrics", "--log-level", "warn",
    ]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{http_port}");
    load_model(&base, "chat", "1").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/v1/chat/completions"))
        .json(&json!({
            "model": "chat",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": true,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "HTTP 状态码由首个 SSE 响应固定");
    let body = tokio::time::timeout(Duration::from_secs(15), resp.text())
        .await
        .expect("SSE 响应必须在超时内关闭")
        .unwrap();
    assert!(body.contains("data: {\"id\""), "首个 chunk 正常: {body}");
    assert!(
        body.contains("\"error\""),
        "流中途错误必须携带在后续 data: 事件内(OpenAI SSE 惯例): {body}"
    );

    let _ = std::fs::remove_dir_all(&repo);
}

// ---------------------------------------------------------------------------
// P-TRACE（对账 C，feature=telemetry）：traceparent 端到端传播 + SIGTERM flush
// ---------------------------------------------------------------------------

/// traceparent 回显模型：把 worker 收到的 RequestMeta.headers["traceparent"]
/// 回传给客户端。
#[cfg(all(unix, feature = "telemetry"))]
fn write_tp_echo_repo(repo: &std::path::Path) {
    let dir = repo.join("tp_echo/1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("model.py"),
        r#"from lite_server import LitAPI


class TpEchoAPI(LitAPI):
    def setup(self, device):
        pass

    def predict(self, x, ctx=None):
        tp = ""
        if ctx is not None and getattr(ctx, "meta", None) is not None:
            tp = ctx.meta.headers.get("traceparent", "")
        return {"tp": tp}

    def encode_response(self, output):
        return output
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("config.yaml"),
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
}

#[cfg(all(unix, feature = "telemetry"))]
const TP_TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
#[cfg(all(unix, feature = "telemetry"))]
const TP_VALUE: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

/// HTTP：带 traceparent 的请求，worker 收到的 RequestMeta.headers 必须含
/// 同 trace-id 的 traceparent（propagator 提取→注入链路的端到端证据）。
/// SIGTERM：telemetry 开启时进程须在窗口内干净退出（force_flush 有界，
/// 无 #2715 死锁）。
#[cfg(all(unix, feature = "telemetry"))]
#[tokio::test]
#[serial]
async fn test_telemetry_traceparent_e2e_and_sigterm_flush_http() {
    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);

    let repo = std::env::temp_dir().join(format!("lite-server-ptrace-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    write_tp_echo_repo(&repo);

    let tmp_dir = std::env::temp_dir()
        .join(format!("lite-server-ptrace-yaml-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18214\n  timeout: 10.0\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\ntelemetry:\n  enabled: true\n  otlp_endpoint: \"http://127.0.0.1:4319\"\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();

    let mut child = start_server(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "tp_echo", "1").await;

    // HTTP: traceparent 头 → worker 回显的 trace-id 必须一致。
    let resp = reqwest::Client::new()
        .post(format!("{}/v2/models/tp_echo/infer", base))
        .header("content-type", "application/json")
        .header("traceparent", TP_VALUE)
        .body(r#"{"x":1}"#)
        .send()
        .await
        .expect("http infer");
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let echoed = body["tp"].as_str().unwrap_or("");
    assert!(
        echoed.contains(TP_TRACE_ID),
        "worker 收到的 traceparent 须含入站 trace-id {TP_TRACE_ID}; got: {echoed:?}"
    );

    // SIGTERM:telemetry 开启时优雅退出(force_flush 有界,不死锁)。
    send_sigterm(&child);
    let exited = wait_for_exit(&mut child, 15).await;
    if !exited {
        let _ = child.kill();
        let _ = child.wait();
    }
    assert!(exited, "telemetry 开启时 SIGTERM 须有界退出(flush 不死锁)");

    let _ = std::fs::remove_dir_all(&repo);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// gRPC:metadata traceparent → 同款传播断言。
#[cfg(all(unix, feature = "telemetry"))]
#[tokio::test]
#[serial]
async fn test_telemetry_traceparent_e2e_grpc() {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;
    use std::collections::HashMap;

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);

    let repo = std::env::temp_dir().join(format!("lite-server-ptraceg-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    write_tp_echo_repo(&repo);

    let tmp_dir = std::env::temp_dir()
        .join(format!("lite-server-ptraceg-yaml-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let server_yaml = tmp_dir.join("server.yaml");
    std::fs::write(
        &server_yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18215\n  timeout: 10.0\n  log_level: warn\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\ntelemetry:\n  enabled: true\n  otlp_endpoint: \"http://127.0.0.1:4319\"\nmodel_repository:\n  path: {repo}\n",
            http_port = http_port,
            grpc_port = grpc_port,
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();

    let _guard = ServerGuard::start(&["--config", &server_yaml.to_string_lossy()]);
    wait_for_server(http_port, 60).await;
    let base = format!("http://127.0.0.1:{}", http_port);
    load_model(&base, "tp_echo", "1").await;

    let channel = grpc_tcp_channel(grpc_port).await;
    let mut client = LiteServerClient::new(channel);
    let mut req = tonic::Request::new(InferRequest {
        model_name: "tp_echo".to_string(),
        version: "1".to_string(),
        data: bytes::Bytes::from(serde_json::to_vec(&json!({"x": 1})).unwrap()),
        headers: HashMap::new(),
        ..Default::default()
    });
    req.metadata_mut()
        .insert("traceparent", tonic::metadata::MetadataValue::from_static(TP_VALUE));
    let resp = client.infer(req).await.expect("grpc infer").into_inner();
    let body: Value = serde_json::from_slice(&resp.data).unwrap();
    let echoed = body["tp"].as_str().unwrap_or("");
    assert!(
        echoed.contains(TP_TRACE_ID),
        "gRPC:worker 收到的 traceparent 须含入站 trace-id; got: {echoed:?}"
    );

    let _ = std::fs::remove_dir_all(&repo);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

// ---------------------------------------------------------------------------
// G5: WorkerEof terminal frame (T5)
// ---------------------------------------------------------------------------

/// A worker dying mid-stream must reach the SSE client as an explicit error
/// frame — never a silent EOF indistinguishable from a normal [DONE] end
/// (G5: recycle / health kill / unload all produce WorkerEof).
#[tokio::test]
async fn should_send_terminal_error_frame_when_worker_dies_mid_stream() {
    // Clean leftover workers from a previous failed run — the worker is
    // found by pgrep on its unique model name below.
    let _ = std::process::Command::new("pkill")
        .args(["-9", "-f", "lite_server.worker.inference --model-name eof_model"])
        .status();
    let base = g_base().await;
    load_model(&base, "eof_model", "1").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/eof_model/events", base))
        .json(&json!({"input": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // SIGKILL the worker mid-stream (the model yields a chunk every 0.2s for
    // 6s total, so the stream is in flight when the kill lands).
    let out = std::process::Command::new("pgrep")
        .args(["-f", "lite_server.worker.inference --model-name eof_model"])
        .output()
        .expect("pgrep runs");
    let pid = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.trim().parse::<u32>().ok())
        .next()
        .expect("eof_model worker process found");
    let killed = std::process::Command::new("kill")
        .args(["-9", &pid.to_string()])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    assert!(killed, "must SIGKILL the worker (pid {pid})");

    // The stream must terminate with an explicit error frame, bounded. Two
    // server paths can produce it, both G5-correct: the forwarder's
    // WorkerEof arm ("worker exited mid-stream") when the dead peer is
    // detected as a channel close, or a synthesized worker Error frame when
    // a send to the dead worker fails first (EAGAIN) — the race between
    // them is inherent; what matters is the client never sees a silent EOF.
    let body = tokio::time::timeout(Duration::from_secs(20), resp.text())
        .await
        .expect("SSE body must close promptly after the worker dies")
        .unwrap();
    assert!(
        body.contains("\"error\""),
        "G5: a killed worker must surface as an error frame, not a silent EOF: {body}"
    );
    assert!(
        !body.contains("[DONE]"),
        "a killed worker must not look like a normal completion: {body}"
    );
}

// ---------------------------------------------------------------------------
// G3: streams count toward max_requests (T4) + ensemble per-node counting (T4b)
// ---------------------------------------------------------------------------

const QUICK_STREAM_PY: &str = r#"from lite_server import LitAPI


class QuickStreamAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request.get("input", 1)

    def predict(self, x):
        return {"output": x}

    def stream_predict(self, request):
        yield {"chunk": 1}
        yield {"chunk": 2}

    def encode_response(self, output):
        return output
"#;

fn rolling_recycle_count(body: &str, model: &str) -> f64 {
    body.lines()
        .find(|l| l.starts_with(&format!(
            "liteserver_worker_respawns_total{{model=\"{model}\",reason=\"rolling_recycle\",version=\"1\"}}"
        )))
        .and_then(|l| l.rsplit_once(' '))
        .and_then(|(_, n)| n.parse::<f64>().ok())
        .unwrap_or(0.0)
}

/// T4: pure streaming load must roll-recycle a worker (G3) — streams count
/// toward max_requests at open. The sibling model with
/// count_streams_toward_max_requests: false proves the escape hatch keeps
/// legacy behavior. 5 sequential streams over 2 slots with threshold 2:
/// some slot opens ≥3 streams (pigeonhole) and crosses.
#[tokio::test]
async fn should_roll_recycle_on_pure_streaming_load() {
    let base = g_base().await;
    let client = reqwest::Client::new();
    load_model(&base, "stream_counted", "1").await;
    load_model(&base, "stream_legacy", "1").await;

    for model in ["stream_counted", "stream_legacy"] {
        for i in 0..5 {
            let resp = client
                .post(format!("{}/v2/models/{}/events", base, model))
                .json(&json!({"input": i}))
                .send().await.unwrap();
            assert_eq!(resp.status(), 200, "{model} stream {i} must be served");
            let body = tokio::time::timeout(Duration::from_secs(15), resp.text())
                .await
                .expect("stream body closes")
                .unwrap();
            assert!(body.contains("data:"), "{model} stream {i} has chunks: {body}");
        }
    }

    // The counted model must have roll-recycled at least one worker.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut recycled = false;
    while tokio::time::Instant::now() < deadline {
        let body = client
            .get(format!("{}/metrics", g_metrics_base()))
            .send().await.unwrap().text().await.unwrap();
        if rolling_recycle_count(&body, "stream_counted") >= 1.0 {
            recycled = true;
            break;
        }
        sleep(Duration::from_millis(200)).await;
    }
    assert!(recycled, "G3: pure streaming load must trigger a rolling recycle");

    // The escape-hatch model must NOT have recycled (the counted model's
    // recycle window above gave it ample time to prove the negative).
    let body = client
        .get(format!("{}/metrics", g_metrics_base()))
        .send().await.unwrap().text().await.unwrap();
    assert_eq!(
        rolling_recycle_count(&body, "stream_legacy"),
        0.0,
        "count_streams_toward_max_requests: false must keep legacy behavior"
    );
}

/// T4b: ensemble streams count toward the budget PER DAG NODE (Q4) — the
/// streaming tail's workers roll-recycle on ensemble load alone.
#[tokio::test]
async fn should_count_ensemble_streams_per_dag_node_toward_max_requests() {
    let base = g_base().await;
    let client = reqwest::Client::new();
    load_model(&base, "ens_pre", "1").await;
    load_model(&base, "ens_tail", "1").await;
    load_model(&base, "ens_budget_model", "1").await;

    // 5 sequential ensemble streams over 2 tail slots with threshold 2 —
    // pigeonhole forces some tail slot to open ≥3 node streams and cross.
    for i in 0..5 {
        let resp = client
            .post(format!("{}/v2/models/ens_budget_model/events", base))
            .json(&json!({"text": "a b"}))
            .send().await.unwrap();
        assert_eq!(resp.status(), 200, "ensemble stream {i} must be served");
        let body = tokio::time::timeout(Duration::from_secs(15), resp.text())
            .await
            .expect("ensemble stream body closes")
            .unwrap();
        assert!(body.contains("data:"), "ensemble stream {i} has chunks: {body}");
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut recycled = false;
    while tokio::time::Instant::now() < deadline {
        let body = client
            .get(format!("{}/metrics", g_metrics_base()))
            .send().await.unwrap().text().await.unwrap();
        if rolling_recycle_count(&body, "ens_tail") >= 1.0 {
            recycled = true;
            break;
        }
        sleep(Duration::from_millis(200)).await;
    }
    assert!(recycled, "T4b: ensemble streams must count per DAG node — the tail must roll-recycle");
}

// ---------------------------------------------------------------------------
// G4: max_concurrent_streams (T6)
// ---------------------------------------------------------------------------

/// Over-cap stream opens are rejected 429 + Retry-After; a closing stream
/// re-admits. The model streams ~3s so two concurrent streams fill the cap
/// while a third is attempted.
#[tokio::test]
async fn should_reject_streams_over_max_concurrent_streams() {
    let base = g_base().await;
    load_model(&base, "cap_model", "1").await;

    let client = reqwest::Client::new();
    let open_stream = || {
        let client = client.clone();
        let base = base.clone();
        async move {
            client
                .post(format!("{}/v2/models/cap_model/events", base))
                .json(&json!({"input": 1}))
                .send()
                .await
                .unwrap()
        }
    };

    // Two concurrent streams fill the cap; confirm each is actually
    // streaming (first chunk arrived) before probing the cap.
    let mut s1 = open_stream().await;
    let mut s2 = open_stream().await;
    assert_eq!(s1.status(), 200);
    assert_eq!(s2.status(), 200);
    let _ = tokio::time::timeout(Duration::from_secs(10), s1.chunk())
        .await.expect("s1 first chunk").unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(10), s2.chunk())
        .await.expect("s2 first chunk").unwrap();

    // Third concurrent open → 429 + Retry-After.
    let s3 = open_stream().await;
    assert_eq!(s3.status(), 429, "over-cap stream must be rejected");
    assert!(
        s3.headers().get("retry-after").is_some(),
        "a capacity rejection must tell the client to retry"
    );

    // Drain both streams to completion (natural end releases the permits),
    // then a new stream is admitted again.
    let _ = tokio::time::timeout(Duration::from_secs(20), s1.text()).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(20), s2.text()).await.unwrap();
    let s4 = open_stream().await;
    assert_eq!(s4.status(), 200, "a closed stream's permit must re-admit");
}

// ---------------------------------------------------------------------------
// Q2: negotiated close on recycle eviction (grace cancel)
// ---------------------------------------------------------------------------

/// A long decoupled stream crossing a rolling recycle must NOT be cut with
/// an eviction error when the model cooperates: the grace cancel flags
/// `sender.closing`, the model pushes a final chunk and closes — the client
/// sees a normal [DONE], and the worker still recycles.
#[tokio::test]
async fn should_close_decoupled_stream_cleanly_within_recycle_grace() {
    let base = g_base().await;
    load_model(&base, "grace_model", "1").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v2/models/grace_model/decoupled", base))
        .json(&json!({"input": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // max_requests=1: this very stream crosses the budget at open → drain
    // (1s) times out → grace cancel (3s) → the model sees `closing`, pushes
    // {"final": true} and closes → the client ends with a normal [DONE].
    let body = tokio::time::timeout(Duration::from_secs(30), resp.text())
        .await
        .expect("stream closes within the grace window")
        .unwrap();
    assert!(
        body.contains("\"final\": true") || body.contains("\"final\":true"),
        "the model's wrap-up chunk must reach the client: {body}"
    );
    assert!(
        body.contains("[DONE]"),
        "a cooperative model ends the stream with a normal [DONE]: {body}"
    );
    assert!(
        !body.contains("worker recycling"),
        "a cooperative close must NOT surface the eviction error: {body}"
    );

    // The rolling recycle still happened (the worker is replaced afterwards).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut recycled = false;
    while tokio::time::Instant::now() < deadline {
        let body = client
            .get(format!("{}/metrics", g_metrics_base()))
            .send().await.unwrap().text().await.unwrap();
        if rolling_recycle_count(&body, "grace_model") >= 1.0 {
            recycled = true;
            break;
        }
        sleep(Duration::from_millis(200)).await;
    }
    assert!(recycled, "the worker must still roll-recycle after the graceful close");
}

// ---------------------------------------------------------------------------
// Q2-at-shutdown: negotiated close for in-flight streams on server shutdown
// ---------------------------------------------------------------------------

/// A decoupled model that never checks `sender.closing` — the grace cancel
/// expires unanswered and the server must evict the stream with a terminal
/// error frame.
const SHUTDOWN_SLOW_PY: &str = r#"import asyncio
from lite_server import LitAPI


class ShutdownSlowAPI(LitAPI):
    def setup(self, device):
        pass

    def decode_request(self, request):
        return request

    def predict(self, x):
        return x

    async def predict_decoupled(self, data, sender):
        async def _push():
            for i in range(600):
                await sender.send({"index": i})
                await asyncio.sleep(0.5)
        asyncio.create_task(_push())

    def encode_response(self, output):
        return output
"#;

/// Repo + server.yaml for the shutdown stream tests: one cooperative model
/// (the recycle grace model's code) and one that ignores the grace cancel.
/// control_mode "all" loads both at boot. Returns (config_path, http_port,
/// metrics_port).
#[cfg(unix)]
fn write_shutdown_stream_models(repo: &std::path::Path) {
    let write_model = |name: &str, py: &str| {
        let dir = repo.join(name).join("1");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("model.py"), py).unwrap();
        std::fs::write(
            dir.join("config.yaml"),
            "max_batch_size: 1\nbatch_timeout: 0.0\nstream: true\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
        )
        .unwrap();
    };
    write_model("shutdown_grace", G_GRACE_PY);
    write_model("shutdown_slow", SHUTDOWN_SLOW_PY);
}

#[cfg(unix)]
fn shutdown_stream_server(tag: &str, graceful_timeout: f32, grace_ms: u32) -> (String, u16, u16) {
    let repo = std::env::temp_dir().join(format!("lite-server-{tag}-{}", std::process::id()));
    write_shutdown_stream_models(&repo);

    let http_port = next_test_port();
    let grpc_port = next_test_port();
    let metrics_port = next_test_port();
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);
    kill_stale_on_port(metrics_port);
    let cfg_dir =
        std::env::temp_dir().join(format!("lite-server-{tag}-cfg-{}", std::process::id()));
    std::fs::create_dir_all(&cfg_dir).unwrap();
    let cfg = cfg_dir.join("server.yaml");
    std::fs::write(
        &cfg,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: {metrics_port}\n  log_level: warn\n  graceful_timeout: {graceful_timeout}\n  shutdown_stream_grace_ms: {grace_ms}\nmetrics:\n  enabled: true\ngrpc:\n  enabled: false\norchestration:\n  control_mode: all\nmodel_repository:\n  path: {repo}\n",
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();
    (cfg.to_string_lossy().to_string(), http_port, metrics_port)
}

/// UDS variant of [`shutdown_stream_server`]: the HTTP server binds a Unix
/// socket (`server.host: unix:<path>`); metrics and gRPC are off.
#[cfg(unix)]
fn shutdown_stream_server_uds(
    tag: &str,
    sock: &str,
    graceful_timeout: f32,
    grace_ms: u32,
) -> String {
    let repo = std::env::temp_dir().join(format!("lite-server-{tag}-{}", std::process::id()));
    write_shutdown_stream_models(&repo);
    let cfg_dir =
        std::env::temp_dir().join(format!("lite-server-{tag}-cfg-{}", std::process::id()));
    std::fs::create_dir_all(&cfg_dir).unwrap();
    let cfg = cfg_dir.join("server.yaml");
    std::fs::write(
        &cfg,
        format!(
            "server:\n  host: unix:{sock}\n  log_level: warn\n  graceful_timeout: {graceful_timeout}\n  shutdown_stream_grace_ms: {grace_ms}\nmetrics:\n  enabled: false\ngrpc:\n  enabled: false\norchestration:\n  control_mode: all\nmodel_repository:\n  path: {repo}\n",
            repo = repo.to_string_lossy()
        ),
    )
    .unwrap();
    cfg.to_string_lossy().to_string()
}

/// POST an SSE request over a Unix socket via curl (reqwest has no UDS
/// client). Returns a task collecting the full body plus a receiver that
/// fires when the first `data:` chunk arrives.
#[cfg(unix)]
fn uds_sse_post(
    sock: &str,
    uri: &str,
    body: &str,
) -> (
    tokio::task::JoinHandle<String>,
    tokio::sync::oneshot::Receiver<()>,
) {
    let mut child = tokio::process::Command::new("curl")
        .args([
            "--unix-socket",
            sock,
            "-N",
            "-s",
            "-X",
            "POST",
            "-H",
            "content-type: application/json",
            "-d",
            body,
            &format!("http://localhost{uri}"),
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("curl must spawn");
    let stdout = child.stdout.take().unwrap();
    let (first_tx, first_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let mut out = String::new();
        let mut lines = BufReader::new(stdout).lines();
        let mut first_tx = Some(first_tx);
        while let Ok(Some(line)) = lines.next_line().await {
            out.push_str(&line);
            out.push('\n');
            if line.starts_with("data:") {
                if let Some(tx) = first_tx.take() {
                    let _ = tx.send(());
                }
            }
        }
        let _ = child.wait().await;
        out
    });
    (handle, first_rx)
}

/// UDS parity: the negotiated stream close also works over a Unix socket.
/// The UDS accept loop must drain in-flight connections on shutdown
/// (previously it abandoned them, which also killed the shutdown stream
/// closer before it could fire).
#[cfg(unix)]
#[tokio::test]
async fn uds_sigterm_grace_closes_in_flight_stream_with_done() {
    let sock = unique_uds_path("http-shutdown-grace");
    let cfg = shutdown_stream_server_uds("uds-shutdown-grace", &sock, 6.0, 2000);
    let mut server = start_server(&["--config", &cfg]);

    // Wait for the socket to accept connections.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        match tokio::net::UnixStream::connect(&sock).await {
            Ok(_) => break,
            Err(_) if tokio::time::Instant::now() < deadline => {
                sleep(Duration::from_millis(200)).await
            }
            Err(e) => panic!("UDS never came up at {sock}: {e}"),
        }
    }

    let (body_task, first_rx) = uds_sse_post(
        &sock,
        "/v2/models/shutdown_grace/decoupled",
        r#"{"input": 1}"#,
    );
    tokio::time::timeout(Duration::from_secs(10), first_rx)
        .await
        .expect("first SSE chunk arrives")
        .unwrap();

    send_sigterm(&server);

    let body = tokio::time::timeout(Duration::from_secs(30), body_task)
        .await
        .expect("stream closes within the drain window")
        .unwrap();
    assert!(
        body.contains("\"final\": true") || body.contains("\"final\":true"),
        "the model's wrap-up chunk must reach the client: {body}"
    );
    assert!(
        body.contains("[DONE]"),
        "a cooperative model ends the stream with a normal [DONE]: {body}"
    );
    assert!(
        !body.contains("server shutting down"),
        "a cooperative close must NOT surface the eviction error: {body}"
    );

    let exited = wait_for_exit(&mut server, 25).await;
    if !exited {
        stop_server(server);
        panic!("server must exit within 25s of SIGTERM");
    }
    assert!(
        server.wait().unwrap().success(),
        "UDS shutdown after a grace-closed stream must be a clean exit"
    );
}

/// A long decoupled stream crossing a server shutdown must close like it
/// does on a rolling recycle: near the end of the drain window the server
/// grace-cancels in-flight streams, the cooperative model pushes its wrap-up
/// chunk and closes — the client sees a normal [DONE] instead of a dropped
/// connection at the backstop.
#[cfg(unix)]
#[tokio::test]
async fn sigterm_grace_closes_in_flight_stream_with_done() {
    let (cfg, http_port, _metrics_port) = shutdown_stream_server("shutdown-grace", 6.0, 2000);
    let mut server = start_server(&["--config", &cfg]);
    wait_for_server(http_port, 60).await;

    let client = reqwest::Client::new();
    let mut resp = client
        .post(format!(
            "http://127.0.0.1:{http_port}/v2/models/shutdown_grace/decoupled"
        ))
        .json(&json!({"input": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    // Confirm the stream is actually producing before signaling.
    let first = tokio::time::timeout(Duration::from_secs(10), resp.chunk())
        .await
        .expect("first chunk arrives")
        .unwrap();
    assert!(first.is_some(), "stream must be open before SIGTERM");

    send_sigterm(&server);

    let body = tokio::time::timeout(Duration::from_secs(30), resp.text())
        .await
        .expect("stream closes within the drain window")
        .unwrap();
    assert!(
        body.contains("\"final\": true") || body.contains("\"final\":true"),
        "the model's wrap-up chunk must reach the client: {body}"
    );
    assert!(
        body.contains("[DONE]"),
        "a cooperative model ends the stream with a normal [DONE]: {body}"
    );
    assert!(
        !body.contains("server shutting down"),
        "a cooperative close must NOT surface the eviction error: {body}"
    );

    let exited = wait_for_exit(&mut server, 25).await;
    if !exited {
        stop_server(server);
        panic!("server must exit within 25s of SIGTERM");
    }
    assert!(
        server.wait().unwrap().success(),
        "shutdown after a grace-closed stream must be a clean exit"
    );
}

/// An uncooperative stream (ignores the grace cancel) is evicted at the end
/// of the grace window with a client-visible terminal error instead of a
/// silently dropped connection, and the shutdown metrics (draining gauge,
/// evicted counter) are scrapeable during the drain window.
#[cfg(unix)]
#[tokio::test]
async fn sigterm_evicts_uncooperative_stream_and_records_metrics() {
    let (cfg, http_port, metrics_port) = shutdown_stream_server("shutdown-evict", 8.0, 2000);
    let mut server = start_server(&["--config", &cfg]);
    wait_for_server(http_port, 60).await;

    let client = reqwest::Client::new();
    let mut resp = client
        .post(format!(
            "http://127.0.0.1:{http_port}/v2/models/shutdown_slow/decoupled"
        ))
        .json(&json!({"input": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let first = tokio::time::timeout(Duration::from_secs(10), resp.chunk())
        .await
        .expect("first chunk arrives")
        .unwrap();
    assert!(first.is_some(), "stream must be open before SIGTERM");

    // Scrape metrics through the whole drain window until the server dies:
    // the evicted counter only appears in the last ~second of the window.
    let scrape = tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut saw_draining = false;
        let mut saw_evicted = false;
        for _ in 0..200 {
            match client
                .get(format!("http://127.0.0.1:{metrics_port}/metrics"))
                .send()
                .await
            {
                Ok(r) => {
                    let body = r.text().await.unwrap_or_default();
                    if body.contains("liteserver_draining 1") {
                        saw_draining = true;
                    }
                    let evicted = body
                        .lines()
                        .find(|l| l.starts_with(
                            "liteserver_shutdown_streams_evicted_total{model=\"shutdown_slow\"",
                        ))
                        .and_then(|l| l.rsplit_once(' '))
                        .and_then(|(_, n)| n.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    if evicted >= 1.0 {
                        saw_evicted = true;
                    }
                }
                Err(_) => break, // metrics server is gone — shutdown finished
            }
            sleep(Duration::from_millis(100)).await;
        }
        (saw_draining, saw_evicted)
    });

    sleep(Duration::from_millis(500)).await;
    send_sigterm(&server);

    let body = tokio::time::timeout(Duration::from_secs(30), resp.text())
        .await
        .expect("evicted stream terminates within the drain window")
        .unwrap();
    assert!(
        body.contains("server shutting down"),
        "an evicted stream must surface the terminal error, not a bare drop: {body}"
    );

    let (saw_draining, saw_evicted) = scrape.await.unwrap();
    assert!(saw_draining, "liteserver_draining must be 1 during the drain window");
    assert!(
        saw_evicted,
        "the eviction must count toward liteserver_shutdown_streams_evicted_total"
    );

    let exited = wait_for_exit(&mut server, 30).await;
    if !exited {
        stop_server(server);
        panic!("server must exit within 30s of SIGTERM");
    }
    assert!(
        server.wait().unwrap().success(),
        "shutdown after an eviction must still be a clean exit"
    );
}
