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
    wait_for_server(http_port, 20).await;
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
    let client = reqwest::Client::new();

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
    wait_for_server(http_port, 20).await;
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
    wait_for_server(http_port, 20).await;
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
    wait_for_server(http_port, 20).await;

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
    wait_for_server(http_port, 20).await;
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
    wait_for_server(http_port, 20).await;
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
    match infer2 {
        Ok(Ok(_)) => panic!("new gRPC RPC after SIGTERM must be rejected, got Ok"),
        _ => {} // Err or timeout — both mean rejected / not served
    }

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
    wait_for_server(http_port, 20).await;
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
// §4.1: max_requests reload must target the triggering version
// ---------------------------------------------------------------------------

/// v1 hits max_requests while v2 is the active version: the auto-recycle must
/// reload v1 and leave the active v2 untouched. (The old ReloadSignal carried
/// only the model name, so the listener reloaded the *active* version.)
#[tokio::test]
async fn test_max_requests_reload_targets_triggering_version() {
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
    let base_cfg = "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n";
    for (v, cfg) in [("1", format!("{}max_requests: 1\n", base_cfg)), ("2", base_cfg.to_string())] {
        let dir = tmp_dir.join("reload_model").join(v);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("model.py"), model_py).unwrap();
        std::fs::write(dir.join("config.yaml"), cfg).unwrap();
    }

    let port = next_test_port();
    kill_stale_on_port(port);
    let _server = ServerGuard::start(&[
        "--port", &port.to_string(),
        "--model-repo", &tmp_dir.to_string_lossy(),
        "--no-metrics",
        "--no-grpc",
        "--log-level", "warn",
    ]);
    wait_for_server(port, 30).await;
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
    assert!(v2_loaded_at.as_u64().is_some(), "v2 loaded_at: {:?}", v2);
    let body = get_health().await;
    let v1_loaded_at = health_entry(&body, "1").expect("v1 loaded")["loaded_at"].clone();
    assert!(v1_loaded_at.as_u64().is_some(), "v1 loaded_at: {:?}", v1_loaded_at);

    // Make v2 the active version — the buggy code reloaded the active one.
    let resp = client
        .post(format!("{}/v2/models/reload_model/versions/2/activate", base))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);

    // Hit v1's max_requests via the versioned path.
    let resp = client
        .post(format!("{}/v2/models/reload_model/versions/1/infer", base))
        .json(&json!({"input": 21}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);

    // Poll: v1 must be observed recycling (entry briefly unregistered, or
    // re-registered with a new loaded_at) while v2 stays ready with its
    // original loaded_at throughout.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut v1_recycled = false;
    while tokio::time::Instant::now() < deadline {
        let body = get_health().await;
        let v2 = health_entry(&body, "2").expect("v2 must stay registered");
        assert_eq!(v2["status"], "ready", "active v2 must be untouched: {:?}", v2);
        assert_eq!(v2["loaded_at"], v2_loaded_at, "active v2 must not be reloaded");
        match health_entry(&body, "1") {
            None => { v1_recycled = true; break; } // unload phase of the recycle
            Some(v1) if v1["loaded_at"] != v1_loaded_at => { v1_recycled = true; break; }
            _ => {}
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(v1_recycled, "v1 was not recycled within 60s of hitting max_requests");

    // v1 comes back ready; v2 still untouched.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut v1_back = false;
    while tokio::time::Instant::now() < deadline {
        let body = get_health().await;
        let v2 = health_entry(&body, "2").expect("v2 must stay registered");
        assert_eq!(v2["loaded_at"], v2_loaded_at, "active v2 must not be reloaded");
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
    wait_for_server(port, 30).await;
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
    wait_for_server(port, 30).await;
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
    wait_for_server(port, 30).await;
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
    wait_for_server(port, 30).await;
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
    wait_for_server(port, 30).await;
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
    wait_for_server(http_port, 20).await;
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
    wait_for_server(http_port, 20).await;
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
    for i in 0..3 {
        assert!(!frames[i].is_final, "chunk {} must be non-final", i);
        let v: Value = serde_json::from_slice(&frames[i].data).expect("chunk data is JSON");
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
    wait_for_server(http_port, 20).await;
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
    wait_for_server(http_port, 20).await;
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
    assert!(
        body.contains("liteserver_requests_total{model=\"no_such_model\",status=\"4xx\",version=\"\"}"),
        "4xx series missing: {}", body
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
    wait_for_server(http_port, 20).await;
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
    wait_for_server(http_port, 20).await;
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
    wait_for_server(http_port, 20).await;
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
    wait_for_server(http_port, 20).await;
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
// Ensemble total timeout bounds serial layers (#3)
// ---------------------------------------------------------------------------

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
    wait_for_server(http_port, 20).await;
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
    // Load succeeds — the cycle is only detected when the config is parsed at
    // infer time, so the model registers and becomes ready.
    load_model(&base, "ensemble_cycle", "1").await;

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
        "cyclic ensemble DAG must surface as a gRPC error, not OK; got: {:?}",
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
    wait_for_server(http_port, 20).await;
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
    wait_for_server(http_port, 20).await;
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
