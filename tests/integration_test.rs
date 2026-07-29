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

fn start_server(args: &[&str]) -> std::process::Child {
    let mut cmd = Command::new(lite_server_bin());
    cmd.arg("serve")
        .current_dir(project_root())
        // Nobody reads the child's stdout: a pipe would fill its 64KB buffer
        // and block the server on write. null also keeps the orphaned-server
        // footprint minimal when cleanup never runs.
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());

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
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\npolicies:\n  rate_limit: { requests_per_minute: 3, burst: 3 }\n  cors:\n    allow_origins: [\"https://app.example.com\"]\n",
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
    let http_port = 18072u16;
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
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "text/event-stream"
    );
    assert_eq!(resp.headers().get("x-route").unwrap(), "ticks");
    let body = resp.text().await.unwrap();
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

    let body = resp.text().await.unwrap();
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
    let port = 18020;
    let metrics_port = 18021;
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
    let port = 18040;
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

    // SSE success path must carry CORS headers (attach_cors_headers wraps the
    // whole entry, including the stream-start response).
    let resp = client
        .post(format!("{}/v2/models/{}/events", base, POLICY_MODEL))
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
    // Drain the SSE body
    let _ = resp.text().await.unwrap();

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
    // OPTIONS preflight with 204 + ACAO. The versioned routes used to 500
    // because inference_options_handler took Path<String> on a 2-param route.
    let routes = [
        format!("/v2/models/{}/infer", POLICY_MODEL),
        format!("/v2/models/{}/versions/1/infer", POLICY_MODEL),
        format!("/v2/models/{}/events", POLICY_MODEL),
        format!("/v2/models/{}/versions/1/events", POLICY_MODEL),
    ];
    for path in &routes {
        let resp = client
            .request(reqwest::Method::OPTIONS, format!("{}{}", base, path))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            204,
            "OPTIONS {} should be 204, got {}",
            path,
            resp.status()
        );
        let acao = resp
            .headers()
            .get("access-control-allow-origin")
            .unwrap_or_else(|| panic!("ACAO missing on OPTIONS {}", path));
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
    let port = 18042;
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

    // Success: 200 + ACAO (attach_cors_headers wraps unary infer too).
    let resp = client
        .post(format!("{}/v2/models/{}/infer", base, POLICY_MODEL))
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
    // carry ACAO (attach_cors_headers wraps the Err path), and Retry-After
    // must be a sane 1..=60 seconds (C1 Rust defense against rpm<=0 → u64::MAX).
    let resp = client
        .post(format!("{}/v2/models/{}/infer", base, POLICY_MODEL))
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

    let port = 18030;
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

    let port = 18031;
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

    let port = 18090;
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
    let port = 18091;
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

    let port = 18101;
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

    let port = 18102;
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

/// Weights drive bare-request distribution; x-lite-version pins explicitly;
/// activate is a hard cutover. v1 computes x*2, v2 computes x*3 so responses
/// identify the serving version.
#[tokio::test]
async fn test_weighted_routing_canary() {
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

    let tmp_dir = std::env::temp_dir().join(format!("lite-server-canary-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp_dir);
    let cfg = "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n";
    for (v, factor) in [("1", 2), ("2", 3)] {
        let dir = tmp_dir.join("canary_model").join(v);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("model.py"), model_py(factor)).unwrap();
        std::fs::write(dir.join("config.yaml"), cfg).unwrap();
    }

    let port = 18092;
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
    let http_port = 18070u16;
    let grpc_port = 18071u16;
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
// gRPC health checking (grpc.health.v1, phase 3)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_grpc_health_service() {
    use tonic_health::pb::health_check_response::ServingStatus;
    use tonic_health::pb::health_client::HealthClient;
    use tonic_health::pb::HealthCheckRequest;

    // Dedicated server WITH gRPC enabled (the shared server runs --no-grpc).
    let http_port = 18074u16;
    let grpc_port = 18075u16;
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

    let http_port = 18080u16;
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
