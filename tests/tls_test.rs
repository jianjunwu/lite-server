//! P5-1 TLS/mTLS end-to-end (蓝图 §4.3): real `lite-server-core` binary, real
//! rustls handshakes, rcgen-generated PKI. Covers: HTTP one-way TLS /health +
//! infer, HTTP mTLS accept/reject, gRPC TLS Infer + Health, gRPC mTLS reject,
//! SIGHUP-driven hot rotation, and the single-rustls-version guard (评审 2.4).

use serde_json::{json, Value};
use serial_test::serial;
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::time::sleep;

// ---------------------------------------------------------------------------
// Server process helpers (same discipline as integration_test.rs)
// ---------------------------------------------------------------------------

fn project_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn lite_server_bin() -> std::path::PathBuf {
    project_root().join("target").join("debug").join("lite-server-core")
}

fn start_server(args: &[&str]) -> std::process::Child {
    let mut cmd = Command::new(lite_server_bin());
    cmd.arg("serve")
        .current_dir(project_root())
        // A pipe nobody reads fills its 64KB buffer and blocks the server;
        // null keeps the orphaned-server footprint minimal too.
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    for arg in args {
        cmd.arg(arg);
    }
    #[cfg(unix)]
    unsafe {
        use std::os::unix::process::CommandExt;
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

/// RAII guard that kills the server on drop — including on panic, so a failing
/// test never orphans a server holding the runner's stderr pipe open.
struct ServerGuard(Option<std::process::Child>);

impl ServerGuard {
    fn start(args: &[&str]) -> Self {
        ServerGuard(Some(start_server(args)))
    }

    #[cfg(unix)]
    fn send_sighup(&self) {
        if let Some(child) = &self.0 {
            unsafe {
                libc::kill(child.id() as i32, libc::SIGHUP);
            }
        }
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(child) = self.0.take() {
            stop_server(child);
        }
    }
}

#[cfg(unix)]
fn kill_stale_on_port(port: u16) {
    let output = Command::new("lsof").args(["-ti", &format!(":{}", port)]).output();
    if let Ok(out) = output {
        let pids = String::from_utf8_lossy(&out.stdout);
        for pid_str in pids.lines() {
            if let Ok(pid) = pid_str.trim().parse::<i32>() {
                let ps = Command::new("ps").args(["-p", &pid.to_string(), "-o", "comm="]).output();
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

// ---------------------------------------------------------------------------
// Model repo: single doubling model (test_model/1)
// ---------------------------------------------------------------------------

const MODEL: &str = "test_model";

fn create_test_model_repo() -> std::path::PathBuf {
    let tmp = std::env::temp_dir().join(format!("lite-server-tls-repo-{}", std::process::id()));
    let model_dir = tmp.join("test_model/1");
    std::fs::create_dir_all(&model_dir).unwrap();
    std::fs::write(
        model_dir.join("model.py"),
        r#"from lite_server import LitAPI


class TestAPI(LitAPI):
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
        "max_batch_size: 1\nbatch_timeout: 0.0\nstream: false\naccelerator: cpu\ndevices: 1\nworkers_per_device: 1\n",
    )
    .unwrap();
    tmp
}

fn test_model_repo() -> &'static std::path::PathBuf {
    static REPO: OnceLock<std::path::PathBuf> = OnceLock::new();
    REPO.get_or_init(create_test_model_repo)
}

// ---------------------------------------------------------------------------
// rcgen PKI helpers
// ---------------------------------------------------------------------------

struct TestPki {
    ca_cert: rcgen::Certificate,
    ca_key: rcgen::KeyPair,
    ca_pem: String,
}

impl TestPki {
    fn new(cn: &str) -> Self {
        let mut params = rcgen::CertificateParams::default();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.distinguished_name.push(rcgen::DnType::CommonName, cn);
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        Self {
            ca_pem: cert.pem(),
            ca_cert: cert,
            ca_key: key,
        }
    }

    /// Server cert usable for 127.0.0.1 and localhost.
    fn sign_server(&self) -> (String, String) {
        let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        params.subject_alt_names = vec![
            rcgen::SanType::DnsName("localhost".try_into().unwrap()),
            rcgen::SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        ];
        params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.ca_cert, &self.ca_key).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    fn sign_client(&self, uri: &str) -> (String, String) {
        let mut params = rcgen::CertificateParams::default();
        params.subject_alt_names = vec![rcgen::SanType::URI(uri.try_into().unwrap())];
        params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.ca_cert, &self.ca_key).unwrap();
        (cert.pem(), key.serialize_pem())
    }
}

fn write_0600(dir: &std::path::Path, name: &str, content: &str) -> String {
    let path = dir.join(name);
    std::fs::write(&path, content).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    path.to_string_lossy().to_string()
}

fn tls_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "lite-server-tls-e2e-{}-{}",
        tag,
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// ---------------------------------------------------------------------------
// HTTPS client helpers (reqwest + native-tls against the rustls server)
// ---------------------------------------------------------------------------

fn https_client(ca_pem: &str, identity_pem: Option<&str>) -> reqwest::Client {
    let mut builder = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(ca_pem.as_bytes()).expect("CA pem"));
    if let Some(id) = identity_pem {
        builder = builder.identity(reqwest::Identity::from_pem(id.as_bytes()).expect("identity"));
    }
    builder.build().expect("reqwest client")
}

async fn wait_for_https_server(client: &reqwest::Client, port: u16, timeout_secs: u64) {
    let url = format!("https://127.0.0.1:{}/health", port);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    while tokio::time::Instant::now() < deadline {
        if let Ok(resp) = client.get(&url).timeout(Duration::from_secs(1)).send().await {
            if resp.status() == 200 {
                return;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("HTTPS server did not start within {} seconds", timeout_secs);
}

async fn load_model_https(client: &reqwest::Client, base: &str, model: &str, version: &str) {
    let resp = client
        .post(format!(
            "{}/v2/repository/models/{}/versions/{}/load",
            base, model, version
        ))
        .send()
        .await
        .expect("load request failed");
    assert_eq!(resp.status(), 200, "load failed: {:?}", resp.text().await);

    let url = format!("{}/v2/models/{}/ready", base, model);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        if let Ok(resp) = client.get(&url).timeout(Duration::from_secs(1)).send().await {
            if let Ok(body) = resp.json::<Value>().await {
                if body["ready"].as_bool() == Some(true) {
                    return;
                }
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("model {} did not become ready", model);
}

/// test_model doubles the input (21 -> 42) over HTTPS.
async fn assert_https_infer(client: &reqwest::Client, base: &str) {
    let resp = client
        .post(format!("{}/v2/models/{}/infer", base, MODEL))
        .json(&json!({"input": 21}))
        .send()
        .await
        .expect("HTTPS infer failed");
    assert_eq!(resp.status(), 200, "infer status: {:?}", resp.text().await);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["output"], 42, "21 * 2 = 42: {:?}", body);
}

// ---------------------------------------------------------------------------
// gRPC TLS channel helpers
// ---------------------------------------------------------------------------

fn grpc_tls_config(pki: &TestPki, client: Option<(String, String)>) -> tonic::transport::ClientTlsConfig {
    let mut cfg = tonic::transport::ClientTlsConfig::new().ca_certificate(
        tonic::transport::Certificate::from_pem(&pki.ca_pem),
    );
    if let Some((cert, key)) = client {
        cfg = cfg.identity(tonic::transport::Identity::from_pem(cert, key));
    }
    cfg
}

async fn grpc_channel(
    port: u16,
    tls: tonic::transport::ClientTlsConfig,
) -> Result<tonic::transport::Channel, tonic::transport::Error> {
    tonic::transport::Endpoint::from_shared(format!("https://127.0.0.1:{}", port))
        .expect("grpc endpoint")
        .tls_config(tls)
        .expect("tls config")
        .connect()
        .await
}

/// Infer + Health over an established channel; test_model doubles 21 -> 42.
async fn assert_grpc_infer_and_health(channel: tonic::transport::Channel) {
    use lite_server::proto::liteserver::lite_server_client::LiteServerClient;
    use lite_server::proto::liteserver::InferRequest;
    use std::collections::HashMap;

    let mut health = tonic_health::pb::health_client::HealthClient::new(channel.clone());
    let hresp = health
        .check(tonic_health::pb::HealthCheckRequest { service: String::new() })
        .await
        .expect("Health.check over TLS must succeed")
        .into_inner();
    assert_eq!(hresp.status, tonic_health::ServingStatus::Serving as i32);

    let mut infer = LiteServerClient::new(channel);
    let resp = infer
        .infer(InferRequest {
            model_name: MODEL.to_string(),
            version: "1".to_string(),
            data: br#"{"input":21}"#.to_vec().into(),
            headers: HashMap::new(),
            sequence_id: None,
        })
        .await
        .expect("Infer over TLS must succeed")
        .into_inner();
    let body = String::from_utf8_lossy(&resp.data);
    assert!(body.contains("42"), "test_model doubles 21 -> 42; got: {}", body);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// 蓝图测试项: HTTP 单向 TLS /health+infer 正常; gRPC TLS Infer 正常
/// (含 grpc.health.v1)。一个服务器实例同时覆盖双侧, 并验证明文不再可用。
#[tokio::test]
#[serial]
async fn test_tls_one_way_http_and_grpc() {
    let http_port = 18120u16;
    let grpc_port = 18121u16;
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);

    let pki = TestPki::new("tls-e2e-ca");
    let dir = tls_dir("oneway");
    let (cert_pem, key_pem) = pki.sign_server();
    let cert = write_0600(&dir, "server.crt", &cert_pem);
    let key = write_0600(&dir, "server.key", &key_pem);
    let repo = test_model_repo();
    let yaml = dir.join("server.yaml");
    std::fs::write(
        &yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18122\n  log_level: warn\n  tls_cert_path: {cert}\n  tls_key_path: {key}\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\n  tls_cert_path: {cert}\n  tls_key_path: {key}\nmodel_repository:\n  path: {}\n",
            repo.to_string_lossy()
        ),
    )
    .unwrap();

    let _server = ServerGuard::start(&["--config", &yaml.to_string_lossy()]);
    let client = https_client(&pki.ca_pem, None);
    wait_for_https_server(&client, http_port, 20).await;
    let base = format!("https://127.0.0.1:{}", http_port);

    // HTTP: health + infer over one-way TLS.
    load_model_https(&client, &base, MODEL, "1").await;
    assert_https_infer(&client, &base).await;

    // Plaintext HTTP against the TLS port must fail.
    let plain = reqwest::Client::new();
    let plaintext = plain
        .get(format!("http://127.0.0.1:{}/health", http_port))
        .timeout(Duration::from_secs(2))
        .send()
        .await;
    assert!(plaintext.is_err(), "plaintext must not work on the TLS port");

    // gRPC: Infer + Health over one-way TLS.
    let channel = grpc_channel(grpc_port, grpc_tls_config(&pki, None))
        .await
        .expect("gRPC TLS connect");
    assert_grpc_infer_and_health(channel).await;

    let _ = std::fs::remove_dir_all(&dir);
}

/// 蓝图测试项: HTTP mTLS 带合法证书正常 / 无证书拒绝; gRPC mTLS 无客户端
/// 证书握手拒绝, 带证书 Infer 正常。
#[tokio::test]
#[serial]
async fn test_mtls_http_and_grpc() {
    let http_port = 18123u16;
    let grpc_port = 18124u16;
    kill_stale_on_port(http_port);
    kill_stale_on_port(grpc_port);

    let pki = TestPki::new("mtls-e2e-ca");
    let dir = tls_dir("mtls");
    let (cert_pem, key_pem) = pki.sign_server();
    let cert = write_0600(&dir, "server.crt", &cert_pem);
    let key = write_0600(&dir, "server.key", &key_pem);
    let ca = write_0600(&dir, "ca.crt", &pki.ca_pem);
    let repo = test_model_repo();
    let yaml = dir.join("server.yaml");
    std::fs::write(
        &yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: {grpc_port}\n  metrics_port: 18125\n  log_level: warn\n  tls_cert_path: {cert}\n  tls_key_path: {key}\n  mtls_ca_path: {ca}\nmetrics:\n  enabled: false\ngrpc:\n  enabled: true\n  tls_cert_path: {cert}\n  tls_key_path: {key}\n  mtls_ca_path: {ca}\nmodel_repository:\n  path: {}\n",
            repo.to_string_lossy()
        ),
    )
    .unwrap();

    let _server = ServerGuard::start(&["--config", &yaml.to_string_lossy()]);
    let (client_cert, client_key) = pki.sign_client("spiffe://e2e/client");
    let identity_pem = format!("{}{}", client_cert, client_key);
    let mtls_client = https_client(&pki.ca_pem, Some(&identity_pem));
    wait_for_https_server(&mtls_client, http_port, 20).await;
    let base = format!("https://127.0.0.1:{}", http_port);

    // HTTP: no client certificate → handshake rejected.
    let no_cert = https_client(&pki.ca_pem, None);
    let rejected = no_cert
        .get(format!("{}/health", base))
        .timeout(Duration::from_secs(3))
        .send()
        .await;
    assert!(rejected.is_err(), "mTLS must reject a client without a certificate");

    // HTTP: valid client certificate → health + infer.
    load_model_https(&mtls_client, &base, MODEL, "1").await;
    assert_https_infer(&mtls_client, &base).await;

    // gRPC: no client certificate → connection or first RPC must fail
    // (TLS 1.3 may surface the rejection at handshake or on first read).
    let no_cert_result = grpc_channel(grpc_port, grpc_tls_config(&pki, None)).await;
    match no_cert_result {
        Err(_) => {} // handshake-time rejection
        Ok(channel) => {
            let mut health = tonic_health::pb::health_client::HealthClient::new(channel);
            let first_rpc = health
                .check(tonic_health::pb::HealthCheckRequest { service: String::new() })
                .await;
            assert!(first_rpc.is_err(), "mTLS must reject the first RPC without a client cert");
        }
    }

    // gRPC: valid client certificate → Infer + Health.
    let channel = grpc_channel(grpc_port, grpc_tls_config(&pki, Some((client_cert, client_key))))
        .await
        .expect("gRPC mTLS connect with client cert");
    assert_grpc_infer_and_health(channel).await;

    let _ = std::fs::remove_dir_all(&dir);
}

/// 蓝图 D28: SIGHUP 触发热轮换——只信新 CA 的客户端在轮换前握手失败、
/// SIGHUP 后成功, 全程无重启。
#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_tls_cert_rotation_via_sighup() {
    let http_port = 18126u16;
    kill_stale_on_port(http_port);

    let old_pki = TestPki::new("rotation-old-ca");
    let new_pki = TestPki::new("rotation-new-ca");
    let dir = tls_dir("rotation");
    let (cert1, key1) = old_pki.sign_server();
    let cert = write_0600(&dir, "server.crt", &cert1);
    let key = write_0600(&dir, "server.key", &key1);
    let repo = test_model_repo();
    let yaml = dir.join("server.yaml");
    std::fs::write(
        &yaml,
        format!(
            "server:\n  host: 127.0.0.1\n  http_port: {http_port}\n  grpc_port: 18127\n  metrics_port: 18128\n  log_level: warn\n  tls_cert_path: {cert}\n  tls_key_path: {key}\nmetrics:\n  enabled: false\ngrpc:\n  enabled: false\nmodel_repository:\n  path: {}\n",
            repo.to_string_lossy()
        ),
    )
    .unwrap();

    let server = ServerGuard::start(&["--config", &yaml.to_string_lossy()]);
    let old_client = https_client(&old_pki.ca_pem, None);
    wait_for_https_server(&old_client, http_port, 20).await;

    // A client trusting only the NEW CA is rejected before rotation.
    let new_client = https_client(&new_pki.ca_pem, None);
    let before = new_client
        .get(format!("https://127.0.0.1:{}/health", http_port))
        .timeout(Duration::from_secs(2))
        .send()
        .await;
    assert!(before.is_err(), "new-CA client must fail before rotation");

    // Swap cert files to the new CA's cert, then SIGHUP.
    let (cert2, key2) = new_pki.sign_server();
    std::fs::write(&cert, &cert2).unwrap();
    std::fs::write(&key, &key2).unwrap();
    server.send_sighup();

    // The new cert must go live without a restart (reload is synchronous on
    // SIGHUP; poll briefly for the swap to land).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut rotated = false;
    while tokio::time::Instant::now() < deadline {
        if let Ok(resp) = new_client
            .get(format!("https://127.0.0.1:{}/health", http_port))
            .timeout(Duration::from_secs(1))
            .send()
            .await
        {
            if resp.status() == 200 {
                rotated = true;
                break;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(rotated, "SIGHUP must rotate the certificate without a restart");

    // The old CA's client must now fail (the server switched chains). Build a
    // FRESH client: the pre-rotation one may hold a pooled keep-alive
    // connection, which legitimately survives rotation (rotation applies to
    // new handshakes only).
    let stale_client = https_client(&old_pki.ca_pem, None);
    let after = stale_client
        .get(format!("https://127.0.0.1:{}/health", http_port))
        .timeout(Duration::from_secs(2))
        .send()
        .await;
    assert!(after.is_err(), "old-CA client must fail after rotation");

    let _ = std::fs::remove_dir_all(&dir);
}

/// 评审 2.4 守护: 依赖树中 rustls 必须只有单一版本(多版本会让 mTLS 的
/// rustls::ServerConfig 类型在 tonic/axum-server 路径间分裂)。
#[test]
fn test_rustls_single_version_in_tree() {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let output = Command::new(cargo)
        .args(["tree", "-i", "rustls", "--prefix", "none"])
        .current_dir(project_root())
        .output()
        .expect("cargo tree must run");
    assert!(output.status.success(), "cargo tree -i rustls failed: {}", String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut versions: Vec<&str> = stdout
        .lines()
        .filter_map(|l| l.strip_prefix("rustls v"))
        .map(|v| v.split_whitespace().next().unwrap())
        .collect();
    versions.sort_unstable();
    versions.dedup();
    assert_eq!(
        versions.len(),
        1,
        "exactly one rustls version must exist in the dependency tree (评审 2.4); found {:?}\n{}",
        versions,
        stdout
    );
}
