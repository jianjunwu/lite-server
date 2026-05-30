use serial_test::serial;
use std::process::{Command, Stdio};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[cfg(unix)]
use tokio::net::UnixStream;

fn lite_server_bin() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("debug")
        .join("lite-server-core")
}

fn ensure_model_repo() -> std::path::PathBuf {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("model_repo");
    std::fs::create_dir_all(&path).ok();
    path
}

#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_unix_socket_server() {
    let bin = lite_server_bin();
    let socket_path = "/tmp/lite-server-uds-test.sock";
    let _ = std::fs::remove_file(socket_path);

    let repo = ensure_model_repo();
    let mut child = Command::new(&bin)
        .arg("serve")
        .arg("--host")
        .arg(format!("unix:{}", socket_path))
        .arg("--model-repo")
        .arg(&repo)
        .arg("--no-metrics")
        .arg("--no-grpc")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to start server");

    // Wait for socket (endpoint worker startup can take ~4s)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !std::path::Path::new(socket_path).exists() {
        if tokio::time::Instant::now() > deadline {
            let _ = child.kill();
            panic!("Server did not create socket within 20 seconds");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Send HTTP request via UDS
    let mut stream = UnixStream::connect(socket_path).await.unwrap();
    stream
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();

    let mut buf = vec![0u8; 1024];
    let n = stream.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]).to_lowercase();

    assert!(
        response.contains("200 ok"),
        "Expected 200 OK, got: {}",
        response
    );
    assert!(
        response.contains("ok"),
        "Expected 'ok' body, got: {}",
        response
    );

    // Cleanup
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_file(socket_path);
}

#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_keepalive_disabled_via_connection_close() {
    let bin = lite_server_bin();
    let socket_path = "/tmp/lite-server-ka-test.sock";
    let _ = std::fs::remove_file(socket_path);

    let repo = ensure_model_repo();
    let mut child = Command::new(&bin)
        .arg("serve")
        .arg("--host")
        .arg(format!("unix:{}", socket_path))
        .arg("--keepalive-timeout")
        .arg("0")
        .arg("--model-repo")
        .arg(&repo)
        .arg("--no-metrics")
        .arg("--no-grpc")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to start server");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !std::path::Path::new(socket_path).exists() {
        if tokio::time::Instant::now() > deadline {
            let _ = child.kill();
            panic!("Server did not create socket within 20 seconds");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Send HTTP/1.1 request (which defaults to keep-alive)
    let mut stream = UnixStream::connect(socket_path).await.unwrap();
    stream
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();

    let mut buf = vec![0u8; 1024];
    let n = stream.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]).to_lowercase();

    assert!(
        response.contains("connection: close"),
        "Expected 'connection: close' header when keep-alive is disabled, got: {}",
        response
    );
    assert!(
        response.contains("200 ok"),
        "Expected 200 OK, got: {}",
        response
    );

    // Cleanup
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_file(socket_path);
}

#[cfg(unix)]
#[tokio::test]
#[serial]
async fn test_graceful_shutdown_respects_timeout() {
    let bin = lite_server_bin();
    let socket_path = "/tmp/lite-server-graceful-test.sock";
    let _ = std::fs::remove_file(socket_path);

    let repo = ensure_model_repo();
    let mut child = Command::new(&bin)
        .arg("serve")
        .arg("--host")
        .arg(format!("unix:{}", socket_path))
        .arg("--graceful-timeout")
        .arg("1")
        .arg("--model-repo")
        .arg(&repo)
        .arg("--no-metrics")
        .arg("--no-grpc")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to start server");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !std::path::Path::new(socket_path).exists() {
        if tokio::time::Instant::now() > deadline {
            let _ = child.kill();
            panic!("Server did not create socket within 20 seconds");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send SIGTERM
    let pid = child.id() as i32;
    unsafe { libc::kill(pid, libc::SIGTERM) };

    // Server should exit within 3 seconds (1s graceful + overhead)
    let start = std::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        let _ = child.wait();
    })
    .await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Server did not exit within 5 seconds");
    assert!(
        elapsed < Duration::from_secs(3),
        "Server took too long to exit: {:?}",
        elapsed
    );

    let _ = std::fs::remove_file(socket_path);
}
