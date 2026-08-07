//! gRPC server reflection（评审低#12, opt-in）：`grpc.reflection: true` 时挂载
//! v1 reflection 服务，grpcurl/grpcui 免本地 proto 副本即可发现 LiteServer /
//! Admin / health。默认关闭。
//!
//! 挂 **Admin 访问类**——schema 元数据属 admin 面信息：fail-closed（未配置
//! `access_control` 时仅 loopback 可达），与 Admin service 同一暴露口径。
//!
//! 服务构建（`tonic_reflection::server::Builder`）内联在 `start_grpc_server`
//! 挂载点——`build_v1` 返回 `impl Trait`，无法在此为独立 helper 命名类型。

#[cfg(test)]
mod tests {
    //! 验收：grpc.reflection=true → 反射可枚举服务；默认 false → Unimplemented。
    use crate::callback::CallbackRunner;
    use crate::config::Config;
    use crate::grpc::start_grpc_server;
    use crate::inference_queue::InferenceQueue;
    use crate::registry::ModelRegistry;
    use crate::worker::WorkerManager;
    use std::sync::Arc;
    use std::time::Duration;
    use tonic::Status;
    use tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient;
    use tonic_reflection::pb::v1::server_reflection_request::MessageRequest;
    use tonic_reflection::pb::v1::server_reflection_response::MessageResponse;
    use tonic_reflection::pb::v1::ServerReflectionRequest;

    #[test]
    fn reflection_config_defaults_off_and_parses() {
        assert!(!crate::config::GrpcConfig::default().reflection);
        let parsed: crate::config::GrpcConfig =
            serde_yaml::from_str("reflection: true").unwrap();
        assert!(parsed.reflection);
    }

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .expect("bind ephemeral")
            .local_addr()
            .expect("local addr")
            .port()
    }

    /// 向 `port` 发一次 ListServices 反射查询。服务未挂载 → Err(Unimplemented)；
    /// 端口尚未就绪 → Err(Unavailable)（调用方据此重试）。
    async fn list_services(port: u16) -> Result<Vec<String>, Status> {
        let channel =
            tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
                .expect("valid endpoint")
                .connect()
                .await
                .map_err(|e| Status::unavailable(e.to_string()))?;
        let mut client = ServerReflectionClient::new(channel);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.send(ServerReflectionRequest {
            host: String::new(),
            message_request: Some(MessageRequest::ListServices(String::new())),
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        drop(tx);
        let mut stream = client
            .server_reflection_info(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await?
            .into_inner();
        let resp = stream.message().await?.expect("one reflection response");
        match resp.message_response {
            Some(MessageResponse::ListServicesResponse(list)) => {
                Ok(list.service.into_iter().map(|s| s.name).collect())
            }
            other => panic!("unexpected reflection response: {other:?}"),
        }
    }

    /// 以真实 `start_grpc_server` 起服务（loopback，全默认态 + reflection 开关）。
    async fn start_server(port: u16, reflection: bool) -> tokio::sync::oneshot::Sender<()> {
        let registry = Arc::new(ModelRegistry::new());
        let queue = Arc::new(InferenceQueue::new());
        let wm = Arc::new(WorkerManager::new(
            registry.clone(),
            std::env::temp_dir(),
            queue,
            "error".to_string(),
            Arc::new(CallbackRunner::new()),
        ));
        let mut config = Config::default();
        config.grpc.reflection = reflection;
        let grpc_config = config.grpc.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = start_grpc_server(
                crate::grpc::GrpcServerOptions {
                    host: "127.0.0.1".to_string(),
                    port,
                    registry,
                    worker_manager: wm,
                    streaming_metrics: false,
                    canary_override: false,
                    callback_runner: Arc::new(CallbackRunner::new()),
                    shutdown_state: Arc::new(crate::server::ShutdownState::new()),
                    server_timeout: Duration::from_secs(5),
                    grpc_config,
                    rate_limiter: Arc::new(crate::rate_limit::RateLimiter::default()),
                    tls: None,
                    config,
                    has_hot_reload: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                },
                rx,
            )
            .await;
        });
        tx
    }

    #[tokio::test]
    async fn reflection_lists_services_when_enabled() {
        let port = free_port();
        let shutdown = start_server(port, true).await;
        let mut names = None;
        for _ in 0..50 {
            match list_services(port).await {
                Ok(n) => {
                    names = Some(n);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
        let names = names.expect("reflection service must respond when enabled");
        for expected in ["liteserver.LiteServer", "liteserver.Admin", "grpc.health.v1.Health"] {
            assert!(
                names.iter().any(|n| n == expected),
                "{expected} must be listed; got {names:?}"
            );
        }
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn reflection_absent_by_default() {
        let port = free_port();
        let shutdown = start_server(port, false).await;
        let mut outcome = None;
        for _ in 0..50 {
            match list_services(port).await {
                Ok(n) => {
                    outcome = Some(Ok(n));
                    break;
                }
                Err(s) if s.code() == tonic::Code::Unimplemented => {
                    outcome = Some(Err(s));
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
        match outcome {
            Some(Err(s)) => assert_eq!(s.code(), tonic::Code::Unimplemented),
            Some(Ok(names)) => panic!("reflection must be absent by default, got {names:?}"),
            None => panic!("server never came up"),
        }
        let _ = shutdown.send(());
    }
}
