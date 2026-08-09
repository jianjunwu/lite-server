//! D27（蓝图 §4.2 P7-1）控制面结构化审计：gRPC Admin 与 HTTP admin 双侧共用。
//! 每条 mutation 记录 action/model/version/request_id/client_ip/principal/
//! key_fingerprint/details，target = `lite_server::audit`（独立 target，
//! EnvFilter 需用下划线形式，见 cdad02e）。

use crate::access_control::{AccessControl, EndpointClass};
use crate::callback::Protocol;
use crate::request_context::RequestContext;

/// Emit one structured audit record for a control-plane mutation.
///
/// - `cx`: T1 请求上下文（request_id/client_ip/mTLS principal）；直连单元
///   测试等未过中间件的场景传 None，字段置空。
/// - `ac`: 提供配置 key 的 SHA-256 指纹（`key_fingerprint` 字段）——归因
///   「用了哪把 key」而不落密钥本体；非 key 模式（public/loopback）为 None。
/// - `details`: 操作细节，含前后值（如 `previous_active=Some("1") -> 2`）。
pub fn control_plane(
    cx: Option<&RequestContext>,
    ac: &AccessControl,
    protocol: Protocol,
    action: &str,
    model: &str,
    version: Option<&str>,
    details: &str,
) {
    let (rid, ip, principal) = match cx {
        Some(c) => (c.request_id.as_str(), c.client_ip.as_str(), c.principal.as_deref()),
        None => ("", "", None),
    };
    tracing::info!(
        target: "lite_server::audit",
        action = action,
        model = %model,
        version = ?version,
        request_id = %rid,
        client_ip = %ip,
        principal = ?principal,
        key_fingerprint = ?ac.key_fingerprint(EndpointClass::Admin, protocol),
        details = %details,
        "admin control-plane mutation",
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AccessControlConfig, EndpointControl, ProtocolControl};
    use std::sync::{Arc, Mutex};

    /// 捕获 lite_server::audit 目标事件的字段（G3 同款：scoped dispatch +
    /// rebuild_interest_cache，防 callsite interest 缓存 NEVER 短路）。
    #[derive(Default)]
    struct Rec {
        fields: Vec<(String, String)>,
    }

    struct Layer(Arc<Mutex<Rec>>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Layer {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if event.metadata().target() != "lite_server::audit" {
                return;
            }
            struct V<'a>(&'a mut Vec<(String, String)>);
            impl tracing::field::Visit for V<'_> {
                fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                    self.0.push((f.name().to_string(), format!("{v:?}")));
                }
                fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
                    self.0.push((f.name().to_string(), v.to_string()));
                }
            }
            let mut v = V(&mut self.0.lock().unwrap().fields);
            event.record(&mut v);
        }
    }

    fn test_cx() -> RequestContext {
        RequestContext {
            request_id: "rid-1".to_string(),
            client_ip: "10.0.0.9".to_string(),
            trace_cx: opentelemetry::Context::new(),
            protocol: Protocol::Http,
            principal: Some("cn=admin".to_string()),
            api_protocol: None,
        }
    }

    fn key_ac() -> AccessControl {
        AccessControl::build(&AccessControlConfig {
            admin: ProtocolControl {
                http: Some(EndpointControl::Key {
                    key: "x-admin-key".to_string(),
                    value: Some("audit-secret".to_string()),
                    value_env: None,
                    value_file: None,
                }),
                grpc: None,
            },
            ..Default::default()
        })
        .unwrap()
    }

    #[test]
    fn control_plane_record_carries_all_d27_fields() {
        use tracing_subscriber::layer::SubscriberExt;
        let rec: Arc<Mutex<Rec>> = Default::default();
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(Layer(rec.clone())));
        let handle = std::thread::spawn(move || {
            let _guard = tracing::dispatcher::set_default(&dispatch);
            tracing::callsite::rebuild_interest_cache();
            control_plane(
                Some(&test_cx()),
                &key_ac(),
                Protocol::Http,
                "set_routing",
                "m1",
                None,
                "weights {\"1\": 70} -> {\"2\": 100}",
            );
        });
        handle.join().unwrap();

        let fields = &rec.lock().unwrap().fields;
        let get = |name: &str| {
            fields
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| panic!("缺字段 {name}: {fields:?}"))
        };
        assert_eq!(get("action"), "set_routing");
        assert_eq!(get("model"), "m1");
        assert_eq!(get("request_id"), "rid-1");
        assert_eq!(get("client_ip"), "10.0.0.9");
        assert!(get("principal").contains("cn=admin"));
        assert_eq!(get("details").trim_matches('"'), "weights {\"1\": 70} -> {\"2\": 100}");
        // D27 key 指纹：key 模式必有值且非明文
        let fp = get("key_fingerprint");
        assert!(fp.contains("Some("), "key 模式必须有指纹: {fp}");
        assert!(!fp.contains("audit-secret"), "指纹不得含密钥明文: {fp}");
    }

    #[test]
    fn control_plane_key_fingerprint_none_when_not_key_mode() {
        use tracing_subscriber::layer::SubscriberExt;
        let rec: Arc<Mutex<Rec>> = Default::default();
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(Layer(rec.clone())));
        let handle = std::thread::spawn(move || {
            let _guard = tracing::dispatcher::set_default(&dispatch);
            tracing::callsite::rebuild_interest_cache();
            control_plane(None, &AccessControl::default(), Protocol::Http, "load", "m1", Some("1"), "loaded");
        });
        handle.join().unwrap();
        let fields = &rec.lock().unwrap().fields;
        let fp = fields.iter().find(|(n, _)| n == "key_fingerprint").map(|(_, v)| v.clone());
        assert_eq!(fp.as_deref(), Some("None"), "非 key 模式指纹必须为 None: {fields:?}");
        // cx=None 时 request_id/client_ip 置空不 panic（上面 join 未炸即证）
    }
}
