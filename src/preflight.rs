//! 启动配置预检（蓝图 §6.2，D30 配套交付物——替代 deprecated 开关的硬切换保护）。
//! 检测三类旧配置形态并给出迁移告警（warn 级，点名 `docs/migration.md` 条目）：
//! ① 多 origin CORS（M2：旧版非法 join 单值 ACAO → 新版按请求 Origin 精确匹配其一）；
//! ② 限流 `key=ip` 但 `server.trusted_proxies` 为空（M1：P-XFF 后默认不再信任
//!    客户端 XFF/X-Real-IP，ip 维度限流按直连 peer addr 计桶）；
//! ③ 远程 admin 依赖「绑定即开放」（M4：P7-1 后未配置 access_control 的 admin
//!    仅 loopback 可达）。
//! 全部为 warn（非 fail-fast）：这些形态在新语义下仍能工作，只是行为已变。

use crate::access_control::AccessControl;
use crate::config::{unix_socket_path, Config, ModelConfig};
use std::net::IpAddr;
use std::path::Path;

/// 运行预检，返回迁移告警列表（空 = 未发现旧配置形态）。
pub fn startup_preflight(cfg: &Config) -> Vec<String> {
    let mut warnings = Vec::new();
    check_cors_multi_origin(cfg, &mut warnings);
    check_ip_rate_limit_without_trusted_proxies(cfg, &mut warnings);
    check_remote_admin_bind_open(cfg, &mut warnings);
    warnings
}

/// ① 多 origin CORS（server.cors 全局 + 逐版本 model policies.cors）。
fn check_cors_multi_origin(cfg: &Config, warnings: &mut Vec<String>) {
    let mut hits: Vec<String> = Vec::new();
    if cfg.server.cors.as_ref().is_some_and(|c| c.allow_origins.len() > 1) {
        hits.push("server.cors".to_string());
    }
    for (model, version, mc) in load_model_configs(cfg) {
        if mc.policies.cors.as_ref().is_some_and(|c| c.allow_origins.len() > 1) {
            hits.push(format!("{model}/{version} policies.cors"));
        }
    }
    if !hits.is_empty() {
        warnings.push(format!(
            "M2: 多 origin CORS 语义已变（旧：多 origin 非法 join 为单值 ACAO；新：按请求 \
             Origin 精确匹配其一）——命中: {}。见 docs/migration.md#M2",
            hits.join(", ")
        ));
    }
}

/// ② 限流 key=ip + trusted_proxies 为空。
fn check_ip_rate_limit_without_trusted_proxies(cfg: &Config, warnings: &mut Vec<String>) {
    if !cfg.server.trusted_proxies.is_empty() {
        return;
    }
    let hits: Vec<String> = load_model_configs(cfg)
        .into_iter()
        .filter(|(_, _, mc)| mc.policies.rate_limit.as_ref().is_some_and(|rl| rl.key == "ip"))
        .map(|(m, v, _)| format!("{m}/{v}"))
        .collect();
    if !hits.is_empty() {
        warnings.push(format!(
            "M1: 限流 key=ip 但 server.trusted_proxies 为空——P-XFF 后默认不再信任客户端 \
             XFF/X-Real-IP，ip 限流将按直连 peer addr 计桶；前置网关部署请配置 \
             trusted_proxies——命中: {}。见 docs/migration.md#M1",
            hits.join(", ")
        ));
    }
}

/// ③ 绑定非 loopback 且 admin 未配置 access_control（旧「绑定即开放」失效）。
fn check_remote_admin_bind_open(cfg: &Config, warnings: &mut Vec<String>) {
    let grpc_host = crate::grpc::resolve_grpc_host(cfg.grpc.host.as_deref(), &cfg.server.host);
    if !bind_is_remote(&cfg.server.host) && !bind_is_remote(&grpc_host) {
        return;
    }
    // AccessControl::build 失败（坏配置）由主路径报告，预检不重复。
    let Ok(ac) = AccessControl::build(&cfg.access_control) else {
        return;
    };
    if ac.admin_denies_non_loopback() {
        warnings.push(
            "M4: 绑定非 loopback 且未配置 access_control.admin——P7-1 后远程 admin 不再\
             「绑定即开放」（仅 loopback 可达）；远程 admin 请配 access_control.admin 或 \
             grpc.admin_bind（UDS），Prometheus 抓取改用 metrics_port。见 docs/migration.md#M4"
                .to_string(),
        );
    }
}

/// 判定绑定地址是否远程可达（非 loopback TCP）。UDS 判 false；主机名等无法
/// 解析为 IP 的形态保守判 false——宁可漏报不误报。
fn bind_is_remote(host: &str) -> bool {
    if unix_socket_path(host).is_some() {
        return false;
    }
    bind_ip(host).is_some_and(|ip| !ip.is_loopback())
}

fn bind_ip(host: &str) -> Option<IpAddr> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Some(ip);
    }
    if let Ok(sa) = host.parse::<std::net::SocketAddr>() {
        return Some(sa.ip());
    }
    // [v6]:port / host:port 形态兜底
    if let Some((h, _)) = host.rsplit_once(':') {
        return h.trim_start_matches('[').trim_end_matches(']').parse::<IpAddr>().ok();
    }
    None
}

/// 扫描 model repo（<repo>/<model>/<version>/config.yaml 两级结构，与
/// reconcile 一致）加载全部模型配置；IO/解析失败静默跳过（主路径会再报）。
fn load_model_configs(cfg: &Config) -> Vec<(String, String, ModelConfig)> {
    let mut out = Vec::new();
    let repo = Path::new(&cfg.model_repository.path);
    let Ok(models) = std::fs::read_dir(repo) else {
        return out;
    };
    for model in models.flatten() {
        let Ok(versions) = std::fs::read_dir(model.path()) else {
            continue;
        };
        for version in versions.flatten() {
            let config_path = version.path().join("config.yaml");
            if !config_path.is_file() {
                continue;
            }
            if let Ok(mc) = crate::config::load_model_config(&config_path) {
                out.push((
                    model.file_name().to_string_lossy().into_owned(),
                    version.file_name().to_string_lossy().into_owned(),
                    mc,
                ));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CorsPolicy;
    use std::fs;
    use std::path::Path;

    fn test_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("lite-server-preflight-{name}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_model(repo: &Path, model: &str, version: &str, yaml: &str) {
        let dir = repo.join(model).join(version);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("config.yaml"), yaml).unwrap();
    }

    fn loopback_cfg(repo: &Path) -> Config {
        let mut cfg = Config::default();
        cfg.server.host = "127.0.0.1".to_string(); // loopback → ③ 不触发
        cfg.model_repository.path = repo.to_string_lossy().into_owned();
        cfg
    }

    #[test]
    fn clean_config_has_no_warnings() {
        let repo = test_dir("clean");
        let cfg = loopback_cfg(&repo);
        assert!(startup_preflight(&cfg).is_empty());
    }

    #[test]
    fn warns_multi_origin_cors_server_and_model() {
        let repo = test_dir("cors");
        let mut cfg = loopback_cfg(&repo);
        cfg.server.cors = Some(CorsPolicy {
            allow_origins: vec!["https://a.example".into(), "https://b.example".into()],
            ..Default::default()
        });
        write_model(
            &repo,
            "m",
            "1",
            "name: m\npolicies:\n  cors:\n    allow_origins: [\"https://a.example\", \"https://b.example\"]\n",
        );
        let warnings = startup_preflight(&cfg);
        let m2: Vec<_> = warnings.iter().filter(|w| w.starts_with("M2:")).collect();
        assert_eq!(m2.len(), 1, "全局+模型两处多 origin 应合并为一条 M2: {warnings:?}");
        assert!(m2[0].contains("server.cors") && m2[0].contains("m/1"), "须点名命中位置: {m2:?}");
    }

    #[test]
    fn single_origin_cors_does_not_warn() {
        let repo = test_dir("cors-single");
        let mut cfg = loopback_cfg(&repo);
        cfg.server.cors = Some(CorsPolicy {
            allow_origins: vec!["https://a.example".into()],
            ..Default::default()
        });
        assert!(startup_preflight(&cfg).iter().all(|w| !w.starts_with("M2:")));
    }

    #[test]
    fn warns_ip_rate_limit_only_without_trusted_proxies() {
        let repo = test_dir("rl");
        let mut cfg = loopback_cfg(&repo);
        write_model(&repo, "m", "1", "name: m\npolicies:\n  rate_limit:\n    requests_per_minute: 100\n    key: ip\n");
        // 空 trusted_proxies → M1 且点名模型
        let warnings = startup_preflight(&cfg);
        assert!(
            warnings.iter().any(|w| w.starts_with("M1:") && w.contains("m/1")),
            "key=ip + 空 trusted_proxies 应告警: {warnings:?}"
        );
        // 配了 trusted_proxies → 无 M1
        cfg.server.trusted_proxies = vec!["10.0.0.0/8".to_string()];
        assert!(startup_preflight(&cfg).iter().all(|w| !w.starts_with("M1:")));
    }

    #[test]
    fn route_key_rate_limit_does_not_warn() {
        let repo = test_dir("rl-route");
        let mut cfg = loopback_cfg(&repo);
        write_model(&repo, "m", "1", "name: m\npolicies:\n  rate_limit:\n    requests_per_minute: 100\n    key: route\n");
        assert!(startup_preflight(&cfg).iter().all(|w| !w.starts_with("M1:")));
    }

    #[test]
    fn warns_remote_admin_bind_open() {
        let repo = test_dir("admin");
        let mut cfg = Config::default(); // host 默认 0.0.0.0 = 远程可达
        cfg.model_repository.path = repo.to_string_lossy().into_owned();
        assert!(
            startup_preflight(&cfg).iter().any(|w| w.starts_with("M4:")),
            "远程绑定 + 未配置 access_control 应告警（旧「绑定即开放」失效）"
        );
        // loopback 绑定 → 无 M4
        cfg.server.host = "127.0.0.1".to_string();
        assert!(startup_preflight(&cfg).iter().all(|w| !w.starts_with("M4:")));
    }

    #[test]
    fn no_m4_when_admin_access_control_configured() {
        let repo = test_dir("admin-ac");
        let mut cfg = Config::default(); // 0.0.0.0 远程
        cfg.model_repository.path = repo.to_string_lossy().into_owned();
        cfg.access_control.admin.http = Some(crate::config::EndpointControl::Public);
        cfg.access_control.admin.grpc = Some(crate::config::EndpointControl::Public);
        assert!(startup_preflight(&cfg).iter().all(|w| !w.starts_with("M4:")));
    }

    #[test]
    fn uds_bind_does_not_warn_m4() {
        let repo = test_dir("admin-uds");
        let mut cfg = Config::default();
        cfg.server.host = "unix:/tmp/lite-server-preflight.sock".to_string();
        cfg.model_repository.path = repo.to_string_lossy().into_owned();
        assert!(startup_preflight(&cfg).iter().all(|w| !w.starts_with("M4:")));
    }
}
