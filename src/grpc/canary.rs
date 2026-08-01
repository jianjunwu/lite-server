//! Canary pin (`x-lite-version`) extraction/validation (P5-2, 蓝图 §4.4 D16)
//! and bidi version resolution (explicit > pin > routing_pick > active).

use super::error::err;
use crate::registry::ModelRegistry;
use std::collections::HashMap;
use tonic::metadata::MetadataMap;
use tonic::Status;

/// P5-2 (蓝图 §4.4, D16): 提取并校验 `x-lite-version` canary pin——metadata 优先，
/// fallback proto headers map（bidi 无 headers map，调用方传空 map → 仅 metadata）。
///
/// - `canary_override=false`（默认）→ `Ok(None)` + debug 日志：pin 完全不参与解析
///   （连非法值也不校验，与 HTTP 侧开关关行为一致）。
/// - 开关开：非法 pin → InvalidArgument（与 HTTP validate_version 同一守卫，B4
///   parity）；pin 版本未注册 → NotFound。
/// - pin 命中在当前 span 记 `pinned_version`（bidi 的 span 在解析后才创建，
///   由调用方自行 record）。
pub(super) fn canary_pin(
    registry: &ModelRegistry,
    canary_override: bool,
    model_name: &str,
    metadata: &MetadataMap,
    proto_headers: &HashMap<String, String>,
) -> Result<Option<String>, Status> {
    let pin = metadata
        .get("x-lite-version")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            proto_headers
                .get("x-lite-version")
                .filter(|s| !s.is_empty())
                .cloned()
        });
    let Some(pin) = pin else { return Ok(None) };
    if !canary_override {
        tracing::debug!(
            model = %model_name,
            pinned_version = %pin,
            "x-lite-version pin ignored (features.canary_override=false)"
        );
        return Ok(None);
    }
    if let Err(e) = crate::validation::validate_version(&pin) {
        return Err(err(Status::invalid_argument(e.to_string())));
    }
    if registry.get(model_name, Some(&pin)).is_none() {
        return Err(err(Status::not_found(format!(
            "{} version {} not found",
            model_name, pin
        ))));
    }
    tracing::Span::current().record("pinned_version", pin.as_str());
    Ok(Some(pin))
}

/// Resolve the serving version for `bidi_stream` (P0-2 parity with
/// unary/batch/stream): version="" → canary pin (P5-2, 开关开时由
/// [`canary_pin`] 提供) → weighted routing pick (§4.3), falling
/// back to the active version; explicit version passes through. Stamps
/// `last_used_at` for LRU eviction on the resolved version.
///
/// The protocol layer only passes parameters — the actual routing decision
/// is delegated to the registry (`routing_pick` / `get_active_version`).
pub(super) fn resolve_bidi_version(
    registry: &ModelRegistry,
    model_name: &str,
    version: Option<&str>,
    pin: Option<String>,
) -> Result<String, Status> {
    let resolved = match version {
        Some(v) => v.to_string(),
        None => match pin {
            Some(p) => p,
            None => registry
                .routing_pick(model_name)
                .or_else(|| registry.get_active_version(model_name))
                .ok_or_else(|| err(Status::not_found(format!("{} has no active version", model_name))))?,
        },
    };
    registry.touch_last_used(model_name, &resolved);
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ModelConfig;
    use crate::registry::types::ModelType;

    // --- P0-2: bidi version resolution parity (routing_pick + touch_last_used) ---

    fn bidi_test_registry() -> ModelRegistry {
        let reg = ModelRegistry::new();
        let dir = std::env::temp_dir().join(format!("lite-server-grpc-test-{}", std::process::id()));
        for v in ["1", "2"] {
            reg.register(
                "m1",
                v,
                ModelConfig { max_batch_size: 1, ..Default::default() },
                ModelType::LitAPI,
                dir.clone(),
            )
            .unwrap();
            reg.mark_ready("m1", v).unwrap();
        }
        reg
    }

    #[test]
    fn test_bidi_resolve_version_uses_weighted_routing() {
        let reg = bidi_test_registry();
        reg.activate_version("m1", "1").unwrap();
        // Weight 100/0 → deterministic pick of the weighted version, even
        // though "1" is the active version.
        reg.set_weights("m1", &HashMap::from([("1".into(), 0u32), ("2".into(), 100)]))
            .unwrap();

        let resolved = resolve_bidi_version(&reg, "m1", None, None).unwrap();
        assert_eq!(resolved, "2");
    }

    #[test]
    fn test_bidi_resolve_version_falls_back_to_active_without_routing() {
        let reg = bidi_test_registry();
        reg.activate_version("m1", "1").unwrap();

        let resolved = resolve_bidi_version(&reg, "m1", None, None).unwrap();
        assert_eq!(resolved, "1");
    }

    #[test]
    fn test_bidi_resolve_version_touches_last_used() {
        let reg = bidi_test_registry();
        reg.activate_version("m1", "1").unwrap();
        assert!(reg.get("m1", Some("1")).unwrap().last_used_at.is_none());

        let resolved = resolve_bidi_version(&reg, "m1", None, None).unwrap();
        assert_eq!(
            reg.get("m1", Some(&resolved)).unwrap().last_used_at.is_some(),
            true,
            "bidi version resolution must stamp last_used_at like unary/batch/stream"
        );
    }

    #[test]
    fn test_bidi_resolve_version_explicit_passthrough() {
        let reg = bidi_test_registry();
        reg.activate_version("m1", "1").unwrap();

        // Explicit version bypasses routing/active resolution entirely.
        let resolved = resolve_bidi_version(&reg, "m1", Some("2"), None).unwrap();
        assert_eq!(resolved, "2");
    }

    // --- P5-2: canary_override 开关 + x-lite-version pin（蓝图 §4.4, D16）---

    fn canary_metadata(pin: &str) -> MetadataMap {
        let mut md = MetadataMap::new();
        md.insert("x-lite-version", pin.parse().unwrap());
        md
    }

    #[test]
    fn test_canary_pin_absent_is_none() {
        let reg = bidi_test_registry();
        let pin = canary_pin(&reg, true, "m1", &MetadataMap::new(), &HashMap::new()).unwrap();
        assert_eq!(pin, None);
    }

    #[test]
    fn test_canary_pin_prefers_metadata_over_proto_headers() {
        let reg = bidi_test_registry();
        let headers = HashMap::from([("x-lite-version".to_string(), "2".to_string())]);
        let pin = canary_pin(&reg, true, "m1", &canary_metadata("1"), &headers).unwrap();
        assert_eq!(pin.as_deref(), Some("1"), "metadata 优先于 proto headers map");
    }

    #[test]
    fn test_canary_pin_falls_back_to_proto_headers() {
        let reg = bidi_test_registry();
        let headers = HashMap::from([("x-lite-version".to_string(), "2".to_string())]);
        let pin = canary_pin(&reg, true, "m1", &MetadataMap::new(), &headers).unwrap();
        assert_eq!(pin.as_deref(), Some("2"));
    }

    #[test]
    fn test_canary_pin_switch_off_ignores_pin() {
        let reg = bidi_test_registry();
        let pin = canary_pin(&reg, false, "m1", &canary_metadata("1"), &HashMap::new()).unwrap();
        assert_eq!(pin, None, "canary_override=false → pin 被忽略");
        // 非法 pin 在开关关时同样不校验、不报错。
        let pin = canary_pin(&reg, false, "m1", &canary_metadata("a b"), &HashMap::new()).unwrap();
        assert_eq!(pin, None);
    }

    #[test]
    fn test_canary_pin_invalid_is_invalid_argument() {
        let reg = bidi_test_registry();
        let err = canary_pin(&reg, true, "m1", &canary_metadata("a b"), &HashMap::new()).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_canary_pin_unknown_version_is_not_found() {
        let reg = bidi_test_registry();
        let err = canary_pin(&reg, true, "m1", &canary_metadata("9"), &HashMap::new()).unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[test]
    fn test_bidi_resolve_version_pin_beats_weights() {
        let reg = bidi_test_registry();
        reg.activate_version("m1", "1").unwrap();
        reg.set_weights("m1", &HashMap::from([("1".into(), 100u32)])).unwrap();

        let resolved = resolve_bidi_version(&reg, "m1", None, Some("2".to_string())).unwrap();
        assert_eq!(resolved, "2", "pin（开关开）优先级高于 routing_pick");
    }

    #[test]
    fn test_bidi_resolve_version_explicit_beats_pin() {
        let reg = bidi_test_registry();
        let resolved = resolve_bidi_version(&reg, "m1", Some("1"), Some("2".to_string())).unwrap();
        assert_eq!(resolved, "1", "显式 version 优先级高于 pin");
    }
}
