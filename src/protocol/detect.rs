//! 协议检测(D11 P2.1,批次 2):T1 预筛(header,零成本)+ T2 信封主判(请求体)。
//!
//! 时序规则(C9):`Inference-Header-Content-Length` 存在即 T1 直接定
//! `Kserve`(header 是强信号,不等 T2 信封判定)——extractor 期 400/413/415
//! (发生在 T2 之前)对 Triton 客户端也渲染扁平错误体;Triton 二进制/Raw
//! 路径零 T2 成本(T1 短路)。

use super::ApiProtocol;
use axum::http::HeaderMap;

/// T1 预筛:强信号立即定协议。
/// - `Inference-Header-Content-Length` 存在(含 "0")→ Kserve(C9);
/// - 未来批次 5:/v1/ 前缀 → OpenaiCompact(openai-compact 立项时加);
/// - 其他 → None(T2 主判在 run_infer 边界)。
pub(crate) fn t1_prefilter(path: &str, headers: &HeaderMap) -> Option<ApiProtocol> {
    let _ = path; // 批次 5 /v1/ 前缀用
    if headers.contains_key(crate::http::handlers::INFERENCE_HEADER_CONTENT_LENGTH) {
        return Some(ApiProtocol::Kserve);
    }
    None
}

/// T2 信封双条件判定(D5):顶层 `inputs` 非空数组 + 每元素有
/// name/shape/datatype——防自有 schema 撞名(项目现有 JSON 是自由 schema,
/// 单条件会误触发)。解析失败 → false。
///
/// 部分反序列化(近零分配):仅取 inputs 的结构探针,不物化 DOM
/// (bench_t2_envelope_check 门禁 < 2× RawValue)。
pub(crate) fn t2_kserve_envelope(body: &[u8]) -> bool {
    let Ok(head) = serde_json::from_slice::<EnvelopeProbe>(body) else {
        return false;
    };
    !head.inputs.is_empty() && head.inputs.iter().all(|i| i.is_complete())
}

/// T2 探针:只关心 inputs 非空 + 每元素三字段存在。
#[derive(serde::Deserialize)]
struct EnvelopeProbe<'a> {
    #[serde(borrow, default)]
    inputs: Vec<EnvelopeInputProbe<'a>>,
}

#[derive(serde::Deserialize)]
struct EnvelopeInputProbe<'a> {
    #[serde(borrow, default)]
    name: Option<&'a serde_json::value::RawValue>,
    #[serde(default)]
    shape: Option<&'a serde_json::value::RawValue>,
    #[serde(default)]
    datatype: Option<&'a serde_json::value::RawValue>,
}

impl<'a> EnvelopeInputProbe<'a> {
    fn is_complete(&self) -> bool {
        self.name.is_some() && self.shape.is_some() && self.datatype.is_some()
    }
}

/// resolve:T1 有 → T1(强信号,T2 天然同意);T1 无 → T2 命中 → Kserve;
/// 否则 Legacy。
pub(crate) fn resolve(t1: Option<ApiProtocol>, body: &[u8]) -> ApiProtocol {
    match t1 {
        Some(p) => p,
        None => {
            if t2_kserve_envelope(body) {
                ApiProtocol::Kserve
            } else {
                ApiProtocol::Legacy
            }
        }
    }
}
