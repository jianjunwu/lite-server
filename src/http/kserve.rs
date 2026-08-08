//! KServe V2 信封 codec(阶段 2,D5/D6/D7)。
//!
//! - 请求特征检测(D5):`binary_data_output` / `outputs[].binary_data` flag +
//!   双条件防御(信封形状,防自有 schema 撞名);
//! - 响应转换(D6):worker JSON 信封 → JSON 头 + 二进制尾 +
//!   `Inference-Header-Content-Length`(datatype 表 D7)。
//!
//! 仅 KServe-mode 请求(T1/T2 定 `ApiProtocol::Kserve`)触发,默认关——非
//! KServe 请求零成本、byte-identical。

use crate::error::AppError;
use crate::http::handlers::{RequestBody, INFERENCE_HEADER_CONTENT_LENGTH};
use axum::response::Response;
use serde_json::{json, Value};

/// 响应转换的 body 读取上限(worker 输出已受 proto 层限制,此值仅护栏)。
const CONVERT_BODY_LIMIT: usize = 512 * 1024 * 1024;

/// 请求特征(D5):binary_data_output 请求。
#[derive(Debug)]
pub(crate) struct BinaryOutputRequest {
    /// 请求 id 回显(无则省略)。
    pub id: Option<String>,
    pub model_name: String,
    /// 有版本路由时填、无则省略(规范允许)。
    pub model_version: Option<String>,
    /// None = 全部输出二进制化(顶层 `binary_data_output: true`);
    /// Some = 按名覆盖(`outputs[].parameters.binary_data: true`)。
    pub binary_outputs: Option<Vec<String>>,
}

/// 解析请求特征(D5):信封双条件(顶层 inputs 非空数组 + 每元素
/// name/shape/datatype——与 `detect::t2_kserve_envelope` 同源)+ flag。
/// - TritonBinary 请求解析 JSON 头(Triton 客户端 flag 在头内);
/// - Raw(无 header 的裸二进制)→ None;
/// - 非信封(自有 schema 撞名)→ None(双条件防御)。
pub(crate) fn parse_binary_output_request(
    body: &RequestBody,
    model_name: &str,
    model_version: Option<String>,
) -> Option<BinaryOutputRequest> {
    let json_head: &[u8] = match body {
        RequestBody::Json(b) => b.as_ref(),
        RequestBody::TritonBinary { body, json_head_len } => &body[..*json_head_len],
        RequestBody::Raw(..) => return None,
    };
    // 双条件防御:非信封形状(含解析失败)一律不启用(D5)。
    if !crate::protocol::detect::t2_kserve_envelope(json_head) {
        return None;
    }
    let Ok(v) = serde_json::from_slice::<Value>(json_head) else {
        return None;
    };
    // 顶层 flag:parameters.binary_data_output == true → 全部输出二进制化。
    let top_flag = v
        .get("parameters")
        .and_then(|p| p.get("binary_data_output"))
        .and_then(|b| b.as_bool())
        .unwrap_or(false);
    // 按名覆盖:outputs[].parameters.binary_data == true。
    let per_output: Vec<String> = v
        .get("outputs")
        .and_then(|o| o.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|o| {
                    let name = o.get("name")?.as_str()?;
                    let bin = o.get("parameters")?.get("binary_data")?.as_bool()?;
                    bin.then(|| name.to_string())
                })
                .collect()
        })
        .unwrap_or_default();
    if !top_flag && per_output.is_empty() {
        return None;
    }
    let id = v.get("id").and_then(|i| i.as_str()).map(String::from);
    Some(BinaryOutputRequest {
        id,
        model_name: model_name.to_string(),
        model_version,
        binary_outputs: if top_flag { None } else { Some(per_output) },
    })
}

/// SSE 边界(D10):仅判定请求是否带 binary 输出 flag(双条件)——SSE 是
/// 文本通道,该组合 400;WS/h2 bidi 自有协议承载二进制流式。
pub(crate) fn request_binary_output_flag(body: &RequestBody) -> bool {
    parse_binary_output_request(body, "", None).is_some()
}

/// 响应转换(D6):worker JSON 信封 + 请求 flag → JSON 头 + 二进制尾 +
/// `Inference-Header-Content-Length`。非 JSON worker 输出(G21:media_type
/// 优先)与非 2xx(worker 错误响应)原样返回。
pub(crate) async fn convert_response(
    resp: Response,
    req: &BinaryOutputRequest,
) -> Result<Response, AppError> {
    // G21:worker 显式非 JSON media_type 优先于请求 flag(此句即上游 §9.2
    // 「media_type 优先级届时定义」的裁定)。
    let saved_headers = resp.headers().clone();
    let status = resp.status();
    let ct = saved_headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    if !ct.starts_with("application/json") || !status.is_success() {
        return Ok(resp);
    }
    let body = axum::body::to_bytes(resp.into_body(), CONVERT_BODY_LIMIT)
        .await
        .map_err(|e| AppError::Internal(format!("read worker response: {e}")))?;
    let value: Value = serde_json::from_slice(&body).map_err(|_| {
        AppError::InvalidRequestBody(
            "binary_data_output requested but response is not a KServe envelope".to_string(),
        )
    })?;
    let Some(outputs) = value.get("outputs").and_then(|o| o.as_array()) else {
        return Err(AppError::InvalidRequestBody(
            "binary_data_output requested but response is not a KServe envelope".to_string(),
        ));
    };

    let mut new_outputs = Vec::with_capacity(outputs.len());
    let mut binary_tail: Vec<u8> = Vec::new();
    for out in outputs {
        let name = out.get("name").and_then(|n| n.as_str()).unwrap_or("");
        let hit = match &req.binary_outputs {
            None => true,
            Some(names) => names.iter().any(|n| n == name),
        };
        if !hit {
            new_outputs.push(out.clone());
            continue;
        }
        // 转换必需信息,不猜:缺 datatype/shape/data → 400(D6)。
        let datatype = out.get("datatype").and_then(|d| d.as_str()).ok_or_else(|| {
            AppError::InvalidRequestBody(format!(
                "binary_data_output: output {name} is missing datatype"
            ))
        })?;
        if out.get("shape").is_none() {
            return Err(AppError::InvalidRequestBody(format!(
                "binary_data_output: output {name} is missing shape"
            )));
        }
        let data = out.get("data").and_then(|d| d.as_array()).ok_or_else(|| {
            AppError::InvalidRequestBody(format!(
                "binary_data_output: output {name} is missing a JSON data array"
            ))
        })?;
        let mut block = Vec::new();
        for el in data {
            block.extend(encode_value(datatype, el)?);
        }
        binary_tail.extend_from_slice(&block);
        // JSON 头重写:删 data + parameters.binary_data_size(混合输出时
        // 未命中者保留原 data——tail 顺序 = outputs[] 声明顺序中命中者)。
        let mut new_out = out.clone();
        if let Value::Object(m) = &mut new_out {
            m.remove("data");
            let params = m.entry("parameters").or_insert_with(|| json!({}));
            if let Value::Object(p) = params {
                p.insert("binary_data_size".into(), json!(block.len()));
            }
        }
        new_outputs.push(new_out);
    }

    // 响应 JSON 头:model_name 从路由解析;model_version 有则填;id 回显。
    let mut head = serde_json::Map::new();
    head.insert("model_name".into(), json!(req.model_name));
    if let Some(v) = &req.model_version {
        head.insert("model_version".into(), json!(v));
    }
    if let Some(id) = &req.id {
        head.insert("id".into(), json!(id));
    }
    head.insert("outputs".into(), json!(new_outputs));
    let head_bytes = serde_json::to_vec(&Value::Object(head))
        .map_err(AppError::Serialization)?;
    let head_len = head_bytes.len();

    let mut full = head_bytes;
    full.extend_from_slice(&binary_tail);

    let mut builder = Response::builder()
        .status(status)
        .header(axum::http::header::CONTENT_TYPE, "application/octet-stream")
        .header(INFERENCE_HEADER_CONTENT_LENGTH, head_len);
    // 保留 worker 注入的响应 header(除被重写的 content-type/length)。
    for (k, v) in saved_headers.iter() {
        let lower = k.as_str().to_ascii_lowercase();
        if lower == "content-type" || lower == "content-length" {
            continue;
        }
        builder = builder.header(k.clone(), v.clone());
    }
    builder
        .body(axum::body::Body::from(full))
        .map_err(|e| AppError::Internal(format!("build response: {e}")))
}

/// 单元素编码(D6/D7):数值类型按 datatype itemsize 小端;BYTES = 4B LE
/// 长度前缀 + UTF-8;BOOL = 1 字节。JSON 侧对 BYTES 的元素是字符串。
fn encode_value(datatype: &str, v: &Value) -> Result<Vec<u8>, AppError> {
    match datatype {
        "BOOL" => Ok(vec![if boolish(v) { 1 } else { 0 }]),
        "INT8" => int_le::<1>(v),
        "UINT8" => uint_le::<1>(v),
        "INT16" => int_le::<2>(v),
        "UINT16" => uint_le::<2>(v),
        "INT32" => int_le::<4>(v),
        "UINT32" => uint_le::<4>(v),
        "INT64" => int_le::<8>(v),
        "UINT64" => uint_le::<8>(v),
        "FP16" => Ok(f64_to_f16_bits(float_of(v)?.into()).to_le_bytes().to_vec()),
        "FP32" => Ok(float_of(v)?.to_le_bytes().to_vec()),
        "FP64" => Ok(float_of64(v)?.to_le_bytes().to_vec()),
        "BF16" => Ok(f64_to_bf16_bits(float_of(v)?.into()).to_le_bytes().to_vec()),
        "BYTES" => {
            let s = v.as_str().ok_or_else(|| {
                AppError::InvalidRequestBody(
                    "binary_data_output: BYTES data elements must be JSON strings".to_string(),
                )
            })?;
            let content = s.as_bytes();
            let mut out = Vec::with_capacity(4 + content.len());
            out.extend_from_slice(&(content.len() as u32).to_le_bytes());
            out.extend_from_slice(content);
            Ok(out)
        }
        other => Err(AppError::InvalidRequestBody(format!(
            "binary_data_output: unknown datatype {other}"
        ))),
    }
}

fn boolish(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        _ => false,
    }
}

fn float_of(v: &Value) -> Result<f32, AppError> {
    v.as_f64()
        .map(|f| f as f32)
        .ok_or_else(|| {
            AppError::InvalidRequestBody(
                "binary_data_output: data element is not a JSON number".to_string(),
            )
        })
}

fn float_of64(v: &Value) -> Result<f64, AppError> {
    v.as_f64().ok_or_else(|| {
        AppError::InvalidRequestBody(
            "binary_data_output: data element is not a JSON number".to_string(),
        )
    })
}

fn int_le<const N: usize>(v: &Value) -> Result<Vec<u8>, AppError> {
    let i = v.as_i64().ok_or_else(|| {
        AppError::InvalidRequestBody(
            "binary_data_output: data element is not a JSON integer".to_string(),
        )
    })?;
    Ok(i.to_le_bytes()[..N].to_vec())
}

fn uint_le<const N: usize>(v: &Value) -> Result<Vec<u8>, AppError> {
    let u = v.as_u64().ok_or_else(|| {
        AppError::InvalidRequestBody(
            "binary_data_output: data element is not a JSON unsigned integer".to_string(),
        )
    })?;
    Ok(u.to_le_bytes()[..N].to_vec())
}

/// f64 → IEEE 754 half-precision bits(FP16,round-to-nearest-even)。
/// FP16/BF16 在 dataplane 中无 JSON 表示(D7)——此转换是防御性完备
/// (worker 若声明 FP16 datatype + JSON data,仍正确编码)。
fn f64_to_f16_bits(v: f64) -> u16 {
    let bits = v.to_bits();
    let sign = ((bits >> 48) & 0x8000) as u16;
    let exp = ((bits >> 52) & 0x7FF) as i32;
    let mant = bits & 0xF_FFFF_FFFF_FFFF;
    if exp == 0x7FF {
        // inf / nan → half 的 inf / quiet nan
        let mant_q = if mant != 0 { 0x200 } else { 0 };
        return sign | 0x7C00 | mant_q;
    }
    if exp == 0 {
        return sign; // ±0(及极小 f64 denormal → 0,防御路径)
    }
    let new_exp = exp - 1023 + 15;
    if new_exp >= 0x1F {
        return sign | 0x7C00; // 溢出 → inf
    }
    if new_exp <= 0 {
        // half denormal:RNE 于 2^-24 粒度。实际指数 = new_exp - 15,
        // fraction = m × 2^(new_exp - 15 + 24) / 2^52 = m >> (43 - new_exp)。
        let m = (mant | 0x10_0000_0000_0000) as u128; // 隐式 1
        let shift = (43 - new_exp) as u32;
        let round = (m >> (shift - 1)) & 1;
        let sticky = if shift >= 2 { m & ((1u128 << (shift - 1)) - 1) } else { 0 };
        let mut hm = (m >> shift) as u32;
        if round == 1 && (sticky != 0 || hm & 1 == 1) {
            hm += 1;
        }
        return sign | hm as u16;
    }
    // normal:mantissa 截断 42 位 + RNE
    let hm = (mant >> 42) as u32;
    let round = (mant >> 41) & 1;
    let sticky = mant & ((1 << 41) - 1);
    if round == 1 && (sticky != 0 || hm & 1 == 1) {
        if hm + 1 == 0x400 {
            // mantissa 进位 → exp+1(半精度 65504+ 进位到 inf 由上方
            // new_exp 检查覆盖;此处正常进到下一指数)
            return sign | (((new_exp + 1) as u16) << 10);
        }
        return sign | (((new_exp as u16) << 10) | (hm + 1) as u16);
    }
    sign | (((new_exp as u16) << 10) | hm as u16)
}

/// f64 → BF16 bits(round-to-nearest-even,经 f32 舍入)。
fn f64_to_bf16_bits(v: f64) -> u16 {
    let bits = (v as f32).to_bits();
    let round = (bits >> 16) & 1;
    let sticky = bits & 0xFFFF;
    let mut out = bits >> 16;
    if round == 1 && (sticky != 0 || out & 1 == 1) {
        out += 1;
    }
    out as u16
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use bytes::Bytes;

    fn envelope_response(body_json: &str, ct: &str) -> Response {
        Response::builder()
            .status(200)
            .header("content-type", ct)
            .body(Body::from(body_json.to_string()))
            .unwrap()
    }

    fn req(head: &str) -> BinaryOutputRequest {
        parse_binary_output_request(
            &RequestBody::Json(Bytes::from(head.to_string())),
            "m1",
            Some("1".to_string()),
        )
        .expect("envelope request must parse")
    }

    // ===== 请求特征(D5 双条件) =====

    #[test]
    fn test_parse_top_level_flag() {
        let r = req(r#"{"id":"r1","inputs":[{"name":"a","shape":[2],"datatype":"FP32","data":[1,2]}],"parameters":{"binary_data_output":true}}"#);
        assert_eq!(r.id.as_deref(), Some("r1"));
        assert_eq!(r.model_name, "m1");
        assert_eq!(r.model_version.as_deref(), Some("1"));
        assert!(r.binary_outputs.is_none(), "顶层 flag → 全部输出");
    }

    #[test]
    fn test_parse_per_output_flag() {
        let r = req(r#"{"inputs":[{"name":"a","shape":[1],"datatype":"FP32","data":[1]}],"outputs":[{"name":"o1","parameters":{"binary_data":true}}]}"#);
        assert_eq!(r.binary_outputs.as_deref(), Some(&["o1".to_string()][..]));
    }

    #[test]
    fn test_parse_flag_without_envelope_shape_none() {
        // 自有 schema 撞名:有同名 flag 但非信封形状 → 不启用(双条件防御)
        let body = RequestBody::Json(Bytes::from(
            r#"{"outputs":[{"binary_data":true}]}"#.to_string(),
        ));
        assert!(parse_binary_output_request(&body, "m", None).is_none());
    }

    #[test]
    fn test_parse_no_flag_none() {
        let body = RequestBody::Json(Bytes::from(
            r#"{"inputs":[{"name":"a","shape":[1],"datatype":"FP32","data":[1]}]}"#.to_string(),
        ));
        assert!(parse_binary_output_request(&body, "m", None).is_none());
        // Raw(无 header 裸二进制)→ None
        let raw = RequestBody::Raw(Bytes::from(vec![1u8]), "application/octet-stream".into());
        assert!(parse_binary_output_request(&raw, "m", None).is_none());
    }

    #[test]
    fn test_parse_triton_binary_head_flag() {
        // Triton 客户端 flag 在 JSON 头内
        let head = r#"{"id":"r1","inputs":[{"name":"a","shape":[2],"datatype":"FP32","parameters":{"binary_data_size":8}}],"outputs":[{"name":"o1","parameters":{"binary_data":true}}]}"#;
        let mut body_bytes = head.as_bytes().to_vec();
        body_bytes.extend_from_slice(&[0u8; 8]);
        let tb = RequestBody::TritonBinary {
            body: Bytes::from(body_bytes),
            json_head_len: head.len(),
        };
        let r = parse_binary_output_request(&tb, "m1", None).expect("head flag must parse");
        assert_eq!(r.binary_outputs.as_deref(), Some(&["o1".to_string()][..]));
    }

    // ===== 编码(D6/D7) =====

    #[tokio::test]
    async fn test_encode_fp32_le() {
        let resp = convert_response(
            envelope_response(
                r#"{"outputs":[{"name":"o","shape":[2],"datatype":"FP32","data":[1.0,2.0]}]}"#,
                "application/json",
            ),
            &req(r#"{"inputs":[{"name":"a","shape":[1],"datatype":"FP32","data":[1]}],"parameters":{"binary_data_output":true}}"#),
        )
        .await
        .unwrap();
        let head_len: usize = resp
            .headers()
            .get(INFERENCE_HEADER_CONTENT_LENGTH)
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        assert_eq!(&body[head_len..head_len + 4], &1.0f32.to_le_bytes(), "FP32 小端编码");
        assert_eq!(&body[head_len + 4..], &2.0f32.to_le_bytes());
    }

    #[tokio::test]
    async fn test_encode_bytes_prefix() {
        let resp = convert_response(
            envelope_response(
                r#"{"outputs":[{"name":"o","shape":[1],"datatype":"BYTES","data":["test"]}]}"#,
                "application/json",
            ),
            &req(r#"{"inputs":[{"name":"a","shape":[1],"datatype":"FP32","data":[1]}],"parameters":{"binary_data_output":true}}"#),
        )
        .await
        .unwrap();
        let head_len: usize = resp
            .headers()
            .get(INFERENCE_HEADER_CONTENT_LENGTH)
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        assert_eq!(&body[body.len() - 8..], b"\x04\x00\x00\x00test", "BYTES = 4B 长度前缀 + 内容");
        let _ = head_len;
    }

    #[test]
    fn test_encode_fp16() {
        // 1.0 → 0x3C00;0.5 → 0x3800;-2.0 → 0xC000(精确半精度)
        assert_eq!(f64_to_f16_bits(1.0), 0x3C00);
        assert_eq!(f64_to_f16_bits(0.5), 0x3800);
        assert_eq!(f64_to_f16_bits(-2.0), 0xC000);
        assert_eq!(f64_to_f16_bits(f64::INFINITY), 0x7C00);
        assert_eq!(f64_to_f16_bits(f64::NAN) & 0x7C00, 0x7C00);
        assert_eq!(f64_to_f16_bits(65504.0), 0x7BFF); // max half
        assert_eq!(f64_to_f16_bits(65520.0), 0x7C00); // → inf
        assert_eq!(f64_to_f16_bits(2f64.powi(-14)), 0x0400); // min normal
        assert_eq!(f64_to_f16_bits(2f64.powi(-15)), 0x0200); // denormal
    }

    #[test]
    fn test_encode_bf16() {
        assert_eq!(f64_to_bf16_bits(1.0), 0x3F80);
        assert_eq!(f64_to_bf16_bits(-2.0), 0xC000);
    }

    // ===== 响应转换(D6) =====

    #[tokio::test]
    async fn test_binary_output_fp32_full() {
        let r = req(r#"{"id":"r1","inputs":[{"name":"a","shape":[1],"datatype":"FP32","data":[1]}],"parameters":{"binary_data_output":true}}"#);
        let resp = convert_response(
            envelope_response(
                r#"{"outputs":[{"name":"o","shape":[2],"datatype":"FP32","data":[1.0,2.0]}]}"#,
                "application/json",
            ),
            &r,
        )
        .await
        .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "application/octet-stream"
        );
        let head_len: usize = resp
            .headers()
            .get(INFERENCE_HEADER_CONTENT_LENGTH)
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        assert_eq!(body.len() - head_len, 8, "二进制尾 = 2 × FP32");
        assert_eq!(&body[head_len..head_len + 4], &1.0f32.to_le_bytes());
        assert_eq!(&body[head_len + 4..], &2.0f32.to_le_bytes());
        // JSON 头:id 回显 + model_name/version + binary_data_size
        let head: Value = serde_json::from_slice(&body[..head_len]).unwrap();
        assert_eq!(head["id"], "r1");
        assert_eq!(head["model_name"], "m1");
        assert_eq!(head["model_version"], "1");
        assert_eq!(head["outputs"][0]["parameters"]["binary_data_size"], 8);
        assert!(head["outputs"][0].get("data").is_none(), "data 必须从 JSON 头删除");
    }

    #[tokio::test]
    async fn test_binary_output_per_output_flag_mixed() {
        // 混合输出:仅命中者进二进制尾,未命中保留 JSON data
        let r = req(r#"{"inputs":[{"name":"a","shape":[1],"datatype":"FP32","data":[1]}],"outputs":[{"name":"bin_out","parameters":{"binary_data":true}}]}"#);
        let resp = convert_response(
            envelope_response(
                r#"{"outputs":[{"name":"json_out","shape":[1],"datatype":"FP32","data":[9.0]},{"name":"bin_out","shape":[1],"datatype":"FP32","data":[1.5]}]}"#,
                "application/json",
            ),
            &r,
        )
        .await
        .unwrap();
        let head_len: usize = resp
            .headers()
            .get(INFERENCE_HEADER_CONTENT_LENGTH)
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        assert_eq!(body.len() - head_len, 4, "仅命中者进二进制尾");
        assert_eq!(&body[head_len..], &1.5f32.to_le_bytes());
        let head: Value = serde_json::from_slice(&body[..head_len]).unwrap();
        assert_eq!(head["outputs"][0]["data"], json!([9.0]), "未命中保留 JSON data");
        assert_eq!(head["outputs"][1]["parameters"]["binary_data_size"], 4);
    }

    #[tokio::test]
    async fn test_binary_output_non_envelope_400() {
        let r = req(r#"{"inputs":[{"name":"a","shape":[1],"datatype":"FP32","data":[1]}],"parameters":{"binary_data_output":true}}"#);
        let resp = convert_response(
            envelope_response(r#"{"result": 42}"#, "application/json"),
            &r,
        )
        .await;
        assert!(
            matches!(resp, Err(AppError::InvalidRequestBody(_))),
            "非信封 worker 输出 + flag → 400 明确报错"
        );
    }

    #[tokio::test]
    async fn test_binary_output_missing_datatype_400() {
        let r = req(r#"{"inputs":[{"name":"a","shape":[1],"datatype":"FP32","data":[1]}],"parameters":{"binary_data_output":true}}"#);
        let resp = convert_response(
            envelope_response(
                r#"{"outputs":[{"name":"o","shape":[1],"data":[1]}]}"#,
                "application/json",
            ),
            &r,
        )
        .await;
        assert!(matches!(resp, Err(AppError::InvalidRequestBody(_))));
    }

    #[tokio::test]
    async fn test_binary_output_worker_media_type_wins() {
        // G21:worker 非 JSON media_type + flag → 原样返回(不转换)
        let r = req(r#"{"inputs":[{"name":"a","shape":[1],"datatype":"FP32","data":[1]}],"parameters":{"binary_data_output":true}}"#);
        let resp = convert_response(
            envelope_response("not-json-bytes", "application/octet-stream"),
            &r,
        )
        .await
        .unwrap();
        assert_eq!(resp.headers().get("content-type").unwrap(), "application/octet-stream");
        let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        assert_eq!(body, "not-json-bytes");
    }

    #[tokio::test]
    async fn test_binary_output_worker_error_untouched() {
        // 非 2xx(worker 错误响应)→ 原样
        let r = req(r#"{"inputs":[{"name":"a","shape":[1],"datatype":"FP32","data":[1]}],"parameters":{"binary_data_output":true}}"#);
        let err_resp = Response::builder()
            .status(400)
            .header("content-type", "application/json")
            .body(Body::from(r#"{"error":{"message":"bad"}}"#))
            .unwrap();
        let resp = convert_response(err_resp, &r).await.unwrap();
        assert_eq!(resp.status(), 400);
    }

    #[tokio::test]
    async fn test_binary_output_id_omitted_when_absent() {
        let r = req(r#"{"inputs":[{"name":"a","shape":[1],"datatype":"FP32","data":[1]}],"parameters":{"binary_data_output":true}}"#);
        let resp = convert_response(
            envelope_response(
                r#"{"outputs":[{"name":"o","shape":[1],"datatype":"FP32","data":[1.0]}]}"#,
                "application/json",
            ),
            &r,
        )
        .await
        .unwrap();
        let head_len: usize = resp
            .headers()
            .get(INFERENCE_HEADER_CONTENT_LENGTH)
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let head: Value = serde_json::from_slice(&body[..head_len]).unwrap();
        assert!(head.get("id").is_none(), "无请求 id 则省略");
    }
}
