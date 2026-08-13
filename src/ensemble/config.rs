use crate::error::AppError;
use bytes::Bytes;
use indexmap::IndexMap;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;

use super::*;

/// E7 (batch 4④, R13/D11/R14): multi-sink validation.
///  - `output` × `outputs` are mutually exclusive (E2 二选一);
///  - alias names are identifiers; each ref must be a legal sink ref
///    (`$stepX` / `$stepX.ALIAS[.path]` / `$inputs.NAME[.path]`, R13) —
///    refs to absentable steps are ALLOWED (the D5 null channel);
///  - a streaming DAG with outputs must have EXACTLY one alias pointing at
///    the streaming step (D11/R14) — outputs is then validation-only, the
///    response stays the stream.
fn validate_outputs_rules(
    steps: &[EnsembleStep],
    outputs: Option<&IndexMap<String, String>>,
    single_output_omitted: bool,
    output_step: usize,
    // R5/R13: `$inputs` refs in outputs obey the same namespace rules as
    // step inputs (declaration required; the name must be declared).
    inputs_decl: Option<&IndexMap<String, InputDecl>>,
) -> Result<(), AppError> {
    let Some(outputs) = outputs else {
        return Ok(());
    };
    if !single_output_omitted {
        return Err(AppError::Config(
            "ensemble.output and ensemble.outputs are mutually exclusive \
             (E2 × E7 — pick one)"
                .to_string(),
        ));
    }
    for (alias, ref_str) in outputs {
        if !IDENT_RE.is_match(alias) {
            return Err(AppError::Config(format!(
                "ensemble.outputs alias '{alias}' is not a valid identifier (R13)"
            )));
        }
        let caps = REF_RE.captures(ref_str).ok_or_else(|| {
            AppError::Config(format!(
                "ensemble.outputs alias '{alias}': invalid reference '{}' (R13)",
                ref_str
            ))
        })?;
        let source = caps.get(1).unwrap().as_str();
        let rest = caps.get(2).map(|m| m.as_str());
        match source {
            "request" => {
                return Err(AppError::Config(format!(
                    "ensemble.outputs alias '{alias}': $request is not a sink \
                     reference (R13)"
                )));
            }
            "inputs" => {
                // R5: the $inputs namespace requires a declaration; R13: the
                // name must be DECLARED (step inputs enforce the same rule —
                // outputs must not be the silent-null loophole for typos);
                // R3: binary inputs are whole-only refs.
                let name = rest.and_then(|r| r.split('.').next()).unwrap_or("");
                if name.is_empty() {
                    return Err(AppError::Config(format!(
                        "ensemble.outputs alias '{alias}': '{}' is missing the \
                         input name (R13)",
                        ref_str
                    )));
                }
                let Some(decl) = inputs_decl else {
                    return Err(AppError::Config(format!(
                        "ensemble.outputs alias '{alias}': '{}' requires an \
                         ensemble.inputs declaration (R5)",
                        ref_str
                    )));
                };
                let d = decl.get(name).ok_or_else(|| {
                    AppError::Config(format!(
                        "ensemble.outputs alias '{alias}': '{}' references \
                         undeclared input '{}' (R13)",
                        ref_str, name
                    ))
                })?;
                if d.ty == InputType::Binary
                    && rest.map(|r| r.contains('.')).unwrap_or(false)
                {
                    return Err(AppError::Config(format!(
                        "ensemble.outputs alias '{alias}': binary input '{}' \
                         must be referenced whole (R3)",
                        ref_str
                    )));
                }
            }
            step_name => {
                let src = steps.iter().find(|s| s.name == step_name).ok_or_else(|| {
                    AppError::Config(format!(
                        "ensemble.outputs alias '{alias}' references unknown step \
                         '{}' (R13)",
                        ref_str
                    ))
                })?;
                if let Some(decl) = &src.outputs_decl {
                    let a = rest.and_then(|r| r.split('.').next()).ok_or_else(|| {
                        AppError::Config(format!(
                            "ensemble.outputs alias '{alias}': '{}' — step '{}' \
                             declares outputs and must be referenced by alias (R13)",
                            ref_str, step_name
                        ))
                    })?;
                    if !decl.contains_key(a) {
                        return Err(AppError::Config(format!(
                            "ensemble.outputs alias '{alias}': '{}' is not a \
                             declared alias of step '{}' (R13)",
                            ref_str, step_name
                        )));
                    }
                }
            }
        }
    }
    // D11/R14: streaming DAGs — outputs is validation-only; exactly one
    // alias, pointing at the streaming (output) step.
    if steps[output_step].stream {
        if outputs.len() != 1 {
            return Err(AppError::Config(format!(
                "ensemble.outputs on a streaming DAG must have exactly ONE alias \
                 pointing at the streaming step (D11) — {} found",
                outputs.len()
            )));
        }
        let (alias, ref_str) = outputs.first().unwrap();
        let caps = REF_RE.captures(ref_str)
            .expect("refs validated above");
        let source = caps.get(1).unwrap().as_str();
        if source != steps[output_step].name.as_str() {
            return Err(AppError::Config(format!(
                "ensemble.outputs alias '{alias}' references '{}' — a streaming \
                 DAG's outputs must point at the streaming step '{}' (R14)",
                ref_str, steps[output_step].name
            )));
        }
    }
    Ok(())
}

/// E8-2 (batch 5): parse the `when: "<ref> OP <literal>"` grammar —
/// whitelisted operators only (`== != contains in`), the literal is a JSON
/// value, `in` requires an array literal. The R16 target whitelist is
/// checked separately (validate_when_refs — it needs the type env).
fn parse_when_expr(raw: &str) -> Result<WhenExpr, AppError> {
    let caps = WHEN_RE.captures(raw).ok_or_else(|| {
        AppError::Config(format!(
            "invalid when expression '{raw}' — expected \"$ref OP literal\" \
             with OP in == != contains in (E8-2)"
        ))
    })?;
    let target = parse_when_target(caps.get(1).unwrap().as_str(), raw)?;
    let op = match caps.get(2).unwrap().as_str() {
        "==" => WhenOp::Eq,
        "!=" => WhenOp::Neq,
        "contains" => WhenOp::Contains,
        _ => WhenOp::In,
    };
    let lit_raw = caps.get(3).unwrap().as_str();
    // The plan's literal form is single-quoted ('x', ['a','b']); JSON
    // accepts double quotes — try JSON first, then swap quotes (strings
    // containing double quotes are out of the literal subset, E8-2).
    let literal: Value = match serde_json::from_str::<Value>(lit_raw) {
        Ok(v) => v,
        Err(_) => {
            let swapped: String = lit_raw
                .chars()
                .map(|c| if c == '\'' { '"' } else { c })
                .collect();
            serde_json::from_str(&swapped).map_err(|e| {
                AppError::Config(format!("invalid when literal in '{raw}': {e}"))
            })?
        }
    };
    if op == WhenOp::In && !literal.is_array() {
        return Err(AppError::Config(format!(
            "when '{raw}': 'in' requires an array literal (E8-2)"
        )));
    }
    Ok(WhenExpr { target, op, literal })
}

/// E8-2 (batch 5): the when target — the R16/m2 whitelist: `$request.dag` /
/// `$request.client_ip` / `$inputs.NAME[.path]`.
fn parse_when_target(target: &str, raw: &str) -> Result<WhenTarget, AppError> {
    if target == "request.dag" {
        return Ok(WhenTarget::Dag);
    }
    if target == "request.client_ip" {
        return Ok(WhenTarget::ClientIp);
    }
    if let Some(rest) = target.strip_prefix("request.") {
        return Err(AppError::Config(format!(
            "when '{raw}': $request.{rest} is not whitelisted (R16/m2 — \
             dag and client_ip only)"
        )));
    }
    if let Some(rest) = target.strip_prefix("inputs.") {
        let mut segs = rest.split('.');
        let name = segs.next().unwrap_or("").to_string();
        if name.is_empty() {
            return Err(AppError::Config(format!(
                "when '{raw}': $inputs requires a declared input name (R16)"
            )));
        }
        let path_segs: Vec<&str> = segs.collect();
        let path = if path_segs.is_empty() {
            None
        } else {
            Some(path_segs.join("."))
        };
        return Ok(WhenTarget::Input { name, path });
    }
    Err(AppError::Config(format!(
        "when '{raw}': invalid target '{target}' (R16)"
    )))
}

/// E8-2 (batch 5): the R16 whitelist check — input names must be declared
/// and json-typed (binary has no comparison semantics), `$inputs` requires
/// a declaration; D34 rule 6: `when` × `stream: true` is rejected; when
/// refs to optional inputs mark the step CONDITIONAL (the E6-skip channel).
fn validate_when_refs(
    steps: &[EnsembleStep],
    inputs_decl: Option<&IndexMap<String, InputDecl>>,
    conditional_refs: &mut [Vec<String>],
) -> Result<(), AppError> {
    for (i, step) in steps.iter().enumerate() {
        let Some(expr) = &step.when else {
            continue;
        };
        // D34 rule 6: a streaming response promises a stream unconditionally.
        if step.stream {
            return Err(AppError::Config(format!(
                "step '{}': when cannot combine with stream: true \
                 (a streaming response promises a stream unconditionally, D34)",
                step.name
            )));
        }
        match &expr.target {
            WhenTarget::Dag | WhenTarget::ClientIp => {}
            WhenTarget::Input { name, .. } => {
                let Some(decl) = inputs_decl else {
                    return Err(AppError::Config(format!(
                        "step '{}': when references $inputs.{name} — requires an \
                         ensemble.inputs declaration (R16)",
                        step.name
                    )));
                };
                let d = decl.get(name).ok_or_else(|| {
                    AppError::Config(format!(
                        "step '{}': when references undeclared input '{name}' (R16)",
                        step.name
                    ))
                })?;
                if d.ty == InputType::Binary {
                    return Err(AppError::Config(format!(
                        "step '{}': when cannot compare binary input '{name}' \
                         (no comparison semantics on bytes, R16)",
                        step.name
                    )));
                }
                // R4/D13: an optional-input ref makes the step conditional
                // (absent → the E6-skip channel).
                if !d.required && d.default.is_none() {
                    conditional_refs[i].push(name.clone());
                }
            }
        }
    }
    Ok(())
}

/// E8-2 (batch 5): evaluate a when expression against the request context.
/// Strict type equality (no coercion — `1 == '1'` is false); contains/in
/// only on strings+arrays; an absent input compares as null (`!= null` is
/// the R16 absence check).
pub fn eval_when(
    expr: &WhenExpr,
    opts: &EnsembleExecOpts,
    context: &HashMap<String, EnsembleValue>,
) -> Result<bool, AppError> {
    let left: Option<Value> = match &expr.target {
        WhenTarget::Dag => opts.dag_selector.as_deref().map(|s| json!(s)),
        WhenTarget::ClientIp => Some(json!(opts.client_ip)),
        WhenTarget::Input { name, path } => {
            let key = format!("inputs.{name}");
            match context.get(&key) {
                None => None, // absent — compares as null below
                Some(EnsembleValue::Json(v)) => match path {
                    None => Some(v.clone()),
                    Some(p) => Some(project_json_path(v, p).map_err(|e| {
                        AppError::InvalidRequestBody(format!("when path: {e}"))
                    })?),
                },
                Some(_) => {
                    return Err(AppError::InvalidRequestBody(format!(
                        "when cannot resolve binary input '{name}' (R16)"
                    )));
                }
            }
        }
    };
    // R16: absence is DISTINCT from null — `!= null` is the absence check
    // (absent → true), `== null` matches an explicit null only.
    Ok(match expr.op {
        WhenOp::Eq => left == Some(expr.literal.clone()),
        WhenOp::Neq => left != Some(expr.literal.clone()),
        WhenOp::Contains => match (&left, &expr.literal) {
            (Some(Value::String(s)), Value::String(sub)) => s.contains(sub.as_str()),
            (Some(Value::Array(a)), _) => a.contains(&expr.literal),
            _ => false, // absent / non-string/array: strict semantics
        },
        WhenOp::In => match &expr.literal {
            Value::Array(a) => left.as_ref().map(|l| a.contains(l)).unwrap_or(false),
            _ => false, // parse-rejected (In requires an array literal)
        },
    })
}

/// E8-2 (batch 5): the runtime gate — a when-false step is skipped (the
/// E6-skip channel); absent steps keep every other rule intact.
pub(crate) fn when_passes(
    step_idx: usize,
    plan: &EnsemblePlan,
    opts: &EnsembleExecOpts,
    context: &HashMap<String, EnsembleValue>,
) -> Result<bool, AppError> {
    let Some(expr) = &plan.steps[step_idx].when else {
        return Ok(true);
    };
    eval_when(expr, opts, context)
}

/// E8-1 (batch 5, D22/D38): resolve the request's DAG set — the dags form
/// picks by name (None = "default"); an unknown name is a 400 (explicit
/// contract, never a silent default fallback — a typo must surface).
/// Single-form plans reject any selector (it can only name nothing).
pub fn select_dag_set<'a>(
    plan: &'a EnsemblePlan,
    selector: Option<&str>,
) -> Result<&'a EnsemblePlan, AppError> {
    match &plan.dag_sets {
        None => {
            if let Some(s) = selector {
                return Err(AppError::InvalidRequestBody(format!(
                    "x-lite-dag selector '{s}' provided but this ensemble declares                      no dags (D22)"
                )));
            }
            Ok(plan)
        }
        Some(sets) => {
            let name = selector.unwrap_or("default");
            sets.get(name).map(|p| p.as_ref()).ok_or_else(|| {
                AppError::InvalidRequestBody(format!(
                    "unknown dag '{name}' — declared sets: {} (D22)",
                    sets.keys().cloned().collect::<Vec<_>>().join(", ")
                ))
            })
        }
    }
}

/// D38 (batch 5): extract + D22-validate the dag selector from the HTTP
/// transport metadata channel (request / SSE / WS-upgrade / h2 stream
/// headers — one key name, one client mental model).
pub fn dag_selector_from_http(
    headers: &axum::http::HeaderMap,
) -> Result<Option<String>, AppError> {
    match headers.get("x-lite-dag") {
        None => Ok(None),
        Some(v) => v
            .to_str()
            .map_err(|_| {
                AppError::InvalidRequestBody(
                    "x-lite-dag header is not valid ASCII (D22)".to_string(),
                )
            })
            .and_then(validate_dag_selector)
            .map(Some),
    }
}

/// D38 (batch 5): the gRPC transport metadata channel (`x-lite-dag` key —
/// same name as HTTP, the deadline metadata precedent).
pub fn dag_selector_from_grpc(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<Option<String>, AppError> {
    match metadata.get("x-lite-dag") {
        None => Ok(None),
        Some(v) => v
            .to_str()
            .map_err(|_| {
                AppError::InvalidRequestBody(
                    "x-lite-dag metadata is not valid ASCII (D22)".to_string(),
                )
            })
            .and_then(validate_dag_selector)
            .map(Some),
    }
}

/// D22 (batch 5): selector value validation — non-empty, ≤64 chars,
/// `[A-Za-z0-9_-]` only. The endpoints call this at EXTRACTION (transport
/// metadata channel, D38) so a malformed value 400s before execution.
pub fn validate_dag_selector(value: &str) -> Result<String, AppError> {
    if value.is_empty()
        || value.len() > 64
        || !value.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(AppError::InvalidRequestBody(format!(
            "invalid x-lite-dag value '{value}' — must be 1-64 chars of              [A-Za-z0-9_-] (D22)"
        )));
    }
    Ok(value.to_string())
}

/// E7 (batch 4④, D31/D5): build the DAG response — priority
/// outputs (E7) > output (E2) > steps.last() (historical). Multi-sink
/// responses are KServe envelopes: JSON head `{model_name, outputs:
/// [{name, data}]}` + binary tail for Binary aliases (binary_data_size
/// refilled in header order); absent aliases (skip/absent optional input)
/// → `data: null` + warn (D5). A tail-less envelope degrades to plain JSON
/// (Envelope { tail: empty } is never produced).
pub(crate) fn build_response(
    plan: &EnsemblePlan,
    model_name: &str,
    context: &HashMap<String, EnsembleValue>,
) -> Result<EnsembleOutcome, AppError> {
    let Some(outputs) = &plan.outputs else {
        // Historical single-output path (byte-identical).
        let value = context.get(&plan.steps[plan.output_step].name).cloned()
            .ok_or_else(|| AppError::Internal("ensemble produced no output".to_string()))?;
        let value = select_output_field(
            &plan.steps[plan.output_step].name, value, plan.output_field.as_deref(),
        )?;
        return Ok(EnsembleOutcome::Unary(value));
    };

    let mut head_outputs = Vec::with_capacity(outputs.len());
    let mut tail: Vec<u8> = Vec::new();
    for (alias, ref_str) in outputs {
        let value = match resolve_ref(plan, ref_str, context) {
            Ok(ResolvedRef::Value(v)) => Some(v),
            Ok(ResolvedRef::Absent(_)) => None,
            Err(e) => {
                // R13 parse-validated the ref — a runtime error here means
                // the source is ABSENT (skipped step / missing optional
                // input): the D5 null channel.
                warn!(alias = %alias, ref_ = %ref_str, error = %e, "ensemble outputs alias is null (absent source, D5)");
                None
            }
        };
        match value {
            None => head_outputs.push(json!({"name": alias, "data": null})),
            Some(EnsembleValue::Json(v)) => {
                head_outputs.push(json!({"name": alias, "data": v}));
            }
            // P2 (batch 6): a raw-resident alias embeds its ORIGINAL bytes
            // into the envelope head (RawValue validates once and writes
            // verbatim — no re-serialize).
            Some(EnsembleValue::RawJson(raw)) => {
                let embedded = serde_json::value::RawValue::from_string(
                    String::from_utf8_lossy(&raw.bytes).into_owned(),
                )
                .map_err(|e| {
                    AppError::Internal(format!(
                        "ensemble outputs alias '{alias}' value is not valid JSON: {e}"
                    ))
                })?;
                head_outputs.push(json!({"name": alias, "data": embedded}));
            }
            Some(EnsembleValue::Binary(b, _ct, shape, datatype)) => {
                // Binary alias → head element + tail slice (header order).
                let mut el = serde_json::Map::new();
                el.insert("name".to_string(), json!(alias));
                el.insert(
                    "parameters".to_string(),
                    json!({"binary_data_size": b.len()}),
                );
                if let Some(shape) = shape {
                    el.insert("shape".to_string(), json!(shape));
                }
                if let Some(dt) = datatype {
                    el.insert("datatype".to_string(), json!(dt));
                }
                head_outputs.push(Value::Object(el));
                tail.extend_from_slice(&b);
            }
            Some(EnsembleValue::Envelope { .. }) => {
                return Err(AppError::Internal(
                    "envelope reached the response builder".to_string(),
                ));
            }
        }
    }
    let head = json!({"model_name": model_name, "outputs": head_outputs});
    if tail.is_empty() {
        Ok(EnsembleOutcome::Unary(EnsembleValue::Json(head)))
    } else {
        Ok(EnsembleOutcome::Unary(EnsembleValue::Envelope {
            head,
            tail: Bytes::from(tail),
        }))
    }
}

/// D32: encode the LSBE-1 in-frame container — `"LSB1"` magic ‖ u64 LE head
/// length ‖ JSON head ‖ binary tail. The gRPC unary multi-sink response
/// form (InferResponse has no headers map — the container is
/// self-describing); round-trips through [`split_envelope`].
pub fn encode_lsbe1(head: &Value, tail: &[u8]) -> Bytes {
    let head_bytes = serde_json::to_vec(head).expect("Value serialization is infallible");
    let mut blob = Vec::with_capacity(12 + head_bytes.len() + tail.len());
    blob.extend_from_slice(b"LSB1");
    blob.extend_from_slice(&(head_bytes.len() as u64).to_le_bytes());
    blob.extend_from_slice(&head_bytes);
    blob.extend_from_slice(tail);
    Bytes::from(blob)
}

/// MIMO (R6, batch 4③): step.outputs declaration validation — alias names
/// are identifiers; projection paths are `$.a.b` dot segments (D29). Runs
/// for both config modes (declarations are mode-independent).
fn validate_step_output_decls(steps: &[EnsembleStep]) -> Result<(), AppError> {
    for step in steps {
        let Some(decl) = &step.outputs_decl else {
            continue;
        };
        for (alias, d) in decl {
            if !IDENT_RE.is_match(alias) {
                return Err(AppError::Config(format!(
                    "step '{}' output alias '{alias}' is not a valid identifier \
                     ([A-Za-z_][A-Za-z0-9_]*, R6)",
                    step.name
                )));
            }
            if let Some(path) = &d.path {
                if !JSON_PATH_RE.is_match(path) {
                    return Err(AppError::Config(format!(
                        "step '{}' alias '{alias}': path '{path}' is not a $.a.b dot \
                         segment path (R6/D29 — no array subscripts or filters)",
                        step.name
                    )));
                }
            }
        }
    }
    Ok(())
}

/// MIMO (R11/R12, batch 4①): the parse-decided step input assembly — with a
/// declared type environment every ref's type is known at parse time, so the
/// runtime has no type branches (§5.5.6). None (legacy configs) keeps the
/// historical dynamic dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMode {
    /// All-Json inputs → object assembly (+ params override, E3).
    GroupJson,
    /// Exactly one whole-Binary input → raw bytes passthrough (D8).
    BinaryPassThrough,
}

/// Parse + validate + topologically sort a config file into the cached
/// [`EnsemblePlan`] (P0, D6): the plan is structured once at load time,
/// streaming/MIMO validation stacks on top of it (no parse logic double
/// write). The caller owns the file read — the cache re-parses only on
/// miss/eviction.
pub fn parse_ensemble_plan(content: &str, config_path: &std::path::Path) -> Result<EnsemblePlan, AppError> {
    let config: EnsembleConfig = serde_yaml::from_str(content)
        .map_err(|e| AppError::Config(format!("failed to parse ensemble config: {}", e)))?;

    // E8-1 (batch 5): the dags form — every set parses through the SAME
    // pipeline (R15: independent per-set validation) and the outer plan
    // carries them by name; the single-set top-level fields are forbidden
    // (no ambiguity about which one is the default).
    if let Some(dags) = config.ensemble.dags {
        if !config.ensemble.steps.is_empty()
            || config.ensemble.output.is_some()
            || config.ensemble.outputs.is_some()
            || config.ensemble.inputs.is_some()
        {
            return Err(AppError::Config(
                "ensemble.dags forbids top-level steps/output/outputs/inputs —                  move them into the sets (E8-1)"
                    .to_string(),
            ));
        }
        if dags.is_empty() {
            return Err(AppError::Config(
                "ensemble.dags declares no sets (E8-1)".to_string(),
            ));
        }
        let mut sets = IndexMap::new();
        for (name, set) in dags {
            let plan = parse_ensemble_set(
                set.steps, set.output, set.outputs, set.inputs, config_path,
            )
            .map_err(|e| {
                AppError::Config(format!("dag set '{name}': {}", e))
            })?;
            sets.insert(name, Arc::new(plan));
        }
        return Ok(EnsemblePlan {
            // The outer plan is a pure container — execution always selects
            // a set first (select_dag_set).
            steps: Vec::new(),
            layers: Vec::new(),
            output_step: 0,
            output_field: None,
            chains: Vec::new(),
            inputs_decl: None,
            input_modes: Vec::new(),
            conditional_refs: Vec::new(),
            step_dep_keys: Vec::new(),
            step_raw_eligible: Vec::new(),
            outputs: None,
            dag_sets: Some(sets),
            config_path: config_path.to_path_buf(),
            source_mtime: None,
        });
    }

    parse_ensemble_set(
        config.ensemble.steps,
        config.ensemble.output,
        config.ensemble.outputs,
        config.ensemble.inputs,
        config_path,
    )
}

/// The single-set parse pipeline — shared by the historical single-set form
/// and every E8-1 dag set (R15: one pipeline, per-set validation).
fn parse_ensemble_set(
    steps_raw: Vec<EnsembleStepRaw>,
    output: Option<String>,
    outputs: Option<IndexMap<String, String>>,
    inputs_decl: Option<IndexMap<String, InputDecl>>,
    config_path: &std::path::Path,
) -> Result<EnsemblePlan, AppError> {
    let steps: Vec<EnsembleStep> = steps_raw.into_iter().map(|s| {
        // E5: per-step timeout must be a positive, finite duration.
        if let Some(t) = s.timeout_secs {
            if !t.is_finite() || t <= 0.0 {
                return Err(AppError::Config(format!(
                    "step '{}': timeout_secs must be a positive finite number, got {}",
                    s.name, t
                )));
            }
        }
        Ok(EnsembleStep {
            name: s.name,
            model: s.model,
            // E4: "latest" == omitted (execution-time active resolution).
            version: s.version.filter(|v| v != "latest"),
            inputs: s.inputs,
            stream: s.stream,
            params: s.params,
            timeout_secs: s.timeout_secs,
            on_error: s.on_error.unwrap_or_default(),
            retries: s.retries.unwrap_or(0),
            outputs_decl: s.outputs,
            // E8-2: grammar parse here; the R16 whitelist check runs once
            // the type environment exists (validate_when_refs).
            when: s.when.as_deref().map(parse_when_expr).transpose()?,
        })
    }).collect::<Result<_, AppError>>()?;

    validate_dag(&steps)?;
    // E2: resolve the explicit output BEFORE chain construction / streaming
    // validation — both anchor on the output step.
    let (output_step, output_field) = resolve_output(output.as_deref(), &steps)?;
    // MIMO (batch 4①): the static type environment — inputs declaration
    // validation (R1/R2), ref analysis (R3/R5/R9/R12), input-mode dispatch
    // (R11) and conditional-step discovery (R4).
    validate_step_output_decls(&steps)?;
    let (input_modes, mut conditional_refs) = analyze_static_types(&steps, inputs_decl.as_ref())?;
    // E8-2 (batch 5): the R16 whitelist check + D34 streaming rule + the
    // conditional extension (when refs to optional inputs make the step
    // absentable).
    validate_when_refs(&steps, inputs_decl.as_ref(), &mut conditional_refs)?;
    // E7 (batch 4④): multi-sink validation — R13 ref shape, E2 mutual
    // exclusion, D11/R14 streaming rules.
    // The E2 × E7 exclusion needs the raw `output:` PRESENCE (output_field
    // is None both when output is omitted and when it names a whole step).
    let single_output_omitted = output.is_none();
    validate_outputs_rules(
        &steps,
        outputs.as_ref(),
        single_output_omitted,
        output_step,
        inputs_decl.as_ref(),
    )?;
    // E6 (D5/D34) + MIMO R4: an absentable step's absence must be statically
    // provable — no downstream references, no single-output reference,
    // never streaming.
    validate_skip_rules(&steps, output_step, &conditional_refs, outputs.is_some())?;
    // MIMO (R7/D10): the single-output contract cannot name aliases — a DAG
    // whose output step declares step.outputs must use ensemble.outputs (E7)
    // to name the sinks (with outputs present the aliases ARE the contract).
    if outputs.is_none() && steps[output_step].outputs_decl.is_some() {
        return Err(AppError::Config(format!(
            "output step '{}' declares step.outputs — the single-output contract \
             cannot name aliases; set ensemble.outputs to select the sinks (E7)",
            steps[output_step].name
        )));
    }
    // Pipeline-form validation + chain construction (P-R1..R5/D26, batch 2);
    // non-pipeline streaming rules apply only when no chain exists.
    let chains = build_chains(&steps, output_step)?;
    if chains.is_empty() {
        validate_stream_rules(&steps, output_step)?;
    }
    // E2 × D11: a streaming DAG's chunks have no field semantics — an
    // explicit output field on a streaming DAG is a parse-time rejection.
    if output_field.is_some() && steps.iter().any(|s| s.stream) {
        return Err(AppError::Config(
            "ensemble.output field projection is not supported on streaming DAGs \
             (chunks have no field semantics, D11)"
                .to_string(),
        ));
    }
    let index_of: HashMap<&str, usize> = steps
        .iter()
        .enumerate()
        .map(|(i, s)| (s.name.as_str(), i))
        .collect();
    let layers = topological_layers(&steps)
        .into_iter()
        .map(|layer| layer.into_iter().map(|s| index_of[s.name.as_str()]).collect())
        .collect();
    // P1 (batch 6): per-step referenced context keys — spawn-time clones
    // select these instead of the whole table. Runs after ref validation,
    // so every ref shape is parse-proven.
    let step_dep_keys = steps
        .iter()
        .map(|s| {
            s.inputs.values().try_fold(Vec::new(), |mut keys, r| {
                let key = context_key_for_ref(&steps, r)?;
                if !keys.contains(&key) {
                    keys.push(key);
                }
                Ok::<_, AppError>(keys)
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    // P2/P8 (batch 6): raw residency — any ref carrying a path segment onto
    // an undeclared step forces the parse (field projection needs the Value;
    // the lazy cache covers the mixed whole+field case). Declared steps
    // always parse (R6 projection semantics).
    let mut step_raw_eligible = vec![true; steps.len()];
    for (i, s) in steps.iter().enumerate() {
        if s.outputs_decl.is_some() {
            step_raw_eligible[i] = false;
        }
    }
    let mut mark_field_referenced = |r: &str| {
        if let Some(caps) = REF_RE.captures(r) {
            let source = caps.get(1).unwrap().as_str();
            let rest = caps.get(2).map(|m| m.as_str()).unwrap_or("");
            // A non-empty rest on an UNDECLARED step is a single-segment field
            // access (legacy multi-segment is parse-rejected, B5) — declared
            // steps are always ineligible regardless (R6 projections).
            if !rest.is_empty() {
                if let Some(&i) = index_of.get(source) {
                    if steps[i].outputs_decl.is_none() {
                        step_raw_eligible[i] = false;
                    }
                }
            }
        }
    };
    for s in &steps {
        for r in s.inputs.values() {
            mark_field_referenced(r);
        }
    }
    if let Some(o) = &output {
        mark_field_referenced(o);
    }
    if let Some(outs) = &outputs {
        for r in outs.values() {
            mark_field_referenced(r);
        }
    }

    Ok(EnsemblePlan {
        output_step,
        output_field,
        steps,
        layers,
        chains,
        // MIMO (batch 4①): the static type environment (None = legacy single
        // anonymous input, byte-identical behaviour).
        inputs_decl,
        input_modes,
        conditional_refs,
        step_dep_keys,
        step_raw_eligible,
        // E7 (batch 4④): multi-sink output mapping (None = single output).
        outputs,
        // E8-1 (batch 5): a single-set plan carries no named sets.
        dag_sets: None,
        config_path: config_path.to_path_buf(),
        // Set by the production loader (stat-before-read); None for the
        // load-time direct-parse path (insert_ready stats on its own).
        source_mtime: None,
    })
}

