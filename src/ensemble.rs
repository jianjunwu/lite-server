use crate::error::{AppError, ModelErrorData};
use crate::http::state::AppState;
use crate::proto::liteserver as pb;
use crate::registry::types::ModelType;
use bytes::Bytes;
use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use indexmap::IndexMap;
use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Duration};
use tracing::{debug, info, warn};
use uuid::Uuid;

// ===== EnsembleValue: typed step input/output (B3, E6) =====

/// The materialized outputs of one step — `(context key, value)` pairs
/// (MIMO: `step.alias` keys; undeclared steps yield the single `step` key).
type StepResults = Vec<(String, EnsembleValue)>;

/// A value flowing through an ensemble DAG edge.
///
/// `Json` is the historical path — all steps participate in field-level
/// `$ref` resolution and merge into JSON objects. `Binary` is the
/// passthrough path; D8 (MIMO) boundedly opens binary flow between internal
/// steps: whole-value references only, declared types, sole-input
/// consumption (§5.5.2 I1-I3). D31 extends Binary with optional
/// shape/datatype metadata (carried from the KServe envelope head).
///
/// `Envelope` is the MIMO request-side wire form (D31/D32): a KServe JSON
/// head plus the binary tail — internal only, produced by the transport
/// de-framing shims and consumed by [`parse_root_inputs`]; it NEVER flows
/// on a DAG edge (I1-I3 make every step output Json or Binary).
#[derive(Debug, Clone, PartialEq)]
pub enum EnsembleValue {
    Json(serde_json::Value),
    Binary(
        Bytes,
        String,           /* content_type */
        Option<Vec<i64>>, /* shape (D31) */
        Option<String>,   /* datatype (D31) */
    ),
    Envelope {
        head: serde_json::Value,
        tail: Bytes,
    },
}

// ===== Config parsing =====

// D24: the ensemble schema denies unknown fields — keys from a newer release
// (params/when/outputs/dags/inputs/step.outputs, 0.9.0) or plain typos must
// fail fast at load, never be silently ignored (a swallowed `stream:` typo
// would silently disable streaming). NOTE: only the ensemble section denies —
// the top-level file shares space with model-config keys (max_batch_size …),
// so EnsembleConfig itself must stay open.
#[derive(Debug, Clone, Deserialize)]
pub struct EnsembleConfig {
    pub ensemble: EnsembleBlock,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnsembleBlock {
    /// E8-1 (batch 5): required in the single-set form; FORBIDDEN (empty)
    /// in the dags form — everything lives inside the sets.
    #[serde(default)]
    pub steps: Vec<EnsembleStepRaw>,
    /// E2 (batch 3): explicit DAG output — `$stepN` or `$stepN.field`.
    /// Omitted = `steps.last()` (historical semantics).
    #[serde(default)]
    pub output: Option<String>,
    /// MIMO (D8/D9, batch 4①): request-level named inputs — the static type
    /// environment's root. None = the historical single anonymous input
    /// (`$request`); Some = the KServe-envelope wire (D31), `$inputs.NAME`
    /// refs, and no anonymous root (R5).
    #[serde(default)]
    pub inputs: Option<IndexMap<String, InputDecl>>,
    /// E7 (batch 4④): multi-sink outputs `{alias: $ref}` — mutually
    /// exclusive with `output` (E2); the response is a KServe envelope
    /// (JSON head outputs[] + binary tail, D31). Absent = the historical
    /// single-output contract.
    #[serde(default)]
    pub outputs: Option<IndexMap<String, String>>,
    /// E8-1 (batch 5): named DAG sets selected via `x-lite-dag` — each set
    /// carries its own steps/output/outputs/inputs and validates
    /// independently (R15). Present = the dags form (top-level fields
    /// forbidden); absent = the historical single-set form.
    #[serde(default)]
    pub dags: Option<IndexMap<String, EnsembleDagSet>>,
}

/// E8-1 (batch 5): a named DAG set — the same field surface as the
/// single-set form (steps/output/outputs/inputs), validated through the
/// same pipeline (R15: independent per-set validation).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnsembleDagSet {
    pub steps: Vec<EnsembleStepRaw>,
    #[serde(default)]
    pub output: Option<String>,
    #[serde(default)]
    pub outputs: Option<IndexMap<String, String>>,
    #[serde(default)]
    pub inputs: Option<IndexMap<String, InputDecl>>,
}

/// MIMO (D8/D31): a named root input's declaration — the static type of
/// `$inputs.NAME`. `type` is mandatory (the static type environment's
/// foundation); the shape/datatype fields are carried onto the Binary value
/// (D31, hint-only, never enforced).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputDecl {
    #[serde(rename = "type")]
    pub ty: InputType,
    /// R2: required (default true) means the envelope must carry the input.
    #[serde(default = "default_true")]
    pub required: bool,
    /// json only (R2); a default makes the input never-absent (not
    /// conditional, R4).
    pub default: Option<serde_json::Value>,
    /// binary only (R2): expected MIME — documentation/hint, not enforced.
    pub content_type: Option<String>,
    /// binary only (R2, D31): expected shape — hint, carried onto the value.
    pub shape: Option<Vec<i64>>,
    /// binary only (R2, D31): expected datatype — hint, carried onto the value.
    pub datatype: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InputType {
    Json,
    Binary,
}

fn default_true() -> bool {
    true
}

/// MIMO (D10): a step's named output projection against its single worker
/// response. `type: binary` + no `path` = the whole response (non-JSON
/// media_type); `type: binary` + `path` = a `$binary_b64` marker object at
/// that JSON path (secondary in-JSON path); `type: json` = a `$.a.b`-style
/// projection (MIMO②).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepOutputDecl {
    #[serde(rename = "type")]
    pub ty: InputType,
    pub path: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnsembleStepRaw {
    pub name: String,
    pub model: String,
    /// E4 (batch 3): omitted or "latest" resolves at EXECUTION time via the
    /// D15 request-scoped snapshot (registry active); explicit versions keep
    /// the historical behavior. Stored unresolved in the plan — the P0 cache
    /// never invalidates on active-version drift.
    #[serde(default)]
    pub version: Option<String>,
    pub inputs: HashMap<String, String>,
    /// §4.1: tail streaming. The streaming step must be the DAG output
    /// (config `steps.last()`, or the explicit `output` — E2, batch 3).
    #[serde(default)]
    pub stream: bool,
    /// E3 (batch 3): constant step parameters merged into the assembled JSON
    /// payload (params win on key conflicts). Binary assembly has no params
    /// semantics — a non-empty params on a Binary step is rejected at
    /// assembly time.
    #[serde(default)]
    pub params: HashMap<String, Value>,
    /// E5 (batch 3): per-step wall-clock cap (seconds); None = parent
    /// deadline only.
    #[serde(default)]
    pub timeout_secs: Option<f64>,
    /// E6 (batch 4): fault tolerance — `fail` (default, historical) or
    /// `skip` (the step's absence must be parse-provable: no downstream
    /// references, no ensemble.output, never on a streaming step, D5/D34).
    #[serde(default)]
    pub on_error: Option<OnErrorKind>,
    /// E6 (batch 4): worker-inference retries (5xx/timeouts only, exponential
    /// backoff; 4xx is a client contract and never retries). For streaming
    /// steps the window is build-limited (D35: send_stream → first non-Error
    /// frame). Default 0 = historical single attempt.
    #[serde(default)]
    pub retries: Option<u32>,
    /// MIMO (D10, batch 4): step-level named outputs — projections against
    /// the single worker response. None = the historical single output
    /// (`$stepX`, static type json). R10: streaming steps must not declare
    /// outputs (chunks have no named-output semantics, D11).
    #[serde(default)]
    pub outputs: Option<IndexMap<String, StepOutputDecl>>,
    /// E8-2 (batch 5): a when condition — false skips the step at runtime
    /// (E6-skip channel, D34 forbids streaming × when).
    #[serde(default)]
    pub when: Option<String>,
}

/// E6 (batch 4): step fault tolerance modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OnErrorKind {
    #[default]
    Fail,
    Skip,
}

/// E8-2 (batch 5): when-expression operators — the whitelisted set only
/// (no arbitrary expression evaluation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WhenOp {
    Eq,
    Neq,
    Contains,
    In,
}

/// E8-2 (batch 5): the when-expression target — the R16/m2 whitelist:
/// `$request.dag` / `$request.client_ip` / `$inputs.NAME[.path]`.
#[derive(Debug, Clone, PartialEq)]
pub enum WhenTarget {
    Dag,
    ClientIp,
    Input { name: String, path: Option<String> },
}

/// E8-2 (batch 5): a parsed `when: "<ref> <op> <literal>"` expression.
#[derive(Debug, Clone)]
pub struct WhenExpr {
    pub target: WhenTarget,
    pub op: WhenOp,
    pub literal: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct EnsembleStep {
    pub name: String,
    pub model: String,
    /// E4: unresolved form — None = omitted/"latest" (execution-time
    /// resolution via the D15 snapshot).
    pub version: Option<String>,
    pub inputs: HashMap<String, String>,
    pub stream: bool,
    pub params: HashMap<String, Value>,
    pub timeout_secs: Option<f64>,
    /// E6 (batch 4): fault tolerance — `fail` (default) or `skip`.
    pub on_error: OnErrorKind,
    /// E6 (batch 4): worker-inference retry budget (see the raw field).
    pub retries: u32,
    /// MIMO (D10, batch 4): named output declarations (see the raw field).
    pub outputs_decl: Option<IndexMap<String, StepOutputDecl>>,
    /// E8-2 (batch 5): the parsed when condition (parse-validated against
    /// the R16 whitelist; evaluated per request).
    pub when: Option<WhenExpr>,
}

lazy_static::lazy_static! {
    static ref REF_RE: Regex = Regex::new(r"^\$(\w+)(?:\.(.+))?$")
        .expect("invalid ensemble ref regex");
    /// R1: input/alias names — `[A-Za-z_][A-Za-z0-9_]*` (the `$inputs.NAME`
    /// grammar's first segment depends on it).
    static ref IDENT_RE: Regex = Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$")
        .expect("invalid ident regex");
    /// R6: step.outputs projection paths — `$.a.b` dot segments only, no
    /// array subscripts or filters (D29).
    static ref JSON_PATH_RE: Regex =
        Regex::new(r"^\$(\.[A-Za-z_][A-Za-z0-9_]*)+$").expect("invalid json path regex");
    /// E8-2: `when: "$ref OP literal"` — OP in the whitelisted set.
    static ref WHEN_RE: Regex =
        Regex::new(r"^\$(\S+)\s*(==|!=|contains|in)\s*(.+)$").expect("invalid when regex");
}

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
fn when_passes(
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
fn build_response(
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

/// E2 (batch 3): resolve `ensemble.output` — `$stepN` or `$stepN.field` —
/// into the output step index + optional field path. Omitted = `steps.last()`
/// (historical semantics).
fn resolve_output(
    output: Option<&str>,
    steps: &[EnsembleStep],
) -> Result<(usize, Option<String>), AppError> {
    let Some(output) = output else {
        return Ok((steps.len() - 1, None));
    };
    let caps = REF_RE.captures(output).ok_or_else(|| {
        AppError::Config(format!(
            "invalid ensemble.output '{}': expected $stepN or $stepN.field",
            output
        ))
    })?;
    let source = caps.get(1).unwrap().as_str();
    if source == "request" {
        return Err(AppError::Config(
            "ensemble.output must reference a step, not $request".to_string(),
        ));
    }
    let idx = steps
        .iter()
        .position(|s| s.name == source)
        .ok_or_else(|| {
            AppError::Config(format!(
                "ensemble.output references unknown step '{}'",
                source
            ))
        })?;
    Ok((idx, caps.get(2).map(|m| m.as_str().to_string())))
}

/// MIMO (batch 4①, R1-R5/R9/R11/R12): build the static type environment.
/// With an `inputs` declaration every ref's type is parse-decidable:
/// - R1/R2: declaration field validation (names, type gating);
/// - R3/R5: namespace rules (binary paths, legacy `$request`, undeclared `$inputs`);
/// - R9: params × Binary is a static error once the mode is decided;
/// - R4: optional-input refs make the step CONDITIONAL (runtime skip, D13);
/// - R11/R12: per-step input-mode dispatch (GroupJson / BinaryPassThrough, mixed → error).
///
/// Legacy configs (no declaration) return all-None modes (dynamic
/// dispatch, byte-identical) and reject `$inputs` (R5).
#[allow(clippy::type_complexity)] // (modes, conditional) is a natural parse pair
fn analyze_static_types(
    steps: &[EnsembleStep],
    inputs_decl: Option<&IndexMap<String, InputDecl>>,
) -> Result<(Vec<Option<InputMode>>, Vec<Vec<String>>), AppError> {
    let mut modes = Vec::with_capacity(steps.len());
    let mut conditional = Vec::with_capacity(steps.len());

    if let Some(decl) = inputs_decl {
        // R1/R2: declaration-level validation (once per plan).
        for (name, d) in decl {
            if !IDENT_RE.is_match(name) {
                return Err(AppError::Config(format!(
                    "ensemble.inputs name '{name}' is not a valid identifier \
                     ([A-Za-z_][A-Za-z0-9_]*, R1)"
                )));
            }
            match d.ty {
                InputType::Json => {
                    if d.content_type.is_some() || d.shape.is_some() || d.datatype.is_some() {
                        return Err(AppError::Config(format!(
                            "ensemble.inputs '{name}': content_type/shape/datatype are \
                             binary-only fields (R2)"
                        )));
                    }
                }
                InputType::Binary => {
                    if d.default.is_some() {
                        return Err(AppError::Config(format!(
                            "ensemble.inputs '{name}': default is json-only (R2)"
                        )));
                    }
                }
            }
            if d.required && d.default.is_some() {
                return Err(AppError::Config(format!(
                    "ensemble.inputs '{name}': required: true conflicts with a default (R2)"
                )));
            }
        }
    }

    for step in steps {
        // R10: streaming chunks have no named-output semantics (D11).
        if step.stream && step.outputs_decl.is_some() {
            return Err(AppError::Config(format!(
                "step '{}': stream: true cannot declare step.outputs \
                 (streaming chunks have no named outputs, R10/D11)",
                step.name
            )));
        }
        // Ref analysis. The R7/R8 disambiguation of DECLARED step refs
        // applies in both modes (step.outputs declarations are mode-
        // independent); the `$inputs`/`$request` namespace rules and the
        // static input-mode dispatch are declared-mode-only (the legacy
        // root's type is unknown at parse — Raw binary may arrive).
        let mut mode: Option<InputMode> = None;
        let mut cond: Vec<String> = Vec::new();
        for ref_str in step.inputs.values() {
            let caps = REF_RE.captures(ref_str).ok_or_else(|| {
                AppError::Config(format!("invalid reference format: {}", ref_str))
            })?;
            let source = caps.get(1).unwrap().as_str();
            let rest = caps.get(2).map(|m| m.as_str());
            match source {
                "request" => {
                    if inputs_decl.is_some() {
                        return Err(AppError::Config(format!(
                            "step '{}' references '{}' — declared-inputs ensembles have no \
                             anonymous root; use $inputs.NAME (R5)",
                            step.name, ref_str
                        )));
                    }
                    // Legacy root: dynamic type — no static mode.
                }
                "inputs" => {
                    let Some(decl) = inputs_decl else {
                        return Err(AppError::Config(format!(
                            "step '{}' references '{}' — $inputs requires an \
                             ensemble.inputs declaration (R5)",
                            step.name, ref_str
                        )));
                    };
                    let name = rest.and_then(|r| r.split('.').next()).unwrap_or("");
                    let d = decl.get(name).ok_or_else(|| {
                        AppError::Config(format!(
                            "step '{}' references undeclared input '{}' (R5)",
                            step.name, ref_str
                        ))
                    })?;
                    let has_path = rest.map(|r| r.contains('.')).unwrap_or(false);
                    match d.ty {
                        InputType::Json => {
                            if mode == Some(InputMode::BinaryPassThrough) {
                                return Err(AppError::Config(format!(
                                    "step '{}': mixed JSON/Binary inputs are forbidden (R12) \
                                     — a binary input must be the step's sole input",
                                    step.name
                                )));
                            }
                            mode = Some(InputMode::GroupJson);
                            if !d.required && d.default.is_none() {
                                cond.push(name.to_string());
                            }
                        }
                        InputType::Binary => {
                            if has_path {
                                return Err(AppError::Config(format!(
                                    "step '{}': binary input '{}' must be referenced whole \
                                     (R3 — no field projection on binary)",
                                    step.name, ref_str
                                )));
                            }
                            if mode.is_some() {
                                return Err(AppError::Config(format!(
                                    "step '{}': a binary input must be the step's SOLE whole \
                                     input (R12); '{}' combines with other inputs",
                                    step.name, ref_str
                                )));
                            }
                            mode = Some(InputMode::BinaryPassThrough);
                            if !d.required && d.default.is_none() {
                                cond.push(name.to_string());
                            }
                        }
                    }
                }
                _ => {
                    // Step ref — the static type comes from the source step's
                    // declaration (undeclared steps are json, I3).
                    let src = steps
                        .iter()
                        .find(|s| s.name == source)
                        .ok_or_else(|| {
                            AppError::Config(format!(
                                "step '{}' references unknown step '{}'",
                                step.name, source
                            ))
                        })?;
                    match &src.outputs_decl {
                        None => {
                            // Undeclared → static type json (whole or legacy
                            // field path). Legacy configs stay dynamic
                            // (mode None) — declared configs get GroupJson.
                            if inputs_decl.is_some() {
                                if mode == Some(InputMode::BinaryPassThrough) {
                                    return Err(AppError::Config(format!(
                                        "step '{}': mixed JSON/Binary inputs are forbidden (R12)",
                                        step.name
                                    )));
                                }
                                mode = Some(InputMode::GroupJson);
                            }
                        }
                        Some(alias_decl) => {
                            // R7: a declared step must be referenced by alias
                            // (both modes — the whole ref has no type).
                            let alias = rest.and_then(|r| r.split('.').next()).ok_or_else(|| {
                                AppError::Config(format!(
                                    "step '{}': '{}' — step '{}' declares outputs and must be \
                                     referenced by alias ($stepX.ALIAS, R7)",
                                    step.name, ref_str, source
                                ))
                            })?;
                            let d = alias_decl.get(alias).ok_or_else(|| {
                                AppError::Config(format!(
                                    "step '{}': '{}' — '{}' is not a declared alias of step \
                                     '{}' (R7)",
                                    step.name, ref_str, alias, source
                                ))
                            })?;
                            let has_path = rest.map(|r| r.contains('.')).unwrap_or(false);
                            match d.ty {
                                InputType::Binary => {
                                    // R8: binary aliases are whole-only.
                                    if has_path {
                                        return Err(AppError::Config(format!(
                                            "step '{}': binary alias '{}' must be referenced \
                                             whole (R8)",
                                            step.name, ref_str
                                        )));
                                    }
                                    if inputs_decl.is_some() {
                                        if mode.is_some() {
                                            return Err(AppError::Config(format!(
                                                "step '{}': a binary input must be the step's \
                                                 SOLE whole input (R12)",
                                                step.name
                                            )));
                                        }
                                        mode = Some(InputMode::BinaryPassThrough);
                                    }
                                }
                                InputType::Json => {
                                    // MIMO② (D10): json alias projections —
                                    // static type json (GroupJson).
                                    if inputs_decl.is_some() {
                                        if mode == Some(InputMode::BinaryPassThrough) {
                                            return Err(AppError::Config(format!(
                                                "step '{}': mixed JSON/Binary inputs are forbidden (R12)",
                                                step.name
                                            )));
                                        }
                                        mode = Some(InputMode::GroupJson);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        // R9: with the mode parse-decided, params × Binary is static.
        if mode == Some(InputMode::BinaryPassThrough) && !step.params.is_empty() {
            return Err(AppError::Config(format!(
                "step '{}': params cannot combine with a binary input (R9/E3)",
                step.name
            )));
        }
        modes.push(mode);
        conditional.push(cond);
    }
    Ok((modes, conditional))
}

/// E6 (batch 4, D5/D34) + MIMO R4 (batch 4①, D13): an ABSENTABLE step
/// (`on_error: skip`, or conditional — referencing an optional input) is
/// absent from the context when its error fires / its input is missing —
/// every consumer of its output must be statically provable as safe at parse
/// time:
///   1. no OTHER step may reference it (`$name` whole or `$name.field` —
///      both dangle identically at runtime);
///   2. `ensemble.output` may not point at it (the single-output contract
///      has no null channel — only E7's multi-sink outputs do, D5);
///   3. a streaming step may never be absent (D34 rule 6: the streaming
///      response contract promises a stream unconditionally).
fn validate_skip_rules(
    steps: &[EnsembleStep],
    output_step: usize,
    conditional_refs: &[Vec<String>],
    // E7: with ensemble.outputs present the single-output contract is
    // REPLACED — an absentable output step is fine (its aliases use the D5
    // null channel); the rule only binds the implicit single output.
    outputs_present: bool,
) -> Result<(), AppError> {
    let absentable: HashSet<&str> = steps
        .iter()
        .enumerate()
        .filter(|(i, s)| {
            s.on_error == OnErrorKind::Skip
                || !conditional_refs[*i].is_empty()
                || s.when.is_some() // E8-2: a when-false step is absent too
        })
        .map(|(_, s)| s.name.as_str())
        .collect();
    if absentable.is_empty() {
        return Ok(());
    }

    // Rule 3 first (cheapest): stream × absence is a structural
    // contradiction (skip arm names skip; conditional arm names the input).
    for (i, s) in steps.iter().enumerate() {
        if !s.stream {
            continue;
        }
        if s.on_error == OnErrorKind::Skip {
            return Err(AppError::Config(format!(
                "step '{}': on_error: skip cannot combine with stream: true \
                 (a streaming response promises a stream unconditionally, D34)",
                s.name
            )));
        }
        if !conditional_refs[i].is_empty() {
            return Err(AppError::Config(format!(
                "step '{}': a streaming step cannot reference optional inputs ({:?}) \
                 (a streaming response promises a stream unconditionally, D34)",
                s.name, conditional_refs[i]
            )));
        }
    }

    // Rule 1: any input reference whose first segment names an absentable
    // step.
    for s in steps {
        if absentable.contains(s.name.as_str()) {
            continue; // self-references are caught by validate_dag anyway
        }
        for ref_str in s.inputs.values() {
            let Some(caps) = REF_RE.captures(ref_str) else {
                continue; // malformed refs are reported elsewhere (validate_dag)
            };
            let source = caps.get(1).unwrap().as_str();
            if absentable.contains(source) {
                return Err(AppError::Config(format!(
                    "step '{}' references potentially-absent step '{}' via '{}' — \
                     an on_error: skip or optional-input step leaves the DAG dangling \
                     (D5/R4); use ensemble.outputs for a null-when-absent alias",
                    s.name, source, ref_str
                )));
            }
        }
    }

    // Rule 2: the single-output contract has no null channel (E7's
    // multi-sink outputs DO — D5 — so the rule binds only without them).
    let output_step = &steps[output_step];
    if !outputs_present && absentable.contains(output_step.name.as_str()) {
        return Err(AppError::Config(format!(
            "ensemble.output references potentially-absent step '{}' — the single-output \
             contract has no null channel; use ensemble.outputs (alias = null when \
             absent, D5)",
            output_step.name
        )));
    }

    Ok(())
}

/// Production plan loader: resolve the model dir, read config.yaml and parse
/// into a cached plan (P0). Wrapped by [`get_ensemble_plan`].
async fn load_ensemble_plan(
    repo_path: &std::path::Path,
    model: &str,
    version: &str,
) -> Result<Arc<EnsemblePlan>, AppError> {
    let model_dir = crate::validation::resolve_model_dir(repo_path, model, version)?;
    let config_path = model_dir.join("config.yaml");
    // Stat BEFORE the read: a write landing between stat and read leaves the
    // stored mtime OLDER than the file's, so the interval re-check re-parses
    // (safe). The reverse order could pin a fresh mtime onto stale content
    // and serve it indefinitely (the watcher-miss fallback's whole point).
    let source_mtime = std::fs::metadata(&config_path)
        .and_then(|m| m.modified())
        .ok();
    let content = tokio::fs::read_to_string(&config_path)
        .await
        .map_err(|e| AppError::Config(format!("failed to read ensemble config: {}", e)))?;
    let mut plan = parse_ensemble_plan(&content, &config_path)?;
    plan.source_mtime = source_mtime;
    Ok(Arc::new(plan))
}

/// P6 (batch 0 scope): background plan warm, spawned from the lifecycle
/// load_model ensemble branch — prime the P0 cache so the first request
/// never pays the config parse, and pre-check sub-model readiness.
/// Non-blocking (load_model returns immediately; the warm runs detached).
/// The resolved-version side-table and sub-model preloading land with E4
/// (batch 3) — warm here only parses + checks.
pub fn spawn_ensemble_warm(
    repo_path: PathBuf,
    plans: Option<Arc<EnsemblePlanCache>>,
    registry: Arc<crate::registry::ModelRegistry>,
    model_name: String,
    version: String,
) {
    tokio::spawn(async move {
        let Some(plans) = plans else { return; };
        let key = PlanKey {
            model: model_name.clone(),
            version: version.clone(),
        };
        let plan = match plans
            .get_or_load(key, || load_ensemble_plan(&repo_path, &model_name, &version))
            .await
        {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    model = %model_name,
                    error = %e,
                    "ensemble warm: plan parse failed (first request retries)"
                );
                return;
            }
        };
        // E4 (batch 3): pre-resolve every sub-model step and preheat the
        // routing/LRU caches. D27: this warm pass is a TTFT hint ONLY — it is
        // NEVER a resolution source; the request-time D15 snapshot always
        // re-resolves against the registry, so an active drift between warm
        // and first request has no freeze window.
        let warm_snapshot = Arc::new(VersionSnapshot::default());
        for step in &plan.steps {
            match warm_snapshot.resolve(&registry, step) {
                Ok(resolved) => {
                    // touch_last_used: an ensemble's sub-models must count as
                    // used, or LRU eviction can unload a live DAG dependency.
                    registry.touch_last_used(&step.model, &resolved);
                    // routing_pick exercises the weighted-routing cache the
                    // first request would otherwise pay cold.
                    registry.routing_pick(&step.model);
                    if !registry.is_ready(&step.model, Some(&resolved)) {
                        warn!(
                            model = %step.model,
                            version = %resolved,
                            "ensemble warm: sub-model not ready (first request autoloads)"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        model = %step.model,
                        error = %e,
                        "ensemble warm: sub-model version resolution failed"
                    );
                }
            }
        }
    });
}

/// P0 entry: cached plan lookup with single-flight + mtime re-check; falls
/// back to a direct load when no cache is installed (tests construct
/// WorkerManager without one).
pub async fn get_ensemble_plan(
    state: &AppState,
    model_name: &str,
    version: &str,
) -> Result<Arc<EnsemblePlan>, AppError> {
    let key = PlanKey {
        model: model_name.to_string(),
        version: version.to_string(),
    };
    match state.worker_manager.ensemble_plans() {
        Some(cache) => cache
            .get_or_load(key, || load_ensemble_plan(&state.repo_path, model_name, version))
            .await,
        None => load_ensemble_plan(&state.repo_path, model_name, version).await,
    }
}

fn validate_dag(steps: &[EnsembleStep]) -> Result<(), AppError> {
    // An empty DAG has no output step — reject at parse instead of panicking
    // on `steps.len() - 1` (pre-batch-0 this degenerated into a request-time
    // 500; with the plan cache it must be a load-time config error).
    if steps.is_empty() {
        return Err(AppError::Config("ensemble declares no steps".to_string()));
    }
    let step_names: HashSet<&str> = steps.iter().map(|s| s.name.as_str()).collect();

    // Check for duplicate names
    if step_names.len() != steps.len() {
        return Err(AppError::Config("duplicate step names in ensemble".to_string()));
    }

    // Build dependency graph
    let mut dependencies: HashMap<&str, HashSet<&str>> = HashMap::new();
    for step in steps {
        let deps = dependencies.entry(&step.name).or_default();
        for ref_str in step.inputs.values() {
            let caps = REF_RE.captures(ref_str).ok_or_else(|| {
                AppError::Config(format!("invalid reference format: {}", ref_str))
            })?;
            let source = caps.get(1).unwrap().as_str();
            // MIMO: `$inputs.NAME` is a named-input ref, not a step dep (its
            // validity is checked by analyze_static_types).
            if source != "request" && source != "inputs" && !step_names.contains(source) {
                return Err(AppError::Config(format!(
                    "step '{}' references unknown step '{}'",
                    step.name, source
                )));
            }
            if source != "request" && source != "inputs" {
                deps.insert(source);
            }
        }
    }

    // Kahn's algorithm for cycle detection
    let mut in_degree: HashMap<&str, usize> = steps.iter()
        .map(|s| (s.name.as_str(), 0))
        .collect();
    for (step_name, deps) in &dependencies {
        for _dep in deps {
            *in_degree.get_mut(*step_name).unwrap() += 1;
        }
    }

    let mut queue: Vec<&str> = in_degree.iter()
        .filter(|(_, d)| **d == 0)
        .map(|(n, _)| *n)
        .collect();
    let mut visited = 0;

    while let Some(node) = queue.pop() {
        visited += 1;
        for step in steps {
            if dependencies.get(step.name.as_str()).map(|d| d.contains(node)).unwrap_or(false) {
                let deg = in_degree.get_mut(step.name.as_str()).unwrap();
                *deg -= 1;
                if *deg == 0 {
                    queue.push(&step.name);
                }
            }
        }
    }

    if visited != steps.len() {
        return Err(AppError::Config("cycle detected in ensemble DAG".to_string()));
    }

    Ok(())
}

/// §4.0/D16 form-dispatching validation for `stream: true` steps (batch 0:
/// only the non-pipeline form is open). Rule 2 (streaming output not
/// referenced) is the definition of the form split — a referenced streaming
/// output IS the pipeline form, rejected here with an explicit
/// batch-2 message instead of being mis-killed by rules 1-3. The same
/// dispatch point opens pipeline form in batch 2 (no validator rework).
///
/// Rules (non-pipeline form):
/// 1. streaming step must be in the last topological layer — checked
///    explicitly below: "output not referenced" does NOT imply it (Kahn
///    layering puts a no-dependency sink in layer 0 even when later layers
///    exist), and accepting such a DAG would truncate execution at the
///    tail's layer, silently never running the steps in later layers.
/// 3. at most one streaming step per DAG.
/// 4. output-step semantics unified (E2 base, batch 0): with `output`
///    omitted the DAG output is `steps.last()`, which MUST be the streaming
///    step — otherwise a streaming DAG would silently produce nothing
///    streamable (B-m4: explicit `output` lands with E2 in batch 3).
///
/// A linear pipeline chain (§4.2): consecutive STREAMING steps where each
/// step's output is consumed whole by the next (P-R1/P-R2), ending at the
/// DAG output step (P-R3/D26). Steps are indices into `EnsemblePlan.steps`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chain {
    pub nodes: Vec<usize>,
}

/// §4.0/D16 pipeline-form validation + chain construction (batch 2 opens the
/// form the batch-0 validator rejected). Rules P-R1..P-R5 + D26:
///   P-R1  a streaming output has exactly ONE consumer (0 → non-pipeline;
///         ≥2 → error)
///   P-R2  streaming outputs are referenced WHOLE only ($s0; $s0.field → error)
///   P-R3  the chain tail must be the DAG output step (and streaming — the
///         unary-tail shape is rejected here, D26's "pipeline chain tail
///         must be a streaming step")
///   P-R4  chains never share steps (Kahn covers cycles; the linear walk
///         below marks every node visited — a shared step fails)
///   P-R5  the DAG's streaming set is exactly one chain OR one tail streaming
///         step — a chain plus an orphan streaming step is an error
///   D26   a chain's unary consumer has no clean chunk→unary→chunk semantics
///         → the whole form is rejected at parse time
fn build_chains(steps: &[EnsembleStep], output_step: usize) -> Result<Vec<Chain>, AppError> {
    let is_stream: Vec<bool> = steps.iter().map(|s| s.stream).collect();
    let stream_count = is_stream.iter().filter(|b| **b).count();
    if stream_count == 0 {
        return Ok(Vec::new());
    }

    // Consumer edges: streaming step index → (consumer index, field-ref?).
    let mut consumers: HashMap<usize, Vec<(usize, bool)>> = HashMap::new();
    for (i, step) in steps.iter().enumerate() {
        for ref_str in step.inputs.values() {
            let Some(caps) = REF_RE.captures(ref_str) else { continue };
            let source = caps.get(1).unwrap().as_str();
            let Some(src_idx) = steps.iter().position(|s| s.name == source) else { continue };
            if is_stream[src_idx] {
                consumers
                    .entry(src_idx)
                    .or_default()
                    .push((i, caps.get(2).is_some()));
            }
        }
    }

    let mut chains: Vec<Chain> = Vec::new();
    let mut visited: HashSet<usize> = HashSet::new();
    for s in 0..steps.len() {
        if !is_stream[s] || visited.contains(&s) {
            continue;
        }
        match consumers.get(&s).map(|v| v.as_slice()) {
            None => {
                // Zero consumers = non-pipeline (tail-streaming) form. An
                // orphan that is NOT the output step breaks the output
                // semantics (§4.1-4 / P-R5).
                if s != output_step {
                    return Err(AppError::Config(format!(
                        "streaming step '{}' has no consumer and is not the DAG \
                         output step — the DAG mixes a pipeline chain with an \
                         orphan streaming step (P-R5)",
                        steps[s].name
                    )));
                }
            }
            Some([(c, field_ref)]) => {
                // Chain start (P-R1: exactly one consumer).
                if *field_ref {
                    return Err(AppError::Config(format!(
                        "streaming step '{}' output must be referenced whole \
                         ($step), not field-projected (P-R2)",
                        steps[s].name
                    )));
                }
                // Walk the consumer chain. Every node must be streaming —
                // a unary consumer is the D26-rejected chunk→unary→chunk form
                // (P-R3's "pipeline chain tail must be a streaming step").
                let mut nodes = vec![s];
                let mut cur = s;
                let mut next_consumer = *c;
                loop {
                    let c = next_consumer;
                    if !is_stream[c] {
                        return Err(AppError::Config(format!(
                            "pipeline chain step '{}' consumes streaming step \
                             '{}' but is not streaming — the pipeline chain \
                             tail must be a streaming step; unary consumers \
                             on a chain are not defined (D26)",
                            steps[c].name, steps[cur].name
                        )));
                    }
                    if visited.contains(&c) {
                        return Err(AppError::Config(format!(
                            "pipeline chains share step '{}' (P-R4)",
                            steps[c].name
                        )));
                    }
                    nodes.push(c);
                    visited.insert(c);
                    match consumers.get(&c).map(|v| v.as_slice()) {
                        None => break, // chain tail
                        Some([(c2, field_ref2)]) => {
                            if *field_ref2 {
                                return Err(AppError::Config(format!(
                                    "streaming step '{}' output must be referenced \
                                     whole ($step), not field-projected (P-R2)",
                                    steps[c].name
                                )));
                            }
                            cur = c;
                            next_consumer = *c2;
                        }
                        Some(_) => {
                            return Err(AppError::Config(format!(
                                "streaming step '{}' output has multiple \
                                 consumers (P-R1)",
                                steps[c].name
                            )));
                        }
                    }
                }
                let tail = *nodes.last().unwrap();
                if tail != output_step {
                    return Err(AppError::Config(format!(
                        "pipeline chain tail '{}' must be the DAG output step \
                         (config last step, or the explicit `output:`) (P-R3)",
                        steps[tail].name
                    )));
                }
                chains.push(Chain { nodes });
            }
            Some(_) => {
                return Err(AppError::Config(format!(
                    "streaming step '{}' output has multiple consumers (P-R1)",
                    steps[s].name
                )));
            }
        }
    }
    Ok(chains)
}

fn validate_stream_rules(steps: &[EnsembleStep], output_step: usize) -> Result<(), AppError> {
    let stream_steps: Vec<&EnsembleStep> = steps.iter().filter(|s| s.stream).collect();
    if stream_steps.is_empty() {
        return Ok(());
    }
    if stream_steps.len() > 1 {
        return Err(AppError::Config(
            "at most one streaming step per ensemble DAG (rule 3)".to_string(),
        ));
    }
    let tail = stream_steps[0];

    // Rule 1: the streaming step must be in the LAST topological layer.
    // "Output not referenced" only proves it has no dependents — Kahn
    // layering puts a no-dependency sink in layer 0 even when later layers
    // exist, so without this check a streaming step in an early layer would
    // truncate execution (run_layers stops at the tail's layer) and steps in
    // later layers would silently never run. Load-time cost only (parse path).
    let layers = topological_layers(steps);
    let in_last_layer = layers
        .last()
        .map(|l| l.iter().any(|s| s.name == tail.name))
        .unwrap_or(false);
    if !in_last_layer {
        return Err(AppError::Config(format!(
            "streaming step '{}' must be in the last topological layer (rule 1); \
             steps in later layers would silently never execute",
            tail.name
        )));
    }

    // Rule 4 (B-m4, E2 batch 3): the DAG output step — config last step or
    // the explicit `output:` — must be the streaming step.
    if steps[output_step].name != tail.name {
        return Err(AppError::Config(format!(
            "streaming step '{}' must be the DAG output step (config last \
             step, or the explicit `output:`), or the DAG output is ambiguous",
            tail.name
        )));
    }

    Ok(())
}

fn topological_layers(steps: &[EnsembleStep]) -> Vec<Vec<&EnsembleStep>> {
    let mut dependencies: HashMap<&str, HashSet<&str>> = HashMap::new();
    for step in steps {
        let deps = dependencies.entry(&step.name).or_default();
        for ref_str in step.inputs.values() {
            if let Some(caps) = REF_RE.captures(ref_str) {
                let source = caps.get(1).unwrap().as_str();
                // MIMO: `$inputs.NAME` is a named-input ref, not a step dep.
                if source != "request" && source != "inputs" {
                    deps.insert(source);
                }
            }
        }
    }

    let step_map: HashMap<&str, &EnsembleStep> = steps.iter()
        .map(|s| (s.name.as_str(), s))
        .collect();

    let mut in_degree: HashMap<&str, usize> = steps.iter()
        .map(|s| (s.name.as_str(), 0))
        .collect();
    for (step_name, deps) in &dependencies {
        for _dep in deps {
            *in_degree.get_mut(*step_name).unwrap() += 1;
        }
    }

    let mut layers: Vec<Vec<&EnsembleStep>> = Vec::new();
    let mut remaining: HashSet<&str> = steps.iter().map(|s| s.name.as_str()).collect();

    while !remaining.is_empty() {
        let layer: Vec<&EnsembleStep> = remaining.iter()
            .filter(|n| in_degree.get(**n).copied().unwrap_or(0) == 0)
            .map(|n| *step_map.get(n).unwrap())
            .collect();

        if layer.is_empty() {
            break; // Should not happen if validated
        }

        for step in &layer {
            remaining.remove(step.name.as_str());
            for other in &remaining {
                if dependencies.get(*other).map(|d| d.contains(step.name.as_str())).unwrap_or(false) {
                    *in_degree.get_mut(*other).unwrap() -= 1;
                }
            }
        }

        layers.push(layer);
    }

    layers
}

/// E2 (batch 3): apply the explicit `output: "$stepN.field"` projection to
/// the DAG output value. None = whole value; Some = field extraction on a
/// Json output (Binary has no field semantics — D7's rule on the output
/// face). A missing field means the DAG's contract does not match the
/// model's output shape.
fn select_output_field(
    step_name: &str,
    value: EnsembleValue,
    field: Option<&str>,
) -> Result<EnsembleValue, AppError> {
    let Some(field) = field else {
        return Ok(value);
    };
    match value {
        EnsembleValue::Json(v) => {
            let field_val = v.get(field).cloned().ok_or_else(|| {
                AppError::Config(format!(
                    "ensemble.output field '{}' not found in step '{}' output",
                    field, step_name
                ))
            })?;
            Ok(EnsembleValue::Json(field_val))
        }
        EnsembleValue::Binary(..) => Err(AppError::InvalidRequestBody(format!(
            "ensemble.output field '{}' cannot be extracted from binary step \
             output '{}' (no field semantics on bytes)",
            field, step_name
        ))),
        EnsembleValue::Envelope { .. } => unreachable!("envelope never reaches output selection"),
    }
}

/// The result of resolving a `$ref` against the runtime context.
#[derive(Debug)]
pub enum ResolvedRef {
    /// The referenced value (Json or Binary).
    Value(EnsembleValue),
    /// MIMO R4: the referenced named input was absent this request — only
    /// reachable for optional inputs (conditional steps are skipped before
    /// resolution; a step resolving one here is an internal inconsistency).
    Absent(String),
}

/// P1 (batch 6): the context key a `$ref` resolves against — mirrors
/// [`resolve_ref`]'s key derivation exactly (parse-time use; ref shapes are
/// already validated, so unreachable shapes still derive the same key the
/// runtime would compute before erroring).
fn context_key_for_ref(steps: &[EnsembleStep], ref_str: &str) -> Result<String, AppError> {
    let caps = REF_RE.captures(ref_str).ok_or_else(|| {
        AppError::Config(format!("invalid reference: {}", ref_str))
    })?;
    let source = caps.get(1).unwrap().as_str();
    let rest = caps.get(2).map(|m| m.as_str());
    Ok(match source {
        "inputs" => {
            let name = rest.and_then(|r| r.split('.').next()).unwrap_or("");
            format!("inputs.{name}")
        }
        step_name => {
            match steps
                .iter()
                .find(|s| s.name == step_name)
                .and_then(|s| s.outputs_decl.as_ref())
            {
                Some(_decl) => {
                    let alias = rest.and_then(|r| r.split('.').next()).unwrap_or("");
                    format!("{step_name}.{alias}")
                }
                None => step_name.to_string(),
            }
        }
    })
}

/// P1 (batch 6): clone only the context keys a step references — the
/// spawn-time whole-table clone (O(payload × steps) deep copies) is gone.
/// A referenced key always exists in a running step (absent optional inputs
/// skip the step before spawn), so missing keys are simply not selected.
fn select_ctx_keys(
    context: &HashMap<String, EnsembleValue>,
    keys: &[String],
) -> HashMap<String, EnsembleValue> {
    keys.iter()
        .filter_map(|k| context.get(k).map(|v| (k.clone(), v.clone())))
        .collect()
}

/// Resolve a `$ref` against the runtime context. The PLAN's static type
/// environment decides the semantics (parse rules R3/R5/R7/R8 already
/// rejected ill-typed refs — the runtime only projects/forwards):
/// - `$request[.field]` — legacy anonymous root (parse-rejected in
///   declared mode, R5);
/// - `$inputs.NAME[.a.b]` — named root input; absent optional → Absent;
/// - `$stepX[.field]` — undeclared step (static json, legacy single-field);
/// - `$stepX.ALIAS` — declared alias; a binary alias passes through whole
///   (D8), a json alias projects (MIMO②).
///
/// Runtime type mismatches (declared json but the worker produced binary)
/// are step errors (I3's runtime failure #1).
fn resolve_ref(
    plan: &EnsemblePlan,
    ref_str: &str,
    context: &HashMap<String, EnsembleValue>,
) -> Result<ResolvedRef, AppError> {
    let caps = REF_RE.captures(ref_str).ok_or_else(|| {
        AppError::Config(format!("invalid reference: {}", ref_str))
    })?;
    let source = caps.get(1).unwrap().as_str();
    let rest = caps.get(2).map(|m| m.as_str());

    let (key, json_path) = match source {
        "inputs" => {
            let name = rest.and_then(|r| r.split('.').next()).unwrap_or("");
            let key = format!("inputs.{name}");
            let json_path = rest.and_then(|r| r.strip_prefix(name))
                .and_then(|p| p.strip_prefix('.'));
            (key, json_path)
        }
        step_name => {
            let src = plan.steps.iter().find(|s| s.name == step_name);
            match src.and_then(|s| s.outputs_decl.as_ref()) {
                Some(_decl) => {
                    // Declared step: context key is `step.alias`.
                    let alias = rest.and_then(|r| r.split('.').next()).unwrap_or("");
                    let key = format!("{step_name}.{alias}");
                    let json_path = rest.and_then(|r| r.strip_prefix(alias))
                        .and_then(|p| p.strip_prefix('.'));
                    (key, json_path)
                }
                None => {
                    // Undeclared step / request: legacy semantics — the key
                    // is the source name and the field is a SINGLE path
                    // segment. Multi-segment paths stay
                    // MIMO-declaration-only (§5.5.3) — reject them instead
                    // of silently taking the first segment (wrong data).
                    let key = source.to_string();
                    let rest = rest.unwrap_or("");
                    if rest.contains('.') {
                        return Err(AppError::Config(format!(
                            "multi-segment path '{}' is not supported on legacy \
                             steps — declare step.outputs for path projections (R7)",
                            ref_str
                        )));
                    }
                    let json_path = (!rest.is_empty()).then_some(rest);
                    (key, json_path)
                }
            }
        }
    };

    let Some(source_data) = context.get(&key) else {
        if source == "inputs" {
            // MIMO R4: an absent optional named input — Absent (conditional
            // steps are pre-skipped; reaching here is an internal bug).
            return Ok(ResolvedRef::Absent(key));
        }
        return Err(AppError::Config(format!(
            "reference source not found: {}",
            ref_str
        )));
    };

    match source_data {
        EnsembleValue::Binary(..) => match json_path {
            None => {
                // D8: binary values flow along DECLARED binary edges — the
                // legacy root (`$request`), named binary inputs
                // (`$inputs.NAME`), and declared binary aliases
                // (`$stepX.ALIAS`). Anything else is the legacy Option-A
                // boundary (undeclared step outputs — their type is unknown
                // at parse) or a declared-json mismatch (I3 runtime #1).
                let declared_binary_edge = match source {
                    "request" | "inputs" => true,
                    step_name => plan
                        .steps
                        .iter()
                        .find(|s| s.name == step_name)
                        .and_then(|s| s.outputs_decl.as_ref())
                        .and_then(|d| {
                            rest.and_then(|r| r.split('.').next()).and_then(|a| d.get(a))
                        })
                        .map(|dd| dd.ty == InputType::Binary)
                        .unwrap_or(false),
                };
                if declared_binary_edge {
                    Ok(ResolvedRef::Value(source_data.clone()))
                } else {
                    Err(AppError::InvalidRequestBody(format!(
                        "cannot reference binary step output '{}' as a whole; \
                         binary values flow only along declared binary inputs/outputs (D8) \
                         — root input → first layer or final layer → client",
                        ref_str
                    )))
                }
            }
            Some(_) => Err(AppError::InvalidRequestBody(format!(
                "cannot extract field '{}' from binary data; \
                 binary values have no field-level semantics",
                ref_str
            ))),
        },
        EnsembleValue::Json(v) => match json_path {
            None => Ok(ResolvedRef::Value(EnsembleValue::Json(v.clone()))),
            Some(f) => {
                let field_val = project_json_path(v, f).map_err(|_| {
                    AppError::Config(format!("cannot resolve '{}' from {}", ref_str, v))
                })?;
                Ok(ResolvedRef::Value(EnsembleValue::Json(field_val)))
            }
        },
        EnsembleValue::Envelope { .. } => {
            Err(AppError::Internal("envelope reached a DAG edge".to_string()))
        }
    }
}

/// MIMO: dot-segment JSON projection (`a.b.c`, an optional leading `$.` is
/// stripped). No array subscripts or filters (D29: the `$.a.b` subset only).
fn project_json_path(v: &Value, path: &str) -> Result<Value, AppError> {
    let path = path.strip_prefix("$.").unwrap_or(path);
    let mut cur = v;
    for seg in path.split('.') {
        if seg.is_empty() {
            return Err(AppError::Config(format!(
                "empty segment in JSON path '{path}'"
            )));
        }
        cur = cur.get(seg).ok_or_else(|| {
            AppError::Config(format!("path segment '{seg}' not found in '{path}'"))
        })?;
    }
    Ok(cur.clone())
}

/// MIMO (D8/D9): the request root after [`parse_root_inputs`].
/// `Single` is the legacy path (byte-identical); `Named` is the declared
/// multi-input path — `absent` lists optional inputs the envelope did not
/// carry (R4 conditional steps skip on them).
#[derive(Debug)]
pub enum RootInputs {
    Single(EnsembleValue),
    Named {
        values: IndexMap<String, EnsembleValue>,
        absent: Vec<String>,
    },
}

/// MIMO (D31/D32, R18/R19): the DAG entry's root parsing — the SINGLE R18
/// validation point (D39: endpoints de-frame transport only, never validate).
///  - decl = None (legacy): payload passes through untouched, except the
///    reserved `$inputs` namespace (400, D14) and the envelope container
///    form (400 — TritonBinary/LSBE-1 have no legacy semantics);
///  - decl = Some: the payload must be a KServe envelope — Json head with
///    `inputs[]` (plus an optional binary tail from [`EnsembleValue::Envelope`]);
///    elements match by name in header order, binary elements slice the tail
///    cumulatively (`parameters.binary_data_size`), `$binary_b64` marker data
///    decodes in-place (secondary path), defaults fill, absent optionals
///    list.
pub fn parse_root_inputs(
    payload: EnsembleValue,
    decl: Option<&IndexMap<String, InputDecl>>,
    // §5.5.8 (R15): true only for the dags form — a shared client may send
    // the SUPERSET of every set's inputs, so names outside the SELECTED set
    // are ignored with a debug log (never 400). Single-set forms keep the
    // strict R18 unknown-name rejection.
    tolerate_unknown: bool,
) -> Result<RootInputs, AppError> {
    let Some(decl) = decl else {
        match &payload {
            // R19/D14: `$inputs` is a reserved namespace on legacy payloads.
            EnsembleValue::Json(v) => {
                if v.as_object().map(|o| o.contains_key("$inputs")).unwrap_or(false) {
                    return Err(AppError::InvalidRequestBody(
                        "'$inputs' is a reserved namespace (D14) — this ensemble has no \
                         inputs declaration; send a plain JSON body"
                            .to_string(),
                    ));
                }
            }
            // The envelope container (TritonBinary / LSBE-1) has no legacy
            // semantics — undeclared ensembles reject it (historical
            // TritonBinary 400 kept byte-identical).
            EnsembleValue::Envelope { .. } => {
                return Err(AppError::InvalidRequestBody(
                    "Triton Binary Tensor Data Extension requests (JSON head + binary \
                     tail container) are only supported by ensembles with an inputs \
                     declaration"
                        .to_string(),
                ));
            }
            _ => {}
        }
        return Ok(RootInputs::Single(payload));
    };

    let (head, tail) = match payload {
        EnsembleValue::Json(v) => (v, Bytes::new()),
        EnsembleValue::Envelope { head, tail } => (head, tail),
        EnsembleValue::Binary(..) => {
            return Err(AppError::InvalidRequestBody(
                "this ensemble declares named inputs — requests must be a KServe \
                 envelope (JSON with inputs[], or JSON head + binary tail); raw \
                 binary bodies have no envelope semantics (R18)"
                    .to_string(),
            ));
        }
    };
    let inputs = head.get("inputs").and_then(|i| i.as_array()).ok_or_else(|| {
        AppError::InvalidRequestBody(
            "declared-inputs ensemble requires a KServe envelope with an inputs[] \
             array (R18/D31)"
                .to_string(),
        )
    })?;

    let mut values: IndexMap<String, EnsembleValue> = IndexMap::new();
    let mut offset = 0usize;
    for el in inputs {
        let name = el.get("name").and_then(|n| n.as_str()).ok_or_else(|| {
            AppError::InvalidRequestBody("envelope input element is missing 'name'".to_string())
        })?;
        let d = match decl.get(name) {
            Some(d) => d,
            None => {
                if tolerate_unknown {
                    // A tolerated binary element's declared tail slice must
                    // still be consumed — header-order slicing means a skip
                    // that leaves bytes unaccounted misaligns every later
                    // binary element.
                    if let Some(size) = el
                        .get("parameters")
                        .and_then(|p| p.get("binary_data_size"))
                        .and_then(|s| s.as_u64())
                    {
                        let end = offset.checked_add(size as usize).ok_or_else(|| {
                            AppError::InvalidRequestBody("binary tail size overflow".to_string())
                        })?;
                        let _ = tail.get(offset..end).ok_or_else(|| {
                            AppError::InvalidRequestBody(format!(
                                "envelope input '{name}': binary_data_size {size} overruns \
                                 the binary tail (R18)"
                            ))
                        })?;
                        offset = end;
                    }
                    debug!(
                        input = %name,
                        "envelope input ignored — not declared by the selected dag set (§5.5.8)"
                    );
                    continue;
                }
                return Err(AppError::InvalidRequestBody(format!(
                    "envelope declares unknown input '{name}' (not in ensemble.inputs, R18)"
                )));
            }
        };
        let value = match d.ty {
            InputType::Json => {
                let data = el.get("data").ok_or_else(|| {
                    AppError::InvalidRequestBody(format!(
                        "envelope input '{name}' (type json) is missing 'data'"
                    ))
                })?;
                EnsembleValue::Json(data.clone())
            }
            InputType::Binary => {
                // Primary path: header element + tail slice (header order).
                if let Some(marker) = el.get("data") {
                    // Secondary path: `$binary_b64` marker object in-JSON.
                    let (bytes, ct) = decode_binary_marker(marker)?;
                    EnsembleValue::Binary(bytes, ct, None, None)
                } else {
                    let size = el
                        .get("parameters")
                        .and_then(|p| p.get("binary_data_size"))
                        .and_then(|s| s.as_u64())
                        .ok_or_else(|| {
                            AppError::InvalidRequestBody(format!(
                                "envelope input '{name}' (type binary) needs \
                                 parameters.binary_data_size or $binary_b64 data (R18)"
                            ))
                        })? as usize;
                    let end = offset.checked_add(size).ok_or_else(|| {
                        AppError::InvalidRequestBody("binary tail size overflow".to_string())
                    })?;
                    let slice = tail.get(offset..end).ok_or_else(|| {
                        AppError::InvalidRequestBody(format!(
                            "envelope input '{name}': binary_data_size {size} overruns \
                             the binary tail (R18)"
                        ))
                    })?;
                    offset = end;
                    EnsembleValue::Binary(
                        Bytes::copy_from_slice(slice),
                        d.content_type.clone().unwrap_or_else(|| "application/octet-stream".to_string()),
                        d.shape.clone(),
                        d.datatype.clone(),
                    )
                }
            }
        };
        values.insert(name.to_string(), value);
    }
    if offset != tail.len() {
        return Err(AppError::InvalidRequestBody(format!(
            "binary tail has {} byte(s) beyond the declared sizes (R18)",
            tail.len() - offset
        )));
    }

    let mut absent = Vec::new();
    for (name, d) in decl {
        if values.contains_key(name) {
            continue;
        }
        match (d.required, &d.default) {
            (true, _) => {
                return Err(AppError::InvalidRequestBody(format!(
                    "envelope is missing required input '{name}' (R18)"
                )));
            }
            (false, Some(def)) => {
                values.insert(name.clone(), EnsembleValue::Json(def.clone()));
            }
            (false, None) => absent.push(name.clone()),
        }
    }
    Ok(RootInputs::Named { values, absent })
}

/// D31 (secondary in-JSON path): decode a `{"$binary_b64": "...",
/// "content_type": "..."}` marker object into bytes (content_type optional,
/// defaults to application/octet-stream).
fn decode_binary_marker(v: &Value) -> Result<(Bytes, String), AppError> {
    let obj = v.as_object().ok_or_else(|| {
        AppError::InvalidRequestBody(
            "binary input data must be a {\"$binary_b64\": ...} marker object".to_string(),
        )
    })?;
    let b64 = obj.get("$binary_b64").and_then(|b| b.as_str()).ok_or_else(|| {
        AppError::InvalidRequestBody(
            "binary marker object is missing the \"$binary_b64\" field".to_string(),
        )
    })?;
    let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64)
        .map_err(|e| {
            AppError::InvalidRequestBody(format!("invalid base64 in $binary_b64 marker: {e}"))
        })?;
    let ct = obj
        .get("content_type")
        .and_then(|c| c.as_str())
        .unwrap_or("application/octet-stream")
        .to_string();
    Ok((Bytes::from(bytes), ct))
}

/// D32: LSBE-1 — the in-frame self-describing container for transports
/// without a metadata channel (gRPC data slot, WS/h2 frames):
/// `"LSB1"` (4B magic) ‖ u64 LE head length ‖ JSON head (UTF-8) ‖ binary
/// tail. Bare JSON never containerizes (its first byte `{` is naturally
/// distinguishable — no heuristics). Returns the parsed head and the tail;
/// any malformation is a 400 (R18 row).
pub fn split_envelope(blob: &[u8]) -> Result<(serde_json::Value, Option<Bytes>), AppError> {
    let malformed = || {
        AppError::InvalidRequestBody(
            "malformed LSBE-1 envelope container (expected 'LSB1' magic + u64 LE head \
             length + JSON head + binary tail, D32)"
                .to_string(),
        )
    };
    if !blob.starts_with(b"LSB1") {
        return Err(malformed());
    }
    if blob.len() < 12 {
        return Err(malformed());
    }
    let head_len = u64::from_le_bytes(blob[4..12].try_into().unwrap()) as usize;
    let head_end = 12usize.checked_add(head_len).ok_or_else(malformed)?;
    if head_end > blob.len() {
        return Err(AppError::InvalidRequestBody(
            "LSBE-1 envelope head length overruns the frame (D32)".to_string(),
        ));
    }
    let head: serde_json::Value = serde_json::from_slice(&blob[12..head_end]).map_err(|_| {
        AppError::InvalidRequestBody(
            "LSBE-1 envelope head is not valid JSON (D32)".to_string(),
        )
    })?;
    let tail = if head_end == blob.len() {
        None
    } else {
        Some(Bytes::copy_from_slice(&blob[head_end..]))
    };
    Ok((head, tail))
}

/// D32: de-frame an opaque byte payload (gRPC data slot / batch element)
/// into the uniform internal form — transport de-framing only, zero
/// validation (D39). Bare JSON stays Json (first byte `{`); the LSBE-1
/// container splits into Envelope; anything else is the legacy Binary
/// passthrough (parse_root_inputs rejects it with 400 when the ensemble
/// declares inputs, R18).
pub fn ensemble_payload_from_bytes(
    data: &Bytes,
    content_type: Option<String>,
) -> Result<EnsembleValue, AppError> {
    // D32: the LSBE-1 magic can never be valid JSON (JSON starts with `{`,
    // `[`, `"`, a digit, `-`, `t`, `f` or `n`) — check it BEFORE any JSON
    // parsing so a container is never misread as a payload.
    if data.starts_with(b"LSB1") {
        let (head, tail) = split_envelope(data)?;
        return Ok(EnsembleValue::Envelope { head, tail: tail.unwrap_or_default() });
    }
    // §5.5.7 legacy byte-compat: ANY valid JSON — objects, arrays, scalars,
    // whitespace-prefixed — parses as Json (the historical gRPC unary
    // behaviour); malformed falls back to Binary passthrough.
    if let Ok(v) = serde_json::from_slice::<Value>(data) {
        return Ok(EnsembleValue::Json(v));
    }
    Ok(EnsembleValue::Binary(
        data.clone(),
        content_type.unwrap_or_else(|| "application/octet-stream".to_string()),
        None,
        None,
    ))
}

/// D33 (bidi): whether this ensemble declares named inputs — a declared
/// ensemble's envelope is self-describing, so the bidi upstream triggers on
/// the FIRST frame (no end signal); undeclared ensembles keep the legacy
/// multi-frame aggregation (D17).
pub async fn ensemble_declares_inputs(
    state: &Arc<AppState>,
    model_name: &str,
    version: &str,
    // E8-1: the declaration follows the SELECTED set (per-set inputs are
    // independent, R15).
    dag_selector: Option<&str>,
) -> Result<bool, AppError> {
    let plan = get_ensemble_plan(state, model_name, version).await?;
    let plan = select_dag_set(&plan, dag_selector)?;
    Ok(plan.inputs_decl.is_some())
}

/// D33/D32: de-frame a bidi FIRST frame for a DECLARED ensemble — the JSON
/// form (WS text frame / json content-type) carries the bare JSON envelope;
/// the binary form carries the LSBE-1 container. Transport-agnostic: the
/// three bidi endpoints pass their frame kind as `is_json` + bytes.
pub fn bidi_envelope_frame(
    frame: &Bytes,
    is_json: bool,
    ct: Option<String>,
) -> Result<EnsembleValue, AppError> {
    if is_json {
        let v: serde_json::Value = serde_json::from_slice(frame).map_err(|e| {
            AppError::InvalidRequestBody(format!("envelope frame is not valid JSON: {e}"))
        })?;
        return Ok(EnsembleValue::Json(v));
    }
    ensemble_payload_from_bytes(frame, ct)
}

// ===== P0: EnsemblePlan cache (D6 + review ①-④) =====

/// Parsed, validated, layer-sorted ensemble DAG — the object cached by
/// [`EnsemblePlanCache`] so a request never re-reads/parses/validates the
/// config.yaml. Batch 0 starts with the historical parse result; streaming
/// fields (stream/output semantics) land on this struct in batch 0 step 2
/// (D6: parse structured once, stream/MIMO validation stacks on top).
#[derive(Debug, Clone)]
pub struct EnsemblePlan {
    pub steps: Vec<EnsembleStep>,
    /// Topological layers as indices into `steps` (self-owning, no refs).
    pub layers: Vec<Vec<usize>>,
    /// DAG output step (E2 semantics unified at batch 0, §4.1 rule 4): with no
    /// explicit `output` this is `steps.last()`; validation ensures a
    /// streaming DAG's output step is its streaming step.
    pub output_step: usize,
    /// E2 (batch 3): explicit output field path (`output: "$s2.score"`).
    /// None = the output step's whole value.
    pub output_field: Option<String>,
    /// Pipeline chains (§4.2, batch 2): each chain is a linear consumer path
    /// of STREAMING steps (all nodes stream), tail = output step. Empty for
    /// non-pipeline DAGs (tail-streaming only).
    pub chains: Vec<Chain>,
    /// MIMO (D8/D9, batch 4①): the request-level named inputs declaration —
    /// None = legacy single anonymous input (`$request`, byte-identical).
    pub inputs_decl: Option<IndexMap<String, InputDecl>>,
    /// MIMO (R11/R12, batch 4①): per-step static input mode, index-aligned
    /// with `steps`. None = legacy dynamic dispatch (no declared type
    /// environment); Some(mode) = parse-decided assembly (no runtime type
    /// branches).
    pub input_modes: Vec<Option<InputMode>>,
    /// MIMO (R4, batch 4①): per-step optional-input names referenced —
    /// non-empty marks a CONDITIONAL step (absent input → step skipped,
    /// D13/E6-skip channel). Index-aligned with `steps`.
    pub conditional_refs: Vec<Vec<String>>,
    /// P1 (batch 6): per-step referenced CONTEXT keys, parse-computed from
    /// the step's input refs (derivation mirrors [`resolve_ref`]). Spawn-time
    /// clones select these keys instead of the whole table — the deep-copy
    /// of every sibling output per spawn is eliminated. Index-aligned with
    /// `steps`.
    pub step_dep_keys: Vec<Vec<String>>,
    /// E7 (batch 4④): multi-sink aliases `{alias: $ref}` — the response is
    /// a KServe envelope (build_response). None = the historical single
    /// output.
    pub outputs: Option<IndexMap<String, String>>,
    /// E8-1 (batch 5): named DAG sets (the dags form) — the outer plan is a
    /// pure container; execution resolves the set via [`select_dag_set`].
    /// None = the historical single-set form.
    pub dag_sets: Option<IndexMap<String, Arc<EnsemblePlan>>>,
    /// Source config file — mtime re-check (review ②) stats this path.
    pub config_path: PathBuf,
    /// mtime of `config_path` captured BEFORE the file read (stat-before-read):
    /// a write landing mid-load leaves this mtime OLDER than the file's, so
    /// the interval re-check re-parses (safe) — the reverse order could pin a
    /// fresh mtime onto stale content and serve it indefinitely. Set by the
    /// production loader; None for the load-time direct-parse path
    /// (`insert_ready` stats on its own there).
    pub source_mtime: Option<std::time::SystemTime>,
}

impl EnsemblePlan {
    /// MIMO (R11/R12): the parse-decided input mode of step `idx`
    /// (None = legacy dynamic dispatch).
    pub fn input_mode(&self, idx: usize) -> Option<InputMode> {
        self.input_modes.get(idx).copied().flatten()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanKey {
    pub model: String,
    pub version: String,
}

enum PlanCell {
    /// Single-flight in progress: the holder parses; waiters park here. The
    /// generation id disambiguates ownership after an invalidation removed
    /// the holder's cell mid-load (D23 race) — without it the stale holder
    /// could clobber a NEW holder's cell or a load-time `insert_ready`.
    Loading {
        id: u64,
        waiters: Vec<oneshot::Sender<Result<Arc<EnsemblePlan>, AppError>>>,
    },
    Ready {
        plan: Arc<EnsemblePlan>,
        mtime: Option<std::time::SystemTime>,
        last_stat: tokio::time::Instant,
    },
}

/// P0: `DashMap<(model, version), Arc<EnsemblePlan>>` with single-flight
/// loads (review ④), mtime interval re-check (review ②, coarse tokio clock,
/// no syscall on the hot path) and model-prefix invalidation (review ③ —
/// `(m,"latest")` and `(m,"1")` are distinct keys, both must clear).
/// Invalidation hooks: lifecycle `unload_version` (via reload_model too) —
/// single collection point, fired BEFORE registry changes (D23).
pub struct EnsemblePlanCache {
    plans: DashMap<PlanKey, PlanCell>,
    stat_interval: Duration,
    /// Single-flight cell generation counter (ownership after invalidation).
    epoch: std::sync::atomic::AtomicU64,
}

impl Default for EnsemblePlanCache {
    fn default() -> Self {
        Self::new()
    }
}

impl EnsemblePlanCache {
    pub fn new() -> Self {
        Self {
            plans: DashMap::new(),
            stat_interval: Duration::from_secs(1),
            epoch: std::sync::atomic::AtomicU64::new(0),
        }
    }
    /// Get-or-load with single-flight: concurrent first requests share one
    /// parse; a failed load is NOT cached (next call re-parses — a fixed
    /// config heals without reload). Ready entries within `stat_interval`
    /// are returned without touching the filesystem.
    pub async fn get_or_load<F, Fut>(
        &self,
        key: PlanKey,
        load: F,
    ) -> Result<Arc<EnsemblePlan>, AppError>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<Arc<EnsemblePlan>, AppError>> + Send,
    {
        use dashmap::mapref::entry::Entry as MapEntry;

        // Fast path: a fresh Ready entry returns without any filesystem work.
        if let Some(cell) = self.plans.get(&key) {
            if let PlanCell::Ready { plan, last_stat, .. } = &*cell {
                if last_stat.elapsed() < self.stat_interval {
                    return Ok(plan.clone());
                }
            }
        }

        loop {
            match self.plans.entry(key.clone()) {
                MapEntry::Occupied(mut occ) => match occ.get_mut() {
                    PlanCell::Loading { waiters, .. } => {
                        // Not the holder: park on the holder's result.
                        let (tx, rx) = oneshot::channel();
                        waiters.push(tx);
                        drop(occ);
                        return rx.await.map_err(|_| {
                            AppError::Internal("ensemble plan load task dropped".to_string())
                        })?;
                    }
                    PlanCell::Ready { plan, mtime, last_stat } => {
                        if last_stat.elapsed() < self.stat_interval {
                            return Ok(plan.clone());
                        }
                        // Interval elapsed (review ②): re-stat the source file.
                        // Coarse check amortised to once per interval, never on
                        // the hot path; a change evicts and re-loads in-flight.
                        let current_mtime = std::fs::metadata(&plan.config_path)
                            .and_then(|m| m.modified())
                            .ok();
                        if current_mtime == *mtime {
                            *last_stat = tokio::time::Instant::now();
                            return Ok(plan.clone());
                        }
                        occ.remove();
                        continue;
                    }
                },
                MapEntry::Vacant(vacant) => {
                    // We are the single-flight holder (review ④). Insert the
                    // Loading placeholder, drop the shard lock, then parse.
                    let my_id = self.epoch.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    vacant.insert(PlanCell::Loading { id: my_id, waiters: Vec::new() });
                    match load().await {
                        Ok(plan) => {
                            // B2: the mtime comes from the loader's
                            // stat-before-read (a mid-load write can only make
                            // it older → safe re-parse), never a post-load
                            // stat (which could pin a fresh mtime onto stale
                            // content and serve it indefinitely).
                            let mtime = plan.source_mtime;
                            // The cell may be gone (invalidate raced the load,
                            // D23) or replaced (load-time insert_ready / a new
                            // holder): only write back while WE still own it.
                            // No panic paths — unload/reload racing a cold
                            // first load is normal operation.
                            let waiters = match self.plans.get_mut(&key) {
                                Some(mut cell) => match cell.value_mut() {
                                    PlanCell::Loading { id, waiters } if *id == my_id => {
                                        std::mem::take(waiters)
                                    }
                                    _ => Vec::new(),
                                },
                                None => Vec::new(),
                            };
                            for w in waiters {
                                let _ = w.send(Ok(plan.clone()));
                            }
                            if let Some(mut cell) = self.plans.get_mut(&key) {
                                if matches!(&*cell, PlanCell::Loading { id, .. } if *id == my_id) {
                                    *cell.value_mut() = PlanCell::Ready {
                                        plan: plan.clone(),
                                        mtime,
                                        last_stat: tokio::time::Instant::now(),
                                    };
                                }
                            }
                            return Ok(plan);
                        }
                        Err(e) => {
                            // Failed loads must NOT be cached: evict the
                            // placeholder so the next call re-parses (a fixed
                            // config heals without reload — behaviour parity
                            // with the uncached path). Waiters share the failure.
                            // Same ownership rule as the Ok path: only evict
                            // our own cell (an invalidate may have beaten us).
                            let waiters = match self.plans.get_mut(&key) {
                                Some(mut cell) => match cell.value_mut() {
                                    PlanCell::Loading { id, waiters } if *id == my_id => {
                                        Some(std::mem::take(waiters))
                                    }
                                    _ => None,
                                },
                                None => None,
                            };
                            if let Some(waiters) = waiters {
                                self.plans.remove(&key);
                                for w in waiters {
                                    let _ = w.send(Err(AppError::Internal(format!(
                                        "ensemble plan load failed: {e}"
                                    ))));
                                }
                            }
                            return Err(e);
                        }
                    }
                }
            }
        }
    }

    /// Direct insert from the lifecycle load path (P0/P6): the plan was
    /// parsed at load time, so the first request hits the cache without
    /// paying the parse. mtime is stat'ed here (once, at load).
    pub fn insert_ready(&self, key: PlanKey, plan: Arc<EnsemblePlan>) {
        let mtime = std::fs::metadata(&plan.config_path)
            .and_then(|m| m.modified())
            .ok();
        self.plans.insert(
            key,
            PlanCell::Ready {
                plan,
                mtime,
                last_stat: tokio::time::Instant::now(),
            },
        );
    }

    /// Invalidate every version of a model (review ③: latest + pinned
    /// versions are separate keys). Called on unload BEFORE registry
    /// changes (D23).
    pub fn invalidate_model(&self, model: &str) {
        self.plans.retain(|key, _| key.model != model);
    }

    pub fn invalidate_version(&self, model: &str, version: &str) {
        self.plans.remove(&PlanKey {
            model: model.to_string(),
            version: version.to_string(),
        });
    }
}

// ===== §4.3/D17: bidi upstream aggregation =====

/// Bidi upstream aggregation for ensemble DAGs (D17): all-Binary frames →
/// byte concat (content-type from the first frame); all-Json frames → JSON
/// array (a single frame = the value itself, historical single-frame
/// semantics); mixed kinds → 400 (no clean boundary/Content-Type semantics).
/// The cumulative byte cap is `max_request_body_bytes` (D3) — exceeded →
/// PayloadTooLarge (413/ResourceExhausted).
pub struct BidiAggregator {
    max_bytes: usize,
    total_bytes: usize,
    state: BidiAggState,
}

enum BidiAggState {
    Empty,
    Binary {
        parts: Vec<bytes::Bytes>,
        content_type: String,
    },
    Json {
        items: Vec<serde_json::Value>,
    },
}

impl BidiAggregator {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            total_bytes: 0,
            state: BidiAggState::Empty,
        }
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Push one data frame. `is_json` is the frame's declared kind (WS frame
    /// type / content-type header). The first frame fixes the kind; any
    /// later frame of the other kind → 400. Cap enforcement (D3) happens
    /// here so the aggregation can never exceed `max_request_body_bytes`.
    pub fn push(
        &mut self,
        data: bytes::Bytes,
        is_json: bool,
        content_type: Option<&str>,
    ) -> Result<(), AppError> {
        self.total_bytes += data.len();
        if self.total_bytes > self.max_bytes {
            return Err(AppError::PayloadTooLarge {
                max_size: self.max_bytes,
                actual_size: Some(self.total_bytes as u64),
            });
        }
        match &mut self.state {
            BidiAggState::Empty => {
                if is_json {
                    let v: Value = serde_json::from_slice(&data).map_err(|e| {
                        AppError::InvalidRequestBody(format!(
                            "bidi JSON frame is not valid JSON: {e}"
                        ))
                    })?;
                    self.state = BidiAggState::Json { items: vec![v] };
                } else {
                    self.state = BidiAggState::Binary {
                        parts: vec![data],
                        content_type: content_type
                            .unwrap_or("application/octet-stream")
                            .to_string(),
                    };
                }
            }
            BidiAggState::Json { items } => {
                if !is_json {
                    return Err(AppError::InvalidRequestBody(
                        "bidi stream mixes JSON and binary frames; \
                         aggregation requires a single kind (D17)"
                            .to_string(),
                    ));
                }
                let v: Value = serde_json::from_slice(&data).map_err(|e| {
                    AppError::InvalidRequestBody(format!("bidi JSON frame is not valid JSON: {e}"))
                })?;
                items.push(v);
            }
            BidiAggState::Binary { parts, .. } => {
                if is_json {
                    return Err(AppError::InvalidRequestBody(
                        "bidi stream mixes JSON and binary frames; \
                         aggregation requires a single kind (D17)"
                            .to_string(),
                    ));
                }
                parts.push(data);
            }
        }
        Ok(())
    }

    /// Trigger time (D33): produce the aggregated root input. Single Json
    /// frame → the value itself (not wrapped); multiple → `[f0, f1, ...]`;
    /// Binary → concatenated bytes with the first frame's content-type.
    pub fn finish(self) -> Result<EnsembleValue, AppError> {
        match self.state {
            BidiAggState::Empty => Ok(EnsembleValue::Json(json!({}))),
            BidiAggState::Json { items } => {
                if items.len() == 1 {
                    Ok(EnsembleValue::Json(items.into_iter().next().unwrap()))
                } else {
                    Ok(EnsembleValue::Json(Value::Array(items)))
                }
            }
            BidiAggState::Binary { parts, content_type } => {
                let total: usize = parts.iter().map(|p| p.len()).sum();
                let mut buf = Vec::with_capacity(total);
                for p in parts {
                    buf.extend_from_slice(&p);
                }
                Ok(EnsembleValue::Binary(bytes::Bytes::from(buf), content_type, None, None))
            }
        }
    }
}

// ===== Execution =====

/// Execution-face parameters (D37, fixed once at batch 0 — later batches add
/// fields without touching the signature, e.g. `dag_selector` in batch 5;
/// D36's snapshot/depth ride the internal `execute_ensemble_inner`, not the
/// public opts).
/// D15 (E4, batch 3): request-scoped version snapshot — `model → resolved
/// version`, lazily memoized. Every step of one request (across nesting,
/// D36) that references the same model with an unresolved version shares
/// the FIRST resolution, so an active-version drift mid-request can never
/// produce a DAG where step A hit v1 and step B hit v2. Explicit step
/// versions bypass the snapshot entirely.
///
/// E1 (batch 3): the NESTING CHAIN itself is NOT shared — each recursion
/// level extends its own immutable chain ([`extend_ancestor_chain`]) and
/// passes it down, so a concurrent sibling's in-flight child run can never
/// be misread as an ancestor of this branch (B1: the flat shared Vec
/// conflated sibling fan-out with recursion). Only the version table above
/// is request-shared (D36).
pub struct VersionSnapshot {
    resolved: std::sync::Mutex<HashMap<String, String>>,
}

impl Default for VersionSnapshot {
    fn default() -> Self {
        Self {
            resolved: std::sync::Mutex::new(HashMap::new()),
        }
    }
}

impl VersionSnapshot {
    /// E4/D15: resolve a step's version — explicit versions as-is; the first
    /// unresolved resolution per model wins. Resolution source = registry
    /// active ONLY (D27: no canary pin / weighted routing at the step level;
    /// those precedences belong to the outer request's own version).
    pub fn resolve(
        &self,
        registry: &crate::registry::ModelRegistry,
        step: &EnsembleStep,
    ) -> Result<String, AppError> {
        if let Some(v) = step.version.as_deref() {
            return Ok(v.to_string());
        }
        let mut snapshot = self.resolved.lock().unwrap();
        if let Some(v) = snapshot.get(&step.model) {
            return Ok(v.clone());
        }
        let resolved = registry.get_active_version(&step.model).ok_or_else(|| {
            AppError::ModelNotFound(format!(
                "sub-model '{}' has no active version (step version omitted or \"latest\")",
                step.model
            ))
        })?;
        snapshot.insert(step.model.clone(), resolved.clone());
        Ok(resolved)
    }
}

/// E1: is (model, version) on THIS branch's nesting chain? Guards direct
/// self-loops — and, beyond the plan's m6 depth-only tradeoff, cross-model
/// mutual recursion (A→B→A) for free. The chain is immutable per branch:
/// a concurrent sibling's entries live on a DIFFERENT chain, so legal
/// same-layer fan-out to one child ensemble is never misread as recursion
/// (B1).
fn contains_ancestor(chain: &[(String, String)], model: &str, version: &str) -> bool {
    chain.iter().any(|(m, v)| m == model && v == version)
}

/// E1: extend this branch's nesting chain with the current ensemble for the
/// nested run. The parent chain is left untouched — sibling branches built
/// from the same parent stay independent (B1: per-branch chains replace the
/// flat request-global Vec; the version table alone remains shared, D36).
fn extend_ancestor_chain(
    chain: &[(String, String)],
    model: &str,
    version: &str,
) -> Vec<(String, String)> {
    let mut child = chain.to_vec();
    child.push((model.to_string(), version.to_string()));
    child
}

/// E1: runtime nesting depth limit (default 8, counted along the call tree
/// — cross-model nesting is invisible at parse time, G5). Depth 0 = the
/// top-level request.
const MAX_ENSEMBLE_NESTING_DEPTH: u32 = 8;

fn ensure_nesting_depth(depth: u32) -> Result<(), AppError> {
    if depth >= MAX_ENSEMBLE_NESTING_DEPTH {
        return Err(AppError::InvalidRequestBody(format!(
            "ensemble nesting depth limit ({MAX_ENSEMBLE_NESTING_DEPTH}) exceeded"
        )));
    }
    Ok(())
}

/// E5 (batch 3): a step's effective deadline — `min(parent deadline,
/// now + timeout_secs)`. None = no step timeout (parent passes through; a
/// parent of None stays None = unbounded). Both the unary submit/wait and
/// the streaming open (D35: recv_chunk overall takes min(client overall,
/// this cap)) use the result. The same formula applies per streaming step on
/// a pipeline chain (each hop computes its own from its open time).
fn step_effective_deadline(
    parent_deadline_unix_ns: Option<i64>,
    timeout_secs: Option<f64>,
) -> Option<i64> {
    let Some(timeout_secs) = timeout_secs else {
        return parent_deadline_unix_ns;
    };
    let now_unix_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    // Saturating: a parse-legal but absurd cap (e.g. 1e18 s) saturates the
    // f64→i64 cast, and a plain `now +` addition would then overflow — debug
    // builds panic, release wraps negative (every request on the step then
    // fails instantly as "expired"). Saturating keeps the config's intent
    // ("effectively no timeout") without either failure mode.
    let step_deadline = now_unix_ns.saturating_add((timeout_secs * 1e9) as i64);
    match parent_deadline_unix_ns {
        Some(parent) => Some(parent.min(step_deadline)),
        None => Some(step_deadline),
    }
}

/// E1: type-erased entry into the nested execution. `execute_step` awaits
/// this instead of `execute_ensemble_inner` directly — the `dyn Future`
/// erasure breaks the opaque-type cycle the direct call creates
/// (execute_step → execute_ensemble_inner → open_tail_stream → spawn
/// Send-check → execute_step).
#[allow(clippy::too_many_arguments)] // nested-execution plumbing rides together by design
fn execute_nested_ensemble_boxed(
    state: Arc<AppState>,
    model: String,
    version: String,
    payload: EnsembleValue,
    request_id: String,
    opts: EnsembleExecOpts,
    snapshot: Arc<VersionSnapshot>,
    depth: u32,
    ancestors: Vec<(String, String)>, // E1: this branch's chain (owned — moves into the boxed future)
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<EnsembleOutcome, AppError>> + Send>> {
    Box::pin(async move {
        execute_ensemble_inner(state, &model, &version, payload, &request_id, opts, &snapshot, depth, &ancestors)
            .await
    })
}

#[derive(Debug, Clone)]
pub struct EnsembleExecOpts {
    pub client_ip: String,
    pub deadline_unix_ns: Option<i64>,
    /// StreamOpen.decoupled passthrough (batch 0).
    pub decoupled: bool,
    /// E8-1 (batch 5, D38): the request's DAG-set name — extracted by each
    /// endpoint from its transport metadata channel (`x-lite-dag`) and
    /// D22-validated; the orchestration layer sees a pure string, never
    /// transport headers.
    pub dag_selector: Option<String>,
}

/// D25: batch-0 result — Unary keeps the historical byte-identical path;
/// Stream carries the tail stream (and, from batch 2, the whole chain).
pub enum EnsembleOutcome {
    Unary(EnsembleValue),
    Stream(EnsembleStream),
}

/// Chain-handle element (D25): batch 0 always holds exactly the tail stream;
/// batch 2 pushes the chain as "tail first, then upstream". Adapter layers
/// read only the top-level quick-access fields — the seam stays fixed.
#[derive(Clone)]
pub struct StreamHandle {
    pub stream_id: String,
    pub cancel_client: Arc<crate::transport::zmq::WorkerZmqClient>,
    pub abort: tokio::task::AbortHandle,
}

/// The streaming result handed to the adapter layers. `chain` is the single
/// source of truth (tail included) once filled; top-level fields are the
/// tail quick-access the adapters already consume (same type as
/// `open_worker_stream`'s return — the SSE/WS/gRPC forward loops change
/// zero lines).
pub struct EnsembleStream {
    /// Tail stream — same type as `send_stream`'s receiver; the existing
    /// adapter forward loops consume it as-is.
    pub chunk_rx: mpsc::Receiver<pb::StreamResponse>,
    pub stream_id: String,
    pub cancel_client: Arc<crate::transport::zmq::WorkerZmqClient>,
    pub tail_model: String,
    pub tail_version: String,
    /// Step-level metric label (§4.1): the adapters record
    /// `record_ensemble_step_latency(ensemble, tail_step, tail_model,
    /// tail_version, …)` at stream close.
    pub tail_step: String,
    /// D35 (batch 3): E5 `timeout_secs` converted to a step wall-clock cap —
    /// the adapter's recv_chunk overall takes min(client overall, this).
    /// None = inactive.
    pub step_deadline: Option<std::time::Instant>,
    /// D18/D25: chain handles — the single source of truth for the chain's
    /// streams (tail included). Batch 0 = the tail stream alone; batch 2 =
    /// every streaming step on the pipeline chain, pushed by the chain tasks
    /// as they open (tail first — chain[0] always mirrors the top-level
    /// fields, so adapters that only read the quick-access fields stay
    /// unchanged). Cancellation broadcasts over this list.
    pub chain: Arc<std::sync::Mutex<Vec<StreamHandle>>>,
    /// D18: chain task tree unified teardown. Batch 0 = the tail stream
    /// itself (inert placeholder); batch 2 roots the whole chain here.
    pub abort: tokio::task::AbortHandle,
    /// P10 (D40): owned semaphore permit — released when the stream is
    /// dropped (terminal frame / adapter task end), the same single path as
    /// D18's teardown. None = capacity not configured (0/unlimited).
    pub permit: Option<StreamingPermit>,
}

/// P10 (D40): global streaming-DAG capacity — the semaphore that bounds
/// streaming residency (§6.3.1: streaming bypasses the queue, so a fan-out
/// has no other memory bound). Global scope (the derivation M = N × D_stream
/// is a global memory model; per-model caps make the bound unprovable).
/// Configuration: `server.max_concurrent_streaming_dags` (default 128;
/// 0 = unlimited, capacity not installed).
pub struct StreamingCapacityState {
    semaphore: Arc<tokio::sync::Semaphore>,
    in_use: Arc<std::sync::atomic::AtomicUsize>,
}

/// RAII permit handed to [`EnsembleStream`]. The Arc inner decouples the
/// release from the stream struct's own Drop (the adapter moves fields out
/// of EnsembleStream, which forbids a Drop impl on it): when the last
/// reference goes away, the semaphore permit is returned and the gauge is
/// synced (D40: owned permit, released via the D18 teardown path).
#[derive(Clone)]
pub struct StreamingPermit {
    // Held (not read) for RAII lifetime semantics; the inner's Drop returns
    // the semaphore slot and syncs the gauge.
    _inner: Arc<StreamingPermitInner>,
}

struct StreamingPermitInner {
    // Held (not read): owning the permit is the point — dropping it releases
    // the semaphore slot.
    _permit: tokio::sync::OwnedSemaphorePermit,
    in_use: Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for StreamingPermitInner {
    fn drop(&mut self) {
        let prev = self.in_use.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        crate::metrics::prometheus::set_ensemble_streaming_active(prev.saturating_sub(1));
    }
}

impl StreamingCapacityState {
    pub fn new(limit: usize) -> Self {
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(limit)),
            in_use: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// Acquire one streaming-DAG slot. Immediate rejection on exhaustion —
    /// no queueing (queueing turns an unbounded-stream memory problem into a
    /// latency one, and the pre-layers already waited in the queue once).
    pub fn try_acquire(&self) -> Result<StreamingPermit, AppError> {
        self.try_acquire_many(1)
    }

    /// Acquire `n` slots — pipeline chains weight by their streaming-step
    /// count (D40: 末步 = 1, k 链 = k; the residency derivation M = N × D_stream
    /// scales linearly with the chain length).
    pub fn try_acquire_many(&self, n: usize) -> Result<StreamingPermit, AppError> {
        match self.semaphore.clone().try_acquire_many_owned(n as u32) {
            Ok(permit) => {
                let prev = self.in_use.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                crate::metrics::prometheus::set_ensemble_streaming_active(prev + 1);
                Ok(StreamingPermit {
                    _inner: Arc::new(StreamingPermitInner {
                        _permit: permit,
                        in_use: self.in_use.clone(),
                    }),
                })
            }
            Err(_) => Err(AppError::StreamingCapacityExceeded(
                "concurrent streaming ensemble DAG limit reached (429); \
                 reduce concurrency or raise server.max_concurrent_streaming_dags"
                    .to_string(),
            )),
        }
    }
}

pub async fn execute_ensemble(
    state: Arc<AppState>,
    model_name: &str,
    version: &str,
    payload: EnsembleValue,
    request_id: &str,
    opts: EnsembleExecOpts,
) -> Result<EnsembleOutcome, AppError> {
    // D15 (E4): request-scoped version snapshot — shared across every step
    // of this request and (D36) across nested ensemble calls.
    let snapshot = Arc::new(VersionSnapshot::default());
    execute_ensemble_inner(state, model_name, version, payload, request_id, opts, &snapshot, 0, &[])
        .await
}

/// D36 (E1/E4): the internal execution entry — the D15 version snapshot and
/// the nesting depth ride here, so the public signature stays fixed. Nested
/// ensembles recurse into this function with depth+1, the SAME snapshot (a
/// parent and child resolving the same model share one version) and a
/// branch-local ancestors chain (E1: recursion detection checks only THIS
/// branch's path — concurrent siblings never share it, B1).
#[allow(clippy::too_many_arguments)] // execution plumbing: state+ids+snapshot ride together by design
pub(crate) async fn execute_ensemble_inner(
    state: Arc<AppState>,
    model_name: &str,
    version: &str,
    payload: EnsembleValue,
    request_id: &str,
    opts: EnsembleExecOpts,
    snapshot: &Arc<VersionSnapshot>,
    depth: u32,
    ancestors: &[(String, String)], // E1: enclosing ensembles (self NOT included)
) -> Result<EnsembleOutcome, AppError> {
    // E1: depth limit before anything else (cheapest failure first).
    ensure_nesting_depth(depth)?;
    // E1: this branch's nesting chain — the current ensemble plus every
    // enclosing one. Each recursion level extends its OWN copy, so a
    // concurrent sibling branch never sees this entry (B1: the flat shared
    // Vec misread sibling fan-out as recursion).
    let ancestors = extend_ancestor_chain(ancestors, model_name, version);
    // P0 (D6): plan comes from the cache — parse/validate/layers run once per
    // config version, not per request. In-flight requests hold their Arc and
    // finish on the old plan even across a reload (D23).
    let plan = get_ensemble_plan(&state, model_name, version).await?;
    // §5.5.8 (R15): only the dags form tolerates superset envelopes.
    let is_dags_form = plan.dag_sets.is_some();
    // E8-2 (B6 fix): `$request.dag` must see the RESOLVED set name — with no
    // header the dags form selects "default", so when conditions on the
    // default set must compare against "default", not absence. Normalize
    // before selection so when-eval and select_dag_set agree.
    let mut opts = opts;
    if is_dags_form {
        opts.dag_selector = Some(
            opts.dag_selector
                .take()
                .unwrap_or_else(|| "default".to_string()),
        );
    }
    // E8-1 (D38/D22): resolve the request's DAG set (dags form) — single-
    // form plans pass through; unknown names 400 here.
    let plan = select_dag_set(&plan, opts.dag_selector.as_deref())?;
    let deadline_unix_ns = opts.deadline_unix_ns;
    let tail_idx = plan.output_step;

    // MIMO (D31/R18): the single root-parsing point — the KServe envelope
    // (declared mode) or the legacy payload (byte-identical passthrough).
    let mut context: HashMap<String, EnsembleValue> = HashMap::new();
    let absent_inputs: HashSet<String> = match parse_root_inputs(payload, plan.inputs_decl.as_ref(), is_dags_form)? {
        RootInputs::Single(v) => {
            context.insert("request".to_string(), v);
            HashSet::new()
        }
        RootInputs::Named { values, absent } => {
            for (name, v) in values {
                context.insert(format!("inputs.{name}"), v);
            }
            absent.into_iter().collect()
        }
    };

    if !plan.steps[tail_idx].stream {
        // Historical unary path — byte-identical behaviour (E7: multi-sink
        // responses build from the same context via build_response).
        run_layers(
            &state, plan, &plan.layers, &mut context, &absent_inputs,
            model_name, version, request_id, &opts, deadline_unix_ns, snapshot, depth, &ancestors,
        ).await?;
        return build_response(plan, model_name, &context);
    }

    // §4.2 pipeline branch (batch 2): the DAG contains a chain — run the
    // pre-layers before the chain head, then spawn the whole chain and
    // return the tail stream immediately (D25's split, now the chain
    // branch; the tail-stream branch below remains for non-pipeline DAGs).
    if !plan.chains.is_empty() {
        let chain = &plan.chains[0];
        // §4.2: non-chain parts advance by layer — every non-chain step up
        // to the chain tail still runs (chain nodes excluded). The historical
        // executor runs whole layers, so pass layers filtered to non-chain
        // indices; they never depend on chain streaming outputs (P-R1/D26),
        // so running them before the chain is always valid.
        let mut pre_layers: Vec<Vec<usize>> = Vec::new();
        for layer in &plan.layers {
            let non_chain: Vec<usize> = layer
                .iter()
                .copied()
                .filter(|&i| !chain.nodes.contains(&i))
                .collect();
            if !non_chain.is_empty() {
                pre_layers.push(non_chain);
            }
        }
        run_layers(
            &state, plan, &pre_layers, &mut context, &absent_inputs,
            model_name, version, request_id, &opts, deadline_unix_ns, snapshot, depth, &ancestors,
        )
        .await?;
        let mut stream = spawn_chain(
            &state, plan, chain, &context, request_id, &opts, deadline_unix_ns, snapshot,
        )
        .await?;
        // P10 (D40): the permit is WEIGHTED by the chain's streaming-step
        // count (末步 = 1, k 链 = k — residency scales linearly with the
        // chain length).
        if let Some(capacity) = state.worker_manager.streaming_capacity() {
            stream.permit = Some(capacity.try_acquire_many(chain.nodes.len())?);
        }
        return Ok(EnsembleOutcome::Stream(stream));
    }

    // Streaming DAG (D25: run_pre_layers / open_tail_stream split — batch 2
    // extends the split into a chain without rewriting the main flow).
    let tail_layer = plan.layers.iter().position(|l| l.contains(&tail_idx))
        .ok_or_else(|| AppError::Internal("output step missing from layers".to_string()))?;
    // P7: the stream-open preflight (route pick + meta build — nothing that
    // depends on the pre-layer results) runs IN PARALLEL with the pre-layers,
    // so the TTFT critical path after the layers is just assemble + send.
    // The autoload path stays serial (preflight only when already ready).
    let preflight_fut = async {
        tail_stream_preflight(
            &state, &plan.steps[tail_idx], request_id, &opts, deadline_unix_ns, snapshot,
        )
        .await
    };
    let layers_fut = async {
        run_layers(
            &state, plan, &plan.layers[..tail_layer], &mut context, &absent_inputs,
            model_name, version, request_id, &opts, deadline_unix_ns, snapshot, depth, &ancestors,
        )
        .await
    };
    let (preflight, layers_res) = tokio::join!(preflight_fut, layers_fut);
    layers_res?;

    // P10 (D40): acquire a streaming-DAG slot BEFORE opening the stream —
    // immediate 429 rejection on exhaustion (no queueing; the pre-layers
    // already waited in the queue once). The owned permit rides the stream
    // and releases when the adapter's forward task ends (D18 teardown path).
    let mut stream = open_tail_stream(
        &state, plan, tail_idx, tail_layer, &context, &absent_inputs,
        request_id, &opts, deadline_unix_ns, preflight?, snapshot, depth, &ancestors,
    ).await?;
    if let Some(capacity) = state.worker_manager.streaming_capacity() {
        stream.permit = Some(capacity.try_acquire()?);
    }
    Ok(EnsembleOutcome::Stream(stream))
}

/// P3/P11 (batch 6): a layer step future's completion — (step name, result).
type StepFutOutput = (String, Result<StepResults, AppError>);

/// P3/P11 (batch 6): build one step's execution future — the E8-2 when and
/// MIMO R4 conditional-absence checks run up front (a skipped step yields
/// None with a warn); the E4/D15 metric-label resolution runs here too, so
/// a failed resolution aborts the layer (the same error execute_step would
/// surface for that step). The future carries the step's latency recording.
#[allow(clippy::too_many_arguments)] // layer plumbing: state+plan+ctx+ids ride together by design
fn build_step_fut(
    state: &Arc<AppState>,
    plan_run: &Arc<EnsemblePlan>,
    step_idx: usize,
    context: &HashMap<String, EnsembleValue>,
    absent_inputs: &HashSet<String>,
    opts: &EnsembleExecOpts,
    model_name: &str,
    request_id: &str,
    snapshot: &Arc<VersionSnapshot>,
    depth: u32,
    ancestors: &[(String, String)],
    deadline_unix_ns: Option<i64>,
) -> Result<Option<impl Future<Output = StepFutOutput> + 'static>, AppError> {
    // E8-2 (batch 5): a when-false step is skipped (E6 channel).
    if !when_passes(step_idx, plan_run, opts, context)? {
        warn!(
            step = %plan_run.steps[step_idx].name,
            "ensemble step skipped (when condition false)"
        );
        return Ok(None);
    }
    // MIMO R4 (D13): a conditional step whose optional input is absent
    // this request is skipped — the E6-skip channel.
    if plan_run.conditional_refs[step_idx]
        .iter()
        .any(|n| absent_inputs.contains(n))
    {
        warn!(
            step = %plan_run.steps[step_idx].name,
            "ensemble step skipped (optional input absent)"
        );
        return Ok(None);
    }
    let state = state.clone();
    // P1 (batch 6): the step sees only the keys it references.
    let ctx = select_ctx_keys(context, &plan_run.step_dep_keys[step_idx]);
    let step = plan_run.steps[step_idx].clone();
    let plan_spawn = plan_run.clone();
    let ensemble_name = model_name.to_string();
    let request_id = request_id.to_string();
    let client_ip = opts.client_ip.clone();
    // E4/D15: resolve the metric label BEFORE the future runs (a failed
    // resolution aborts the layer here — the same error execute_step
    // would surface for that step). The resolved label is the truthful
    // version the step will call.
    let resolved_label = snapshot.resolve(&state.registry, &step)?;
    // m4: the metric label records the actual version only for EXPLICIT
    // versions — unresolved ("latest"/omitted) normalizes to "latest" so
    // active drift cannot grow the label set (model × step × version).
    let version_label = if step.version.is_some() {
        resolved_label
    } else {
        "latest".to_string()
    };
    let snapshot = snapshot.clone();
    let ancestors = ancestors.to_vec();
    Ok(Some(async move {
        let start = Instant::now();
        let result = execute_step(
            state, &plan_spawn, step_idx, &step, &ctx, &request_id, &client_ip,
            deadline_unix_ns, &snapshot, depth, &ancestors,
        )
        .await;
        let latency = start.elapsed().as_secs_f64();
        crate::metrics::prometheus::record_ensemble_step_latency(
            &ensemble_name, &step.name, &step.model, &version_label, depth, latency,
        );
        (step.name, result)
    }))
}

/// P3/P11 (batch 6): a completed step's result — named outputs land in the
/// context; a skip step's failure leaves it ABSENT and the layer continues
/// (parse rules guarantee nothing references it); any other error
/// propagates (B3 — client errors keep their status codes, no Internal
/// wrapping).
fn handle_step_result(
    name: &str,
    result: Result<StepResults, AppError>,
    context: &mut HashMap<String, EnsembleValue>,
    skip_set: &HashSet<&str>,
) -> Result<(), AppError> {
    match result {
        Ok(values) => {
            // MIMO: materialized named outputs land under their
            // `step.alias` keys (undeclared steps: the single `step` key).
            for (key, value) in values {
                context.insert(key, value);
            }
            Ok(())
        }
        Err(e) => {
            // E6 (batch 4): a skip step's failure leaves it ABSENT from the
            // context and the layer continues (parse rules guarantee
            // nothing references it).
            if skip_set.contains(name) {
                warn!(step = %name, error = %e, "ensemble step skipped (on_error: skip)");
                Ok(())
            } else {
                // B3: propagate step errors directly — client errors (e.g.
                // InvalidRequestBody from resolve_ref E7 rules) must reach
                // the HTTP/gRPC layer with their correct status code, not
                // be wrapped in Internal(500).
                Err(e)
            }
        }
    }
}

/// P11 (batch 6): drive one layer's step futures in-task (zero spawn) — the
/// first failing step propagates immediately and the remaining in-flight
/// futures are dropped with it (the historical JoinSet first-err +
/// drop-abort semantics, P-FLOW §4.0.9 preserved).
async fn drive_step_futs<F>(
    mut futs: FuturesUnordered<F>,
    context: &mut HashMap<String, EnsembleValue>,
    skip_set: &HashSet<&str>,
) -> Result<(), AppError>
where
    F: Future<Output = StepFutOutput>,
{
    while let Some((name, result)) = futs.next().await {
        handle_step_result(&name, result, context, skip_set)?;
    }
    Ok(())
}

/// Layer-barrier executor (historical engine): runs `layers` serially,
/// driving each layer IN-TASK (P3 + P11, batch 6 — zero spawn; the
/// historical per-step tokio::spawn is gone), writing step outputs into
/// `context`. Step errors propagate directly (B3). The whole run is bounded
/// by a single shared deadline (P-DEADLINE §4.0.10): an N-layer ensemble can
/// never exceed the parent; the per-step timeout in execute_step is the
#[allow(clippy::too_many_arguments)] // layer-engine plumbing: state+plan+ctx+ids ride together by design
/// inner safety net, this outer deadline bounds the total.
async fn run_layers(
    state: &Arc<AppState>,
    plan: &EnsemblePlan,
    layers: &[Vec<usize>],
    context: &mut HashMap<String, EnsembleValue>,
    absent_inputs: &HashSet<String>,
    model_name: &str,
    version: &str,
    request_id: &str,
    opts: &EnsembleExecOpts,
    deadline_unix_ns: Option<i64>,
    snapshot: &Arc<VersionSnapshot>,
    depth: u32, // E1: nesting depth — carried into execute_step for recursion
    ancestors: &[(String, String)], // E1: this branch's chain (B1: never shared across sibling branches)
) -> Result<(), AppError> {
    let total_budget = crate::deadline::remaining(deadline_unix_ns);
    let plan_run = Arc::new(plan.clone());
    // E6 (batch 4): steps allowed to be absent on error (parse rules D5/D34
    // guarantee nothing references them — absence is safe by construction).
    let skip_set: HashSet<&str> = plan
        .steps
        .iter()
        .filter(|s| s.on_error == OnErrorKind::Skip)
        .map(|s| s.name.as_str())
        .collect();
    let ensemble_run = async {
        for layer in layers {
            // P3 + P11 (batch 6): zero-spawn layer driving. A single-step
            // layer is awaited directly (the historical JoinSet spawned a
            // task even here); a multi-step layer is driven in-task via
            // FuturesUnordered. P-FLOW cancel semantics are preserved: on
            // any early exit — a step error, the outer total-budget timeout,
            // or the parent request being dropped (client disconnect) — the
            // layer's in-flight futures are dropped with the executor, so a
            // cancelled ensemble never leaves sub-steps computing on workers.
            if layer.len() == 1 {
                let step_idx = layer[0];
                let Some(fut) = build_step_fut(
                    state, &plan_run, step_idx, context, absent_inputs, opts,
                    model_name, request_id, snapshot, depth, ancestors,
                    deadline_unix_ns,
                )?
                else {
                    continue;
                };
                let (name, result) = fut.await;
                handle_step_result(&name, result, context, &skip_set)?;
                continue;
            }
            let futs: FuturesUnordered<_> = FuturesUnordered::new();
            for &step_idx in layer {
                if let Some(fut) = build_step_fut(
                    state, &plan_run, step_idx, context, absent_inputs, opts,
                    model_name, request_id, snapshot, depth, ancestors,
                    deadline_unix_ns,
                )? {
                    futs.push(fut);
                }
            }
            drive_step_futs(futs, context, &skip_set).await?;
        }
        Ok::<(), AppError>(())
    };

    match total_budget {
        Some(b) => {
            tokio::time::timeout(b, ensemble_run)
                .await
                .map_err(|_| AppError::InferenceTimeout(format!(
                    "ensemble {} {} exceeded total deadline of {:.1}s",
                    model_name, version, b.as_secs_f64()
                )))??;
        }
        // No deadline (no client spec AND server.timeout<=0): unbounded DAG run.
        None => {
            ensemble_run.await?;
        }
    }
    Ok(())
}

/// D25: open the tail stream — build/route the stream-open and return the
/// stream handle. Batch 0: the tail's same-layer sibling unary steps run in
/// parallel with the stream (rule 3: they produce no DAG output — results
#[allow(clippy::too_many_arguments)] // layer-engine plumbing: state+plan+ctx+ids ride together by design
/// dropped, failures warn only); batch 2 replaces this function's body with
/// the chain spawn without touching the caller seam.
async fn open_tail_stream(
    state: &Arc<AppState>,
    plan: &EnsemblePlan,
    tail_idx: usize,
    tail_layer: usize,
    context: &HashMap<String, EnsembleValue>,
    absent_inputs: &HashSet<String>,
    request_id: &str,
    opts: &EnsembleExecOpts,
    deadline_unix_ns: Option<i64>,
    preflight: Option<TailPreflight>,
    snapshot: &Arc<VersionSnapshot>,
    depth: u32, // E1: carried into sibling unary steps (they can recurse)
    ancestors: &[(String, String)], // E1: this branch's chain (B1: never shared across sibling branches)
) -> Result<EnsembleStream, AppError> {
    // §4.1 rule 3: same-layer sibling unary steps still run in parallel with
    // the streaming step. They never enter `context` (output semantics belong
    // to the streaming step) and cannot affect the stream contract, so their
    // results are dropped; the JoinSet gives them the same P-FLOW drop-abort
    // cancellation as every other layer.
    let siblings: Vec<usize> = plan.layers[tail_layer]
        .iter()
        .copied()
        .filter(|&i| i != tail_idx)
        .collect();

    let run_stream = execute_stream_step(state, plan, tail_idx, context, request_id, opts, deadline_unix_ns, preflight, snapshot);
    tokio::pin!(run_stream);

    if siblings.is_empty() {
        return run_stream.await;
    }

    let run_siblings = async {
        let mut set: tokio::task::JoinSet<(String, Result<StepResults, AppError>)> =
            tokio::task::JoinSet::new();
        for idx in siblings {
            // E8-2: a when-false sibling is skipped (E6 channel); an eval
            // error is warn + skip (sibling results are dropped anyway).
            match when_passes(idx, plan, opts, context) {
                Ok(true) => {}
                Ok(false) => {
                    warn!(
                        step = %plan.steps[idx].name,
                        "tail-layer sibling skipped (when condition false)"
                    );
                    continue;
                }
                Err(e) => {
                    warn!(step = %plan.steps[idx].name, error = %e, "tail-layer sibling when-eval failed (skipped)");
                    continue;
                }
            }
            // MIMO R4 (D13): conditional siblings with an absent optional
            // input are skipped (same channel as E6-skip).
            if plan.conditional_refs[idx]
                .iter()
                .any(|n| absent_inputs.contains(n))
            {
                warn!(
                    step = %plan.steps[idx].name,
                    "tail-layer sibling skipped (optional input absent)"
                );
                continue;
            }
            let state = state.clone();
            // P1 (batch 6): the sibling sees only its referenced keys.
            let ctx = select_ctx_keys(context, &plan.step_dep_keys[idx]);
            let step = plan.steps[idx].clone();
            let plan_spawn = Arc::new(plan.clone());
            let request_id = request_id.to_string();
            let client_ip = opts.client_ip.clone();
            let snapshot = snapshot.clone();
            let ancestors = ancestors.to_vec();
            set.spawn(async move {
                let name = step.name.clone();
                execute_step(state, &plan_spawn, idx, &step, &ctx, &request_id, &client_ip, deadline_unix_ns, &snapshot, depth, &ancestors).await
                    .map(|v| (name.clone(), Ok(v)))
                    .unwrap_or_else(|e| (name.clone(), Err(e)))
            });
        }
        while let Some(joined) = set.join_next().await {
            if let Ok((name, Err(e))) = joined {
                warn!(step = %name, error = %e, "tail-layer sibling step failed (result dropped)");
            }
        }
    };
    tokio::pin!(run_siblings);

    // Whichever finishes first wins: if the stream opens first, the siblings
    // future is dropped — its JoinSet drop-aborts in-flight steps (P-FLOW),
    // exactly like a cancelled layer. If the siblings finish first, we await
    // the stream (their failures were already logged).
    let stream_result = tokio::select! {
        _ = &mut run_siblings => None,
        r = &mut run_stream => Some(r),
    };
    match stream_result {
        Some(r) => r,
        None => run_stream.await,
    }
}

/// P7: stream-open preflight — everything that does NOT depend on the
/// pre-layer results (route pick + meta build; autoload readiness check) is
/// done in parallel with the pre-layers, so the TTFT critical path only
/// waits for the assembled payload before send_stream. Returns None when the
/// sub-model was not ready at preflight time — the autoload path stays
/// serial after the pre-layers (P7 constraint: never preflight a stream for
/// a model still loading).
struct TailPreflight {
    meta: pb::RequestMeta,
    worker_id: usize,
    clients: Vec<Arc<crate::transport::zmq::WorkerZmqClient>>,
}

async fn tail_stream_preflight(
    state: &Arc<AppState>,
    step: &EnsembleStep,
    request_id: &str,
    opts: &EnsembleExecOpts,
    deadline_unix_ns: Option<i64>,
    snapshot: &Arc<VersionSnapshot>,
) -> Result<Option<TailPreflight>, AppError> {
    // E4/D15: execution-time version resolution (same source as execute_step).
    let resolved_version = snapshot.resolve(&state.registry, step)?;
    // P7 constraint: only preflight an already-ready sub-model. Not ready →
    // fall back to the serial path (which runs the autoload + poll).
    if !state.registry.is_ready(&step.model, Some(&resolved_version)) {
        return Ok(None);
    }
    if !streaming_worker_ready(state, &step.model, &resolved_version, &pb::RequestMeta::default()).await {
        return Ok(None);
    }
    let mv = state.registry.get(&step.model, Some(&resolved_version))
        .ok_or_else(|| AppError::ModelNotFound(format!("{} version {}", step.model, resolved_version)))?;
    let clients = state.worker_manager.get_zmq_clients(&step.model, &resolved_version).await
        .ok_or_else(|| AppError::WorkerCrashed(format!("{} {} has no ZMQ clients", step.model, resolved_version)))?;

    // P-TRACE: step-level span + trace injection (content-type is patched in
    // after assembly — it depends on the payload shape).
    let mut step_headers = HashMap::new();
    {
        let step_span = tracing::info_span!(
            "ensemble.step",
            step = %step.name,
            model = %step.model,
            trace_id = tracing::field::Empty,
            span_id = tracing::field::Empty,
        );
        crate::telemetry::link_parent(&step_span, &opentelemetry::Context::current());
        let _guard = step_span.enter();
        crate::telemetry::inject(&mut step_headers);
    }
    // E5 (batch 3): the preflight meta carries the STEP deadline — the same
    // min(parent, now + timeout_secs) formula the serial path would compute.
    let preflight_deadline = step_effective_deadline(deadline_unix_ns, step.timeout_secs);
    let meta = build_step_meta(
        &format!("{}:{}", request_id, step.name), &opts.client_ip, preflight_deadline, step_headers, bytes::Bytes::new(),
    );
    let outlier = state.worker_manager.get_outlier_state(&step.model, &resolved_version).await;
    let seq_registry = state.inference_queue.sequence_registry();
    let worker_id = crate::worker::pick_streaming_worker(
        &meta, mv.workers.len(), outlier.as_deref(), seq_registry, &step.model, &resolved_version,
    ).map_err(|e| AppError::Validation(e.0))?;
    if worker_id >= clients.len() {
        return Err(AppError::WorkerCrashed("invalid worker index".to_string()));
    }
    Ok(Some(TailPreflight { meta, worker_id, clients }))
}

/// §4.2: forward a worker stream into a downstream channel. Only Chunk
/// frames pass through — a per-chunk sub-stream's Done is an intermediate
/// milestone, NOT the chain's end (the tail hop synthesizes the final Done);
/// an Error frame passes through and terminates the whole chain (a node
/// failure cancels its worker and propagates upstream, D18). When the
/// downstream closes (adapter disconnect / next hop exiting), send
/// StreamCancel to THIS worker and end.
/// Returns `true` when the stream ended with an Error frame (chain stop).
async fn forward_stream(
    mut rx: mpsc::Receiver<pb::StreamResponse>,
    tx: mpsc::Sender<pb::StreamResponse>,
    cancel_client: Arc<crate::transport::zmq::WorkerZmqClient>,
    stream_id: String,
) -> bool {
    // m4: cumulative time this hop's downstream channel was FULL (the 64-slot
    // bound is backpressure, not a drop — saturation measures how long it held).
    let mut saturation_secs: f64 = 0.0;
    while let Some(chunk) = rx.recv().await {
        match chunk.payload {
            Some(pb::stream_response::Payload::Chunk(_)) => {
                let send_start = Instant::now();
                let send_res = tx.send(chunk).await;
                saturation_secs += send_start.elapsed().as_secs_f64();
                if send_res.is_err() {
                    let _ = cancel_client
                        .send_raw(crate::streaming::build_stream_cancel(stream_id))
                        .await;
                    crate::metrics::prometheus::record_ensemble_pipeline_channel_saturation_seconds(
                        saturation_secs,
                    );
                    return false;
                }
            }
            Some(pb::stream_response::Payload::Error(_)) => {
                // Propagate the failure downstream, then stop the chain.
                let _ = tx.send(chunk).await;
                crate::metrics::prometheus::record_ensemble_pipeline_channel_saturation_seconds(
                    saturation_secs,
                );
                return true;
            }
            Some(pb::stream_response::Payload::Done(_)) => {
                crate::metrics::prometheus::record_ensemble_pipeline_channel_saturation_seconds(
                    saturation_secs,
                );
                return false;
            }
            _ => {}
        }
    }
    crate::metrics::prometheus::record_ensemble_pipeline_channel_saturation_seconds(
        saturation_secs,
    );
    false
}

/// §4.2: a pipeline chain consumer — one nested send_stream sub-call PER
/// upstream chunk (chunk → 组包 → sub-stream → forward). D20: each sub-call
/// carries request_id `{parent}:{step}:{chunk_seq}`. D18: a failed
/// downstream send cancels this step's worker; the chain-handle list is
/// updated per sub-stream (tail inserted at index 0, others appended).
#[allow(clippy::too_many_arguments)] // chain-hop plumbing: state+plan+ctx+ids ride together by design
async fn consume_stream_consumer(
    state: &Arc<AppState>,
    plan: &EnsemblePlan,
    step_idx: usize,
    prev_step_name: &str,
    mut upstream: mpsc::Receiver<pb::StreamResponse>,
    downstream: mpsc::Sender<pb::StreamResponse>,
    context: &HashMap<String, EnsembleValue>,
    request_id: &str,
    opts: &EnsembleExecOpts,
    deadline_unix_ns: Option<i64>,
    chain_handles: &Arc<std::sync::Mutex<Vec<StreamHandle>>>,
    is_tail: bool,
    snapshot: &Arc<VersionSnapshot>,
) -> Result<(), AppError> {
    let step = &plan.steps[step_idx];
    // P1 (batch 6): the per-chunk context clone selects only this step's
    // referenced keys (parse-computed) — the whole-table clone per chunk
    // is gone; the upstream chunk value is inserted per chunk below.
    let base_ctx = select_ctx_keys(context, &plan.step_dep_keys[step_idx]);
    let mut seq: u64 = 0;
    let mut error_terminated = false;
    while let Some(chunk) = upstream.recv().await {
        match &chunk.payload {
            Some(pb::stream_response::Payload::Chunk(c)) => {
                // Chunk → the previous step's value in a per-chunk context.
                let chunk_value: Value = serde_json::from_slice(&c.data).map_err(|e| {
                    AppError::Internal(format!(
                        "pipeline chunk from '{}' is not valid JSON: {e}",
                        prev_step_name
                    ))
                })?;
                let mut ctx = base_ctx.clone();
                ctx.insert(
                    prev_step_name.to_string(),
                    EnsembleValue::Json(chunk_value),
                );
                let sub_request_id = format!("{}:{}:{}", request_id, step.name, seq);
                let sub = execute_stream_step(
                    state, plan, step_idx, &ctx, &sub_request_id, opts, deadline_unix_ns, None,
                    snapshot,
                )
                .await?;
                {
                    let mut handles = chain_handles.lock().unwrap();
                    let handle = StreamHandle {
                        stream_id: sub.stream_id.clone(),
                        cancel_client: Arc::clone(&sub.cancel_client),
                        abort: tokio::spawn(async {}).abort_handle(),
                    };
                    if is_tail {
                        // D25: tail at index 0 — the top-level quick-access
                        // fields mirror it.
                        handles.insert(0, handle);
                    } else {
                        handles.push(handle);
                    }
                }
                let error_terminated = forward_stream(
                    sub.chunk_rx,
                    downstream.clone(),
                    sub.cancel_client.clone(),
                    sub.stream_id.clone(),
                )
                .await;
                if error_terminated {
                    // A node failed: cancel its worker and stop consuming
                    // upstream — the upstream sender drops with us (D18).
                    let _ = sub.cancel_client
                        .send_raw(crate::streaming::build_stream_cancel(sub.stream_id.clone()))
                        .await;
                    return Ok(());
                }
                seq += 1;
            }
            Some(pb::stream_response::Payload::Done(_)) => break,
            // An upstream Error frame is terminal (§4.4: every mid-stream
            // failure must reach the client as an Error frame). A hop's own
            // sub-stream Error is forwarded by forward_stream; a frame
            // arriving HERE comes from the head hop (which has no forwarding
            // hop) or an upstream hop's channel — forward it once, then stop.
            // Do not synthesize a Done after it.
            Some(pb::stream_response::Payload::Error(_)) => {
                let _ = downstream.send(chunk).await;
                error_terminated = true;
                break;
            }
            _ => {}
        }
    }
    // Tail hop: upstream exhausted = the chain finished normally — synthesize
    // the final Done so the adapter's close收口 fires with a clean reason
    // (the per-chunk sub-stream Dones were never forwarded).
    if is_tail && !error_terminated {
        let _ = downstream
            .send(pb::StreamResponse {
                stream_id: String::new(),
                payload: Some(pb::stream_response::Payload::Done(pb::StreamDone {
                    metrics: None,
                })),
            })
            .await;
    }
    Ok(())
}

/// §4.2: spawn the pipeline chain as one task tree and return the tail
/// stream immediately. The head opens its worker stream synchronously
/// (build failures surface as real error codes); every consumer hop then
/// runs as a task: per-chunk nested sub-streams, flows through bounded
/// mpsc channels (64, D2 — full = backpressure, never dropped). The chain
/// root's AbortHandle is the D18 teardown point.
#[allow(clippy::too_many_arguments)] // chain plumbing: state+plan+ctx+ids ride together by design
async fn spawn_chain(
    state: &Arc<AppState>,
    plan: &EnsemblePlan,
    chain: &Chain,
    context: &HashMap<String, EnsembleValue>,
    request_id: &str,
    opts: &EnsembleExecOpts,
    deadline_unix_ns: Option<i64>,
    snapshot: &Arc<VersionSnapshot>,
) -> Result<EnsembleStream, AppError> {
    let nodes: Vec<usize> = chain.nodes.clone();
    let tail_idx = *nodes.last().unwrap();
    let tail = &plan.steps[tail_idx];
    // E4/D15: the metric label carries the RESOLVED tail version (the chain
    // nodes resolve via the same snapshot when their streams open).
    let resolved_tail = snapshot.resolve(&state.registry, tail)?;
    // m4: label normalization — explicit versions only; unresolved
    // ("latest"/omitted) records "latest" (no cardinality growth on drift).
    let tail_version = if tail.version.is_some() {
        resolved_tail
    } else {
        "latest".to_string()
    };

    // Inter-hop channels (nodes.len() - 1) + the tail output channel.
    let mut hop_txs: Vec<mpsc::Sender<pb::StreamResponse>> = Vec::new();
    let mut hop_rxs: Vec<mpsc::Receiver<pb::StreamResponse>> = Vec::new();
    for _ in 1..nodes.len() {
        // D2: inter-hop channel capacity 64, aligned with STREAM_CHANNEL_SIZE.
        let (tx, rx) = mpsc::channel(64);
        hop_txs.push(tx);
        hop_rxs.push(rx);
    }
    let (tail_tx, tail_rx) = mpsc::channel(64);

    // D18: chain-handle collection shared with the consumer tasks.
    let chain_handles: Arc<std::sync::Mutex<Vec<StreamHandle>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    // Head: open the first worker stream from the pre-layer context
    // SYNCHRONOUSLY — build failures surface as real error codes before the
    // stream is returned (the chain consumers then run detached).
    let head_stream = execute_stream_step(
        state, plan, nodes[0], context, request_id, opts, deadline_unix_ns, None, snapshot,
    )
    .await?;
    {
        // D18: record the head handle (appended; the tail lands at index 0).
        let mut h = chain_handles.lock().unwrap();
        h.push(StreamHandle {
            stream_id: head_stream.stream_id.clone(),
            cancel_client: Arc::clone(&head_stream.cancel_client),
            abort: tokio::spawn(async {}).abort_handle(),
        });
    }
    // The top-level cancel_client is the head stream's client (synchronously
    // available) — adapters that only read the quick-access fields stay
    // unchanged; chain cancellation broadcasts over `chain` via
    // [`cancel_ensemble_stream`].
    let cancel_client = {
        let handles = chain_handles.lock().unwrap();
        Arc::clone(&handles[0].cancel_client)
    };

    let state_h = state.clone();
    let plan_h = plan.clone();
    // P1 (batch 6): per-hop context subsets — each hop resolves only its
    // own step's refs; the whole-table clone per hop task is gone.
    let mut hop_ctxs: Vec<HashMap<String, EnsembleValue>> = nodes
        .iter()
        .skip(1)
        .map(|&node| select_ctx_keys(context, &plan.step_dep_keys[node]))
        .collect();
    let request_id_h = request_id.to_string();
    let opts_h = opts.clone();
    let handles_h = chain_handles.clone();
    let tail_tx_h = tail_tx.clone();
    let snapshot_h = snapshot.clone();
    let chain_depth = nodes.len();
    crate::metrics::prometheus::record_ensemble_pipeline_chain_depth(chain_depth);
    let root = tokio::spawn(async move {
        // Consumer hops: hop i consumes hop i-1's output and forwards into
        // hop i's channel (or the tail channel).
        let mut tasks: Vec<tokio::task::JoinHandle<Result<(), AppError>>> = Vec::new();
        let mut prev_rx = head_stream.chunk_rx;
        for (i, &node) in nodes.iter().enumerate().skip(1) {
            let is_tail = i == nodes.len() - 1;
            let down_tx = if is_tail {
                tail_tx_h.clone()
            } else {
                hop_txs[i - 1].clone()
            };
            let up_rx = std::mem::replace(&mut prev_rx, hop_rxs.remove(0));
            let state = state_h.clone();
            let plan = plan_h.clone();
            let ctx = hop_ctxs.remove(0);
            let req = request_id_h.clone();
            let opts = opts_h.clone();
            let handles = handles_h.clone();
            let snapshot = snapshot_h.clone();
            let prev_name = plan_h.steps[nodes[i - 1]].name.clone();
            tasks.push(tokio::spawn(async move {
                consume_stream_consumer(
                    &state, &plan, node, &prev_name, up_rx, down_tx, &ctx, &req, &opts,
                    deadline_unix_ns, &handles, is_tail, &snapshot,
                )
                .await
            }));
        }
        for t in tasks {
            t.await
                .map_err(|e| AppError::Internal(format!("chain task join error: {e}")))??;
        }
        Ok::<(), AppError>(())
    });

    let stream_id = format!("chain-{}", Uuid::new_v4());

    Ok(EnsembleStream {
        chunk_rx: tail_rx,
        stream_id,
        cancel_client,
        tail_model: tail.model.clone(),
        tail_version,
        tail_step: tail.name.clone(),
        step_deadline: None,
        chain: chain_handles,
        // D18: the chain task tree's root handle — the adapter aborts it on
        // disconnect as the teardown backstop.
        abort: root.abort_handle(),
        permit: None,
    })
}

/// D18: cancel an ensemble stream from the adapters. A pipeline chain
/// broadcasts over every chain handle (each streaming step's worker) and
/// aborts the chain task tree; the top-level client is the fallback (empty
/// handles = non-chain, or a chain whose consumers have not opened yet).
pub async fn cancel_chain(
    chain: Option<&Arc<std::sync::Mutex<Vec<StreamHandle>>>>,
    abort: Option<&tokio::task::AbortHandle>,
    fallback_stream_id: &str,
    fallback_client: &Arc<crate::transport::zmq::WorkerZmqClient>,
) {
    match chain {
        Some(chain) => {
            // Collect the cancel targets under the lock, then send outside it
            // (MutexGuard must not cross an await — the future stays Send).
            let targets: Vec<(String, Arc<crate::transport::zmq::WorkerZmqClient>)> = {
                let handles = chain.lock().unwrap();
                if handles.is_empty() {
                    vec![(fallback_stream_id.to_string(), fallback_client.clone())]
                } else {
                    handles
                        .iter()
                        .map(|h| (h.stream_id.clone(), h.cancel_client.clone()))
                        .collect()
                }
            };
            for (sid, client) in targets {
                let req = crate::streaming::build_stream_cancel(sid);
                let _ = client.send_raw(req).await;
            }
            if let Some(a) = abort {
                a.abort();
            }
        }
        None => {
            let req = crate::streaming::build_stream_cancel(fallback_stream_id.to_string());
            let _ = fallback_client.send_raw(req).await;
        }
    }
}

/// D19: streaming readiness predicate — the sub-model must be reachable via
/// routing (pick_streaming_worker returns a valid worker), NOT merely
/// registry-ready: mark_ready and the streaming route table registration have
/// a window between them, so a pick failure is a retryable state, never an
/// instant 500.
async fn streaming_worker_ready(
    state: &Arc<AppState>,
    model: &str,
    version: &str,
    meta: &pb::RequestMeta,
) -> bool {
    let Some(mv) = state.registry.get(model, Some(version)) else { return false; };
    if mv.workers.is_empty() {
        return false;
    }
    let Some(clients) = state.worker_manager.get_zmq_clients(model, version).await else {
        return false;
    };
    if clients.is_empty() {
        return false;
    }
    let outlier = state.worker_manager.get_outlier_state(model, version).await;
    let seq = state.inference_queue.sequence_registry();
    match crate::worker::pick_streaming_worker(meta, mv.workers.len(), outlier.as_deref(), seq, model, version) {
        Ok(w) => w < clients.len(),
        Err(_) => false,
    }
}

/// Shared sub-model autoload (unary + streaming execution faces): config
/// parse/validation errors surface as ModelNotReady; load failures surface
/// directly. The caller polls its own readiness predicate afterwards (unary =
/// registry ready, streaming = `streaming_worker_ready`). Callers pass the
/// RESOLVED version (E4/D15 — resolution happens once per step).
async fn ensure_sub_model_loaded(
    state: &Arc<AppState>,
    model: &str,
    version: &str,
) -> Result<(), AppError> {
    if state.registry.is_ready(model, Some(version)) {
        return Ok(());
    }
    let load_start = Instant::now();
    info!("Auto-loading sub-model {} v{} for ensemble", model, version);
    let sub_model_dir = crate::validation::resolve_model_dir(
        &state.repo_path, model, version,
    )?;
    // 配置解析/校验失败必须可见(同 reconcile:不再 unwrap_or_default
    // 静默回退默认配置;M7 迁移哨兵依赖此错误上浮)。
    let mut config = match crate::config::load_model_config(
        &sub_model_dir.join("config.yaml")
    ) {
        Ok(c) => c,
        Err(e) => {
            return Err(AppError::ModelNotReady(format!(
                "sub-model {} v{} has invalid config.yaml: {}", model, version, e
            )));
        }
    };
    state.config.apply_model_defaults(&mut config);
    if let Err(e) = state.worker_manager.load_model(model, version, &config).await {
        // Concurrent autoload race (parallel steps / batch elements of the
        // same cold sub-model): the load error usually means a SIBLING's
        // load won — the registry flips ready when that load completes.
        // Poll briefly before surfacing the failure, so a benign conflict
        // never fails a whole step/element.
        for _ in 0..30 {
            if state.registry.is_ready(model, Some(version)) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        warn!("Failed to auto-load sub-model {} v{}: {}", model, version, e);
        return Err(AppError::ModelNotReady(format!(
            "sub-model {} v{} not ready: {}", model, version, e
        )));
    }
    // m4: autoload wait (the dominant cold-TTFT term for ensemble DAGs).
    crate::metrics::prometheus::record_ensemble_autoload_wait_seconds(
        load_start.elapsed().as_secs_f64(),
    );
    Ok(())
}

/// Execute the streaming tail step: resolve inputs, assemble payload (same
#[allow(clippy::too_many_arguments)] // layer-engine plumbing: state+plan+ctx+ids ride together by design
/// rules as unary), D19 readiness poll (skipped when a P7 preflight is
/// provided), build meta, route and open the stream. Returns the stream
/// handle consumed by the adapter layers.
async fn execute_stream_step(
    state: &Arc<AppState>,
    plan: &EnsemblePlan,
    step_idx: usize,
    context: &HashMap<String, EnsembleValue>,
    request_id: &str,
    opts: &EnsembleExecOpts,
    deadline_unix_ns: Option<i64>,
    preflight: Option<TailPreflight>,
    snapshot: &Arc<VersionSnapshot>,
) -> Result<EnsembleStream, AppError> {
    let step = &plan.steps[step_idx];
    // E4/D15: execution-time version resolution (same source as execute_step;
    // the metric label tail_version below is the resolved version).
    let resolved_version = snapshot.resolve(&state.registry, step)?;
    // E5/D35 (batch 3): the step's wall-clock cap — the readiness poll, D19
    // checks and the meta cascade below are bounded by min(parent deadline,
    // now + timeout_secs). Each chain hop computes its own from its own
    // open time (the same formula per streaming step).
    let step_deadline = step_effective_deadline(deadline_unix_ns, step.timeout_secs);

    // Resolve inputs into EnsembleValues (identical to the unary path).
    let mut resolved: HashMap<String, EnsembleValue> = HashMap::new();
    for (key, ref_str) in &step.inputs {
        match resolve_ref(plan, ref_str, context)? {
            ResolvedRef::Value(v) => {
                resolved.insert(key.clone(), v);
            }
            ResolvedRef::Absent(name) => {
                // Streaming steps cannot be conditional (D34) — reaching
                // here is an internal inconsistency.
                return Err(AppError::Internal(format!(
                    "streaming step '{}' resolved absent input '{}' (D34 violation)",
                    step.name, name
                )));
            }
        }
    }
    let (payload_bytes, content_type_for_step) = assemble_step_payload(
        &step.name, &resolved, &step.params, plan.input_mode(step_idx),
    )?;

    // P7: when the preflight already routed/built meta in parallel with the
    // pre-layers, skip the serial readiness/meta/pick block.
    let (mut meta, worker_id, clients) = match preflight {
        Some(pf) => (pf.meta, pf.worker_id, pf.clients),
        None => {
            // D19: ensure the sub-model is loaded, then poll STREAMING
            // readiness (pick non-empty) with backoff. A pick failure is a
            // retryable state.
            ensure_sub_model_loaded(state, &step.model, &resolved_version).await?;
            let mut retries = 0;
            let max_retries = 30;
            let mut delay = Duration::from_millis(50);
            while !streaming_worker_ready(state, &step.model, &resolved_version, &pb::RequestMeta::default()).await {
                if retries >= max_retries {
                    return Err(AppError::ModelNotReady(format!(
                        "sub-model {} v{} streaming not ready", step.model, resolved_version
                    )));
                }
                // D19: quick-fail when the remaining budget cannot cover one
                // more poll — never spin to timeout (E5: the STEP budget).
                if let Some(rem) = crate::deadline::remaining(step_deadline) {
                    if rem < delay {
                        return Err(AppError::InferenceTimeout(format!(
                            "ensemble step {}: deadline exhausted while waiting for sub-model {} v{}",
                            step.name, step.model, resolved_version
                        )));
                    }
                }
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_millis(500));
                retries += 1;
            }

            let mv = state.registry.get(&step.model, Some(&resolved_version))
                .ok_or_else(|| AppError::ModelNotFound(format!("{} version {}", step.model, resolved_version)))?;
            // E1/D4: ensembles have no streaming form (zero workers) — a
            // streaming step calling one is an unsupported combination, not a
            // readiness problem. Fail fast instead of exhausting the D19 poll.
            if mv.model_type == ModelType::Ensemble {
                return Err(AppError::InvalidRequestBody(format!(
                    "streaming step '{}' calls ensemble model '{}' — ensembles have no \
                     streaming form (D4)",
                    step.name, step.model
                )));
            }
            let clients = state.worker_manager.get_zmq_clients(&step.model, &resolved_version).await
                .ok_or_else(|| AppError::WorkerCrashed(format!("{} {} has no ZMQ clients", step.model, resolved_version)))?;

            // P-TRACE (同 execute_step): step-level span links to the parent
            // trace and injects its context into the step headers so the
            // worker spans land as children of the step.
            let mut step_headers = HashMap::new();
            if let Some(ct) = &content_type_for_step {
                step_headers.insert("content-type".to_string(), ct.clone());
            }
            {
                let step_span = tracing::info_span!(
                    "ensemble.step",
                    step = %step.name,
                    model = %step.model,
                    trace_id = tracing::field::Empty,
                    span_id = tracing::field::Empty,
                );
                crate::telemetry::link_parent(&step_span, &opentelemetry::Context::current());
                let _guard = step_span.enter();
                crate::telemetry::inject(&mut step_headers);
            }

            // Streaming meta carries no payload — the body rides StreamOpen.data.
            let meta = build_step_meta(
                &format!("{}:{}", request_id, step.name), &opts.client_ip, step_deadline, step_headers, bytes::Bytes::new(),
            );

            let outlier = state.worker_manager.get_outlier_state(&step.model, &resolved_version).await;
            let seq_registry = state.inference_queue.sequence_registry();
            let worker_id = crate::worker::pick_streaming_worker(
                &meta, mv.workers.len(), outlier.as_deref(), seq_registry, &step.model, &resolved_version,
            ).map_err(|e| AppError::Validation(e.0))?;
            if worker_id >= clients.len() {
                return Err(AppError::WorkerCrashed("invalid worker index".to_string()));
            }
            (meta, worker_id, clients)
        }
    };

    // The preflight built meta before the payload was assembled — patch in
    // the content-type for binary passthrough (B3/E7).
    if let Some(ct) = &content_type_for_step {
        meta.headers
            .entry("content-type".to_string())
            .or_insert_with(|| ct.clone());
    }

    // D19: final deadline check before opening the stream — an expired
    // budget must not open one (deadline-exhausted mapping, §4.4; E5: the
    // STEP budget).
    if let Some(rem) = crate::deadline::remaining(step_deadline) {
        if rem.is_zero() {
            return Err(AppError::InferenceTimeout(format!(
                "ensemble step {}: deadline exhausted before stream open", step.name
            )));
        }
    }
    crate::metrics::prometheus::record_worker_inference(&step.model, &resolved_version, worker_id, 1);

    // E6 (batch 4, D35): streaming retry is BUILD-WINDOW limited — the
    // window is send_stream → first non-Error frame; a first-frame Error /
    // build timeout / pre-frame close rebuilds with backoff and a re-pick
    // (can land on a healthy instance), each attempt a fresh stream_id.
    // Chunk/Start/Done commits the stream (no replay — P5/§6.3). retries == 0
    // keeps the historical no-peek path byte-for-byte.
    let (chunk_rx, stream_id, cancel_client, wrapper_abort) = if step.retries == 0 {
        let client = &clients[worker_id];
        let stream_id = format!("stream-{}", Uuid::new_v4());
        let open_req = crate::streaming::build_stream_open(
            stream_id.clone(), payload_bytes.clone(), Some(meta.clone()), opts.decoupled,
        );
        let chunk_rx = client.send_stream(open_req, stream_id.clone()).await?;
        (chunk_rx, stream_id, Arc::clone(client), None)
    } else {
        open_stream_with_retry(
            state, step, &resolved_version, payload_bytes.clone(), &meta, opts,
            step_deadline, &clients, worker_id, step.retries,
        )
        .await?
    };

    // D25: chain handles — batch 0 = the tail stream itself as the single
    // element (chain[0] === the top-level fields; adapters never read chain).
    // E6: with retries a first-frame forwarder runs — its AbortHandle is the
    // teardown point (aborting it drops the receiver, worker idle reclaim).
    let abort = wrapper_abort.unwrap_or_else(|| tokio::spawn(async {}).abort_handle());
    let chain = vec![StreamHandle {
        stream_id: stream_id.clone(),
        cancel_client: Arc::clone(&cancel_client),
        abort: abort.clone(),
    }];

    Ok(EnsembleStream {
        chunk_rx,
        stream_id,
        cancel_client,
        tail_model: step.model.clone(),
        // m4: label normalization — explicit versions only; unresolved
        // ("latest"/omitted) records "latest" (no cardinality growth on
        // active drift).
        tail_version: if step.version.is_some() {
            resolved_version.clone()
        } else {
            "latest".to_string()
        },
        tail_step: step.name.clone(),
        // D35 (batch 3): E5 timeout_secs → step wall-clock cap measured from
        // the open instant; the adapters' recv_chunk overall takes min(client
        // overall, this). None = no step timeout. Absurdly large caps are
        // parse-legal — an unrepresentable Duration degrades to "no cap"
        // (None) instead of a from_secs_f64 overflow panic.
        step_deadline: step.timeout_secs.and_then(|t| {
            Instant::now().checked_add(Duration::try_from_secs_f64(t).ok()?)
        }),
        chain: Arc::new(std::sync::Mutex::new(chain)),
        abort,
        // P10: filled by execute_ensemble after open_tail_stream returns.
        permit: None,
    })
}

/// E6 (batch 4, D35): open the step's worker stream under the build-window
/// retry contract. The window closes when a Chunk/Start/Done frame arrives
/// (committed) — a first-frame Error, a build timeout, a pre-frame channel
/// close, or a failing `send_stream` call rebuilds with exponential backoff
/// and a FRESH worker pick (a retry can land on a healthy instance; the old
/// stream is cancelled so it never leaks). On exhaustion the committed path
/// still runs — the first Error frame returns in-stream (the adapter emits
/// the Error frame + close, §4.4) and a build timeout hands the raw receiver
/// to the adapter (its recv_chunk bounds take over). Because the peek
/// consumes the first frame, a committed stream is wrapped by a zero-copy
/// forwarder that re-emits it first — the adapter forward loops stay
/// unchanged (D25 seam).
#[allow(clippy::too_many_arguments)] // stream-open plumbing: state+meta+clients ride together by design
async fn open_stream_with_retry(
    state: &Arc<AppState>,
    step: &EnsembleStep,
    resolved_version: &str,
    payload_bytes: bytes::Bytes,
    meta: &pb::RequestMeta,
    opts: &EnsembleExecOpts,
    step_deadline: Option<i64>,
    clients: &[Arc<crate::transport::zmq::WorkerZmqClient>],
    mut worker_id: usize,
    retries: u32,
) -> Result<
    (
        mpsc::Receiver<pb::StreamResponse>,
        String,
        Arc<crate::transport::zmq::WorkerZmqClient>,
        Option<tokio::task::AbortHandle>,
    ),
    AppError,
> {
    // First-frame peek bound: the step budget remaining; with no deadline the
    // idle budget (same escape hatch as the adapters' recv_chunk: 0 = none).
    let peek_bound = crate::deadline::remaining(step_deadline).or_else(|| {
        crate::deadline::idle_budget(state.config.server.decoupled_idle_timeout_secs)
    });
    let mut attempt: u32 = 0;
    let mut backoff = Duration::from_millis(50);
    loop {
        let stream_id = format!("stream-{}", Uuid::new_v4());
        let client = &clients[worker_id];
        let open_req = crate::streaming::build_stream_open(
            stream_id.clone(), payload_bytes.clone(), Some(meta.clone()), opts.decoupled,
        );
        // E6/D35: the window starts AT send_stream — a build failure is
        // retryable (worker crash race), no 4xx/5xx distinction on the
        // streaming wire (there are no status codes).
        let mut rx = match client.send_stream(open_req, stream_id.clone()).await {
            Ok(rx) => rx,
            Err(e) => {
                if attempt < retries && retry_sleep_budget(step_deadline, backoff).is_some() {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_millis(500));
                    attempt += 1;
                    worker_id = repick_streaming_worker(state, step, resolved_version, meta, clients).await?;
                    continue;
                }
                return Err(e);
            }
        };

        // Peek the first frame (bounded). Committed frames: Chunk/Start/Done.
        let first = match peek_bound {
            Some(b) => match tokio::time::timeout(b, rx.recv()).await {
                Ok(f) => f,
                Err(_) => {
                    // Build timeout — retry; exhausted → hand the raw
                    // receiver to the adapter (its recv_chunk bounds rule).
                    if attempt < retries && retry_sleep_budget(step_deadline, backoff).is_some() {
                        let _ = client
                            .send_raw(crate::streaming::build_stream_cancel(stream_id))
                            .await;
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_millis(500));
                        attempt += 1;
                        worker_id = repick_streaming_worker(state, step, resolved_version, meta, clients).await?;
                        continue;
                    }
                    return Ok((rx, stream_id, Arc::clone(client), None));
                }
            },
            None => rx.recv().await,
        };

        let first_is_error = matches!(
            &first,
            Some(f) if matches!(&f.payload, Some(pb::stream_response::Payload::Error(_)))
        );
        let first_is_none = first.is_none();
        if (first_is_error || first_is_none) && attempt < retries {
            // First-frame Error / pre-frame close (worker died) — retryable.
            if retry_sleep_budget(step_deadline, backoff).is_some() {
                let _ = client
                    .send_raw(crate::streaming::build_stream_cancel(stream_id))
                    .await;
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_millis(500));
                attempt += 1;
                worker_id = repick_streaming_worker(state, step, resolved_version, meta, clients).await?;
                continue;
            }
        }

        // Committed (Chunk/Start/Done, or Error/close with retries
        // exhausted): preserve the consumed first frame behind a zero-copy
        // forwarder — capacity aligned with STREAM_CHANNEL_SIZE (D2).
        let (tx, out_rx) = mpsc::channel(64);
        let task = tokio::spawn(async move {
            if let Some(f) = first {
                if tx.send(f).await.is_err() {
                    return;
                }
            }
            while let Some(f) = rx.recv().await {
                if tx.send(f).await.is_err() {
                    return;
                }
            }
        });
        return Ok((out_rx, stream_id, Arc::clone(client), Some(task.abort_handle())));
    }
}

/// E6 (batch 4): re-pick a streaming worker for a retry attempt — the same
/// pick path as the serial open (fresh pick can land on a healthy instance
/// after a crash race).
async fn repick_streaming_worker(
    state: &Arc<AppState>,
    step: &EnsembleStep,
    resolved_version: &str,
    meta: &pb::RequestMeta,
    clients: &[Arc<crate::transport::zmq::WorkerZmqClient>],
) -> Result<usize, AppError> {
    let mv = state.registry.get(&step.model, Some(resolved_version)).ok_or_else(|| {
        AppError::ModelNotFound(format!("{} version {}", step.model, resolved_version))
    })?;
    let outlier = state.worker_manager.get_outlier_state(&step.model, resolved_version).await;
    let seq_registry = state.inference_queue.sequence_registry();
    let worker_id = crate::worker::pick_streaming_worker(
        meta, mv.workers.len(), outlier.as_deref(), seq_registry, &step.model, resolved_version,
    )
    .map_err(|e| AppError::Validation(e.0))?;
    if worker_id >= clients.len() {
        return Err(AppError::WorkerCrashed("invalid worker index".to_string()));
    }
    Ok(worker_id)
}

/// E6 (batch 4): the retry sleep — None when the step budget cannot cover
/// it (fast-fail instead of sleeping into the deadline, §4.4 deadline row).
fn retry_sleep_budget(step_deadline: Option<i64>, backoff: Duration) -> Option<Duration> {
    match crate::deadline::remaining(step_deadline) {
        Some(rem) if rem < backoff => None,
        _ => Some(backoff),
    }
}

/// Shared step RequestMeta (unary + streaming): request_id `{parent}:{step}`
/// suffix, trace-injected headers, deadline cascade, content-type passthrough
/// (B3/E7). `payload` is the serialized body — empty for streaming, where the
/// body rides StreamOpen.data.
fn build_step_meta(
    request_id: &str,
    client_ip: &str,
    deadline_unix_ns: Option<i64>,
    step_headers: HashMap<String, String>,
    payload: bytes::Bytes,
) -> pb::RequestMeta {
    pb::RequestMeta {
        route: "/predict".to_string(),
        headers: step_headers,
        client_ip: client_ip.to_string(),
        // The request_id passed in is ALREADY the full step-scoped id
        // (callers append `:{step}` or `:{step}:{chunk_seq}` for pipeline
        // sub-calls, D20) — no suffixing here so pipeline chunk calls can
        // carry their own seq.
        request_id: request_id.to_string(),
        timestamp_ns: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64,
        payload,
        // P-DEADLINE cascade: child step shares the parent deadline so a single
        // step cannot exceed it (budget = parent − already elapsed).
        deadline_unix_ns,
        ..Default::default()
    }
}

/// P9: cheap size estimate for with_capacity — recursive walk, no allocation
/// (numbers fall back to a fixed f64-bound; the estimate only needs to be
/// within a constant factor of the serialized size to eliminate the realloc
/// chain in assemble_step_payload).
fn json_value_len_estimate(v: &serde_json::Value) -> usize {
    match v {
        Value::Null => 4,
        Value::Bool(b) => {
            if *b {
                4
            } else {
                5
            }
        }
        Value::Number(_) => 16,
        Value::String(s) => s.len() + 2,
        Value::Array(a) => {
            a.iter().map(json_value_len_estimate).sum::<usize>() + a.len() + 2
        }
        Value::Object(o) => o
            .iter()
            .map(|(k, v)| k.len() + 3 + json_value_len_estimate(v))
            .sum::<usize>()
            + 2,
    }
}

/// B3 (E7): step input assembly — three branches.
///   all Json                                → build a JSON object (historical path)
///   exactly one input, a whole Binary       → raw bytes passthrough + content-type
///   any other combination involving Binary  → 400 (Option B scope)
///
/// E3 (batch 3): `params` merge into the assembled JSON object AFTER the
/// inputs — params win on key conflicts. The merge only shapes the payload
/// layer (m3: it never touches the sub-model's own model_defaults/server
/// overrides). Binary assembly has no params semantics — a non-empty params
/// on a Binary step is rejected here (the input type is only decidable at
/// assembly; this is the earliest enforcement point).
///
/// P9: the Json branch pre-sizes its buffer from a cheap estimate (no
/// realloc chain) and hands the Vec straight to `Bytes` (ownership transfer,
/// zero copy — the queue payload stays Bytes end-to-end, §1.4); the Binary
/// branch passes the Bytes through by refcount (P2: the old `to_vec` copy is
/// gone).
fn assemble_step_payload(
    step_name: &str,
    resolved: &HashMap<String, EnsembleValue>,
    params: &HashMap<String, Value>,
    // MIMO (R11/R12): parse-decided mode — Some = static dispatch (no
    // runtime type branches); None = legacy dynamic dispatch (the historical
    // runtime checks stay byte-identical).
    mode: Option<InputMode>,
) -> Result<(bytes::Bytes, Option<String>), AppError> {
    let binary_count = resolved
        .values()
        .filter(|v| matches!(v, EnsembleValue::Binary(..)))
        .count();

    // MIMO: the static environment pre-decided the branch — the runtime
    // checks below are unreachable for Some(mode) (R12/R9 parse-guaranteed).
    match mode {
        Some(InputMode::GroupJson) => {
            return assemble_group_json(step_name, resolved, params);
        }
        Some(InputMode::BinaryPassThrough) => {
            return assemble_binary_passthrough(step_name, resolved, params);
        }
        None => {}
    }

    if binary_count == 0 {
        // All Json → build JSON object (historical path).
        return assemble_group_json(step_name, resolved, params);
    }
    if resolved.len() == 1 && binary_count == 1 {
        // Exactly one input and it is Binary → raw bytes passthrough.
        return assemble_binary_passthrough(step_name, resolved, params);
    }
    // Mixed or multiple inputs with any Binary → 400 (Option B scope).
    Err(AppError::InvalidRequestBody(format!(
        "step '{}' has {} input(s) with mixed JSON/Binary; \
         a binary input must be the step's sole whole input (Option B scope)",
        step_name,
        resolved.len()
    )))
}

/// MIMO (R12): GroupJson assembly — all-Json inputs build one object (E3
/// params override after the inputs). Shared by the static dispatch and the
/// legacy dynamic path (identical bytes).
fn assemble_group_json(
    _step_name: &str,
    resolved: &HashMap<String, EnsembleValue>,
    params: &HashMap<String, Value>,
) -> Result<(bytes::Bytes, Option<String>), AppError> {
    let mut obj = serde_json::Map::new();
    for (key, val) in resolved {
        match val {
            EnsembleValue::Json(v) => {
                obj.insert(key.clone(), v.clone());
            }
            // Unreachable: legacy routes here only with binary_count == 0;
            // static GroupJson is parse-checked (R12).
            EnsembleValue::Binary(..) | EnsembleValue::Envelope { .. } => unreachable!(),
        }
    }
    // E3: constant step params override the assembled inputs.
    for (key, val) in params {
        obj.insert(key.clone(), val.clone());
    }
    let estimate: usize = obj
        .iter()
        .map(|(k, v)| k.len() + 3 + json_value_len_estimate(v))
        .sum::<usize>()
        + 2;
    let mut buf = Vec::with_capacity(estimate);
    let mut ser = serde_json::Serializer::new(&mut buf);
    // Value serialization is infallible (E8 cleanup note).
    serde::Serialize::serialize(&Value::Object(obj), &mut ser)
        .expect("Value serialization is infallible");
    Ok((bytes::Bytes::from(buf), None))
}

/// MIMO (R12/D8): BinaryPassThrough assembly — the sole whole Binary input's
/// bytes pass through (D8's bounded internal binary flow).
fn assemble_binary_passthrough(
    step_name: &str,
    resolved: &HashMap<String, EnsembleValue>,
    params: &HashMap<String, Value>,
) -> Result<(bytes::Bytes, Option<String>), AppError> {
    // Unreachable under R9 in static mode — kept as the defensive mirror of
    // the legacy runtime check.
    if !params.is_empty() {
        return Err(AppError::InvalidRequestBody(format!(
            "step '{}': params cannot be combined with a binary input (E3/R9)",
            step_name
        )));
    }
    let (data, ct, ..) = match resolved.values().next().unwrap() {
        EnsembleValue::Binary(data, ct, ..) => (data.clone(), ct.clone()),
        // Unreachable: legacy routes here only with a sole binary value;
        // static BinaryPassThrough is parse-checked (R12).
        _ => unreachable!(),
    };
    Ok((data, Some(ct)))
}

/// B3 (E8): typed step output — mirrors the unary media_type dispatch
/// (inference.rs:266-267). A non-JSON media_type declares the payload opaque
/// bytes; the JSON path is validated and errors are NOT swallowed (the old
/// code collapsed invalid JSON to `{}` — this is the regression pin).
fn parse_step_output(step_name: &str, single: pb::SingleResponse) -> Result<EnsembleValue, AppError> {
    let is_binary = !single.media_type.is_empty()
        && !single.media_type.starts_with("application/json");
    if is_binary {
        Ok(EnsembleValue::Binary(single.data, single.media_type, None, None))
    } else if single.data.is_empty() {
        Ok(EnsembleValue::Json(json!({})))
    } else {
        let v: Value = serde_json::from_slice(&single.data).map_err(|e| {
            AppError::Internal(format!(
                "ensemble step {} returned invalid JSON: {}",
                step_name, e
            ))
        })?;
        Ok(EnsembleValue::Json(v))
    }
}

/// MIMO (D10, batch 4①: the binary half): materialize a step's raw worker
/// response into its declared named outputs — context keys are `step.alias`
/// (an undeclared step keeps the single `step` key). Binary alias semantics
/// (R6): path-less = the whole response (worker non-JSON media_type);
/// path-specified = a `$binary_b64` marker object at that JSON path. A
/// type mismatch (declared binary / worker JSON, or vice versa) is an
/// ordinary step error (I3's runtime failure #1 — on_error semantics apply).
/// Json aliases land with MIMO② — the declaration shape is already final.
fn materialize_step_outputs(
    step: &EnsembleStep,
    raw: EnsembleValue,
) -> Result<Vec<(String, EnsembleValue)>, AppError> {
    let Some(decl) = &step.outputs_decl else {
        return Ok(vec![(step.name.clone(), raw)]);
    };
    let mut out = Vec::with_capacity(decl.len());
    for (alias, d) in decl {
        let key = format!("{}.{}", step.name, alias);
        match (&raw, d.ty) {
            (EnsembleValue::Binary(data, ct, shape, dt), InputType::Binary) => {
                if d.path.is_some() {
                    return Err(AppError::Internal(format!(
                        "step '{}' alias '{}': a path-specified binary output needs a \
                         JSON response (the marker object lives in JSON)",
                        step.name, alias
                    )));
                }
                out.push((
                    key,
                    EnsembleValue::Binary(
                        data.clone(),
                        ct.clone(),
                        shape.clone(),
                        dt.clone(),
                    ),
                ));
            }
            (EnsembleValue::Binary(..), InputType::Json) => {
                return Err(AppError::InvalidRequestBody(format!(
                    "step '{}' alias '{}' is declared json but the worker returned \
                     binary (type mismatch, I3)",
                    step.name, alias
                )));
            }
            (EnsembleValue::Json(v), InputType::Binary) => {
                let path = d.path.as_deref().ok_or_else(|| {
                    AppError::InvalidRequestBody(format!(
                        "step '{}' alias '{}' is declared binary (whole response) but \
                         the worker returned JSON (type mismatch, I3)",
                        step.name, alias
                    ))
                })?;
                let marker = project_json_path(v, path).map_err(|_| {
                    AppError::InvalidRequestBody(format!(
                        "step '{}' alias '{}': path '{}' not found in the response",
                        step.name, alias, path
                    ))
                })?;
                let (bytes, ct) = decode_binary_marker(&marker)?;
                out.push((key, EnsembleValue::Binary(bytes, ct, None, None)));
            }
            (EnsembleValue::Json(v), InputType::Json) => {
                // MIMO② (D10): the default path is `$.<alias>`; an explicit
                // path projects the Json pointer subset (D29).
                let path = d.path.as_deref().unwrap_or(alias);
                let projected = project_json_path(v, path).map_err(|_| {
                    AppError::InvalidRequestBody(format!(
                        "step '{}' alias '{}': path '{}' not found in the response",
                        step.name, alias, path
                    ))
                })?;
                out.push((key, EnsembleValue::Json(projected)));
            }
            (EnsembleValue::Envelope { .. }, _) => {
                return Err(AppError::Internal(
                    "envelope reached step materialization".to_string(),
                ));
            }
        }
    }
    Ok(out)
}

/// E6 (batch 4): retry classification — only transient worker-side failures
/// retry. 5xx model errors (upstream overload/crash race) and timeouts are
/// transient; 4xx is a deterministic client contract (retrying cannot fix
/// it), queue pressure retrying makes worse, and crashes/readiness belong to
/// the autoload path, not the retry path.
fn is_retryable_error(e: &AppError) -> bool {
    match e {
        AppError::InferenceTimeout(_) => true,
        AppError::ModelError(data) => data.status_code >= 500,
        _ => false,
    }
}

#[allow(clippy::too_many_arguments)] // step plumbing: state+plan+ctx+ids+snapshot ride together by design
async fn execute_step(
    state: Arc<AppState>,
    plan: &EnsemblePlan,
    step_idx: usize,
    step: &EnsembleStep,
    context: &HashMap<String, EnsembleValue>,
    request_id: &str,
    client_ip: &str,
    deadline_unix_ns: Option<i64>,
    snapshot: &Arc<VersionSnapshot>,
    depth: u32, // E1: nesting depth — +1 on recursion into an ensemble step
    ancestors: &[(String, String)], // E1: THIS branch's nesting chain (B1: never shared across sibling branches)
) -> Result<Vec<(String, EnsembleValue)>, AppError> {
    // E4/D15: resolve the sub-model version at execution time — explicit
    // versions as-is, unresolved ("latest"/omitted) via the request-scoped
    // snapshot (first resolution per model wins; registry active is the only
    // source, D27). Every readiness/queue/registry call below uses the
    // resolved version.
    let resolved_version = snapshot.resolve(&state.registry, step)?;

    // E5 (batch 3): the step's wall-clock cap — the meta cascade and the
    // response wait below are bounded by min(parent deadline, now +
    // timeout_secs).
    let step_deadline = step_effective_deadline(deadline_unix_ns, step.timeout_secs);
    // E5: a cap that expired BEFORE submit fails fast (§4.4 deadline row).
    if let Some(rem) = crate::deadline::remaining(step_deadline) {
        if rem.is_zero() {
            return Err(AppError::InferenceTimeout(format!(
                "ensemble step {}: step timeout exhausted before submit",
                step.name
            )));
        }
    }

    // Resolve inputs into EnsembleValues.
    let mut resolved: HashMap<String, EnsembleValue> = HashMap::new();
    for (key, ref_str) in &step.inputs {
        match resolve_ref(plan, ref_str, context)? {
            ResolvedRef::Value(v) => {
                resolved.insert(key.clone(), v);
            }
            ResolvedRef::Absent(name) => {
                return Err(AppError::Internal(format!(
                    "step '{}' resolved absent input '{}' — conditional steps \
                     are skipped before execution (R4)",
                    step.name, name
                )));
            }
        }
    }

    // MIMO (R11/R12): static input-mode dispatch in declared configs; legacy
    // configs keep the dynamic runtime dispatch (byte-identical).
    let (payload_bytes, content_type_for_step) = assemble_step_payload(
        &step.name, &resolved, &step.params, plan.input_mode(step_idx),
    )?;

    // Ensure sub-model is ready (shared autoload; unary readiness predicate)
    ensure_sub_model_loaded(&state, &step.model, &resolved_version).await?;
    // Poll with exponential backoff for worker readiness
    let mut retries = 0;
    let max_retries = 30;
    let mut delay = Duration::from_millis(50);
    while !state.registry.is_ready(&step.model, Some(&resolved_version)) && retries < max_retries {
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_millis(500));
        retries += 1;
    }

    if !state.registry.is_ready(&step.model, Some(&resolved_version)) {
        return Err(AppError::ModelNotReady(format!(
            "sub-model {} v{} is not ready", step.model, resolved_version
        )));
    }

    // Get model version info
    let mv = state.registry.get(&step.model, Some(&resolved_version))
        .ok_or_else(|| AppError::ModelNotFound(format!("{} version {}", step.model, resolved_version)))?;

    if mv.model_type == ModelType::Ensemble {
        // E1 (batch 3): nested ensemble — recurse with depth+1, the SAME
        // version snapshot (D36: a parent and child resolving the same model
        // share one resolution) and the shared parent deadline (E1: 递归共享
        // 父 deadline). The child request is the step's assembled payload
        // (params already merged, E3) re-materialized as a value.
        if contains_ancestor(ancestors, &step.model, &resolved_version) {
            return Err(AppError::InvalidRequestBody(format!(
                "ensemble recursion detected: {} v{} is already on the active nesting chain",
                step.model, resolved_version
            )));
        }
        // D4: a nested ensemble must not contain streaming steps (the
        // combination is out of scope — checked before recursing so the
        // error names the offending child DAG). E8-1: a dags-form child is a
        // pure container whose outer plan has EMPTY steps — nested calls
        // carry no selector, so the check inspects the SELECTED default set
        // (indexing the outer plan would panic).
        let child_plan = get_ensemble_plan(&state, &step.model, &resolved_version).await?;
        let child_plan = select_dag_set(&child_plan, None)?;
        if child_plan.steps[child_plan.output_step].stream || !child_plan.chains.is_empty() {
            return Err(AppError::InvalidRequestBody(format!(
                "nested ensemble '{}' contains a streaming step — nested ensembles \
                 must be unary-only (D4)",
                step.model
            )));
        }
        let child_payload = match &content_type_for_step {
            Some(ct) => EnsembleValue::Binary(payload_bytes.clone(), ct.clone(), None, None),
            None => EnsembleValue::Json(
                serde_json::from_slice(&payload_bytes).map_err(|e| {
                    AppError::Internal(format!(
                        "ensemble step {} payload reparse failed: {}",
                        step.name, e
                    ))
                })?,
            ),
        };
        let child_request_id = format!("{}:{}", request_id, step.name);
        let child_opts = EnsembleExecOpts {
            client_ip: client_ip.to_string(),
            deadline_unix_ns,
            decoupled: false, // the child is unary-only (D4)
            dag_selector: None, // E8-1: nested ensembles are unary-only, no selector passthrough
        };
        let nested = execute_nested_ensemble_boxed(
            state.clone(),
            step.model.clone(),
            resolved_version.clone(),
            child_payload,
            child_request_id,
            child_opts,
            Arc::clone(snapshot),
            depth + 1,
            ancestors.to_vec(),
        );
        // E5 (batch 3): the step wall-clock cap bounds the nested run too —
        // the child execution IS this step's execution, same local timeout
        // wrap as the unary response wait below. E1's "recursion shares the
        // PARENT deadline" is preserved: child_opts still carries the parent
        // deadline; the cap is enforced here at the step boundary.
        let outcome = match crate::deadline::remaining(step_deadline) {
            Some(rem) => match tokio::time::timeout(rem, nested).await {
                Ok(r) => r?,
                Err(_) => {
                    return Err(AppError::InferenceTimeout(format!(
                        "ensemble step {} timed out", step.name
                    )))
                }
            },
            None => nested.await?,
        };
        return match outcome {
            EnsembleOutcome::Unary(v) => materialize_step_outputs(step, v),
            // Unreachable under D4 (child plans with streaming are rejected
            // above) — kept as the type-level backstop.
            EnsembleOutcome::Stream(_) => Err(AppError::Internal(
                "nested ensemble returned a stream (D4 violation)".to_string(),
            )),
        };
    }

    let num_workers = mv.workers.len();
    if num_workers == 0 {
        return Err(AppError::WorkerCrashed(format!("{} has no workers", step.model)));
    }

    // P-TRACE (蓝图 §4.3 ensemble 接线，防 trace 断裂): the sub-step RequestMeta
    // would otherwise carry empty headers, orphaning every step span from the
    // parent request trace. Build a child `ensemble.step` span linked to the
    // current (parent) trace and inject its context into the step headers so the
    // worker spans land as children of the step (request_id is already
    // `{parent}:{step}`, trace follows the same shape).
    let mut step_headers = HashMap::new();
    // B3 (E7): when the step input is binary, tell the worker via content-type.
    if let Some(ref ct) = content_type_for_step {
        step_headers.insert("content-type".to_string(), ct.clone());
    }
    {
        let step_span = tracing::info_span!(
            "ensemble.step",
            step = %step.name,
            model = %step.model,
            // m6 (E1): nesting depth so an abnormal fan-out is discoverable.
            depth = depth,
            trace_id = tracing::field::Empty,
            span_id = tracing::field::Empty,
        );
        crate::telemetry::link_parent(&step_span, &opentelemetry::Context::current());
        let _guard = step_span.enter();
        crate::telemetry::inject(&mut step_headers);
    }

    let meta = build_step_meta(
        &format!("{}:{}", request_id, step.name), client_ip, step_deadline, step_headers,
        payload_bytes.clone(),
    );

    // E6 (batch 4): worker-inference retry loop — submit → bounded wait →
    // classify. Retryable errors (5xx/timeout, is_retryable_error) re-enter
    // with exponential backoff and a fresh uid (queue identity) while the
    // payload (Bytes refcount) and meta (Arc clone) are reused. 4xx, queue
    // pressure and crashes fall through immediately.
    let mut attempt: u32 = 0;
    let mut backoff = Duration::from_millis(50);
    loop {
        // Send inference request through the unified queue
        let uid = format!("ensemble_{}_{}_{}", step.model, resolved_version, Uuid::new_v4());

        let (response_tx, response_rx) = oneshot::channel();
        let item = crate::inference_queue::QueueItem {
            uid,
            data: payload_bytes.clone(),
            meta: Some(std::sync::Arc::new(meta.clone())),
            response_tx,
            inflight_guard: None,
            enqueued_at: std::time::Instant::now(),
        };

        match state.inference_queue.try_submit(&step.model, &resolved_version, item) {
            Ok(()) => {}
            Err(crate::inference_queue::QueueError::Full) => {
                // E6: queue pressure is NOT retryable (a retry adds pressure).
                return Err(AppError::QueueFull(format!(
                    "Queue full for {} {}", step.model, resolved_version
                )));
            }
            Err(_) => {
                return Err(AppError::ModelNotReady(format!(
                    "Queue not available for {} {}", step.model, resolved_version
                )));
            }
        }

        let result = async {
            // P-DEADLINE cascade (E5): bound this attempt by the STEP
            // deadline's remaining budget — min(parent, now + timeout_secs).
            // None = no deadline → unbounded inner wait, outer DAG bound
            // still applies via execute_ensemble's total_budget.
            let response = match crate::deadline::remaining(step_deadline) {
                Some(timeout_duration) => match timeout(timeout_duration, response_rx).await {
                    Ok(Ok(resp)) => resp,
                    Ok(Err(_)) => {
                        return Err(AppError::InferenceTimeout(format!(
                            "ensemble step {} response channel closed", step.name
                        )));
                    }
                    Err(_) => {
                        return Err(AppError::InferenceTimeout(format!(
                            "ensemble step {} timed out", step.name
                        )));
                    }
                },
                None => match response_rx.await {
                    Ok(resp) => resp,
                    Err(_) => {
                        return Err(AppError::InferenceTimeout(format!(
                            "ensemble step {} response channel closed", step.name
                        )));
                    }
                },
            };
            unary_response_to_value(&step.name, response)
        }.await;

        match result {
            Ok(value) => return materialize_step_outputs(step, value),
            Err(e) => {
                if attempt < step.retries && is_retryable_error(&e) {
                    // E6: don't sleep past the step budget — the next attempt
                    // would fail immediately anyway (fast-fail, §4.4 deadline
                    // row semantics).
                    if let Some(rem) = crate::deadline::remaining(step_deadline) {
                        if rem < backoff {
                            return Err(e);
                        }
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_millis(500));
                    attempt += 1;
                    continue;
                }
                return Err(e);
            }
        }
    }
}

/// E6 (batch 4): map a unary queue response to the step value (extracted
/// from execute_step so the retry loop can classify the error before
/// deciding). B3: typed output; §4.4 status mapping — a numeric Status.message
/// → ModelError (4xx passes through, 5xx maps 500), a non-numeric message
/// means the worker itself is broken → WorkerCrashed.
fn unary_response_to_value(step_name: &str, response: pb::Response) -> Result<EnsembleValue, AppError> {
    match response.payload {
        Some(pb::response::Payload::Single(single)) => {
            let code = single.status.as_ref().map(|s| s.code.as_str()).unwrap_or("Ok");
            match code {
                "Ok" => {
                    // B3 (E8): typed output (see parse_step_output).
                    parse_step_output(step_name, single)
                }
                "Error" => {
                    let msg = single.status.as_ref().and_then(|s| {
                        if s.message.is_empty() { None } else { Some(s.message.clone()) }
                    }).unwrap_or_else(|| "unknown worker error".to_string());
                    // §4.4: the worker carries the numeric HTTP status in
                    // Status.message (common._make_error_response). Parity
                    // with the unary queue path (inference.rs:425-465): a
                    // numeric status → ModelError (4xx passes through, 5xx
                    // maps 500 — B3, never sanitized to 503); a non-numeric
                    // message means the worker itself is broken →
                    // WorkerCrashed.
                    match msg.parse::<u16>() {
                        Ok(http_status) => {
                            let data: Value = if single.data.is_empty() {
                                json!({})
                            } else {
                                serde_json::from_slice(&single.data).unwrap_or(json!({}))
                            };
                            let err_obj = data.get("error");
                            Err(AppError::ModelError(Box::new(ModelErrorData {
                                status_code: http_status,
                                error_type: err_obj.and_then(|e| e.get("type"))
                                    .and_then(|t| t.as_str())
                                    .unwrap_or("model_error")
                                    .to_string(),
                                detail: err_obj.and_then(|e| e.get("message"))
                                    .and_then(|m| m.as_str())
                                    .unwrap_or("model error")
                                    .to_string(),
                                code: err_obj.and_then(|e| e.get("code"))
                                    .and_then(|c| c.as_str())
                                    .map(|s| s.to_string()),
                                param: err_obj.and_then(|e| e.get("param"))
                                    .and_then(|p| p.as_str())
                                    .map(|s| s.to_string()),
                                headers: if single.headers.is_empty() {
                                    None
                                } else {
                                    Some(single.headers.clone())
                                },
                            })))
                        }
                        Err(_) => Err(AppError::WorkerCrashed(msg)),
                    }
                }
                _ => Err(AppError::WorkerCrashed(
                    single.status.as_ref().and_then(|s| {
                        if s.message.is_empty() { None } else { Some(s.message.clone()) }
                    }).unwrap_or_else(|| "ensemble step inference error".to_string())
                )),
            }
        }
        _ => Err(AppError::WorkerCrashed("unexpected response type".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_dag_ok() {
        let steps = vec![
            EnsembleStep {
                name: "step1".to_string(),
                model: "m1".to_string(),
                version: Some("1".to_string()),

                params: HashMap::new(),

                timeout_secs: None,
                stream: false,
                on_error: OnErrorKind::Fail,
                retries: 0,
                outputs_decl: None,
                when: None,
                inputs: [("input".to_string(), "$request".to_string())].into(),
            },
            EnsembleStep {
                name: "step2".to_string(),
                model: "m2".to_string(),
                version: Some("1".to_string()),

                params: HashMap::new(),

                timeout_secs: None,
                stream: false,
                on_error: OnErrorKind::Fail,
                retries: 0,
                outputs_decl: None,
                when: None,
                inputs: [("data".to_string(), "$step1".to_string())].into(),
            },
        ];
        assert!(validate_dag(&steps).is_ok());
    }

    #[test]
    fn test_validate_dag_cycle() {
        let steps = vec![
            EnsembleStep {
                name: "step1".to_string(),
                model: "m1".to_string(),
                version: Some("1".to_string()),

                params: HashMap::new(),

                timeout_secs: None,
                stream: false,
                on_error: OnErrorKind::Fail,
                retries: 0,
                outputs_decl: None,
                when: None,
                inputs: [("input".to_string(), "$step2".to_string())].into(),
            },
            EnsembleStep {
                name: "step2".to_string(),
                model: "m2".to_string(),
                version: Some("1".to_string()),

                params: HashMap::new(),

                timeout_secs: None,
                stream: false,
                on_error: OnErrorKind::Fail,
                retries: 0,
                outputs_decl: None,
                when: None,
                inputs: [("input".to_string(), "$step1".to_string())].into(),
            },
        ];
        assert!(validate_dag(&steps).is_err());
    }

    #[test]
    fn test_validate_dag_unknown_ref() {
        let steps = vec![
            EnsembleStep {
                name: "step1".to_string(),
                model: "m1".to_string(),
                version: Some("1".to_string()),

                params: HashMap::new(),

                timeout_secs: None,
                stream: false,
                on_error: OnErrorKind::Fail,
                retries: 0,
                outputs_decl: None,
                when: None,
                inputs: [("input".to_string(), "$unknown".to_string())].into(),
            },
        ];
        assert!(validate_dag(&steps).is_err());
    }

    #[test]
    fn test_topological_layers() {
        let steps = vec![
            EnsembleStep {
                name: "a".to_string(),
                model: "m1".to_string(),
                version: Some("1".to_string()),

                params: HashMap::new(),

                timeout_secs: None,
                stream: false,
                on_error: OnErrorKind::Fail,
                retries: 0,
                outputs_decl: None,
                when: None,
                inputs: [("x".to_string(), "$request".to_string())].into(),
            },
            EnsembleStep {
                name: "b".to_string(),
                model: "m2".to_string(),
                version: Some("1".to_string()),

                params: HashMap::new(),

                timeout_secs: None,
                stream: false,
                on_error: OnErrorKind::Fail,
                retries: 0,
                outputs_decl: None,
                when: None,
                inputs: [("x".to_string(), "$request".to_string())].into(),
            },
            EnsembleStep {
                name: "c".to_string(),
                model: "m3".to_string(),
                version: Some("1".to_string()),

                params: HashMap::new(),

                timeout_secs: None,
                stream: false,
                on_error: OnErrorKind::Fail,
                retries: 0,
                outputs_decl: None,
                when: None,
                inputs: [("x".to_string(), "$a".to_string())].into(),
            },
        ];
        let layers = topological_layers(&steps);
        assert_eq!(layers.len(), 2);
        assert_eq!(layers[0].len(), 2); // a and b (no deps)
        assert_eq!(layers[1].len(), 1); // c (depends on a)
    }

    /// Legacy (undeclared) plan for resolve_ref tests.
    fn legacy_plan() -> EnsemblePlan {
        parse_ensemble_plan(
            "ensemble:\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs: {x: \"$request\"}\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap()
    }

    #[test]
    fn test_resolve_ref() {
        let plan = legacy_plan();
        let mut context = HashMap::new();
        context.insert("request".to_string(), EnsembleValue::Json(json!({"image": "cat.jpg"})));
        context.insert("step1".to_string(), EnsembleValue::Json(json!({"output": 42})));

        assert_eq!(
            match resolve_ref(&plan, "$request", &context).unwrap() {
                ResolvedRef::Value(EnsembleValue::Json(v)) => v,
                _ => panic!("expected Json"),
            },
            json!({"image": "cat.jpg"})
        );
        assert_eq!(
            match resolve_ref(&plan, "$request.image", &context).unwrap() {
                ResolvedRef::Value(EnsembleValue::Json(v)) => v,
                _ => panic!("expected Json"),
            },
            json!("cat.jpg")
        );
        assert_eq!(
            match resolve_ref(&plan, "$step1.output", &context).unwrap() {
                ResolvedRef::Value(EnsembleValue::Json(v)) => v,
                _ => panic!("expected Json"),
            },
            json!(42)
        );
    }

    // === B3: resolve_ref Binary rules (E7) ===

    #[test]
    fn b3_resolve_ref_request_whole_binary_passthrough() {
        let plan = legacy_plan();
        let mut context = HashMap::new();
        context.insert(
            "request".to_string(),
            EnsembleValue::Binary(Bytes::from_static(b"hello"), "text/plain".to_string(), None, None),
        );
        match resolve_ref(&plan, "$request", &context).unwrap() {
            ResolvedRef::Value(EnsembleValue::Binary(data, ct, ..)) => {
                assert_eq!(data.as_ref(), b"hello");
                assert_eq!(ct, "text/plain");
            }
            _ => panic!("expected Binary passthrough"),
        }
    }

    #[test]
    fn b3_resolve_ref_request_field_on_binary_is_400() {
        let plan = legacy_plan();
        let mut context = HashMap::new();
        context.insert(
            "request".to_string(),
            EnsembleValue::Binary(Bytes::from_static(b"hello"), "text/plain".to_string(), None, None),
        );
        let err = resolve_ref(&plan, "$request.field", &context).unwrap_err();
        assert!(
            matches!(err, AppError::InvalidRequestBody(_)),
            "field access on binary must be 400, got {err:?}"
        );
        assert!(
            err.to_string().contains("field"),
            "error must mention field extraction, got: {err}"
        );
    }

    #[test]
    fn b3_resolve_ref_step_binary_is_400() {
        let plan = legacy_plan();
        let mut context = HashMap::new();
        context.insert(
            "step1".to_string(),
            EnsembleValue::Binary(Bytes::from_static(b"hello"), "text/plain".to_string(), None, None),
        );
        // Whole step reference on binary → 400 (Option A boundary).
        let err = resolve_ref(&plan, "$step1", &context).unwrap_err();
        assert!(
            matches!(err, AppError::InvalidRequestBody(_)),
            "step binary reference must be 400, got {err:?}"
        );
        // Field access on step binary → same 400.
        let err = resolve_ref(&plan, "$step1.field", &context).unwrap_err();
        assert!(
            matches!(err, AppError::InvalidRequestBody(_)),
            "step binary field access must be 400, got {err:?}"
        );
    }

    // === P1 (batch 6): per-step dependency-key cloning ===

    #[test]
    fn p1_step_dep_keys_legacy_and_mimo() {
        let plan = parse_ensemble_plan(
            "ensemble:\n  inputs:\n    text:\n      type: json\n    image:\n      type: binary\n  steps:\n    - name: tok\n      model: pre\n      version: \"1\"\n      inputs:\n        text: \"$inputs.text\"\n    - name: enc\n      model: vis_enc\n      version: \"1\"\n      outputs:\n        thumb:\n          type: binary\n          path: \"$.thumb\"\n        emb:\n          type: json\n          path: \"$.emb\"\n      inputs:\n        img: \"$inputs.image\"\n    - name: out\n      model: echo\n      version: \"1\"\n      inputs:\n        data: \"$tok\"\n        emb: \"$enc.emb\"\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
        assert_eq!(plan.step_dep_keys[0], vec!["inputs.text".to_string()]);
        assert_eq!(plan.step_dep_keys[1], vec!["inputs.image".to_string()]);
        // Step inputs are a HashMap — ref order is arbitrary; the key SET
        // is the contract.
        let mut keys = plan.step_dep_keys[2].clone();
        keys.sort();
        assert_eq!(keys, vec!["enc.emb".to_string(), "tok".to_string()]);
    }

    #[test]
    fn p1_step_dep_keys_legacy_root_dedups() {
        let plan = parse_ensemble_plan(
            "ensemble:\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs:\n        x: \"$request\"\n        y: \"$request.input\"\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
        // Both refs resolve against the same root key — one clone, not two.
        assert_eq!(plan.step_dep_keys[0], vec!["request".to_string()]);
    }

    #[test]
    fn p1_step_dep_keys_dag_sets_computed_per_set() {
        let plan = parse_ensemble_plan(
            "ensemble:\n  dags:\n    default:\n      steps:\n        - name: main\n          model: pre\n          version: \"1\"\n          inputs:\n            text: \"$request.text\"\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
        // The outer container runs nothing — no dep keys of its own.
        assert!(plan.step_dep_keys.is_empty());
        let sets = plan.dag_sets.as_ref().unwrap();
        assert_eq!(
            sets["default"].step_dep_keys[0],
            vec!["request".to_string()]
        );
    }

    #[test]
    fn p1_select_ctx_keys_clones_only_referenced_keys() {
        let mut context = HashMap::new();
        context.insert("a".to_string(), EnsembleValue::Json(json!(1)));
        context.insert("b".to_string(), EnsembleValue::Json(json!(2)));
        context.insert("c".to_string(), EnsembleValue::Json(json!(3)));
        let subset = select_ctx_keys(&context, &["a".to_string(), "missing".to_string()]);
        assert_eq!(subset.len(), 1, "only referenced keys are cloned");
        assert!(subset.contains_key("a"));
        assert!(!subset.contains_key("b"));
        assert!(!subset.contains_key("c"));
    }

    // === P3 + P11 (batch 6): zero-spawn layer executor ===

    struct DropFlag(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn p11_first_error_drops_remaining_layer_futures() {
        // A failing step must propagate immediately and the layer's
        // remaining in-flight futures are dropped with it — the historical
        // JoinSet first-err + drop-abort semantics (a failed step's
        // siblings never keep burning worker capacity, P-FLOW §4.0.9).
        use futures::future::BoxFuture;
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let d = dropped.clone();
        let futs: FuturesUnordered<
            BoxFuture<'static, StepFutOutput>,
        > = FuturesUnordered::new();
        futs.push(Box::pin(async move {
            // The slow sibling completes only if it is never dropped.
            let _guard = DropFlag(d);
            tokio::time::sleep(Duration::from_secs(3600)).await;
            ("slow".to_string(), Ok(Vec::new()))
        }));
        futs.push(Box::pin(async {
            (
                "fast".to_string(),
                Err(AppError::Internal("boom".to_string())),
            )
        }));
        let mut context = HashMap::new();
        let skip_set = HashSet::new();
        let err = drive_step_futs(futs, &mut context, &skip_set)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("boom"),
            "the first error must propagate, got {err:?}"
        );
        assert!(
            dropped.load(std::sync::atomic::Ordering::SeqCst),
            "remaining layer futures must be dropped on first error"
        );
        assert!(context.is_empty());
    }

    #[tokio::test]
    async fn p11_layer_success_inserts_outputs_into_context() {
        use futures::future::BoxFuture;
        let futs: FuturesUnordered<
            BoxFuture<'static, StepFutOutput>,
        > = FuturesUnordered::new();
        futs.push(Box::pin(async {
            (
                "a".to_string(),
                Ok(vec![("a".to_string(), EnsembleValue::Json(json!(1)))]),
            )
        }));
        let mut context = HashMap::new();
        let skip_set = HashSet::new();
        drive_step_futs(futs, &mut context, &skip_set).await.unwrap();
        assert!(
            matches!(context.get("a"), Some(EnsembleValue::Json(v)) if *v == json!(1)),
            "completed step outputs land in the context"
        );
    }

    #[tokio::test]
    async fn p11_skip_step_failure_continues_the_layer() {
        use futures::future::BoxFuture;
        let futs: FuturesUnordered<
            BoxFuture<'static, StepFutOutput>,
        > = FuturesUnordered::new();
        futs.push(Box::pin(async {
            (
                "may".to_string(),
                Err(AppError::Internal("boom".to_string())),
            )
        }));
        let mut context = HashMap::new();
        let skip_set: HashSet<&str> = ["may"].into_iter().collect();
        drive_step_futs(futs, &mut context, &skip_set)
            .await
            .expect("a skip step's failure must not fail the layer");
        assert!(
            !context.contains_key("may"),
            "a skipped step stays absent from the context"
        );
    }

    // ===== P-FLOW (§4.0.9): ensemble shared cancel =====

    #[tokio::test]
    async fn p_flow_ensemble_joinset_aborts_inflight_on_drop() {
        // execute_ensemble uses a per-layer JoinSet: dropping it (parent
        // disconnect, total-budget timeout, or a sibling step error) must
        // ABORT in-flight sub-step tasks so workers are not left computing
        // for a cancelled ensemble. This guards the invariant the executor
        // relies on.
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        let ran = Arc::new(AtomicBool::new(false));
        let ran_clone = ran.clone();
        let mut set: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
        set.spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            ran_clone.store(true, Ordering::SeqCst);
        });
        // Simulate the ensemble future being dropped mid-layer.
        drop(set);
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(
            !ran.load(Ordering::SeqCst),
            "dropped JoinSet must abort its in-flight task (ensemble cancel)"
        );
    }

    // === B3: input assembly three branches (E7) ===

    fn bin(data: &'static [u8], ct: &str) -> EnsembleValue {
        EnsembleValue::Binary(Bytes::from_static(data), ct.to_string(), None, None)
    }

    #[test]
    fn b3_assemble_all_json_builds_object() {
        let mut resolved = HashMap::new();
        resolved.insert("a".to_string(), EnsembleValue::Json(json!(1)));
        resolved.insert("b".to_string(), EnsembleValue::Json(json!("x")));
        let (bytes, ct) = assemble_step_payload("s", &resolved, &HashMap::new(), None).unwrap();
        assert!(ct.is_none(), "all-Json assembly must not set a content-type");
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v, json!({"a": 1, "b": "x"}));
    }

    #[test]
    fn b3_assemble_single_binary_passthrough_with_ct() {
        let mut resolved = HashMap::new();
        resolved.insert("img".to_string(), bin(b"\x00\x01\x02", "image/png"));
        let (bytes, ct) = assemble_step_payload("s", &resolved, &HashMap::new(), None).unwrap();
        assert_eq!(bytes.as_ref(), b"\x00\x01\x02", "binary payload must pass verbatim");
        assert_eq!(ct.as_deref(), Some("image/png"), "CT must be forwarded");
    }

    #[test]
    fn b3_assemble_mixed_binary_json_is_400() {
        let mut resolved = HashMap::new();
        resolved.insert("a".to_string(), bin(b"x", "application/octet-stream"));
        resolved.insert("b".to_string(), EnsembleValue::Json(json!(1)));
        let err = assemble_step_payload("s", &resolved, &HashMap::new(), None).unwrap_err();
        assert!(
            matches!(err, AppError::InvalidRequestBody(_)),
            "mixed JSON/Binary inputs must be 400, got {err:?}"
        );
    }

    #[test]
    fn b3_assemble_two_binary_inputs_is_400() {
        // Even two whole-Binary inputs violate the "sole whole input" rule.
        let mut resolved = HashMap::new();
        resolved.insert("a".to_string(), bin(b"x", "application/octet-stream"));
        resolved.insert("b".to_string(), bin(b"y", "application/octet-stream"));
        let err = assemble_step_payload("s", &resolved, &HashMap::new(), None).unwrap_err();
        assert!(
            matches!(err, AppError::InvalidRequestBody(_)),
            "two binary inputs must be 400, got {err:?}"
        );
    }

    // === B3: step output typed parse (E8) ===

    fn single(data: &'static [u8], media_type: &str) -> pb::SingleResponse {
        pb::SingleResponse {
            data: Bytes::from_static(data),
            media_type: media_type.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn b3_output_parse_json_media_type() {
        let out = parse_step_output("s", single(br#"{"a":1}"#, "application/json")).unwrap();
        match out {
            EnsembleValue::Json(v) => assert_eq!(v, json!({"a": 1})),
            _ => panic!("expected Json"),
        }
    }

    #[test]
    fn b3_output_parse_empty_media_type_defaults_json() {
        let out = parse_step_output("s", single(br#"{"a":1}"#, "")).unwrap();
        match out {
            EnsembleValue::Json(v) => assert_eq!(v, json!({"a": 1})),
            _ => panic!("expected Json"),
        }
    }

    #[test]
    fn b3_output_parse_invalid_json_not_swallowed() {
        // Regression pin for the old `:483` behaviour: invalid JSON from a
        // worker must surface as an error naming the step, never collapse
        // into a silent `{}` that the DAG keeps running on.
        let err = parse_step_output("mystep", single(b"{oops", "")).unwrap_err();
        assert!(
            matches!(err, AppError::Internal(_)),
            "invalid JSON must be an Internal error, got {err:?}"
        );
        assert!(
            err.to_string().contains("mystep"),
            "error must name the failing step, got: {err}"
        );
    }

    #[test]
    fn b3_output_parse_binary_media_type() {
        let out = parse_step_output("s", single(b"\x00\xff", "application/octet-stream")).unwrap();
        match out {
            EnsembleValue::Binary(d, ct, ..) => {
                assert_eq!(d.as_ref(), b"\x00\xff");
                assert_eq!(ct, "application/octet-stream");
            }
            _ => panic!("expected Binary"),
        }
    }

    #[test]
    fn b3_output_parse_empty_data_is_empty_object() {
        let out = parse_step_output("s", single(b"", "")).unwrap();
        match out {
            EnsembleValue::Json(v) => assert_eq!(v, json!({})),
            _ => panic!("expected Json empty object"),
        }
    }

    // === §4.0/D16: pipeline-form validation (batch 2) ===

    fn pstep(name: &str, inputs: &[(&str, &str)], stream: bool) -> EnsembleStep {
        sstep(name, inputs, stream)
    }

    #[test]
    fn pipeline_chain_two_streaming_steps_valid() {
        // s0 (streaming) → s1 (streaming, consumes s0 whole) = a valid chain;
        // the chain tail is the config last step (output semantics, §4.1-4).
        let steps = vec![
            pstep("s0", &[("input", "$request")], true),
            pstep("s1", &[("data", "$s0")], true),
        ];
        let chains = build_chains(&steps, 1).expect("valid chain must build");
        assert_eq!(chains.len(), 1, "one chain expected");
        assert_eq!(chains[0].nodes, vec![0, 1], "chain order: s0 → s1");
    }

    #[test]
    fn pipeline_r1_two_consumers_rejected() {
        // P-R1: a streaming step's output must have EXACTLY one consumer.
        let steps = vec![
            pstep("s0", &[("input", "$request")], true),
            pstep("s1", &[("data", "$s0")], true),
            pstep("s2", &[("data", "$s0")], true),
        ];
        let err = build_chains(&steps, 2).unwrap_err();
        assert!(
            err.to_string().contains("consumer"),
            "P-R1 must name the consumer rule, got: {err}"
        );
    }

    #[test]
    fn pipeline_r2_whole_ref_only_rejected() {
        // P-R2: streaming outputs can only be referenced whole ($s0), never
        // field-projected ($s0.field).
        let steps = vec![
            pstep("s0", &[("input", "$request")], true),
            pstep("s1", &[("data", "$s0.token")], true),
        ];
        let err = build_chains(&steps, 1).unwrap_err();
        assert!(
            err.to_string().contains("whole"),
            "P-R2 must reject field references, got: {err}"
        );
    }

    #[test]
    fn pipeline_d26_unary_consumer_rejected() {
        // D26: a chain's unary consumer has no clean chunk→unary→chunk
        // semantics — the form is rejected at parse time ("pipeline chain
        // tail must be a streaming step" covers the unary-tail shape).
        let steps = vec![
            pstep("s0", &[("input", "$request")], true),
            pstep("u1", &[("data", "$s0")], false),
        ];
        let err = build_chains(&steps, 1).unwrap_err();
        assert!(
            err.to_string().contains("streaming") || err.to_string().contains("tail"),
            "D26 must reject the unary-consumer chain form, got: {err}"
        );
    }

    #[test]
    fn pipeline_r5_mixed_forms_rejected() {
        // P-R5: the DAG's streaming set is exactly ONE chain OR one tail
        // streaming step — a chain plus an orphan streaming step is an error.
        let steps = vec![
            pstep("s0", &[("input", "$request")], true),
            pstep("s1", &[("data", "$s0")], true),
            pstep("s2", &[("input", "$request")], true),
        ];
        // Chain s0→s1 (tail = s1 = config last); s2 is an orphan streaming
        // step that is NOT the output step.
        let err = build_chains(&steps, 1).unwrap_err();
        assert!(
            err.to_string().contains("streaming step"),
            "P-R5 must reject chain + orphan streaming step, got: {err}"
        );
    }

    #[test]
    fn pipeline_chain_tail_must_be_output_step() {
        // P-R3: the chain tail must be the DAG output step. Chain s0→s1 but
        // the config last step is s2 (unary) → the chain tail is not the
        // output → rejected (§4.1-4 output semantics).
        let steps = vec![
            pstep("s0", &[("input", "$request")], true),
            pstep("s1", &[("data", "$s0")], true),
            pstep("s2", &[("data", "$request")], false),
        ];
        let err = build_chains(&steps, 2).unwrap_err();
        assert!(
            err.to_string().contains("output step"),
            "chain tail must be the output step, got: {err}"
        );
    }

    #[test]
    fn pipeline_orphan_streaming_step_not_output_rejected() {
        // A zero-consumer streaming step that is NOT the output step is an
        // orphan (§4.1-4: the config last step must be the streaming step).
        let steps = vec![
            pstep("s0", &[("input", "$request")], true),
            pstep("u1", &[("data", "$request")], false),
        ];
        let err = build_chains(&steps, 1).unwrap_err();
        assert!(
            err.to_string().contains("streaming"),
            "orphan streaming step must be rejected, got: {err}"
        );
    }

    // === §4.0/D16: streaming validation (form dispatch, batch 0) ===

    fn sstep(name: &str, inputs: &[(&str, &str)], stream: bool) -> EnsembleStep {
        EnsembleStep {
            name: name.to_string(),
            model: format!("m_{}", name),
            version: Some("1".to_string()),

            params: HashMap::new(),

            timeout_secs: None,
            inputs: inputs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            stream,
            on_error: OnErrorKind::Fail,
            retries: 0,
            outputs_decl: None,
            when: None,
        }
    }

    #[test]
    fn stream_rules_tail_stream_dag_valid() {
        // s1 (unary) → s2 (streaming tail): the only open form in batch 0.
        let steps = vec![
            sstep("s1", &[("input", "$request")], false),
            sstep("s2", &[("data", "$s1")], true),
        ];
        assert!(validate_stream_rules(&steps, 1).is_ok(), "tail-streaming DAG must validate");
    }

    #[test]
    fn stream_rules_two_stream_steps_rejected() {
        // Rule 3: at most one streaming step per DAG.
        let steps = vec![
            sstep("s1", &[("input", "$request")], true),
            sstep("s2", &[("data", "$request")], true),
        ];
        let err = validate_stream_rules(&steps, 1).unwrap_err();
        assert!(
            err.to_string().contains("one streaming step"),
            "must reject two streaming steps, got: {err}"
        );
    }

    #[test]
    fn stream_rules_tail_stream_not_config_last_rejected() {
        // Rule 4 (B-m4): with `output` omitted the DAG output is steps.last(),
        // which must be the streaming step — a streaming step that is not the
        // config last step would silently produce nothing streamable.
        let steps = vec![
            sstep("s1", &[("input", "$request")], true),
            sstep("s2", &[("data", "$request")], false),
        ];
        let err = validate_stream_rules(&steps, 1).unwrap_err();
        assert!(
            err.to_string().contains("output") && err.to_string().contains("s1"),
            "must reject streaming step not at config tail, got: {err}"
        );
    }

    #[test]
    fn stream_rules_plain_dag_unchanged() {
        // No stream: false — behaviour parity with the historical validator.
        let steps = vec![
            sstep("s1", &[("input", "$request")], false),
            sstep("s2", &[("data", "$s1")], false),
        ];
        assert!(validate_stream_rules(&steps, 1).is_ok());
    }

    // === P10: streaming-DAG capacity (D40) ===

    #[test]
    fn p10_capacity_rejects_when_exhausted_and_releases_on_drop() {
        let cap = StreamingCapacityState::new(2);
        let p1 = cap.try_acquire().unwrap();
        let p2 = cap.try_acquire().unwrap();
        let err = cap.try_acquire().err().unwrap();
        assert!(
            matches!(err, AppError::StreamingCapacityExceeded(_)),
            "exhausted capacity must reject immediately (429), got {err:?}"
        );
        // Dropping a permit returns its slot — no queueing, no leak.
        drop(p1);
        let _p3 = cap.try_acquire().expect("slot must be released on permit drop");
        // Clones share the same permit: slot stays held until ALL clones drop.
        let p2_clone = p2.clone();
        drop(p2);
        let err = cap.try_acquire().err().unwrap();
        assert!(
            matches!(err, AppError::StreamingCapacityExceeded(_)),
            "cloned permit must hold the slot until the last reference drops"
        );
        drop(p2_clone);
        let _p4 = cap.try_acquire().expect("last clone drop must release the slot");
    }

    #[test]
    fn p10_capacity_zero_permits_is_immediately_exhausted() {
        // A 0-limit installation rejects everything (misconfiguration guard);
        // production never installs one for 0 (server/mod.rs skips it).
        let cap = StreamingCapacityState::new(0);
        let err = cap.try_acquire().err().unwrap();
        assert!(matches!(err, AppError::StreamingCapacityExceeded(_)));
    }

    // === P0: EnsemblePlan cache (D6 + review ①-④) ===

    fn test_plan(path: &str) -> Arc<EnsemblePlan> {
        Arc::new(EnsemblePlan {
            steps: Vec::new(),
            layers: Vec::new(),
            output_step: 0,
            output_field: None,
            chains: Vec::new(),
            inputs_decl: None,
            input_modes: Vec::new(),
            conditional_refs: Vec::new(),
            step_dep_keys: Vec::new(),
            outputs: None,
            dag_sets: None,
            config_path: PathBuf::from(path),
            source_mtime: None,
        })
    }

    #[tokio::test]
    async fn p0_cache_hit_returns_same_arc() {
        let cache = EnsemblePlanCache::new();
        let key = PlanKey { model: "m".to_string(), version: "1".to_string() };
        let plan = test_plan("/nonexistent");
        let first = cache.get_or_load(key.clone(), || {
            let plan = plan.clone();
            async move { Ok::<_, AppError>(plan) }
        }).await.unwrap();
        assert!(Arc::ptr_eq(&plan, &first), "first load returns the parsed plan");
        let hit = cache.get_or_load(key, || async { panic!("cache hit must not parse again") })
            .await.unwrap();
        assert!(Arc::ptr_eq(&plan, &hit), "hit must return the cached Arc");
    }

    #[tokio::test]
    async fn p0_cache_single_flight_only_one_parse() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let cache = Arc::new(EnsemblePlanCache::new());
        let key = PlanKey { model: "m".to_string(), version: "1".to_string() };
        let parse_count = Arc::new(AtomicUsize::new(0));
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..10 {
            let cache = cache.clone();
            let key = key.clone();
            let parse_count = parse_count.clone();
            set.spawn(async move {
                cache.get_or_load(key, || {
                    let parse_count = parse_count.clone();
                    async move {
                        parse_count.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok::<_, AppError>(test_plan("/nonexistent"))
                    }
                }).await.unwrap()
            });
        }
        let mut plans = Vec::new();
        while let Some(r) = set.join_next().await {
            plans.push(r.unwrap());
        }
        assert_eq!(
            parse_count.load(Ordering::SeqCst),
            1,
            "concurrent first requests must parse exactly once (single-flight, review ④)"
        );
        for p in &plans {
            assert!(Arc::ptr_eq(&plans[0], p), "all waiters must receive the holder's plan");
        }
    }

    #[tokio::test]
    async fn p0_cache_failed_load_not_cached() {
        let cache = EnsemblePlanCache::new();
        let key = PlanKey { model: "m".to_string(), version: "1".to_string() };
        let err = cache.get_or_load(key.clone(), || async {
            Err::<Arc<EnsemblePlan>, _>(AppError::Config("boom".to_string()))
        }).await.unwrap_err();
        assert!(err.to_string().contains("boom"));
        // A failed load must not be cached: the next call re-parses (a fixed
        // config heals without reload — behaviour parity with no cache).
        let ok = cache.get_or_load(key, || async {
            Ok::<_, AppError>(test_plan("/nonexistent"))
        }).await;
        assert!(ok.is_ok(), "failed load must not be cached; next call re-parses");
    }

    #[tokio::test]
    async fn p0_cache_invalidate_model_clears_all_versions() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let cache = EnsemblePlanCache::new();
        let count = Arc::new(AtomicUsize::new(0));
        let k_latest = PlanKey { model: "m".to_string(), version: "latest".to_string() };
        let k_pinned = PlanKey { model: "m".to_string(), version: "1".to_string() };
        let k_other = PlanKey { model: "other".to_string(), version: "1".to_string() };
        for key in [&k_latest, &k_pinned, &k_other] {
            cache.get_or_load(key.clone(), || {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, AppError>(test_plan("/nonexistent"))
                }
            }).await.unwrap();
        }
        cache.invalidate_model("m");
        // Review ③: both "latest" and the pinned version are separate keys
        // and must ALL clear on a model-prefix invalidation.
        for key in [&k_latest, &k_pinned] {
            cache.get_or_load(key.clone(), || {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, AppError>(test_plan("/nonexistent"))
                }
            }).await.unwrap();
        }
        assert_eq!(count.load(Ordering::SeqCst), 5, "invalidate_model must clear every version of the model");
    }

    #[tokio::test]
    async fn p0_cache_invalidate_version_clears_one() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let cache = EnsemblePlanCache::new();
        let count = Arc::new(AtomicUsize::new(0));
        let k1 = PlanKey { model: "m".to_string(), version: "1".to_string() };
        let k2 = PlanKey { model: "m".to_string(), version: "2".to_string() };
        for key in [&k1, &k2] {
            cache.get_or_load(key.clone(), || {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, AppError>(test_plan("/nonexistent"))
                }
            }).await.unwrap();
        }
        cache.invalidate_version("m", "1");
        cache.get_or_load(k1, || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok::<_, AppError>(test_plan("/nonexistent"))
            }
        }).await.unwrap();
        // v2 untouched → no new parse.
        cache.get_or_load(k2, || async { panic!("v2 must stay cached") }).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 3, "only version 1 must re-parse");
    }

    #[tokio::test(start_paused = true)]
    async fn p0_cache_mtime_recheck_after_interval() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let dir = std::env::temp_dir().join(format!("liteserver-ens-p0-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let config_path = dir.join("config.yaml");
        std::fs::write(&config_path, b"v1").unwrap();
        let cache = EnsemblePlanCache::new();
        let key = PlanKey { model: "m".to_string(), version: "1".to_string() };
        let count = Arc::new(AtomicUsize::new(0));
        let load = |count: Arc<AtomicUsize>| {
            let config_path = config_path.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok::<_, AppError>(Arc::new(EnsemblePlan {
                    steps: Vec::new(),
                    layers: Vec::new(),
                    output_step: 0,
                    output_field: None,
                    chains: Vec::new(),
                    inputs_decl: None,
                    input_modes: Vec::new(),
                    conditional_refs: Vec::new(),
                    step_dep_keys: Vec::new(),
                    outputs: None,
                    dag_sets: None,
                    config_path,
                    source_mtime: None,
                }))
            }
        };
        cache.get_or_load(key.clone(), || load(count.clone())).await.unwrap();
        // Within the stat interval: no syscall, same Arc (review ②).
        let within = cache.get_or_load(key.clone(), || load(count.clone())).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1, "hot path must not stat within the interval");
        // Rewrite the file; still within interval → still served from cache.
        std::fs::write(&config_path, b"v2").unwrap();
        let stale = cache.get_or_load(key.clone(), || load(count.clone())).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1, "file change inside the interval is not seen");
        drop(within);
        drop(stale);
        // Advance past the interval: next get must stat, see the mtime change,
        // evict and re-parse (single-flight).
        tokio::time::advance(Duration::from_millis(1500)).await;
        cache.get_or_load(key.clone(), || load(count.clone())).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2, "mtime change after the interval must re-parse");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // === /audit 2026-08-12: batch-0/1 defect repros (read-only; no impl changes) ===

    /// Concurrency assumption (D23): unload_version fires cache invalidation
    /// BEFORE registry changes, so an invalidate can race an in-flight
    /// single-flight load (cold cache + concurrent unload/reload). The holder
    /// must still resolve — not panic on its vanished Loading cell.
    #[tokio::test]
    async fn p0_cache_invalidate_during_inflight_load_must_not_panic() {
        let cache = Arc::new(EnsemblePlanCache::new());
        let key = PlanKey { model: "m".to_string(), version: "1".to_string() };
        let (gate_tx, gate_rx) = oneshot::channel::<()>();
        let c2 = cache.clone();
        let k2 = key.clone();
        let holder = tokio::spawn(async move {
            c2.get_or_load(k2, || async move {
                // Hold the single-flight load open (a slow disk read/parse).
                let _ = gate_rx.await;
                Ok::<_, AppError>(test_plan("/nonexistent"))
            })
            .await
        });
        // Wait until the holder has published the Loading cell.
        for _ in 0..1000 {
            if matches!(cache.plans.get(&key).as_deref(), Some(PlanCell::Loading { .. })) {
                break;
            }
            tokio::task::yield_now().await;
        }
        // D23: unload invalidates before registry changes — this races loads.
        cache.invalidate_model("m");
        let _ = gate_tx.send(());
        let joined = holder.await;
        assert!(
            joined.is_ok(),
            "invalidate racing an in-flight load panicked the holder: {joined:?}"
        );
        assert!(
            joined.unwrap().is_ok(),
            "the racing load must still resolve its plan"
        );
    }

    /// Order assumption (review ②): the loader stats BEFORE the read, so a
    /// write landing between stat and read leaves the stored mtime OLDER than
    /// the file's — the interval re-check must then re-parse (safe). The
    /// reverse order (stat after read) could pin a fresh mtime onto stale
    /// content and serve it indefinitely.
    #[tokio::test(start_paused = true)]
    async fn p0_cache_stat_after_read_must_not_pin_stale_plan() {
        let dir = std::env::temp_dir().join(format!("liteserver-ens-p0-toctou-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let config_path = dir.join("config.yaml");
        std::fs::write(&config_path, b"v1").unwrap();
        // The production loader's stat-before-read mtime.
        let v1_mtime = std::fs::metadata(&config_path).and_then(|m| m.modified()).ok();
        // Real clock: force a distinct mtime for the interleaved write.
        std::thread::sleep(std::time::Duration::from_millis(10));
        let cache = EnsemblePlanCache::new();
        let key = PlanKey { model: "m".to_string(), version: "1".to_string() };
        // First load: returns the v1 plan (output_step=1) with the
        // stat-before-read mtime, but the file becomes v2 before the cache
        // stores the entry — exactly the stat→write→store interleaving the
        // mtime re-check must catch at the next interval.
        let first = cache
            .get_or_load(key.clone(), || {
                let cp = config_path.clone();
                async move {
                    std::fs::write(&cp, b"v2").unwrap(); // interleaved write
                    Ok::<_, AppError>(Arc::new(EnsemblePlan {
                        steps: Vec::new(),
                        layers: Vec::new(),
                        output_step: 1,
                        output_field: None,
                        chains: Vec::new(),
                        inputs_decl: None,
                        input_modes: Vec::new(),
                        conditional_refs: Vec::new(),
                        step_dep_keys: Vec::new(),
                        outputs: None,
                        dag_sets: None,
                        config_path: cp,
                        source_mtime: v1_mtime,
                    }))
                }
            })
            .await
            .unwrap();
        assert_eq!(first.output_step, 1);
        tokio::time::advance(Duration::from_millis(1500)).await;
        // Interval elapsed: the re-check must notice v2 (stored mtime is
        // v1's, older than the file's) and re-parse.
        let second = cache
            .get_or_load(key.clone(), || {
                let cp = config_path.clone();
                async move {
                    Ok::<_, AppError>(Arc::new(EnsemblePlan {
                        steps: Vec::new(),
                        layers: Vec::new(),
                        output_step: 2,
                        output_field: None,
                        chains: Vec::new(),
                        inputs_decl: None,
                        input_modes: Vec::new(),
                        conditional_refs: Vec::new(),
                        step_dep_keys: Vec::new(),
                        outputs: None,
                        dag_sets: None,
                        config_path: cp,
                        source_mtime: None,
                    }))
                }
            })
            .await
            .unwrap();
        assert_eq!(
            second.output_step, 2,
            "a mid-load write must be caught at the next interval re-check"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Control-flow assumption (§4.1 rule 1): a streaming step must be in the
    /// LAST topological layer. "Output not referenced" does NOT imply that —
    /// Kahn layering puts a no-dependency sink in layer 0 even when later
    /// layers exist. Accepting this config truncates execution at the tail's
    /// layer: steps in later layers silently never run.
    #[test]
    fn stream_rules_streaming_step_must_be_in_last_topological_layer() {
        // a (layer 0) ← c (layer 1); b (stream, no deps → layer 0, config-last).
        let yaml = r#"
ensemble:
  steps:
    - name: a
      model: m1
      version: "1"
      inputs: {x: "$request.x"}
    - name: c
      model: m2
      version: "1"
      inputs: {y: "$a"}
    - name: b
      model: m3
      version: "1"
      stream: true
      inputs: {z: "$request.z"}
"#;
        let res = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"));
        assert!(
            res.is_err(),
            "streaming step not in the last topological layer must be rejected (rule 1); \
             accepting it silently drops step c from execution"
        );
    }

    /// Data assumption: an empty steps list is malformed config. It must be a
    /// Config error at parse — not a `steps.len() - 1` underflow panic
    /// (regression: the pre-cache code returned a request-time 500).
    #[test]
    fn parse_ensemble_plan_empty_steps_is_config_error_not_panic() {
        let yaml = "ensemble:\n  steps: []\n";
        let res = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"));
        assert!(
            res.is_err(),
            "empty steps must be a config error, not an arithmetic panic"
        );
    }

    /// Config contract (D24): the ensemble schema must deny unknown fields so
    /// 0.9.0 keys (params/when/outputs/…) — or plain typos — fail fast at
    /// load instead of being silently ignored.
    #[test]
    fn ensemble_schema_rejects_unknown_fields_d24() {
        let yaml = r#"
ensemble:
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
      strem: true
"#;
        let res = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"));
        assert!(
            res.is_err(),
            "unknown step field `strem` must be rejected (D24 deny_unknown_fields); \
             silently ignoring it disables streaming without any error"
        );
    }

    // ===== Batch 3 (E2/E3/E4/E5) parse-layer tests =====

    /// E2: output omitted → steps.last() (historical semantics).
    #[test]
    fn e2_output_defaults_to_last_step() {
        let yaml = r#"
ensemble:
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
    - name: s2
      model: m2
      version: "1"
      inputs: {x: "$s1"}
"#;
        let plan = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml")).unwrap();
        assert_eq!(plan.output_step, 1);
        assert_eq!(plan.output_field, None);
    }

    /// E2: explicit output selects a named step.
    #[test]
    fn e2_output_selects_named_step() {
        let yaml = r#"
ensemble:
  output: "$s1"
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
    - name: s2
      model: m2
      version: "1"
      inputs: {x: "$s1"}
"#;
        let plan = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml")).unwrap();
        assert_eq!(plan.output_step, 0);
        assert_eq!(plan.output_field, None);
    }

    /// E2: `$stepN.field` Json path.
    #[test]
    fn e2_output_field_path() {
        let yaml = r#"
ensemble:
  output: "$s2.score"
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
    - name: s2
      model: m2
      version: "1"
      inputs: {x: "$s1"}
"#;
        let plan = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml")).unwrap();
        assert_eq!(plan.output_step, 1);
        assert_eq!(plan.output_field.as_deref(), Some("score"));
    }

    /// E2: unknown step / missing `$` prefix / `$request` → load-time config
    /// errors, never a silent fallback.
    #[test]
    fn e2_output_rejects_unknown_step_and_bad_format() {
        for output in ["$nope", "s1", "$request"] {
            let yaml = format!(
                r#"
ensemble:
  output: "{output}"
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {{x: "$request"}}
"#
            );
            let res = parse_ensemble_plan(&yaml, &PathBuf::from("/nonexistent/config.yaml"));
            assert!(res.is_err(), "output '{output}' must be a config error");
        }
    }

    /// E2 × D11: with a streaming step, the explicit output must BE that
    /// step (validated at parse — the DAG output IS the stream).
    #[test]
    fn e2_streaming_output_must_point_at_streaming_step() {
        let yaml = r#"
ensemble:
  output: "$s1"
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
    - name: s2
      model: m2
      version: "1"
      stream: true
      inputs: {x: "$s1"}
"#;
        let res = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"));
        assert!(
            res.is_err(),
            "output pointing away from the streaming step must be rejected (D11)"
        );
    }

    /// E4: version omitted / "latest" → None (execution-time resolution);
    /// explicit versions are kept as-is.
    #[test]
    fn e4_version_optional_and_latest_normalized() {
        let yaml = r#"
ensemble:
  steps:
    - name: s1
      model: m1
      inputs: {x: "$request"}
    - name: s2
      model: m2
      version: "latest"
      inputs: {x: "$s1"}
    - name: s3
      model: m3
      version: "1"
      inputs: {x: "$s2"}
"#;
        let plan = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml")).unwrap();
        assert_eq!(plan.steps[0].version, None);
        assert_eq!(plan.steps[1].version, None, "\"latest\" == omitted (E4)");
        assert_eq!(plan.steps[2].version.as_deref(), Some("1"));
    }

    /// E3: params parse into the step (assembly applies them in Step 3).
    #[test]
    fn e3_params_parse_into_step() {
        let yaml = r#"
ensemble:
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
      params:
        temperature: 0.7
        top_p: 0.9
"#;
        let plan = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml")).unwrap();
        let params = &plan.steps[0].params;
        assert_eq!(params.get("temperature").and_then(|v| v.as_f64()), Some(0.7));
        assert_eq!(params.get("top_p").and_then(|v| v.as_f64()), Some(0.9));
    }

    /// E5: timeout_secs parses; non-positive / non-finite rejected at load.
    #[test]
    fn e5_timeout_secs_parse_and_validation() {
        let ok = r#"
ensemble:
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
      timeout_secs: 2.5
"#;
        let plan = parse_ensemble_plan(ok, &PathBuf::from("/nonexistent/config.yaml")).unwrap();
        assert_eq!(plan.steps[0].timeout_secs, Some(2.5));

        for bad in ["0", "-1", ".nan"] {
            let yaml = format!(
                r#"
ensemble:
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {{x: "$request"}}
      timeout_secs: {bad}
"#
            );
            let res = parse_ensemble_plan(&yaml, &PathBuf::from("/nonexistent/config.yaml"));
            assert!(res.is_err(), "timeout_secs={bad} must be a config error");
        }
    }

    // ===== Batch 3 (E4/D15) snapshot resolution tests =====

    fn snapshot_step(model: &str, version: Option<&str>) -> EnsembleStep {
        EnsembleStep {
            name: "s".to_string(),
            model: model.to_string(),
            version: version.map(|v| v.to_string()),
            inputs: HashMap::new(),
            stream: false,
            params: HashMap::new(),
            timeout_secs: None,
            on_error: OnErrorKind::Fail,
            retries: 0,
            outputs_decl: None,
            when: None,
        }
    }

    fn registry_with_active(model: &str, version: &str) -> crate::registry::ModelRegistry {
        let registry = crate::registry::ModelRegistry::new();
        registry.force_pin_active_version(model, version);
        registry
    }

    /// D15: the first resolution for a model wins — a later registry drift
    /// must not change it within the same request.
    #[test]
    fn e4_snapshot_memoizes_first_resolution() {
        let registry = registry_with_active("m", "1");
        let snapshot = VersionSnapshot::default();
        let step = snapshot_step("m", None);
        assert_eq!(snapshot.resolve(&registry, &step).unwrap(), "1");
        // Active drifts to v2 AFTER the first resolution — the snapshot still
        // serves v1 (same-request consistency, D15).
        registry.force_pin_active_version("m", "2");
        assert_eq!(snapshot.resolve(&registry, &step).unwrap(), "1");
    }

    /// E4: explicit versions bypass the snapshot entirely.
    #[test]
    fn e4_explicit_version_bypasses_snapshot() {
        let registry = registry_with_active("m", "1");
        let snapshot = VersionSnapshot::default();
        let step = snapshot_step("m", Some("2"));
        assert_eq!(snapshot.resolve(&registry, &step).unwrap(), "2");
        assert!(
            snapshot.resolved.lock().unwrap().is_empty(),
            "explicit versions must not touch the snapshot"
        );
    }

    /// E4: an unresolved step with no active version is an execution-time
    /// resolution error (ModelNotFound).
    #[test]
    fn e4_unresolved_without_active_version_is_error() {
        let registry = crate::registry::ModelRegistry::new();
        let snapshot = VersionSnapshot::default();
        let step = snapshot_step("m", None);
        assert!(snapshot.resolve(&registry, &step).is_err());
    }

    /// D15: the memoize key is the model — two unresolved steps for the same
    /// model share ONE snapshot entry.
    #[test]
    fn e4_snapshot_keyed_by_model() {
        let registry = registry_with_active("m", "1");
        let snapshot = VersionSnapshot::default();
        let a = snapshot_step("m", None);
        let b = snapshot_step("m", None);
        assert_eq!(snapshot.resolve(&registry, &a).unwrap(), "1");
        assert_eq!(snapshot.resolve(&registry, &b).unwrap(), "1");
        assert_eq!(
            snapshot.resolved.lock().unwrap().len(),
            1,
            "one snapshot entry per model"
        );
    }

    // ===== Batch 3 (E1) nesting tests =====

    /// E1: the ancestor chain detects a self-reference (same model+version
    /// on the active nesting path).
    #[test]
    fn e1_ancestor_chain_detects_self_loop() {
        let chain = vec![("a".to_string(), "1".to_string()), ("b".to_string(), "2".to_string())];
        assert!(contains_ancestor(&chain, "a", "1"));
        assert!(contains_ancestor(&chain, "b", "2"));
        assert!(!contains_ancestor(&chain, "a", "2"));
        assert!(!contains_ancestor(&chain, "other", "1"));
    }

    /// E1: a nested run extends the chain with the child ensemble; the
    /// PARENT chain stays untouched — each branch owns its copy, the chain
    /// is not request-global mutable state.
    #[test]
    fn e1_ancestor_chain_extends_per_branch() {
        let parent = vec![("a".to_string(), "1".to_string())];
        let child = extend_ancestor_chain(&parent, "b", "2");
        assert!(contains_ancestor(&child, "a", "1"));
        assert!(contains_ancestor(&child, "b", "2"));
        assert_eq!(parent.len(), 1, "the parent chain must stay unchanged");
        assert!(!contains_ancestor(&parent, "b", "2"));
    }

    /// E1: `contains_ancestor` answers for the CURRENT branch only — the
    /// chain is immutable per branch (each recursion level extends its own
    /// copy via [`extend_ancestor_chain`]), so a concurrent sibling's
    /// in-flight child run can never appear on this branch's chain. The old
    /// flat shared Vec conflated the two, turning legal same-layer fan-out
    /// (two steps calling one child ensemble; D30 batch elements) into a
    /// spurious "recursion detected" 400 (B1).
    #[test]
    fn e1_sibling_in_flight_child_is_not_recursion() {
        let parent = vec![("p".to_string(), "1".to_string())];
        // Sibling A recurses into the child ensemble and extends ITS branch...
        let _chain_a = extend_ancestor_chain(&parent, "child", "1");
        // ...sibling B checks ITS OWN chain — the sibling's in-flight child
        // run is not an ancestor of B's branch.
        assert!(
            !contains_ancestor(&parent, "child", "1"),
            "a concurrent sibling's in-flight child call is not recursion"
        );
    }

    /// E1: nesting depth limit — depth counts along the call tree; level 0
    /// is the top-level request, so depth 8+ is rejected.
    #[test]
    fn e1_nesting_depth_limit() {
        assert!(ensure_nesting_depth(0).is_ok());
        assert!(ensure_nesting_depth(7).is_ok());
        assert!(ensure_nesting_depth(8).is_err());
    }

    /// E5: `timeout_secs` is parse-legal whenever positive & finite, but the
    /// unix_ns conversion saturates the f64→i64 cast and the `now +` addition
    /// then overflows: debug builds panic (arithmetic overflow), release
    /// wraps negative — every request on that step then fails instantly as
    /// expired. The conversion must clamp instead of overflowing.
    #[test]
    fn e5_huge_timeout_secs_must_not_overflow() {
        // 1e18 s (~3×10^10 years) is absurd but parse-legal (positive, finite).
        let d = step_effective_deadline(None, Some(1e18));
        assert!(
            d.is_some() && d.unwrap() > 0,
            "a huge timeout must clamp to a sane deadline, got {d:?}"
        );
    }

    // ===== Batch 3 (E2/E3) output selection + params tests =====

    /// E2: output_field selection — None passes the whole value through;
    /// Some extracts the field from a Json output.
    #[test]
    fn e2_select_output_field_jsons() {
        let v = EnsembleValue::Json(json!({"score": 0.9, "label": "a"}));
        let whole = select_output_field("s1", v.clone(), None).unwrap();
        assert_eq!(whole, EnsembleValue::Json(json!({"score": 0.9, "label": "a"})));
        let field = select_output_field("s1", v, Some("score")).unwrap();
        assert_eq!(field, EnsembleValue::Json(json!(0.9)));
    }

    /// E2: a missing field is an error (the DAG's contract does not match the
    /// model's output shape).
    #[test]
    fn e2_select_output_field_missing_is_error() {
        let v = EnsembleValue::Json(json!({"score": 0.9}));
        let res = select_output_field("s1", v, Some("label"));
        assert!(res.is_err(), "missing field must error, not default");
    }

    /// E2: field projection on a Binary output is rejected (no field
    /// semantics on bytes — D7's rule applied to the output face).
    #[test]
    fn e2_select_output_field_on_binary_is_error() {
        let v = EnsembleValue::Binary(bytes::Bytes::from_static(b"raw"), "application/octet-stream".to_string(), None, None);
        let res = select_output_field("s1", v, Some("score"));
        assert!(res.is_err());
    }

    /// E2 × D11: a streaming DAG cannot declare an output FIELD (chunks have
    /// no field semantics) — parse-time rejection.
    #[test]
    fn e2_streaming_output_field_rejected_at_parse() {
        let yaml = r#"
ensemble:
  output: "$s2.score"
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
    - name: s2
      model: m2
      version: "1"
      stream: true
      inputs: {x: "$s1"}
"#;
        let res = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"));
        assert!(res.is_err(), "field-projected streaming output must be rejected");
    }

    /// E3: params merge into the assembled Json payload AFTER inputs — params
    /// win on key conflicts.
    #[test]
    fn e3_params_merge_into_payload_params_win() {
        let resolved: HashMap<String, EnsembleValue> = [
            ("a".to_string(), EnsembleValue::Json(json!(1))),
            ("b".to_string(), EnsembleValue::Json(json!(2))),
        ]
        .into();
        let params: HashMap<String, Value> = [
            ("b".to_string(), json!(3)),
            ("c".to_string(), json!(4)),
        ]
        .into();
        let (bytes, ct) = assemble_step_payload("s", &resolved, &params, None).unwrap();
        assert_eq!(ct, None);
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v, json!({"a": 1, "b": 3, "c": 4}), "params override inputs");
    }

    /// E3: Binary assembly has no params semantics — a non-empty params on a
    /// Binary step is rejected at assembly (the earliest point the input type
    /// is decidable).
    #[test]
    fn e3_params_rejected_with_binary_input() {
        let resolved: HashMap<String, EnsembleValue> = [(
            "data".to_string(),
            EnsembleValue::Binary(bytes::Bytes::from_static(b"raw"), "application/octet-stream".to_string(), None, None),
        )]
        .into();
        let params: HashMap<String, Value> = [("temperature".to_string(), json!(0.7))].into();
        let res = assemble_step_payload("s", &resolved, &params, None);
        assert!(res.is_err(), "params × Binary input must be rejected (E3)");
    }

    // ===== Batch 3 (E5) step timeout tests =====

    fn unix_ns_now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64
    }

    /// E5: no step timeout → the parent deadline passes through unchanged
    /// (historical behaviour).
    #[test]
    fn e5_step_effective_deadline_passthrough() {
        assert_eq!(step_effective_deadline(Some(1_000_000), None), Some(1_000_000));
        assert_eq!(step_effective_deadline(None, None), None);
    }

    /// E5: a step timeout produces a wall-clock cap of now + timeout_secs.
    #[test]
    fn e5_step_effective_deadline_timeout_cap() {
        let before = unix_ns_now();
        let deadline = step_effective_deadline(None, Some(2.0)).unwrap();
        let after = unix_ns_now() + 2_000_000_000;
        assert!(
            deadline >= before + 2_000_000_000 && deadline <= after,
            "step cap must be ~now + 2s, got {deadline}"
        );
    }

    /// E5: the tighter bound wins — an earlier parent deadline caps the step.
    #[test]
    fn e5_step_effective_deadline_min_with_parent() {
        let parent = unix_ns_now() + 1_000_000_000; // 1s from now
        let deadline = step_effective_deadline(Some(parent), Some(60.0));
        assert_eq!(deadline, Some(parent), "parent deadline must win when earlier");
    }

    // === E6 (batch 4): on_error/retries ===

    /// E6 (D5): a `skip` step referenced by ANY other step's inputs is a
    /// parse-time rejection — the DAG must never have a dangling reference
    /// when the skip fires at runtime.
    #[test]
    fn e6_skip_step_referenced_by_another_step_is_config_error() {
        let yaml = r#"
ensemble:
  steps:
    - name: may_skip
      model: m1
      version: "1"
      on_error: skip
      inputs: {x: "$request"}
    - name: consumer
      model: m2
      version: "1"
      inputs: {y: "$may_skip"}
"#;
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("skip step referenced downstream must be rejected at parse");
        assert!(
            err.to_string().contains("skip"),
            "error must name the skip rule, got: {err}"
        );
    }

    /// E6 (D5): a `skip` step referenced (even by field) by another step is
    /// rejected — the field projection has the same dangling-reference risk.
    #[test]
    fn e6_skip_step_field_reference_is_config_error() {
        let yaml = r#"
ensemble:
  steps:
    - name: may_skip
      model: m1
      version: "1"
      on_error: skip
      inputs: {x: "$request"}
    - name: consumer
      model: m2
      version: "1"
      inputs: {y: "$may_skip.field"}
"#;
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("skip step field reference must be rejected at parse");
        assert!(
            err.to_string().contains("skip"),
            "error must name the skip rule, got: {err}"
        );
    }

    /// E6 (D5): `ensemble.output` pointing at a skip step is rejected — the
    /// single-output contract has no null channel (only E7 outputs do).
    #[test]
    fn e6_skip_step_as_output_is_config_error() {
        let yaml = r#"
ensemble:
  output: "$may_skip"
  steps:
    - name: may_skip
      model: m1
      version: "1"
      on_error: skip
      inputs: {x: "$request"}
"#;
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("skip step as ensemble.output must be rejected at parse");
        assert!(
            err.to_string().contains("skip"),
            "error must name the skip rule, got: {err}"
        );
    }

    /// E6 (D34 rule 6): a streaming step must never be absent — `on_error:
    /// skip` × `stream: true` is a parse-time rejection (the streaming
    /// response contract promises a stream unconditionally).
    #[test]
    fn e6_streaming_step_with_skip_is_config_error() {
        let yaml = r#"
ensemble:
  steps:
    - name: tail
      model: m1
      version: "1"
      stream: true
      on_error: skip
      inputs: {x: "$request"}
"#;
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("streaming step with on_error: skip must be rejected at parse");
        assert!(
            err.to_string().contains("skip"),
            "error must name the skip rule, got: {err}"
        );
    }

    /// E6: an unreferenced skip step is legal — the skip simply drops the
    /// step from the context and the layer continues.
    #[test]
    fn e6_unreferenced_skip_step_is_accepted() {
        let yaml = r#"
ensemble:
  steps:
    - name: may_skip
      model: m1
      version: "1"
      on_error: skip
      inputs: {x: "$request"}
    - name: main
      model: m2
      version: "1"
      inputs: {x: "$request"}
"#;
        let plan = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect("unreferenced skip step must parse");
        assert_eq!(plan.steps[0].on_error, OnErrorKind::Skip);
        assert_eq!(plan.steps[1].on_error, OnErrorKind::Fail);
    }

    /// E6: an unknown on_error value fails deserialization (the schema
    /// denies typos — a swallowed `on_error: skp` would silently disable
    /// fault tolerance).
    #[test]
    fn e6_unknown_on_error_value_is_config_error() {
        let yaml = r#"
ensemble:
  steps:
    - name: s
      model: m1
      version: "1"
      on_error: skp
      inputs: {x: "$request"}
"#;
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("unknown on_error value must be rejected at parse");
        assert!(err.to_string().contains("on_error"), "got: {err}");
    }

    /// E6: retry classification — only 5xx worker errors and timeouts
    /// retry; 4xx (client contract), queue pressure and crashes never do
    /// (a 4xx is deterministic, a QueueFull retry makes pressure worse).
    #[test]
    fn e6_retryable_error_classification() {
        let err_500 = AppError::ModelError(Box::new(ModelErrorData {
            status_code: 500,
            error_type: "model_error".into(),
            detail: "boom".into(),
            code: None,
            param: None,
            headers: None,
        }));
        let err_503 = AppError::ModelError(Box::new(ModelErrorData {
            status_code: 503,
            error_type: "model_error".into(),
            detail: "overloaded".into(),
            code: None,
            param: None,
            headers: None,
        }));
        let err_400 = AppError::ModelError(Box::new(ModelErrorData {
            status_code: 400,
            error_type: "invalid".into(),
            detail: "bad input".into(),
            code: None,
            param: None,
            headers: None,
        }));
        assert!(is_retryable_error(&err_500), "5xx must retry");
        assert!(is_retryable_error(&err_503), "5xx must retry");
        assert!(!is_retryable_error(&err_400), "4xx must NOT retry");
        assert!(
            is_retryable_error(&AppError::InferenceTimeout("t".into())),
            "timeouts must retry"
        );
        assert!(
            !is_retryable_error(&AppError::QueueFull("full".into())),
            "queue pressure must NOT retry"
        );
        assert!(
            !is_retryable_error(&AppError::WorkerCrashed("crash".into())),
            "worker crashes must NOT retry"
        );
        assert!(
            !is_retryable_error(&AppError::ModelNotReady("not ready".into())),
            "readiness must NOT retry"
        );
    }

    // === MIMO② (batch 4③): D10 json aliases — path projection ===

    /// R6: json alias path projections — default path `$.<alias>`, explicit
    /// `$.a.b` paths, and refs carrying projection paths (parse + runtime).
    #[test]
    fn mimo2_json_alias_projection() {
        let yaml = r#"ensemble:
  inputs:
    x:
      type: json
  steps:
    - name: a
      model: m1
      version: "1"
      outputs:
        score:
          type: json
          path: "$.out.score"
        whole:
          type: json
      inputs: {x: "$inputs.x"}
    - name: b
      model: m2
      version: "1"
      inputs: {x: "$a.score"}
"#;
        let plan = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect("json alias declarations must parse (MIMO②)");
        assert_eq!(plan.input_mode(1), Some(InputMode::GroupJson));

        // Materialize: explicit nested path + default `$.<alias>` path.
        let step = EnsembleStep {
            name: "a".to_string(),
            model: "m1".to_string(),
            version: Some("1".to_string()),
            inputs: HashMap::new(),
            when: None,
            stream: false,
            params: HashMap::new(),
            timeout_secs: None,
            on_error: OnErrorKind::Fail,
            retries: 0,
            outputs_decl: Some(
                [
                    ("score", StepOutputDecl {
                        ty: InputType::Json,
                        path: Some("$.out.score".to_string()),
                    }),
                    ("whole", StepOutputDecl {
                        ty: InputType::Json,
                        path: None,
                    }),
                ]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            ),
        };
        let out = materialize_step_outputs(
            &step,
            EnsembleValue::Json(json!({"out": {"score": 0.9}, "whole": {"a": 1}})),
        )
        .unwrap();
        assert_eq!(out.len(), 2);
        let map: HashMap<&str, &EnsembleValue> = out.iter().map(|(k, v)| (k.as_str(), v)).collect();
        assert_eq!(map["a.score"], &EnsembleValue::Json(json!(0.9)));
        assert_eq!(map["a.whole"], &EnsembleValue::Json(json!({"a": 1})), "default path $.whole");

        // Missing path → step error (I3: declared contract unmet).
        let err = materialize_step_outputs(&step, EnsembleValue::Json(json!({"other": 1})))
            .unwrap_err();
        assert!(
            err.to_string().contains("score"),
            "missing projection path must error naming the alias, got: {err}"
        );

        // Runtime ref with a projection path on a json alias.
        let mut context = HashMap::new();
        context.insert("a.score".to_string(), EnsembleValue::Json(json!({"x": 7})));
        match resolve_ref(&plan, "$a.score.x", &context).unwrap() {
            ResolvedRef::Value(EnsembleValue::Json(v)) => assert_eq!(v, json!(7)),
            other => panic!("expected Json 7, got {other:?}"),
        }
    }

    /// R6: alias names must be identifiers and paths must be `$.a.b` dot
    /// segments — both are parse-time rejections.
    #[test]
    fn mimo2_r6_alias_name_and_path_validation() {
        let bad_name = "ensemble:\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      outputs:\n        9bad:\n          type: json\n      inputs: {x: \"$request\"}\n    - name: b\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$a.9bad\"}\n";
        let err = parse_ensemble_plan(bad_name, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("invalid alias name must be rejected (R6)");
        assert!(err.to_string().contains("alias"), "got: {err}");
        let bad_path = "ensemble:\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      outputs:\n        score:\n          type: json\n          path: \"out[0].score\"\n      inputs: {x: \"$request\"}\n    - name: b\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$a.score\"}\n";
        let err = parse_ensemble_plan(bad_path, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("array-subscript paths must be rejected (D29)");
        assert!(err.to_string().contains("path"), "got: {err}");
    }

    // === E8-2 (batch 5): when expressions ===

    /// E8-2 parse: the grammar is `<ref> <op> <literal>` — operators
    /// `== != contains in`, refs from the R16/m2 whitelist
    /// ($request.dag / $request.client_ip / $inputs.NAME[.path]).
    #[test]
    fn e8_when_parse_and_whitelist() {
        let ok = "ensemble:\n  inputs:\n    mode:\n      type: json\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      when: \"$request.dag == 'fast'\"\n      inputs: {x: \"$inputs.mode\"}\n    - name: t\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$inputs.mode\"}\n";
        let plan = parse_ensemble_plan(ok, &PathBuf::from("/nonexistent/config.yaml"))
            .expect("whitelisted when refs must parse");
        assert!(plan.steps[0].when.is_some());
        // Non-whitelisted $request field → rejected (m2).
        let bad = "ensemble:\n  inputs:\n    mode:\n      type: json\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      when: \"$request.nope == 'x'\"\n      inputs: {x: \"$inputs.mode\"}\n    - name: t\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$inputs.mode\"}\n";
        let err = parse_ensemble_plan(bad, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("non-whitelisted request field must be rejected (R16)");
        assert!(err.to_string().contains("nope"), "got: {err}");
        // Unknown input → rejected.
        let bad = "ensemble:\n  inputs:\n    mode:\n      type: json\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      when: \"$inputs.nope == 'x'\"\n      inputs: {x: \"$inputs.mode\"}\n    - name: t\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$inputs.mode\"}\n";
        let err = parse_ensemble_plan(bad, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("undeclared input in when must be rejected (R16)");
        assert!(err.to_string().contains("nope"), "got: {err}");
        // Malformed expression → rejected.
        let bad = "ensemble:\n  inputs:\n    mode:\n      type: json\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      when: \"$request.dag === 'x'\"\n      inputs: {x: \"$inputs.mode\"}\n    - name: t\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$inputs.mode\"}\n";
        let err = parse_ensemble_plan(bad, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("malformed when must be rejected");
        let _ = err; // the rejection itself is the assertion
    }

    /// D34 rule 6 (third arm): `when` × `stream: true` is a parse-time
    /// rejection (a streaming response promises a stream unconditionally).
    #[test]
    fn e8_when_x_stream_rejected() {
        let yaml = "ensemble:\n  steps:\n    - name: tail\n      model: m\n      version: \"1\"\n      stream: true\n      when: \"$request.dag == 'fast'\"\n      inputs: {x: \"$request\"}\n";
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("when × stream must be rejected (D34)");
        assert!(err.to_string().contains("when"), "got: {err}");
    }

    /// E8-2: a when-step's absence must be statically provable — downstream
    /// references are rejected exactly like E6-skip (D5 channel).
    #[test]
    fn e8_when_step_downstream_ref_rejected() {
        let yaml = "ensemble:\n  steps:\n    - name: cond\n      model: m\n      version: \"1\"\n      when: \"$request.dag == 'fast'\"\n      inputs: {x: \"$request\"}\n    - name: consumer\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$cond\"}\n";
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("when-step downstream ref must be rejected");
        assert!(err.to_string().contains("absent"), "got: {err}");
    }

    /// E8-2 evaluation: strict type equality (no coercion), contains/in on
    /// strings+arrays, the absent == null check (R16).
    #[test]
    fn e8_when_eval_semantics() {
        let plan = parse_ensemble_plan(
            "ensemble:\n  inputs:\n    a:\n      type: json\n    opt:\n      type: json\n      required: false\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      when: \"$inputs.a == 1\"\n      inputs: {x: \"$inputs.a\"}\n    - name: t\n      model: m2\n      version: \"1\"\n      when: \"$inputs.a == '1'\"\n      inputs: {x: \"$inputs.a\"}\n    - name: u\n      model: m3\n      version: \"1\"\n      when: \"$inputs.a contains 'x'\"\n      inputs: {x: \"$inputs.a\"}\n    - name: v\n      model: m4\n      version: \"1\"\n      when: \"$inputs.a in ['x', 'y']\"\n      inputs: {x: \"$inputs.a\"}\n    - name: w\n      model: m5\n      version: \"1\"\n      when: \"$inputs.opt != null\"\n      inputs: {x: \"$inputs.opt\"}\n    - name: z\n      model: m6\n      version: \"1\"\n      when: \"$request.dag == 'fast'\"\n      inputs: {x: \"$inputs.a\"}\n    - name: out\n      model: m7\n      version: \"1\"\n      inputs: {x: \"$inputs.a\"}\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
        let mut context = HashMap::new();
        context.insert("inputs.a".to_string(), EnsembleValue::Json(json!(1)));
        // opt absent.
        let opts = EnsembleExecOpts {
            client_ip: "127.0.0.1".into(),
            deadline_unix_ns: None,
            decoupled: false,
            dag_selector: Some("fast".into()),
        };
        assert!(eval_when(plan.steps[0].when.as_ref().unwrap(), &opts, &context).unwrap(), "1 == 1");
        assert!(!eval_when(plan.steps[1].when.as_ref().unwrap(), &opts, &context).unwrap(), "1 == '1' must be false (strict, no coercion)");
        assert!(!eval_when(plan.steps[2].when.as_ref().unwrap(), &opts, &context).unwrap(), "number contains → false");
        assert!(!eval_when(plan.steps[3].when.as_ref().unwrap(), &opts, &context).unwrap(), "1 in ['x','y'] must be false");
        // absent != null → true (absence = null, R16)
        assert!(eval_when(plan.steps[4].when.as_ref().unwrap(), &opts, &context).unwrap(), "absent != null must be true");
        assert!(eval_when(plan.steps[5].when.as_ref().unwrap(), &opts, &context).unwrap(), "$request.dag == 'fast'");
    }

    // === E8-1 (batch 5): named DAG sets ===

    /// E8-1: the dags form forbids top-level steps/output/outputs/inputs —
    /// everything lives inside the sets (no ambiguity about the default).
    #[test]
    fn e8_dags_form_forbids_top_level_fields() {
        let yaml = "ensemble:\n  dags:\n    default:\n      steps:\n        - name: a\n          model: m\n          version: \"1\"\n          inputs: {x: \"$request\"}\n  steps:\n    - name: b\n      model: m\n      version: \"1\"\n      inputs: {x: \"$request\"}\n";
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("dags + top-level steps must be rejected");
        assert!(err.to_string().contains("dags"), "got: {err}");
    }

    /// R15: each set validates INDEPENDENTLY — an invalid set fails the
    /// load naming the set; other sets stay untouched.
    #[test]
    fn e8_per_set_independent_validation() {
        let yaml = "ensemble:\n  dags:\n    default:\n      steps:\n        - name: a\n          model: m\n          version: \"1\"\n          inputs: {x: \"$request\"}\n    broken:\n      steps:\n        - name: a\n          model: m\n          version: \"1\"\n          on_error: skip\n          inputs: {x: \"$request\"}\n        - name: b\n          model: m2\n          version: \"1\"\n          inputs: {x: \"$a\"}\n";
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("a broken set must fail the load (R15)");
        assert!(err.to_string().contains("broken"), "must name the set: {err}");
    }

    /// E8-1: set selection — None = "default", Some = exact name; unknown
    /// name → 400 (D22: never a silent default fallback); a selector on a
    /// single-form plan → 400.
    #[test]
    fn e8_select_dag_set() {
        let plan = parse_ensemble_plan(
            "ensemble:\n  dags:\n    default:\n      steps:\n        - name: a\n          model: m\n          version: \"1\"\n          inputs: {x: \"$request\"}\n    fast:\n      steps:\n        - name: b\n          model: m\n          version: \"1\"\n          inputs: {x: \"$request\"}\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
        assert_eq!(select_dag_set(&plan, None).unwrap().steps[0].name, "a");
        assert_eq!(select_dag_set(&plan, Some("fast")).unwrap().steps[0].name, "b");
        let err = select_dag_set(&plan, Some("nope")).unwrap_err();
        assert!(matches!(err, AppError::InvalidRequestBody(_)), "unknown dag → 400 (D22), got {err:?}");
        assert!(err.to_string().contains("nope"), "got: {err}");
        // Single-form plan + selector → 400.
        let single = parse_ensemble_plan(
            "ensemble:\n  steps:\n    - name: a\n      model: m\n      version: \"1\"\n      inputs: {x: \"$request\"}\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
        let err = select_dag_set(&single, Some("fast")).unwrap_err();
        assert!(matches!(err, AppError::InvalidRequestBody(_)), "got {err:?}");
    }

    /// D22: selector value validation — non-empty, ≤64 chars,
    /// `[A-Za-z0-9_-]` only.
    #[test]
    fn e8_d22_selector_validation() {
        assert!(validate_dag_selector("fast").is_ok());
        assert!(validate_dag_selector("fast-v2_x").is_ok());
        assert!(validate_dag_selector("").is_err(), "empty must be rejected");
        assert!(validate_dag_selector("has space").is_err());
        assert!(validate_dag_selector("has!bang").is_err());
        let long = "a".repeat(65);
        assert!(validate_dag_selector(&long).is_err(), ">64 chars must be rejected");
    }

    /// E8-1: per-set inputs declarations are INDEPENDENT (R15) — the
    /// envelope contract follows the selected set.
    #[test]
    fn e8_per_set_inputs_independent() {
        let plan = parse_ensemble_plan(
            "ensemble:\n  dags:\n    default:\n      steps:\n        - name: a\n          model: m\n          version: \"1\"\n          inputs: {x: \"$request\"}\n    named:\n      inputs:\n        text:\n          type: json\n      steps:\n        - name: a\n          model: m\n          version: \"1\"\n          inputs: {x: \"$inputs.text\"}\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
        let default = select_dag_set(&plan, None).unwrap();
        assert!(default.inputs_decl.is_none(), "default set is legacy-form");
        let named = select_dag_set(&plan, Some("named")).unwrap();
        assert!(named.inputs_decl.is_some(), "named set declares inputs");
    }

    // === E7 (batch 4④): multi-sink outputs ===

    /// E7: `output` and `outputs` are mutually exclusive (二选一).
    #[test]
    fn e7_output_x_outputs_mutually_exclusive() {
        let yaml = "ensemble:\n  output: \"$a\"\n  outputs:\n    answer: \"$a\"\n  steps:\n    - name: a\n      model: m\n      version: \"1\"\n      inputs: {x: \"$request\"}\n";
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("output × outputs must be rejected (E7)");
        assert!(err.to_string().contains("outputs"), "got: {err}");
    }

    /// R13: outputs values must be legal refs — unknown sources are
    /// rejected; refs to absentable steps are ALLOWED (null channel, D5).
    #[test]
    fn e7_r13_outputs_ref_validation() {
        let bad = "ensemble:\n  outputs:\n    answer: \"$nope\"\n  steps:\n    - name: a\n      model: m\n      version: \"1\"\n      inputs: {x: \"$request\"}\n";
        let err = parse_ensemble_plan(bad, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("unknown outputs ref must be rejected (R13)");
        assert!(err.to_string().contains("nope"), "got: {err}");
        // Skip-step alias is the D5 null channel.
        let ok = "ensemble:\n  outputs:\n    answer: \"$may\"\n  steps:\n    - name: may\n      model: m1\n      version: \"1\"\n      on_error: skip\n      inputs: {x: \"$request\"}\n    - name: main\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$request\"}\n";
        parse_ensemble_plan(ok, &PathBuf::from("/nonexistent/config.yaml"))
            .expect("skip-step outputs alias must parse (D5)");
        // Declared-step alias refs ($stepX.ALIAS) are legal sink refs.
        let ok = "ensemble:\n  outputs:\n    thumb: \"$a.crop\"\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      outputs:\n        crop:\n          type: binary\n      inputs: {x: \"$request\"}\n    - name: b\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$request\"}\n";
        parse_ensemble_plan(ok, &PathBuf::from("/nonexistent/config.yaml"))
            .expect("declared-alias sink refs must parse (R13)");
    }

    /// R14/D11: outputs × streaming — exactly ONE alias pointing at the
    /// streaming step; anything else is rejected.
    #[test]
    fn e7_r14_streaming_outputs_sole_alias() {
        let base = "ensemble:\n  outputs:\n{out}  steps:\n    - name: pre\n      model: m1\n      version: \"1\"\n      inputs: {x: \"$request\"}\n    - name: tail\n      model: m2\n      version: \"1\"\n      stream: true\n      inputs: {x: \"$pre\"}\n";
        // Sole alias pointing at the streaming step → ok.
        let ok = base.replace("{out}", "    answer: \"$tail\"\n");
        parse_ensemble_plan(&ok, &PathBuf::from("/nonexistent/config.yaml"))
            .expect("sole streaming alias must parse (D11)");
        // Two aliases → rejected.
        let two = base.replace("{out}", "    a: \"$tail\"\n    b: \"$pre\"\n");
        let err = parse_ensemble_plan(&two, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("multi-alias outputs on a streaming DAG must be rejected (D11)");
        assert!(err.to_string().contains("alias"), "got: {err}");
        // Alias NOT pointing at the streaming step → rejected.
        let wrong = base.replace("{out}", "    answer: \"$pre\"\n");
        let err = parse_ensemble_plan(&wrong, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("outputs not referencing the streaming step must be rejected (R14)");
        assert!(err.to_string().contains("stream"), "got: {err}");
    }

    /// build_response: the KServe envelope shape — json aliases in outputs[],
    /// binary aliases into the tail with binary_data_size refilled, absent
    /// aliases (skip/optional) → data: null + warn (D5), declaration order
    /// preserved.
    #[test]
    fn e7_build_response_envelope() {
        let plan = parse_ensemble_plan(
            "ensemble:\n  inputs:\n    a:\n      type: json\n    opt:\n      type: json\n      required: false\n  outputs:\n    answer: \"$main\"\n    thumb: \"$enc.crop\"\n    echo: \"$inputs.a\"\n    maybe: \"$inputs.opt\"\n  steps:\n    - name: enc\n      model: m1\n      version: \"1\"\n      outputs:\n        crop:\n          type: binary\n          path: \"$.crop\"\n      inputs: {x: \"$inputs.a\"}\n    - name: main\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$inputs.a\"}\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
        let mut context = HashMap::new();
        context.insert("main".to_string(), EnsembleValue::Json(json!({"out": 1})));
        context.insert(
            "enc.crop".to_string(),
            EnsembleValue::Binary(Bytes::from_static(b"\x01\x02"), "image/png".into(), Some(vec![2]), None),
        );
        context.insert("inputs.a".to_string(), EnsembleValue::Json(json!("in")));
        // opt absent → maybe → null.
        let outcome = build_response(&plan, "demo", &context).unwrap();
        let EnsembleOutcome::Unary(EnsembleValue::Envelope { head, tail }) = outcome else {
            panic!("multi-sink with a binary alias must be an envelope");
        };
        assert_eq!(tail.as_ref(), b"\x01\x02");
        assert_eq!(head["model_name"], json!("demo"));
        let outs = head["outputs"].as_array().unwrap();
        assert_eq!(outs.len(), 4, "{outs:?}");
        assert_eq!(outs[0], json!({"name": "answer", "data": {"out": 1}}));
        assert_eq!(
            outs[1],
            json!({"name": "thumb", "parameters": {"binary_data_size": 2}, "shape": [2]})
        );
        assert_eq!(outs[2], json!({"name": "echo", "data": "in"}));
        assert_eq!(outs[3], json!({"name": "maybe", "data": null}), "absent alias → null (D5)");
    }

    /// D32 codec: LSBE-1 encode + split round-trips (the gRPC multi-sink
    /// response container).
    #[test]
    fn e7_lsbe1_encode_split_roundtrip() {
        let head = json!({"model_name": "demo", "outputs": [{"name": "a", "data": 1}]});
        let blob = encode_lsbe1(&head, b"\x00\x01");
        let (h, t) = split_envelope(&blob).unwrap();
        assert_eq!(h, head);
        assert_eq!(t.as_deref(), Some(&b"\x00\x01"[..]));
    }

    // === MIMO (batch 4①): inputs declaration R1-R5, wire R18/R19, LSBE-1
    // (D32), static type env R11/R12, step.outputs binary aliases R6-R8/R10 ===

    fn json_decl() -> InputDecl {
        InputDecl {
            ty: InputType::Json,
            required: true,
            default: None,
            content_type: None,
            shape: None,
            datatype: None,
        }
    }

    /// R1: input names must be plain identifiers (the `$inputs.NAME` grammar
    /// depends on it); a `type` is mandatory.
    #[test]
    fn mimo_r1_invalid_input_name_and_missing_type() {
        for bad in ["9lives", "has-dash", "$weird"] {
            let yaml = format!(
                "ensemble:\n  inputs:\n    {bad}:\n      type: json\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs: {{x: \"$inputs.{bad}\"}}\n"
            );
            let err = parse_ensemble_plan(&yaml, &PathBuf::from("/nonexistent/config.yaml"))
                .expect_err(&format!("input name '{bad}' must be rejected (R1)"));
            assert!(err.to_string().contains("input"), "got: {err}");
        }
        let yaml = "ensemble:\n  inputs:\n    a: {}\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs: {x: \"$inputs.a\"}\n";
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("missing type must be rejected (R1)");
        assert!(err.to_string().contains("type"), "got: {err}");
    }

    /// R2: declaration fields are type-gated — default/json only,
    /// content_type/shape/datatype binary only, required+default conflict.
    #[test]
    fn mimo_r2_decl_field_type_gating() {
        let cases = [
            ("default", "default: 1", "json 上允许,二进制上必须拒绝"),
            ("content_type", "content_type: image/png", "json 上必须拒绝"),
            ("shape", "shape: [1, 2]", "json 上必须拒绝"),
            ("datatype", "datatype: FP32", "json 上必须拒绝"),
        ];
        for (name, extra, why) in cases {
            let binary_yaml = format!(
                "ensemble:\n  inputs:\n    {name}:\n      type: binary\n      {extra}\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs: {{x: \"$inputs.{name}\"}}\n"
            );
            let json_yaml = format!(
                "ensemble:\n  inputs:\n    {name}:\n      type: json\n      {extra}\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs: {{x: \"$inputs.{name}\"}}\n"
            );
            match name {
                "default" => {
                    parse_ensemble_plan(&binary_yaml, &PathBuf::from("/nonexistent/config.yaml"))
                        .expect_err("default on binary must be rejected (R2)");
                    // required defaults true and conflicts with a default —
                    // the legal default-on-json form declares required: false.
                    let json_optional = json_yaml.replace(
                        "      default: 1",
                        "      required: false\n      default: 1",
                    );
                    parse_ensemble_plan(&json_optional, &PathBuf::from("/nonexistent/config.yaml"))
                        .expect("default on json is legal (with required: false)");
                }
                _ => {
                    parse_ensemble_plan(&json_yaml, &PathBuf::from("/nonexistent/config.yaml"))
                        .expect_err(why);
                    parse_ensemble_plan(&binary_yaml, &PathBuf::from("/nonexistent/config.yaml"))
                        .expect("binary-only fields are legal on binary");
                }
            }
        }
        // required: true + default → semantic contradiction.
        let yaml = "ensemble:\n  inputs:\n    a:\n      type: json\n      required: true\n      default: 1\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs: {x: \"$inputs.a\"}\n";
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("required+default must be rejected (R2)");
        assert!(err.to_string().contains("default"), "got: {err}");
    }

    /// R3: a binary root input can only be referenced whole (I1) — any path
    /// projection on it is a parse error.
    #[test]
    fn mimo_r3_binary_input_path_projection_rejected() {
        let yaml = "ensemble:\n  inputs:\n    img:\n      type: binary\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs: {x: \"$inputs.img.crop\"}\n";
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("binary input path projection must be rejected (R3)");
        assert!(err.to_string().contains("binary"), "got: {err}");
    }

    /// R4: a step referencing an optional (no-default) input is a
    /// CONDITIONAL step — its absence must be statically provable, so
    /// downstream references are rejected exactly like E6-skip (D13/D5).
    #[test]
    fn mimo_r4_conditional_step_rules() {
        // Downstream reference to a conditional step → rejected.
        let yaml = "ensemble:\n  inputs:\n    opt:\n      type: json\n      required: false\n  steps:\n    - name: cond\n      model: m1\n      version: \"1\"\n      inputs: {x: \"$inputs.opt\"}\n    - name: consumer\n      model: m2\n      version: \"1\"\n      inputs: {y: \"$cond\"}\n";
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("conditional step downstream reference must be rejected (R4)");
        assert!(err.to_string().contains("optional") || err.to_string().contains("conditional"), "got: {err}");
        // Conditional × stream → rejected (D34 rule 6 third arm).
        let yaml = "ensemble:\n  inputs:\n    opt:\n      type: json\n      required: false\n  steps:\n    - name: tail\n      model: m1\n      version: \"1\"\n      stream: true\n      inputs: {x: \"$inputs.opt\"}\n";
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("conditional streaming step must be rejected (D34)");
        let _ = err; // the rejection itself is the assertion
        // With a default the value is always present — NOT conditional.
        let yaml = "ensemble:\n  inputs:\n    opt:\n      type: json\n      required: false\n      default: \"x\"\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      inputs: {x: \"$inputs.opt\"}\n    - name: b\n      model: m2\n      version: \"1\"\n      inputs: {y: \"$a\"}\n";
        parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect("default-carrying input must not make a step conditional (R4)");
        // An unreferenced conditional step is legal (outputs alias null
        // later) — the output step stays non-conditional.
        let yaml = "ensemble:\n  inputs:\n    a:\n      type: json\n    opt:\n      type: json\n      required: false\n  steps:\n    - name: cond\n      model: m1\n      version: \"1\"\n      inputs: {x: \"$inputs.opt\"}\n    - name: main\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$inputs.a\"}\n";
        let _ = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect("an unreferenced conditional step is legal");
    }

    /// R5: the `$inputs` namespace requires a declaration; declared configs
    /// have no anonymous root (`$request` refs are rejected).
    #[test]
    fn mimo_r5_namespace_gating() {
        // Legacy config referencing $inputs → error (namespace undeclared).
        let yaml = "ensemble:\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs: {x: \"$inputs.a\"}\n";
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("$inputs in a legacy config must be rejected (R5)");
        let _ = err;
        // Declared config referencing $request (anonymous root) → error.
        let yaml = "ensemble:\n  inputs:\n    a:\n      type: json\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs: {x: \"$request\"}\n";
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("$request in a declared config must be rejected (R5)");
        let _ = err;
    }

    /// R12: static input-mode dispatch — all-Json → GroupJson, exactly one
    /// whole Binary → BinaryPassThrough, everything else → parse error.
    #[test]
    fn mimo_r12_input_mode_dispatch() {
        let steps = |refs: &str| format!(
            "ensemble:\n  inputs:\n    a:\n      type: json\n    img:\n      type: binary\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs:\n{refs}\n"
        );
        let plan = parse_ensemble_plan(
            &steps("        x: \"$inputs.a\"\n        y: \"$inputs.a\""),
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
        assert_eq!(plan.input_mode(0), Some(InputMode::GroupJson));
        let plan = parse_ensemble_plan(
            &steps("        x: \"$inputs.img\""),
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
        assert_eq!(plan.input_mode(0), Some(InputMode::BinaryPassThrough));
        // Mixed json+binary → rejected.
        let err = parse_ensemble_plan(
            &steps("        x: \"$inputs.a\"\n        y: \"$inputs.img\""),
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .expect_err("mixed json/binary inputs must be rejected (R12)");
        assert!(err.to_string().contains("binary"), "got: {err}");
    }

    /// R9: params × Binary is a static error in declared configs (moved from
    /// the legacy runtime check once the input mode is parse-decidable).
    #[test]
    fn mimo_r9_params_x_binary_static_rejection() {
        let yaml = "ensemble:\n  inputs:\n    img:\n      type: binary\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      params:\n        t: 0.7\n      inputs: {x: \"$inputs.img\"}\n";
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("params × binary must be rejected at parse (R9)");
        assert!(err.to_string().contains("params"), "got: {err}");
    }

    /// R18: envelope parsing — named inputs, binary tail slicing in header
    /// order, defaults, absent optionals, marker decode.
    #[test]
    fn mimo_r18_envelope_parsing() {
        let decl: IndexMap<String, InputDecl> = [
            ("text", json_decl()),
            ("sys", {
                let mut d = json_decl();
                d.required = false;
                d.default = Some(json!("be terse"));
                d
            }),
            ("opt", {
                let mut d = json_decl();
                d.required = false;
                d
            }),
            ("img", InputDecl {
                ty: InputType::Binary,
                required: true,
                default: None,
                content_type: Some("image/png".into()),
                shape: Some(vec![1, 3]),
                datatype: Some("FP32".into()),
            }),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        // Happy: head + binary tail, header-order slicing, shape carried.
        let head = json!({"id": "r1", "inputs": [
            {"name": "text", "data": {"q": "hi"}},
            {"name": "img", "parameters": {"binary_data_size": 3}}
        ]});
        let head_bytes = serde_json::to_vec(&head).unwrap();
        let mut full = head_bytes.clone();
        full.extend_from_slice(b"\x00\x01\x02");
        let root = parse_root_inputs(
            EnsembleValue::Envelope { head, tail: Bytes::from(full).slice(head_bytes.len()..) },
            Some(&decl),
            false,
        )
        .unwrap();
        let RootInputs::Named { values, absent } = root else { panic!("named expected") };
        assert_eq!(values["text"], EnsembleValue::Json(json!({"q": "hi"})));
        assert_eq!(values["sys"], EnsembleValue::Json(json!("be terse")), "default filled");
        match &values["img"] {
            EnsembleValue::Binary(b, ct, shape, dt) => {
                assert_eq!(b.as_ref(), b"\x00\x01\x02");
                assert_eq!(ct, "image/png");
                assert_eq!(shape.as_deref(), Some(&vec![1, 3][..]));
                assert_eq!(dt.as_deref(), Some("FP32"));
            }
            other => panic!("binary expected, got {other:?}"),
        }
        assert_eq!(absent, vec!["opt".to_string()]);

        // Missing required → 400.
        let head = json!({"inputs": [{"name": "text", "data": 1}]});
        let err = parse_root_inputs(EnsembleValue::Json(head), Some(&decl), false).unwrap_err();
        assert!(matches!(err, AppError::InvalidRequestBody(_)), "got {err:?}");

        // Unknown input name → 400.
        let head = json!({"inputs": [
            {"name": "text", "data": 1},
            {"name": "nope", "data": 2},
            {"name": "img", "parameters": {"binary_data_size": 0}}
        ]});
        let err = parse_root_inputs(EnsembleValue::Json(head), Some(&decl), false).unwrap_err();
        assert!(matches!(err, AppError::InvalidRequestBody(_)), "got {err:?}");

        // Tail overrun (binary_data_size beyond the tail) → 400.
        let head = json!({"inputs": [
            {"name": "text", "data": 1},
            {"name": "img", "parameters": {"binary_data_size": 10}}
        ]});
        let err = parse_root_inputs(
            EnsembleValue::Envelope { head, tail: Bytes::from_static(b"short") },
            Some(&decl),
            false,
        )
        .unwrap_err();
        assert!(matches!(err, AppError::InvalidRequestBody(_)), "got {err:?}");

        // Leftover tail bytes (more tail than declared) → 400.
        let head = json!({"inputs": [
            {"name": "text", "data": 1},
            {"name": "img", "parameters": {"binary_data_size": 1}}
        ]});
        let err = parse_root_inputs(
            EnsembleValue::Envelope { head, tail: Bytes::from_static(b"xx") },
            Some(&decl),
            false,
        )
        .unwrap_err();
        assert!(matches!(err, AppError::InvalidRequestBody(_)), "got {err:?}");

        // $binary_b64 marker as data (secondary in-JSON path) → Binary.
        let head = json!({"inputs": [
            {"name": "text", "data": 1},
            {"name": "img", "data": {"$binary_b64": "AAEC", "content_type": "image/jpeg"}}
        ]});
        let root = parse_root_inputs(EnsembleValue::Json(head), Some(&decl), false).unwrap();
        let RootInputs::Named { values, .. } = root else { panic!("named expected") };
        match &values["img"] {
            EnsembleValue::Binary(b, ct, _, _) => {
                assert_eq!(b.as_ref(), b"\x00\x01\x02", "base64 must decode");
                assert_eq!(ct, "image/jpeg");
            }
            other => panic!("binary expected, got {other:?}"),
        }
    }

    /// R19: legacy payloads — `$inputs` top-level key is a reserved
    /// namespace (400, D14); an envelope container without a declaration is
    /// 400 (TritonBinary keeps its historical rejection semantics).
    #[test]
    fn mimo_r19_legacy_reserved_namespace() {
        let err = parse_root_inputs(
            EnsembleValue::Json(json!({"$inputs": [{"name": "a"}]})),
            None,
            false,
        )
        .unwrap_err();
        assert!(matches!(err, AppError::InvalidRequestBody(_)), "got {err:?}");
        let err = parse_root_inputs(
            EnsembleValue::Envelope { head: json!({"inputs": []}), tail: Bytes::new() },
            None,
            false,
        )
        .unwrap_err();
        assert!(matches!(err, AppError::InvalidRequestBody(_)), "got {err:?}");
        // Ordinary legacy payload passes through untouched.
        let root =
            parse_root_inputs(EnsembleValue::Json(json!({"a": 1})), None, false).unwrap();
        assert!(matches!(root, RootInputs::Single(EnsembleValue::Json(_))));
    }

    /// D32: LSBE-1 in-frame container split — happy path and malformed
    /// branches (magic mismatch, head-length overflow, non-JSON head).
    #[test]
    fn mimo_lsbe1_split_envelope() {
        let head = br#"{"inputs": [{"name": "text", "data": 1}]}"#;
        let tail: &[u8] = b"\x01\x02\x03";
        let mut blob = Vec::new();
        blob.extend_from_slice(b"LSB1");
        blob.extend_from_slice(&(head.len() as u64).to_le_bytes());
        blob.extend_from_slice(head);
        blob.extend_from_slice(tail);
        let (v, t) = split_envelope(&blob).expect("valid container must split");
        assert_eq!(v, json!({"inputs": [{"name": "text", "data": 1}]}));
        assert_eq!(t.as_deref(), Some(tail));

        // Magic mismatch → 400.
        let err = split_envelope(b"XXXX................").unwrap_err();
        assert!(matches!(err, AppError::InvalidRequestBody(_)), "got {err:?}");
        // Head length beyond the blob → 400.
        let mut bad = Vec::new();
        bad.extend_from_slice(b"LSB1");
        bad.extend_from_slice(&999u64.to_le_bytes());
        bad.extend_from_slice(head);
        let err = split_envelope(&bad).unwrap_err();
        assert!(matches!(err, AppError::InvalidRequestBody(_)), "got {err:?}");
        // Head is not JSON → 400.
        let mut bad = Vec::new();
        bad.extend_from_slice(b"LSB1");
        bad.extend_from_slice(&5u64.to_le_bytes());
        bad.extend_from_slice(b"nope!");
        let err = split_envelope(&bad).unwrap_err();
        assert!(matches!(err, AppError::InvalidRequestBody(_)), "got {err:?}");
        // Truncated before the header field → 400.
        let err = split_envelope(b"LSB1\x01").unwrap_err();
        assert!(matches!(err, AppError::InvalidRequestBody(_)), "got {err:?}");
    }

    /// R10: a streaming step must not declare step.outputs (chunks have no
    /// named-output semantics, D11).
    #[test]
    fn mimo_r10_streaming_step_outputs_rejected() {
        let yaml = "ensemble:\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      stream: true\n      outputs:\n        crop:\n          type: binary\n      inputs: {x: \"$request\"}\n";
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("streaming step.outputs must be rejected (R10)");
        assert!(err.to_string().contains("outputs"), "got: {err}");
    }

    /// R7/R8: first-segment disambiguation — a declared step must be
    /// referenced by alias; unknown alias → error; binary alias paths → error.
    #[test]
    fn mimo_r7_r8_binary_alias_disambiguation() {
        // Whole ref on a declared step → rejected.
        let yaml = "ensemble:\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      outputs:\n        crop:\n          type: binary\n      inputs: {x: \"$request\"}\n    - name: b\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$a\"}\n";
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("whole ref on a declared step must be rejected (R7)");
        let _ = err;
        // Unknown alias → rejected.
        let yaml = "ensemble:\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      outputs:\n        crop:\n          type: binary\n      inputs: {x: \"$request\"}\n    - name: b\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$a.other\"}\n";
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("unknown alias must be rejected (R7)");
        let _ = err;
        // Binary alias with a further path → rejected (R8).
        let yaml = "ensemble:\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      outputs:\n        crop:\n          type: binary\n      inputs: {x: \"$request\"}\n    - name: b\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$a.crop.x\"}\n";
        let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err("binary alias path must be rejected (R8)");
        let _ = err;
        // Legal: whole binary alias.
        let yaml = "ensemble:\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      outputs:\n        crop:\n          type: binary\n      inputs: {x: \"$request\"}\n    - name: b\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$a.crop\"}\n";
        parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect("whole binary alias ref must parse");
    }

    /// D10 binary half (MIMO①): materialization — whole-response binary for
    /// a path-less alias, `$binary_b64` marker decode for a path-specified
    /// alias, and the type-mismatch error (declared binary, worker JSON).
    #[test]
    fn mimo_materialize_binary_outputs() {
        let step = EnsembleStep {
            name: "det".to_string(),
            model: "m".to_string(),
            version: Some("1".to_string()),
            inputs: HashMap::new(),
            when: None,
            stream: false,
            params: HashMap::new(),
            timeout_secs: None,
            on_error: OnErrorKind::Fail,
            retries: 0,
            outputs_decl: Some(
                [
                    ("thumb", StepOutputDecl {
                        ty: InputType::Binary,
                        path: Some("$.thumb".to_string()),
                    }),
                    ("raw", StepOutputDecl {
                        ty: InputType::Binary,
                        path: None,
                    }),
                ]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            ),
        };
        // JSON response + marker objects → two binary outputs.
        let raw = EnsembleValue::Json(json!({
            "thumb": {"$binary_b64": "AAEC", "content_type": "image/jpeg"}
        }));
        // (path-less alias needs the BINARY response form — separate case)
        let err = materialize_step_outputs(&step, raw).unwrap_err();
        assert!(
            err.to_string().contains("binary"),
            "path-less binary alias on a JSON response must error, got: {err}"
        );
        // Binary response → path-less alias passes through; marker alias
        // errors (it needs a JSON response).
        let raw = EnsembleValue::Binary(Bytes::from_static(b"\x00\x01"), "image/png".into(), None, None);
        let err = materialize_step_outputs(&step, raw).unwrap_err();
        assert!(
            err.to_string().contains("JSON"),
            "marker alias on a binary response must error, got: {err}"
        );
        // Mixed response case: whole-response alias only.
        let step_whole = EnsembleStep {
            outputs_decl: Some(
                [("raw", StepOutputDecl { ty: InputType::Binary, path: None })]
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v))
                    .collect(),
            ),
            ..step.clone()
        };
        let out = materialize_step_outputs(
            &step_whole,
            EnsembleValue::Binary(Bytes::from_static(b"\x00\x01"), "image/png".into(), None, None),
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, "det.raw");
        match &out[0].1 {
            EnsembleValue::Binary(b, ct, _, _) => {
                assert_eq!(b.as_ref(), b"\x00\x01");
                assert_eq!(ct, "image/png");
            }
            other => panic!("binary expected, got {other:?}"),
        }
        // Marker decode happy path (JSON response, marker alias only).
        let step_marker = EnsembleStep {
            outputs_decl: Some(
                [("thumb", StepOutputDecl { ty: InputType::Binary, path: Some("$.thumb".into()) })]
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v))
                    .collect(),
            ),
            ..step
        };
        let out = materialize_step_outputs(
            &step_marker,
            EnsembleValue::Json(json!({"thumb": {"$binary_b64": "AAEC", "content_type": "image/jpeg"}})),
        )
        .unwrap();
        match &out[0].1 {
            EnsembleValue::Binary(b, ct, _, _) => {
                assert_eq!(b.as_ref(), b"\x00\x01\x02");
                assert_eq!(ct, "image/jpeg");
            }
            other => panic!("binary expected, got {other:?}"),
        }
    }

    // ===== audit (batch 4/5 review): defect reproduction tests =====

    /// §5.5.7: legacy configs keep byte-identical payload classification.
    /// The old gRPC unary / server-streaming paths parsed ANY valid JSON
    /// (arrays, scalars, whitespace-prefixed objects) into Json; the
    /// `{`-sniff in ensemble_payload_from_bytes re-classifies them as
    /// Binary (regression, and HTTP/batch keep parsing them as Json —
    /// cross-transport parity break). Only the LSBE-1 magic may pre-empt
    /// JSON parsing (it can never be valid JSON).
    #[test]
    fn test_audit_legacy_payload_json_array_stays_json() {
        let payload =
            ensemble_payload_from_bytes(&Bytes::from_static(b"[1,2]"), None).unwrap();
        match payload {
            EnsembleValue::Json(v) => assert_eq!(v, json!([1, 2])),
            other => panic!("legacy JSON array must stay Json, got {other:?}"),
        }
    }

    /// R5/R13: `$inputs.NAME` refs in ensemble.outputs must name a DECLARED
    /// input — step inputs reject undeclared names at parse, outputs refs
    /// currently slip through and silently degrade to `data: null` at
    /// runtime (the D5 channel is for ABSENT sources, not config typos).
    #[test]
    fn test_audit_outputs_ref_undeclared_input_name_rejected() {
        let yaml = r#"ensemble:
  inputs:
    text: {type: json}
  outputs:
    a: "$inputs.nope"
  steps:
    - name: s
      model: m
      version: "1"
      inputs: {x: "$inputs.text"}
"#;
        let err =
            parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml")).unwrap_err();
        assert!(
            err.to_string().contains("undeclared input"),
            "an undeclared $inputs name in outputs must be a config error, got: {err}"
        );
    }

    /// R5: a legacy config (no inputs declaration) rejects ANY `$inputs.*`
    /// ref — step inputs are rejected, outputs refs currently slip through
    /// and degrade to null at runtime.
    #[test]
    fn test_audit_outputs_ref_inputs_namespace_rejected_legacy() {
        let yaml = r#"ensemble:
  outputs:
    a: "$inputs.x"
  steps:
    - name: s
      model: m
      version: "1"
      inputs: {x: "$request"}
"#;
        let err =
            parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml")).unwrap_err();
        assert!(
            err.to_string().contains("inputs"),
            "a legacy $inputs namespace ref in outputs must be a config error (R5), got: {err}"
        );
    }

    /// §5.5.3: multi-segment paths on UNDECLARED steps stay
    /// declaration-only. `$stepX.a.b` used to fail as a literal-key lookup;
    /// the new resolver silently takes the FIRST segment and drops `.b`,
    /// producing wrong data instead of an error.
    #[test]
    fn test_audit_legacy_multisegment_step_ref_not_truncated() {
        let plan = legacy_plan();
        let mut context = HashMap::new();
        context.insert(
            "s".to_string(),
            EnsembleValue::Json(json!({"a": {"b": 1}})),
        );
        assert!(
            resolve_ref(&plan, "$s.a.b", &context).is_err(),
            "multi-segment legacy step refs must be rejected, not silently truncated \
             to the first segment"
        );
    }

    /// Same rule for the legacy anonymous root: `$request.a.b` must not
    /// silently resolve to `request["a"]`.
    #[test]
    fn test_audit_legacy_multisegment_request_ref_not_truncated() {
        let plan = legacy_plan();
        let mut context = HashMap::new();
        context.insert(
            "request".to_string(),
            EnsembleValue::Json(json!({"a": {"b": 1}})),
        );
        assert!(
            resolve_ref(&plan, "$request.a.b", &context).is_err(),
            "multi-segment legacy request refs must be rejected, not silently truncated"
        );
    }

    /// B3 fix: a tolerated (unknown) binary element in a dags-form envelope
    /// must still consume its declared tail slice — header-order slicing
    /// means a skip that leaves bytes unaccounted misaligns every later
    /// binary element.
    #[test]
    fn test_audit_dags_tolerated_binary_element_consumes_tail() {
        let decl: IndexMap<String, InputDecl> = [(
            "img".to_string(),
            InputDecl {
                ty: InputType::Binary,
                required: true,
                default: None,
                content_type: Some("image/png".to_string()),
                shape: None,
                datatype: None,
            },
        )]
        .into_iter()
        .collect();
        let head = json!({"inputs": [
            {"name": "other_set_img", "parameters": {"binary_data_size": 2}},
            {"name": "img", "parameters": {"binary_data_size": 3}}
        ]});
        let root = parse_root_inputs(
            EnsembleValue::Envelope {
                head,
                tail: Bytes::from_static(b"\x00\x00\x01\x02\x03"),
            },
            Some(&decl),
            true,
        )
        .unwrap();
        let RootInputs::Named { values, .. } = root else { panic!("named expected") };
        match &values["img"] {
            EnsembleValue::Binary(b, _, _, _) => {
                assert_eq!(
                    b.as_ref(),
                    b"\x01\x02\x03",
                    "later elements must slice after the tolerated bytes"
                );
            }
            other => panic!("binary expected, got {other:?}"),
        }
    }
}
