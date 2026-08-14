use bytes::Bytes;
use indexmap::IndexMap;
use regex::Regex;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

// ===== EnsembleValue: typed step input/output (B3, E6) =====

/// The materialized outputs of one step — `(context key, value)` pairs
/// (MIMO: `step.alias` keys; undeclared steps yield the single `step` key).
pub(crate) type StepResults = Vec<(String, EnsembleValue)>;

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
#[derive(Debug, PartialEq)]
pub enum EnsembleValue {
    Json(serde_json::Value),
    /// P2/P8 (batch 6): a raw-resident step output — the worker's JSON bytes
    /// kept unparsed in the context. Whole references splice the original
    /// bytes into downstream payloads (zero parse/re-serialize); a field
    /// access parses ONCE and caches the shared `Arc<Value>`.
    RawJson(Arc<RawJsonValue>),
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

/// P2/P8 (batch 6): raw-resident JSON bytes + a lazy once-parsed cache. The
/// cache holds the shared `Arc<Value>` so field accesses parse once and every
/// consumer shares one allocation.
#[derive(Debug)]
pub struct RawJsonValue {
    pub bytes: Bytes,
    parsed: std::sync::OnceLock<Arc<Value>>,
}

impl PartialEq for RawJsonValue {
    /// Equality on the BYTES (the parse cache is derived state).
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl RawJsonValue {
    pub fn new(bytes: Bytes) -> Self {
        Self {
            bytes,
            parsed: std::sync::OnceLock::new(),
        }
    }

    /// Parse once (racing parses discard the loser — equivalent values).
    pub fn parse(&self) -> Result<&Arc<Value>, serde_json::Error> {
        if let Some(v) = self.parsed.get() {
            return Ok(v);
        }
        let v = Arc::new(serde_json::from_slice(&self.bytes)?);
        let _ = self.parsed.set(v);
        Ok(self.parsed.get().unwrap())
    }
}

impl Clone for EnsembleValue {
    fn clone(&self) -> Self {
        match self {
            EnsembleValue::Json(v) => EnsembleValue::Json(v.clone()),
            EnsembleValue::RawJson(r) => EnsembleValue::RawJson(Arc::clone(r)),
            EnsembleValue::Binary(b, ct, shape, dt) => EnsembleValue::Binary(
                b.clone(),
                ct.clone(),
                shape.clone(),
                dt.clone(),
            ),
            EnsembleValue::Envelope { head, tail } => EnsembleValue::Envelope {
                head: head.clone(),
                tail: tail.clone(),
            },
        }
    }
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
    pub(crate) static ref REF_RE: Regex = Regex::new(r"^\$(\w+)(?:\.(.+))?$")
        .expect("invalid ensemble ref regex");
    /// R1: input/alias names — `[A-Za-z_][A-Za-z0-9_]*` (the `$inputs.NAME`
    /// grammar's first segment depends on it).
    pub(crate) static ref IDENT_RE: Regex = Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$")
        .expect("invalid ident regex");
    /// R6: step.outputs projection paths — `$.a.b` dot segments only, no
    /// array subscripts or filters (D29).
    pub(crate) static ref JSON_PATH_RE: Regex =
        Regex::new(r"^\$(\.[A-Za-z_][A-Za-z0-9_]*)+$").expect("invalid json path regex");
    /// E8-2: `when: "$ref OP literal"` — OP in the whitelisted set.
    pub(crate) static ref WHEN_RE: Regex =
        // `\s+` before the operator: a glued token (`$request.dagin [...]`)
        // must fail fast, not backtrack into target `dag` + op `in`.
        Regex::new(r"^\$(\S+)\s+(==|!=|contains|in)\s*(.+)$").expect("invalid when regex");
}

