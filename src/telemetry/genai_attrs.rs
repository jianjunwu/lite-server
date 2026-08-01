//! P-TRACE (调研 A5 / D34): OpenTelemetry GenAI semantic-convention attribute names,
//! centralized in ONE place.
//!
//! `gen_ai.*` semconv is still **Development** as of 2026-07 (moved to a separate
//! repo in 2026-06, no versioned release, large rename in 2025-08). We therefore
//! **do not pin specific fields** — we centralize the names we emit here so any
//! future semconv churn is a one-file edit, not a hunt across handlers. Re-evaluate
//! when a stable `gen_ai.*` release lands (蓝图 §2.2 观察名单, 6–12 months).
//!
//! These are span attribute *keys*; values are recorded on inference spans
//! (model/version/request_id already ride the existing `info_span!` fields — the
//! gen_ai.* names below are reserved for richer GenAI telemetry in a follow-up and
//! to document the naming surface).

#![allow(dead_code)] // reserved naming surface; consumed as OTel stabilizes

/// Top-level namespace prefix.
pub const NAMESPACE: &str = "gen_ai";

/// `gen_ai.system` — the model provider/system identity (e.g. "lite-server").
pub const SYSTEM: &str = "gen_ai.system";

/// `gen_ai.request.model` — the model name requested by the client.
pub const REQUEST_MODEL: &str = "gen_ai.request.model";

/// `gen_ai.response.model` — the resolved model version that served the request.
pub const RESPONSE_MODEL: &str = "gen_ai.response.model";

/// `gen_ai.operation.name` — e.g. "infer", "stream", "embed".
pub const OPERATION_NAME: &str = "gen_ai.operation.name";

/// `gen_ai.usage.input_tokens` — prompt tokens consumed.
pub const USAGE_INPUT_TOKENS: &str = "gen_ai.usage.input_tokens";

/// `gen_ai.usage.output_tokens` — completion tokens produced.
pub const USAGE_OUTPUT_TOKENS: &str = "gen_ai.usage.output_tokens";

#[cfg(test)]
mod tests {
    use super::*;

    /// Names are centralized and stable strings (D34): a future semconv rename is a
    /// one-file edit. This guards against accidental drift by pinning the set we use.
    #[test]
    fn genai_attribute_names_are_centralized_and_prefixed() {
        for name in [SYSTEM, REQUEST_MODEL, RESPONSE_MODEL, OPERATION_NAME, USAGE_INPUT_TOKENS, USAGE_OUTPUT_TOKENS] {
            assert!(
                name.starts_with("gen_ai."),
                "GenAI semconv attribute must live under gen_ai.* (got {name})"
            );
        }
        assert_eq!(NAMESPACE, "gen_ai");
    }
}
