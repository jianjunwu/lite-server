use crate::error::AppError;
use regex::Regex;

lazy_static::lazy_static! {
    static ref IDENTIFIER_RE: Regex = Regex::new(r"^[a-zA-Z0-9_-]+$").unwrap();
    static ref VERSION_RE: Regex = Regex::new(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$").unwrap();
}

const MAX_IDENTIFIER_LEN: usize = 64;

/// Validate that a model name contains only safe characters.
/// Allowed: alphanumeric, underscore, hyphen. Length: 1-64.
pub fn validate_identifier(s: &str) -> Result<(), AppError> {
    if s.is_empty() {
        return Err(AppError::Validation("identifier cannot be empty".to_string()));
    }
    if s.len() > MAX_IDENTIFIER_LEN {
        return Err(AppError::Validation(format!(
            "identifier exceeds maximum length of {} characters",
            MAX_IDENTIFIER_LEN
        )));
    }
    if !IDENTIFIER_RE.is_match(s) {
        return Err(AppError::Validation(
            "identifier contains invalid characters; allowed: a-z, A-Z, 0-9, _, -".to_string(),
        ));
    }
    Ok(())
}

/// Validate a version string. Like validate_identifier but also allows dots
/// (for semantic versioning like 1.0.0). Rejects ".." to prevent path traversal.
pub fn validate_version(s: &str) -> Result<(), AppError> {
    if s.is_empty() {
        return Err(AppError::Validation("version cannot be empty".to_string()));
    }
    if s.len() > MAX_IDENTIFIER_LEN {
        return Err(AppError::Validation(format!(
            "version exceeds maximum length of {} characters",
            MAX_IDENTIFIER_LEN
        )));
    }
    if s.contains("..") || s.starts_with('.') || s.ends_with('.') {
        return Err(AppError::Validation(
            "version contains invalid characters".to_string(),
        ));
    }
    if !VERSION_RE.is_match(s) {
        return Err(AppError::Validation(
            "version contains invalid characters; allowed: a-z, A-Z, 0-9, _, -, .".to_string(),
        ));
    }
    Ok(())
}

/// Resolve a model directory under the repository root and ensure it does not
/// escape the repository via path traversal. Returns the canonicalized path
/// if it exists, or the original joined path if it does not.
pub fn resolve_model_dir(
    repo_path: &std::path::Path,
    model_name: &str,
    version: &str,
) -> Result<std::path::PathBuf, AppError> {
    let model_dir = repo_path.join(model_name).join(version);

    // Defensive: reject any path containing parent-dir components
    if model_dir.components().any(|c| matches!(c, std::path::Component::ParentDir)) {
        return Err(AppError::ModelNotFound(format!(
            "{} version {} not found",
            model_name, version
        )));
    }

    if model_dir.exists() {
        let canonical = model_dir
            .canonicalize()
            .map_err(|e| AppError::Io(e))?;
        let canonical_repo = repo_path
            .canonicalize()
            .map_err(|e| AppError::Io(e))?;
        if !canonical.starts_with(&canonical_repo) {
            return Err(AppError::ModelNotFound(format!(
                "{} version {} not found",
                model_name, version
            )));
        }
        Ok(canonical)
    } else {
        Ok(model_dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_validate_identifier_accepts_valid() {
        assert!(validate_identifier("bert").is_ok());
        assert!(validate_identifier("model-v1").is_ok());
        assert!(validate_identifier("bert_base").is_ok());
        assert!(validate_identifier("MyModel_123").is_ok());
        assert!(validate_identifier("a").is_ok());
        assert!(validate_identifier(&"x".repeat(64)).is_ok());
    }

    #[test]
    fn test_validate_identifier_rejects_empty() {
        assert!(validate_identifier("").is_err());
    }

    #[test]
    fn test_validate_identifier_rejects_too_long() {
        let long = "a".repeat(65);
        assert!(validate_identifier(&long).is_err());
    }

    #[test]
    fn test_validate_identifier_rejects_traversal_and_special_chars() {
        assert!(validate_identifier("../../etc/passwd").is_err());
        assert!(validate_identifier("..").is_err());
        assert!(validate_identifier("model/name").is_err());
        assert!(validate_identifier("model\\name").is_err());
        assert!(validate_identifier("model name").is_err());
        assert!(validate_identifier("model@name").is_err());
        assert!(validate_identifier("model.name").is_err());
        assert!(validate_identifier("model%20name").is_err());
        assert!(validate_identifier("model\x00name").is_err());
    }

    #[test]
    fn test_validate_version_accepts_semver() {
        assert!(validate_version("1").is_ok());
        assert!(validate_version("1.0").is_ok());
        assert!(validate_version("1.0.0").is_ok());
        assert!(validate_version("v1").is_ok());
        assert!(validate_version("v1.0.0").is_ok());
        assert!(validate_version("my_version_test").is_ok());
    }

    #[test]
    fn test_validate_version_rejects_traversal() {
        assert!(validate_version("..").is_err());
        assert!(validate_version("../etc").is_err());
        assert!(validate_version("a..b").is_err());
    }

    #[test]
    fn test_validate_version_rejects_bad_chars() {
        assert!(validate_version("").is_err());
        assert!(validate_version("1.0.0.0").is_ok()); // 4 segments is fine as version
        assert!(validate_version("version/name").is_err());
        assert!(validate_version("v1.0.0-rc1").is_ok()); // hyphens ok
        assert!(validate_version(".1").is_err()); // leading dot rejected
        assert!(validate_version("1.").is_err()); // trailing dot rejected
    }

    #[test]
    fn test_resolve_model_dir_rejects_outside_repo() {
        let tmp = std::env::temp_dir().join(format!("lite-server-resolve-test-{}", std::process::id()));
        let repo = tmp.join("repo");
        let safe = repo.join("safe_model").join("1");
        std::fs::create_dir_all(&safe).unwrap();

        // Valid path inside repo should succeed
        let result = resolve_model_dir(&repo, "safe_model", "1");
        assert!(result.is_ok());
        let resolved = result.unwrap();
        assert!(resolved.starts_with(repo.canonicalize().unwrap()));

        // Path traversal attempting to escape repo
        let result = resolve_model_dir(&repo, "../outside", "1");
        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::ModelNotFound(_) => {}
            other => panic!("expected ModelNotFound, got {:?}", other),
        }

        // Non-existent path should return the joined path without error
        let result = resolve_model_dir(&repo, "nonexistent", "1");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), repo.join("nonexistent").join("1"));

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
