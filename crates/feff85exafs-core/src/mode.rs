use crate::domain::RunMode;
use feff85exafs_errors::{FeffError, Result, ValidationErrors};

pub const DEFAULT_RUN_MODE: RunMode = RunMode::Modern;

pub fn parse_run_mode(value: &str) -> Result<RunMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "legacy" => Ok(RunMode::Legacy),
        "modern" => Ok(RunMode::Modern),
        _ => Err(invalid_mode_error(value)),
    }
}

pub fn parse_run_mode_or_default(value: Option<&str>) -> Result<RunMode> {
    value.map(parse_run_mode).unwrap_or(Ok(DEFAULT_RUN_MODE))
}

pub fn run_mode_value(mode: RunMode) -> &'static str {
    match mode {
        RunMode::Legacy => "legacy",
        RunMode::Modern => "modern",
    }
}

fn invalid_mode_error(value: &str) -> FeffError {
    let mut errors = ValidationErrors::new();
    errors.push(
        "mode",
        format!("unsupported run mode `{value}` (expected `legacy` or `modern`)"),
    );
    FeffError::Validation(errors)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_run_mode_accepts_legacy_and_modern_case_insensitively() {
        assert_eq!(
            parse_run_mode("legacy").expect("legacy mode should parse"),
            RunMode::Legacy
        );
        assert_eq!(
            parse_run_mode("MoDeRn").expect("modern mode should parse"),
            RunMode::Modern
        );
    }

    #[test]
    fn parse_run_mode_rejects_unknown_values_with_structured_error() {
        let err = parse_run_mode("future")
            .expect_err("invalid mode should produce a structured validation error");
        match err {
            FeffError::Validation(validation) => {
                assert_eq!(validation.len(), 1);
                assert_eq!(validation.issues()[0].field, "mode");
                assert!(
                    validation.issues()[0]
                        .message
                        .contains("expected `legacy` or `modern`")
                );
            }
            other => panic!("unexpected error type: {other}"),
        }
    }

    #[test]
    fn parse_run_mode_or_default_uses_modern_by_default() {
        assert_eq!(
            parse_run_mode_or_default(None).expect("default mode should parse"),
            RunMode::Modern
        );
    }
}
