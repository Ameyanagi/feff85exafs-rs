use std::fs;
use std::path::Path;

use crate::baseline::{generate_noscf_manifests_for_mode, generate_withscf_manifests_for_mode};
use crate::domain::RunMode;
use feff85exafs_errors::{FeffError, Result, ValidationErrors};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BaselineCorpusVariant {
    NoScf,
    WithScf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BaselineRunRequest {
    pub variant: BaselineCorpusVariant,
    pub tests_root: String,
    pub output_root: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunOperation {
    Baseline(BaselineRunRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunRequest {
    pub mode: RunMode,
    pub operation: RunOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunConfigInput {
    InMemory { request: RunRequest },
    FilePath { path: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BaselineRunSuccess {
    pub variant: BaselineCorpusVariant,
    pub version: String,
    pub version_dir: String,
    pub manifest_count: usize,
    pub manifest_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunOperationSuccess {
    Baseline(BaselineRunSuccess),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunSuccess {
    pub mode: RunMode,
    pub result: RunOperationSuccess,
}

impl RunSuccess {
    pub fn baseline(&self) -> Option<&BaselineRunSuccess> {
        self.result.baseline()
    }
}

impl RunOperationSuccess {
    pub fn baseline(&self) -> Option<&BaselineRunSuccess> {
        match self {
            Self::Baseline(result) => Some(result),
        }
    }
}

impl RunRequest {
    pub fn validate(&self) -> Result<()> {
        let mut errors = ValidationErrors::new();

        match &self.operation {
            RunOperation::Baseline(operation) => {
                validate_non_empty(
                    &operation.tests_root,
                    "operation.baseline.tests_root",
                    &mut errors,
                );
                validate_non_empty(
                    &operation.output_root,
                    "operation.baseline.output_root",
                    &mut errors,
                );
                validate_non_empty(
                    &operation.version,
                    "operation.baseline.version",
                    &mut errors,
                );
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(FeffError::Validation(errors))
        }
    }
}

/// Execute a typed FEFF85EXAFS operation without shell wrappers.
///
/// # Examples
///
/// ```no_run
/// use feff85exafs_core::api::{
///     BaselineCorpusVariant, BaselineRunRequest, RunOperation, RunRequest, run,
/// };
/// use feff85exafs_core::domain::RunMode;
///
/// let request = RunRequest {
///     mode: RunMode::Modern,
///     operation: RunOperation::Baseline(BaselineRunRequest {
///         variant: BaselineCorpusVariant::NoScf,
///         tests_root: "feff85exafs/tests".to_string(),
///         output_root: "docs/migration/baselines".to_string(),
///         version: "v1".to_string(),
///     }),
/// };
///
/// let result = run(request)?;
/// let baseline = result.baseline().expect("baseline result");
/// println!("generated {} manifests", baseline.manifest_count);
/// # Ok::<(), feff85exafs_errors::FeffError>(())
/// ```
///
/// ```
/// use feff85exafs_core::api::{
///     BaselineCorpusVariant, BaselineRunRequest, RunOperation, RunRequest, run,
/// };
/// use feff85exafs_core::domain::RunMode;
/// use feff85exafs_errors::FeffError;
///
/// let request = RunRequest {
///     mode: RunMode::Modern,
///     operation: RunOperation::Baseline(BaselineRunRequest {
///         variant: BaselineCorpusVariant::NoScf,
///         tests_root: "feff85exafs/tests".to_string(),
///         output_root: "docs/migration/baselines".to_string(),
///         version: " ".to_string(),
///     }),
/// };
///
/// let err = run(request).expect_err("blank version should fail validation");
/// assert!(matches!(err, FeffError::Validation(_)));
/// ```
pub fn run(request: RunRequest) -> Result<RunSuccess> {
    request.validate()?;
    execute(request)
}

/// Execute a typed FEFF85EXAFS operation from either in-memory config data
/// or a file path to JSON-serialized [`RunRequest`].
pub fn run_with_config(config: RunConfigInput) -> Result<RunSuccess> {
    let request = resolve_run_request(config)?;
    run(request)
}

/// Execute a typed FEFF85EXAFS operation from a JSON config file.
pub fn run_from_file(path: impl AsRef<Path>) -> Result<RunSuccess> {
    let path = path.as_ref().to_string_lossy().into_owned();
    run_with_config(RunConfigInput::FilePath { path })
}

fn execute(request: RunRequest) -> Result<RunSuccess> {
    let result = match request.operation {
        RunOperation::Baseline(operation) => {
            RunOperationSuccess::Baseline(run_baseline_operation(&operation, request.mode)?)
        }
    };

    Ok(RunSuccess {
        mode: request.mode,
        result,
    })
}

fn resolve_run_request(config: RunConfigInput) -> Result<RunRequest> {
    match config {
        RunConfigInput::InMemory { request } => Ok(request),
        RunConfigInput::FilePath { path } => load_request_from_path(&path),
    }
}

fn load_request_from_path(path: &str) -> Result<RunRequest> {
    let mut errors = ValidationErrors::new();
    validate_non_empty(path, "config.path", &mut errors);
    if !errors.is_empty() {
        return Err(FeffError::Validation(errors));
    }

    let raw = fs::read_to_string(path)?;
    serde_json::from_str::<RunRequest>(&raw).map_err(|error| {
        FeffError::InvalidArgument(format!(
            "failed to parse run config JSON from `{path}`: {error}"
        ))
    })
}

fn run_baseline_operation(
    operation: &BaselineRunRequest,
    mode: RunMode,
) -> Result<BaselineRunSuccess> {
    let summary = match operation.variant {
        BaselineCorpusVariant::NoScf => generate_noscf_manifests_for_mode(
            Path::new(&operation.tests_root),
            Path::new(&operation.output_root),
            &operation.version,
            mode,
        )?,
        BaselineCorpusVariant::WithScf => generate_withscf_manifests_for_mode(
            Path::new(&operation.tests_root),
            Path::new(&operation.output_root),
            &operation.version,
            mode,
        )?,
    };

    Ok(BaselineRunSuccess {
        variant: operation.variant,
        version: operation.version.clone(),
        version_dir: path_string(&summary.version_dir),
        manifest_count: summary.case_count,
        manifest_paths: summary
            .manifest_paths
            .iter()
            .map(|path| path_string(path))
            .collect(),
    })
}

fn validate_non_empty(value: &str, field: &str, errors: &mut ValidationErrors) {
    if value.trim().is_empty() {
        errors.push(field, "must not be blank");
    }
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_output_root() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "feff85exafs-core-api-test-{}-{nonce}",
            std::process::id()
        ))
    }

    fn baseline_request(output_root: String) -> RunRequest {
        let tests_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../feff85exafs/tests")
            .to_string_lossy()
            .into_owned();

        RunRequest {
            mode: RunMode::Modern,
            operation: RunOperation::Baseline(BaselineRunRequest {
                variant: BaselineCorpusVariant::NoScf,
                tests_root,
                output_root,
                version: "vtest".to_string(),
            }),
        }
    }

    #[test]
    fn run_returns_structured_baseline_success() {
        let output_root = temp_output_root();
        let request = baseline_request(output_root.to_string_lossy().into_owned());

        let result = run(request).expect("baseline run should succeed");
        let baseline = result
            .baseline()
            .expect("baseline run result should be present");
        assert_eq!(baseline.variant, BaselineCorpusVariant::NoScf);
        assert!(baseline.manifest_count > 0);
        assert_eq!(baseline.manifest_count, baseline.manifest_paths.len());
        assert!(Path::new(&baseline.version_dir).is_dir());
        assert!(
            baseline
                .manifest_paths
                .iter()
                .all(|manifest| Path::new(manifest).is_file())
        );

        if output_root.exists() {
            fs::remove_dir_all(&output_root).expect("should clean temporary output directory");
        }
    }

    #[test]
    fn run_with_config_accepts_in_memory_request() {
        let output_root = temp_output_root();
        let request = baseline_request(output_root.to_string_lossy().into_owned());

        let result = run_with_config(RunConfigInput::InMemory { request })
            .expect("in-memory config run should succeed");
        let baseline = result
            .baseline()
            .expect("baseline run result should be present");
        assert!(baseline.manifest_count > 0);

        if output_root.exists() {
            fs::remove_dir_all(&output_root).expect("should clean temporary output directory");
        }
    }

    #[test]
    fn run_from_file_accepts_json_request() {
        let test_root = temp_output_root();
        let output_root = test_root.join("out");
        fs::create_dir_all(&test_root).expect("should create temp test root");
        let config_path = test_root.join("run-request.json");
        let request = baseline_request(output_root.to_string_lossy().into_owned());
        let request_json =
            serde_json::to_string_pretty(&request).expect("should serialize run request");
        fs::write(&config_path, format!("{request_json}\n")).expect("should write request config");

        let result = run_from_file(&config_path).expect("file-based config run should succeed");
        let baseline = result
            .baseline()
            .expect("baseline run result should be present");
        assert!(baseline.manifest_count > 0);

        if test_root.exists() {
            fs::remove_dir_all(&test_root).expect("should clean temporary test root");
        }
    }

    #[test]
    fn run_with_config_rejects_blank_config_path() {
        let err = run_with_config(RunConfigInput::FilePath {
            path: "   ".to_string(),
        })
        .expect_err("blank config path should fail validation");

        match err {
            FeffError::Validation(validation) => {
                assert_eq!(validation.len(), 1);
                assert_eq!(validation.issues()[0].field, "config.path");
            }
            other => panic!("unexpected error type: {other}"),
        }
    }

    #[test]
    fn run_returns_validation_error_for_blank_request_fields() {
        let err = run(RunRequest {
            mode: RunMode::Modern,
            operation: RunOperation::Baseline(BaselineRunRequest {
                variant: BaselineCorpusVariant::NoScf,
                tests_root: " ".to_string(),
                output_root: "".to_string(),
                version: "\n".to_string(),
            }),
        })
        .expect_err("blank request fields should fail validation");

        match err {
            FeffError::Validation(validation) => {
                let fields = validation
                    .issues()
                    .iter()
                    .map(|issue| issue.field.as_str())
                    .collect::<Vec<_>>();
                assert!(fields.contains(&"operation.baseline.tests_root"));
                assert!(fields.contains(&"operation.baseline.output_root"));
                assert!(fields.contains(&"operation.baseline.version"));
            }
            other => panic!("unexpected error type: {other}"),
        }
    }
}
