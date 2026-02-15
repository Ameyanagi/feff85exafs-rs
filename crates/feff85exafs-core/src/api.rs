use std::fs;
use std::path::Path;

use crate::baseline::{generate_noscf_manifests_for_mode, generate_withscf_manifests_for_mode};
use crate::benchmark::{BenchmarkReport, BenchmarkReportRequest, generate_benchmark_report};
use crate::domain::RunMode;
use crate::parity::{ParityReport, ParityReportRequest, generate_parity_report};
use crate::workflow::{run_legacy_workflow, run_modern_workflow};
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
pub struct WorkflowRunRequest {
    pub input_path: String,
    pub working_root: String,
    #[serde(default)]
    pub legacy_runner: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunOperation {
    Baseline(BaselineRunRequest),
    Workflow(WorkflowRunRequest),
    ParityReport(ParityReportRequest),
    BenchmarkReport(BenchmarkReportRequest),
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BaselineRunSuccess {
    pub variant: BaselineCorpusVariant,
    pub version: String,
    pub version_dir: String,
    pub manifest_count: usize,
    pub manifest_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowRunSuccess {
    pub input_path: String,
    pub working_root: String,
    pub chi_dat: String,
    pub xmu_dat: String,
    pub sig2_dat: Option<String>,
    pub exchange_dat: Option<String>,
    pub fovrg_dat: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunOperationSuccess {
    Baseline(BaselineRunSuccess),
    Workflow(WorkflowRunSuccess),
    ParityReport(ParityReport),
    BenchmarkReport(BenchmarkReport),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunSuccess {
    pub mode: RunMode,
    pub result: RunOperationSuccess,
}

impl RunSuccess {
    pub fn baseline(&self) -> Option<&BaselineRunSuccess> {
        self.result.baseline()
    }

    pub fn workflow(&self) -> Option<&WorkflowRunSuccess> {
        self.result.workflow()
    }

    pub fn parity_report(&self) -> Option<&ParityReport> {
        self.result.parity_report()
    }

    pub fn benchmark_report(&self) -> Option<&BenchmarkReport> {
        self.result.benchmark_report()
    }
}

impl RunOperationSuccess {
    pub fn baseline(&self) -> Option<&BaselineRunSuccess> {
        match self {
            Self::Baseline(result) => Some(result),
            Self::Workflow(_) | Self::ParityReport(_) | Self::BenchmarkReport(_) => None,
        }
    }

    pub fn workflow(&self) -> Option<&WorkflowRunSuccess> {
        match self {
            Self::Workflow(result) => Some(result),
            Self::Baseline(_) | Self::ParityReport(_) | Self::BenchmarkReport(_) => None,
        }
    }

    pub fn parity_report(&self) -> Option<&ParityReport> {
        match self {
            Self::ParityReport(result) => Some(result),
            Self::Baseline(_) | Self::Workflow(_) | Self::BenchmarkReport(_) => None,
        }
    }

    pub fn benchmark_report(&self) -> Option<&BenchmarkReport> {
        match self {
            Self::BenchmarkReport(result) => Some(result),
            Self::Baseline(_) | Self::Workflow(_) | Self::ParityReport(_) => None,
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
            RunOperation::Workflow(operation) => {
                validate_non_empty(
                    &operation.input_path,
                    "operation.workflow.input_path",
                    &mut errors,
                );
                validate_non_empty(
                    &operation.working_root,
                    "operation.workflow.working_root",
                    &mut errors,
                );
                if self.mode == RunMode::Legacy {
                    match operation.legacy_runner.as_deref() {
                        Some(value) => validate_non_empty(
                            value,
                            "operation.workflow.legacy_runner",
                            &mut errors,
                        ),
                        None => errors.push(
                            "operation.workflow.legacy_runner",
                            "must be provided when mode is `legacy`",
                        ),
                    }
                }
            }
            RunOperation::ParityReport(operation) => {
                validate_non_empty(
                    &operation.tests_root,
                    "operation.parity_report.tests_root",
                    &mut errors,
                );
                validate_non_empty(
                    &operation.working_root,
                    "operation.parity_report.working_root",
                    &mut errors,
                );
            }
            RunOperation::BenchmarkReport(operation) => {
                validate_non_empty(
                    &operation.tests_root,
                    "operation.benchmark_report.tests_root",
                    &mut errors,
                );
                validate_non_empty(
                    &operation.working_root,
                    "operation.benchmark_report.working_root",
                    &mut errors,
                );
                validate_non_empty(
                    &operation.legacy_runner,
                    "operation.benchmark_report.legacy_runner",
                    &mut errors,
                );
                if operation.iterations == 0 {
                    errors.push("operation.benchmark_report.iterations", "must be >= 1");
                }
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
        RunOperation::Workflow(operation) => {
            RunOperationSuccess::Workflow(run_workflow_operation(&operation, request.mode)?)
        }
        RunOperation::ParityReport(operation) => {
            RunOperationSuccess::ParityReport(run_parity_report_operation(&operation)?)
        }
        RunOperation::BenchmarkReport(operation) => {
            RunOperationSuccess::BenchmarkReport(run_benchmark_report_operation(&operation)?)
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

fn run_workflow_operation(
    operation: &WorkflowRunRequest,
    mode: RunMode,
) -> Result<WorkflowRunSuccess> {
    match mode {
        RunMode::Modern => {
            let output = run_modern_workflow(
                Path::new(&operation.input_path),
                Path::new(&operation.working_root),
            )?;

            Ok(WorkflowRunSuccess {
                input_path: operation.input_path.clone(),
                working_root: operation.working_root.clone(),
                chi_dat: output.ff2x_output.chi_dat,
                xmu_dat: output.ff2x_output.xmu_dat,
                sig2_dat: Some(output.debye_output.sig2_dat),
                exchange_dat: Some(output.exch_output.exchange_dat),
                fovrg_dat: Some(output.fovrg_output.fovrg_dat),
            })
        }
        RunMode::Legacy => {
            let legacy_runner = operation.legacy_runner.as_deref().ok_or_else(|| {
                let mut errors = ValidationErrors::new();
                errors.push(
                    "operation.workflow.legacy_runner",
                    "must be provided when mode is `legacy`",
                );
                FeffError::Validation(errors)
            })?;

            let output = run_legacy_workflow(
                Path::new(&operation.input_path),
                Path::new(&operation.working_root),
                legacy_runner,
            )?;

            Ok(WorkflowRunSuccess {
                input_path: operation.input_path.clone(),
                working_root: operation.working_root.clone(),
                chi_dat: output.chi_dat,
                xmu_dat: output.xmu_dat,
                sig2_dat: None,
                exchange_dat: None,
                fovrg_dat: None,
            })
        }
    }
}

fn run_parity_report_operation(operation: &ParityReportRequest) -> Result<ParityReport> {
    generate_parity_report(operation)
}

fn run_benchmark_report_operation(operation: &BenchmarkReportRequest) -> Result<BenchmarkReport> {
    generate_benchmark_report(operation)
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

    fn parity_fixture_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../feff85exafs/tests")
    }

    fn workflow_fixture_input() -> String {
        parity_fixture_root()
            .join("Copper/baseline/noSCF/feff.inp")
            .to_string_lossy()
            .into_owned()
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
    fn run_returns_structured_parity_report_success() {
        let temp_root = temp_output_root();
        let tests_root = temp_root.join("tests");
        let baseline_root = tests_root.join("Copper/baseline/noSCF");
        fs::create_dir_all(&baseline_root).expect("baseline fixture root should be created");

        let fixture = parity_fixture_root().join("Copper/baseline/noSCF");
        for file_name in ["feff.inp", "chi.dat", "xmu.dat"] {
            fs::copy(fixture.join(file_name), baseline_root.join(file_name))
                .unwrap_or_else(|error| panic!("failed to copy fixture {file_name}: {error}"));
        }

        let request = RunRequest {
            mode: RunMode::Modern,
            operation: RunOperation::ParityReport(ParityReportRequest {
                tests_root: path_string(&tests_root),
                working_root: path_string(&temp_root.join("work")),
            }),
        };

        let result = run(request).expect("parity report run should succeed");
        let report = result
            .parity_report()
            .expect("parity report result should be present");
        assert_eq!(report.case_count, 1);
        assert_eq!(report.file_count, 2);
        assert_eq!(report.cases[0].file_reports.len(), 2);

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("should clean temporary output directory");
        }
    }

    #[test]
    fn run_returns_structured_workflow_success_in_modern_mode() {
        let temp_root = temp_output_root();
        let request = RunRequest {
            mode: RunMode::Modern,
            operation: RunOperation::Workflow(WorkflowRunRequest {
                input_path: workflow_fixture_input(),
                working_root: path_string(&temp_root.join("workflow")),
                legacy_runner: None,
            }),
        };

        let result = run(request).expect("modern workflow run should succeed");
        let workflow = result
            .workflow()
            .expect("workflow run result should be present");
        assert!(Path::new(&workflow.chi_dat).is_file());
        assert!(Path::new(&workflow.xmu_dat).is_file());
        assert!(
            workflow
                .sig2_dat
                .as_deref()
                .is_some_and(|path| Path::new(path).is_file())
        );
        assert!(
            workflow
                .exchange_dat
                .as_deref()
                .is_some_and(|path| Path::new(path).is_file())
        );
        assert!(
            workflow
                .fovrg_dat
                .as_deref()
                .is_some_and(|path| Path::new(path).is_file())
        );

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("should clean temporary output directory");
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

    #[test]
    fn run_returns_validation_error_for_blank_parity_request_fields() {
        let err = run(RunRequest {
            mode: RunMode::Modern,
            operation: RunOperation::ParityReport(ParityReportRequest {
                tests_root: " ".to_string(),
                working_root: "".to_string(),
            }),
        })
        .expect_err("blank parity request fields should fail validation");

        match err {
            FeffError::Validation(validation) => {
                let fields = validation
                    .issues()
                    .iter()
                    .map(|issue| issue.field.as_str())
                    .collect::<Vec<_>>();
                assert!(fields.contains(&"operation.parity_report.tests_root"));
                assert!(fields.contains(&"operation.parity_report.working_root"));
            }
            other => panic!("unexpected error type: {other}"),
        }
    }

    #[test]
    fn run_returns_validation_error_for_blank_benchmark_request_fields() {
        let err = run(RunRequest {
            mode: RunMode::Modern,
            operation: RunOperation::BenchmarkReport(BenchmarkReportRequest {
                tests_root: " ".to_string(),
                working_root: "".to_string(),
                legacy_runner: "\n".to_string(),
                iterations: 0,
                warmup_iterations: 0,
            }),
        })
        .expect_err("blank benchmark request fields should fail validation");

        match err {
            FeffError::Validation(validation) => {
                let fields = validation
                    .issues()
                    .iter()
                    .map(|issue| issue.field.as_str())
                    .collect::<Vec<_>>();
                assert!(fields.contains(&"operation.benchmark_report.tests_root"));
                assert!(fields.contains(&"operation.benchmark_report.working_root"));
                assert!(fields.contains(&"operation.benchmark_report.legacy_runner"));
                assert!(fields.contains(&"operation.benchmark_report.iterations"));
            }
            other => panic!("unexpected error type: {other}"),
        }
    }

    #[test]
    fn run_returns_validation_error_for_missing_legacy_runner_in_legacy_workflow_mode() {
        let err = run(RunRequest {
            mode: RunMode::Legacy,
            operation: RunOperation::Workflow(WorkflowRunRequest {
                input_path: workflow_fixture_input(),
                working_root: path_string(&temp_output_root().join("workflow-legacy")),
                legacy_runner: None,
            }),
        })
        .expect_err("legacy workflow mode should require explicit legacy runner");

        match err {
            FeffError::Validation(validation) => {
                let fields = validation
                    .issues()
                    .iter()
                    .map(|issue| issue.field.as_str())
                    .collect::<Vec<_>>();
                assert!(fields.contains(&"operation.workflow.legacy_runner"));
            }
            other => panic!("unexpected error type: {other}"),
        }
    }

    #[cfg(unix)]
    mod unix {
        use super::*;
        use std::fs::File;
        use std::io::{self, Write};
        use std::os::unix::fs::PermissionsExt;

        fn write_executable(path: &Path, content: &str) -> io::Result<()> {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }

            let mut file = File::create(path)?;
            file.write_all(content.as_bytes())?;

            let mut permissions = fs::metadata(path)?.permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(path, permissions)?;
            Ok(())
        }

        #[test]
        fn run_returns_structured_workflow_success_in_legacy_mode() {
            let temp_root = temp_output_root();
            let legacy_runner = temp_root.join("fake-legacy-runner.sh");
            write_executable(
                &legacy_runner,
                "#!/bin/sh\nset -eu\nprintf '0.0 0.0\\n' > chi.dat\nprintf '0.0 0.0\\n' > xmu.dat\n",
            )
            .expect("fake legacy runner should be created");

            let request = RunRequest {
                mode: RunMode::Legacy,
                operation: RunOperation::Workflow(WorkflowRunRequest {
                    input_path: workflow_fixture_input(),
                    working_root: path_string(&temp_root.join("workflow-legacy")),
                    legacy_runner: Some(path_string(&legacy_runner)),
                }),
            };

            let result = run(request).expect("legacy workflow run should succeed");
            let workflow = result
                .workflow()
                .expect("workflow run result should be present");
            assert!(Path::new(&workflow.chi_dat).is_file());
            assert!(Path::new(&workflow.xmu_dat).is_file());
            assert!(workflow.sig2_dat.is_none());
            assert!(workflow.exchange_dat.is_none());
            assert!(workflow.fovrg_dat.is_none());

            if temp_root.exists() {
                fs::remove_dir_all(&temp_root).expect("should clean temporary output directory");
            }
        }
    }
}
