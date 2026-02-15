use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process;

use feff85exafs_core::api::{
    BaselineCorpusVariant, BaselineRunRequest, RunOperation, RunRequest, WorkflowRunRequest,
    run as run_modern_api,
};
use feff85exafs_core::benchmark::{BenchmarkReport, BenchmarkReportRequest};
use feff85exafs_core::domain::RunMode;
use feff85exafs_core::legacy::{legacy_stage_name, legacy_stage_order};
use feff85exafs_core::mode::{parse_run_mode_or_default, run_mode_value};
use feff85exafs_core::parity::{ParityReport, ParityReportRequest};
use feff85exafs_errors::{FeffError, Result};

#[derive(Debug, Clone)]
enum CliCommand {
    Baseline(BaselineCommand),
    WorkflowRun(WorkflowRunCommand),
    ParityReport(ParityReportCommand),
    BenchmarkReport(BenchmarkReportCommand),
}

#[derive(Debug, Clone, Copy)]
enum BaselineVariant {
    NoScf,
    WithScf,
}

#[derive(Debug, Clone)]
struct BaselineCommand {
    variant: BaselineVariant,
    mode: RunMode,
    tests_root: PathBuf,
    output_root: PathBuf,
    version: String,
}

#[derive(Debug, Clone)]
struct WorkflowRunCommand {
    mode: RunMode,
    input_path: PathBuf,
    working_root: PathBuf,
    legacy_runner: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct ParityReportCommand {
    tests_root: PathBuf,
    working_root: PathBuf,
    max_abs_delta: Option<f64>,
    max_rms_delta: Option<f64>,
    approved_regression_ids: BTreeSet<String>,
    approved_regression_files: Vec<PathBuf>,
}

#[derive(Debug, Clone)]
struct BenchmarkReportCommand {
    tests_root: PathBuf,
    working_root: PathBuf,
    legacy_runner: PathBuf,
    iterations: usize,
    warmup_iterations: usize,
    json_output: PathBuf,
}

#[derive(Debug, Clone, Copy)]
struct RegressionThresholds {
    max_abs_delta: f64,
    max_rms_delta: f64,
}

#[derive(Debug, Clone)]
struct RegressionFinding {
    id: String,
    max_abs_delta: f64,
    rms_delta: f64,
    approved: bool,
}

impl BaselineVariant {
    fn parse(value: &str) -> Option<Self> {
        let normalized = value.to_ascii_lowercase();
        match normalized.as_str() {
            "noscf" => Some(Self::NoScf),
            "scf" | "withscf" | "with-scf" => Some(Self::WithScf),
            _ => None,
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Self::NoScf => "noSCF",
            Self::WithScf => "withSCF",
        }
    }

    fn to_api_variant(self) -> BaselineCorpusVariant {
        match self {
            Self::NoScf => BaselineCorpusVariant::NoScf,
            Self::WithScf => BaselineCorpusVariant::WithScf,
        }
    }
}

impl ParityReportCommand {
    fn thresholds(&self) -> Option<RegressionThresholds> {
        match (self.max_abs_delta, self.max_rms_delta) {
            (Some(max_abs_delta), Some(max_rms_delta)) => Some(RegressionThresholds {
                max_abs_delta,
                max_rms_delta,
            }),
            _ => None,
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() || args.iter().any(|arg| arg == "-h" || arg == "--help") {
        print_usage();
        return;
    }

    match run_cli(args) {
        Ok(()) => {}
        Err(error) => {
            eprintln!("error: {error}");
            eprintln!();
            print_usage();
            process::exit(1);
        }
    }
}

fn run_cli(args: Vec<String>) -> Result<()> {
    let command = parse_command(&args)?;
    match command {
        CliCommand::Baseline(command) => run_baseline_command(command),
        CliCommand::WorkflowRun(command) => run_workflow_run_command(command),
        CliCommand::ParityReport(command) => run_parity_report_command(command),
        CliCommand::BenchmarkReport(command) => run_benchmark_report_command(command),
    }
}

fn run_baseline_command(command: BaselineCommand) -> Result<()> {
    let request = RunRequest {
        mode: command.mode,
        operation: RunOperation::Baseline(BaselineRunRequest {
            variant: command.variant.to_api_variant(),
            tests_root: command.tests_root.to_string_lossy().into_owned(),
            output_root: command.output_root.to_string_lossy().into_owned(),
            version: command.version.clone(),
        }),
    };

    let run_result = run_modern_api(request)?;
    let summary = run_result
        .baseline()
        .expect("baseline command should always return baseline success");

    println!("Using run mode: {}", run_mode_value(command.mode));
    if command.mode == RunMode::Legacy {
        let legacy_order = legacy_stage_order()
            .iter()
            .map(|stage| legacy_stage_name(*stage))
            .collect::<Vec<_>>()
            .join(" -> ");
        println!("Legacy stage order: {legacy_order}");
        println!("Legacy default behavior: preserve historical baseline filenames");
    }
    println!(
        "Generated {} {} manifests in {}",
        summary.manifest_count,
        command.variant.label(),
        summary.version_dir
    );
    println!("Manifest files:");
    for manifest in &summary.manifest_paths {
        println!("  {manifest}");
    }
    Ok(())
}

fn run_workflow_run_command(command: WorkflowRunCommand) -> Result<()> {
    let default_legacy_runner = PathBuf::from("feff85exafs/bin/f85e");
    let legacy_runner = match command.mode {
        RunMode::Legacy => Some(
            command
                .legacy_runner
                .clone()
                .unwrap_or(default_legacy_runner)
                .to_string_lossy()
                .into_owned(),
        ),
        RunMode::Modern => command
            .legacy_runner
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned()),
    };

    let request = RunRequest {
        mode: command.mode,
        operation: RunOperation::Workflow(WorkflowRunRequest {
            input_path: command.input_path.to_string_lossy().into_owned(),
            working_root: command.working_root.to_string_lossy().into_owned(),
            legacy_runner,
        }),
    };

    let run_result = run_modern_api(request)?;
    let summary = run_result
        .workflow()
        .expect("workflow command should always return workflow success");

    println!("Using run mode: {}", run_mode_value(command.mode));
    if command.mode == RunMode::Legacy {
        let legacy_order = legacy_stage_order()
            .iter()
            .map(|stage| legacy_stage_name(*stage))
            .collect::<Vec<_>>()
            .join(" -> ");
        println!("Legacy stage order: {legacy_order}");
    }
    println!("Input: {}", summary.input_path);
    println!("Working root: {}", summary.working_root);
    println!("Artifacts:");
    println!("  {}", summary.chi_dat);
    println!("  {}", summary.xmu_dat);
    if let Some(sig2_dat) = &summary.sig2_dat {
        println!("  {sig2_dat}");
    }
    if let Some(exchange_dat) = &summary.exchange_dat {
        println!("  {exchange_dat}");
    }
    if let Some(fovrg_dat) = &summary.fovrg_dat {
        println!("  {fovrg_dat}");
    }

    Ok(())
}

fn run_parity_report_command(command: ParityReportCommand) -> Result<()> {
    let request = RunRequest {
        mode: RunMode::Modern,
        operation: RunOperation::ParityReport(ParityReportRequest {
            tests_root: command.tests_root.to_string_lossy().into_owned(),
            working_root: command.working_root.to_string_lossy().into_owned(),
        }),
    };

    let run_result = run_modern_api(request)?;
    let report = run_result
        .parity_report()
        .expect("parity command should always return parity report success");

    print_parity_report(report);

    let thresholds = command.thresholds();
    if let Some(thresholds) = thresholds {
        let approved = load_approved_regressions(&command)?;
        let regressions = evaluate_regressions(report, thresholds, &approved);
        let unapproved = regressions
            .iter()
            .filter(|finding| !finding.approved)
            .count();

        println!();
        println!(
            "Regression gate: max_abs_delta <= {:.6e}, rms_delta <= {:.6e}",
            thresholds.max_abs_delta, thresholds.max_rms_delta
        );
        println!(
            "Detected {} regression(s), {} approved, {} unapproved",
            regressions.len(),
            regressions.len().saturating_sub(unapproved),
            unapproved
        );

        for finding in &regressions {
            let status = if finding.approved {
                "approved"
            } else {
                "unapproved"
            };
            println!(
                "  {status}: {} max_abs={:.6e} rms={:.6e}",
                finding.id, finding.max_abs_delta, finding.rms_delta
            );
        }

        if unapproved > 0 {
            return Err(FeffError::InvalidArgument(format!(
                "parity regression gate failed with {unapproved} unapproved regression(s)"
            )));
        }
    }

    Ok(())
}

fn run_benchmark_report_command(command: BenchmarkReportCommand) -> Result<()> {
    let request = RunRequest {
        mode: RunMode::Modern,
        operation: RunOperation::BenchmarkReport(BenchmarkReportRequest {
            tests_root: command.tests_root.to_string_lossy().into_owned(),
            working_root: command.working_root.to_string_lossy().into_owned(),
            legacy_runner: command.legacy_runner.to_string_lossy().into_owned(),
            iterations: command.iterations,
            warmup_iterations: command.warmup_iterations,
        }),
    };

    let run_result = run_modern_api(request)?;
    let report = run_result
        .benchmark_report()
        .expect("benchmark command should always return benchmark report success");

    print_benchmark_report(report);
    write_benchmark_report_json(report, &command.json_output)?;
    println!(
        "Machine-readable benchmark report: {}",
        command.json_output.to_string_lossy()
    );

    Ok(())
}

fn parse_command(args: &[String]) -> Result<CliCommand> {
    match args.first().map(String::as_str) {
        Some("baseline") => Ok(CliCommand::Baseline(parse_baseline_command(args)?)),
        Some("workflow") => Ok(CliCommand::WorkflowRun(parse_workflow_run_command(args)?)),
        Some("parity") => Ok(CliCommand::ParityReport(parse_parity_report_command(args)?)),
        Some("benchmark") => Ok(CliCommand::BenchmarkReport(parse_benchmark_report_command(
            args,
        )?)),
        Some(other) => Err(FeffError::InvalidArgument(format!(
            "unsupported command `{other}` (expected `baseline`, `workflow run`, `parity report`, or `benchmark report`)"
        ))),
        None => Err(FeffError::InvalidArgument(
            "missing command (expected `baseline`, `workflow run`, `parity report`, or `benchmark report`)"
                .to_string(),
        )),
    }
}

fn parse_baseline_command(args: &[String]) -> Result<BaselineCommand> {
    if args.first().map(String::as_str) != Some("baseline") {
        return Err(FeffError::InvalidArgument(
            "baseline command must start with `baseline`".to_string(),
        ));
    }
    let variant = args
        .get(1)
        .ok_or_else(|| {
            FeffError::InvalidArgument(
                "baseline variant must be one of: `noscf`, `scf`".to_string(),
            )
        })
        .and_then(|value| {
            BaselineVariant::parse(value).ok_or_else(|| {
                FeffError::InvalidArgument(format!(
                    "unsupported baseline variant `{value}` (expected `noscf` or `scf`)"
                ))
            })
        })?;

    let mut tests_root = PathBuf::from("feff85exafs/tests");
    let mut output_root = PathBuf::from("docs/migration/baselines");
    let mut version = "v1".to_string();
    let mut mode_arg: Option<&str> = None;

    let mut idx = 2;
    while idx < args.len() {
        match args[idx].as_str() {
            "--mode" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--mode"))?;
                mode_arg = Some(value.as_str());
            }
            "--tests-root" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--tests-root"))?;
                tests_root = PathBuf::from(value);
            }
            "--output-root" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--output-root"))?;
                output_root = PathBuf::from(value);
            }
            "--version" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--version"))?;
                version = value.clone();
            }
            unknown => {
                return Err(FeffError::InvalidArgument(format!(
                    "unknown argument `{unknown}`"
                )));
            }
        }
        idx += 1;
    }

    if version.trim().is_empty() {
        return Err(FeffError::InvalidArgument(
            "version cannot be empty".to_string(),
        ));
    }

    let mode = parse_run_mode_or_default(mode_arg)?;

    Ok(BaselineCommand {
        variant,
        mode,
        tests_root,
        output_root,
        version,
    })
}

fn parse_workflow_run_command(args: &[String]) -> Result<WorkflowRunCommand> {
    if args.first().map(String::as_str) != Some("workflow")
        || args.get(1).map(String::as_str) != Some("run")
    {
        return Err(FeffError::InvalidArgument(
            "workflow command must be `workflow run`".to_string(),
        ));
    }

    let mut mode_arg: Option<&str> = None;
    let mut input_path = PathBuf::from("feff.inp");
    let mut working_root = PathBuf::from("target/workflow-run");
    let mut legacy_runner: Option<PathBuf> = None;

    let mut idx = 2;
    while idx < args.len() {
        match args[idx].as_str() {
            "--mode" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--mode"))?;
                mode_arg = Some(value.as_str());
            }
            "--input" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--input"))?;
                input_path = PathBuf::from(value);
            }
            "--working-root" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--working-root"))?;
                working_root = PathBuf::from(value);
            }
            "--legacy-runner" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--legacy-runner"))?;
                legacy_runner = Some(PathBuf::from(value));
            }
            unknown => {
                return Err(FeffError::InvalidArgument(format!(
                    "unknown argument `{unknown}`"
                )));
            }
        }
        idx += 1;
    }

    let mode = parse_run_mode_or_default(mode_arg)?;
    Ok(WorkflowRunCommand {
        mode,
        input_path,
        working_root,
        legacy_runner,
    })
}

fn parse_parity_report_command(args: &[String]) -> Result<ParityReportCommand> {
    if args.first().map(String::as_str) != Some("parity")
        || args.get(1).map(String::as_str) != Some("report")
    {
        return Err(FeffError::InvalidArgument(
            "parity command must be `parity report`".to_string(),
        ));
    }

    let mut tests_root = PathBuf::from("feff85exafs/tests");
    let mut working_root = PathBuf::from("target/parity-report");
    let mut max_abs_delta = None;
    let mut max_rms_delta = None;
    let mut approved_regression_ids = BTreeSet::new();
    let mut approved_regression_files = Vec::new();

    let mut idx = 2;
    while idx < args.len() {
        match args[idx].as_str() {
            "--tests-root" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--tests-root"))?;
                tests_root = PathBuf::from(value);
            }
            "--working-root" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--working-root"))?;
                working_root = PathBuf::from(value);
            }
            "--max-abs-delta" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--max-abs-delta"))?;
                max_abs_delta = Some(parse_non_negative_f64("--max-abs-delta", value)?);
            }
            "--max-rms-delta" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--max-rms-delta"))?;
                max_rms_delta = Some(parse_non_negative_f64("--max-rms-delta", value)?);
            }
            "--allow-regression" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--allow-regression"))?;
                if value.trim().is_empty() {
                    return Err(FeffError::InvalidArgument(
                        "--allow-regression value cannot be blank".to_string(),
                    ));
                }
                approved_regression_ids.insert(value.trim().to_string());
            }
            "--allow-regression-file" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--allow-regression-file"))?;
                approved_regression_files.push(PathBuf::from(value));
            }
            unknown => {
                return Err(FeffError::InvalidArgument(format!(
                    "unknown argument `{unknown}`"
                )));
            }
        }
        idx += 1;
    }

    match (max_abs_delta, max_rms_delta) {
        (Some(_), Some(_)) | (None, None) => {}
        _ => {
            return Err(FeffError::InvalidArgument(
                "parity regression gating requires both `--max-abs-delta` and `--max-rms-delta`"
                    .to_string(),
            ));
        }
    }

    Ok(ParityReportCommand {
        tests_root,
        working_root,
        max_abs_delta,
        max_rms_delta,
        approved_regression_ids,
        approved_regression_files,
    })
}

fn parse_benchmark_report_command(args: &[String]) -> Result<BenchmarkReportCommand> {
    if args.first().map(String::as_str) != Some("benchmark")
        || args.get(1).map(String::as_str) != Some("report")
    {
        return Err(FeffError::InvalidArgument(
            "benchmark command must be `benchmark report`".to_string(),
        ));
    }

    let mut tests_root = PathBuf::from("feff85exafs/tests");
    let mut working_root = PathBuf::from("target/benchmark-report");
    let mut legacy_runner = PathBuf::from("feff85exafs/bin/f85e");
    let mut iterations = 1usize;
    let mut warmup_iterations = 0usize;
    let mut json_output = PathBuf::from("target/benchmark-report/benchmark-report.json");

    let mut idx = 2;
    while idx < args.len() {
        match args[idx].as_str() {
            "--tests-root" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--tests-root"))?;
                tests_root = PathBuf::from(value);
            }
            "--working-root" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--working-root"))?;
                working_root = PathBuf::from(value);
            }
            "--legacy-runner" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--legacy-runner"))?;
                legacy_runner = PathBuf::from(value);
            }
            "--iterations" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--iterations"))?;
                iterations = parse_positive_usize("--iterations", value)?;
            }
            "--warmup-iterations" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--warmup-iterations"))?;
                warmup_iterations = parse_non_negative_usize("--warmup-iterations", value)?;
            }
            "--json-output" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--json-output"))?;
                json_output = PathBuf::from(value);
            }
            unknown => {
                return Err(FeffError::InvalidArgument(format!(
                    "unknown argument `{unknown}`"
                )));
            }
        }
        idx += 1;
    }

    Ok(BenchmarkReportCommand {
        tests_root,
        working_root,
        legacy_runner,
        iterations,
        warmup_iterations,
        json_output,
    })
}

fn load_approved_regressions(command: &ParityReportCommand) -> Result<BTreeSet<String>> {
    let mut approved = command.approved_regression_ids.clone();

    for path in &command.approved_regression_files {
        let raw = fs::read_to_string(path)?;
        for line in raw.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            approved.insert(trimmed.to_string());
        }
    }

    Ok(approved)
}

fn evaluate_regressions(
    report: &ParityReport,
    thresholds: RegressionThresholds,
    approved: &BTreeSet<String>,
) -> Vec<RegressionFinding> {
    let mut findings = Vec::new();

    for case in &report.cases {
        for file in &case.file_reports {
            if file.delta.max_abs_delta > thresholds.max_abs_delta
                || file.delta.rms_delta > thresholds.max_rms_delta
            {
                findings.push(RegressionFinding {
                    id: file.id.clone(),
                    max_abs_delta: file.delta.max_abs_delta,
                    rms_delta: file.delta.rms_delta,
                    approved: approved.contains(&file.id),
                });
            }
        }
    }

    findings.sort_by(|left, right| left.id.cmp(&right.id));
    findings
}

fn print_parity_report(report: &ParityReport) {
    println!(
        "Parity report generated for {} case(s), {} file(s)",
        report.case_count, report.file_count
    );
    println!("Working root: {}", report.working_root);

    for case in &report.cases {
        println!("Case: {}/{}", case.material, case.variant);
        for file in &case.file_reports {
            println!(
                "  {} max_abs={:.6e} rms={:.6e} mean_abs={:.6e} compared={} rows={}/{}",
                file.id,
                file.delta.max_abs_delta,
                file.delta.rms_delta,
                file.delta.mean_abs_delta,
                file.delta.compared_value_count,
                file.delta.candidate_row_count,
                file.delta.baseline_row_count
            );
        }
    }
}

fn print_benchmark_report(report: &BenchmarkReport) {
    println!(
        "Benchmark report generated for {} case(s)",
        report.summary.case_count
    );
    println!(
        "Iterations: {} measured, {} warmup",
        report.iterations, report.warmup_iterations
    );
    println!("Legacy runner: {}", report.legacy_runner);
    println!(
        "Summary: rust_mean_total={:.3} ms legacy_mean_total={:.3} ms",
        report.summary.rust_total_mean_ms, report.summary.legacy_total_mean_ms
    );
    if let (Some(ratio), Some(speedup)) = (
        report.summary.rust_vs_legacy_ratio,
        report.summary.speedup_percent,
    ) {
        println!(
            "Summary ratio: rust/legacy={:.4} ({:+.2}% speedup)",
            ratio, speedup
        );
    }

    for case in &report.cases {
        println!(
            "  {}/{} rust_mean={:.3} ms legacy_mean={:.3} ms ratio={}",
            case.material,
            case.variant,
            case.rust_stats.mean_ms,
            case.legacy_stats.mean_ms,
            case.rust_vs_legacy_ratio
                .map(|value| format!("{value:.4}"))
                .unwrap_or_else(|| "n/a".to_string())
        );
    }
}

fn write_benchmark_report_json(report: &BenchmarkReport, output_path: &PathBuf) -> Result<()> {
    if let Some(parent) = output_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let raw = serde_json::to_string_pretty(report).map_err(|error| {
        FeffError::InvalidArgument(format!(
            "failed to serialize benchmark report JSON: {error}"
        ))
    })?;
    fs::write(output_path, format!("{raw}\n"))?;
    Ok(())
}

fn parse_non_negative_f64(flag: &str, value: &str) -> Result<f64> {
    let parsed = value.parse::<f64>().map_err(|error| {
        FeffError::InvalidArgument(format!("failed to parse {flag} value `{value}`: {error}"))
    })?;

    if !parsed.is_finite() || parsed < 0.0 {
        return Err(FeffError::InvalidArgument(format!(
            "{flag} must be a finite value >= 0"
        )));
    }

    Ok(parsed)
}

fn parse_positive_usize(flag: &str, value: &str) -> Result<usize> {
    let parsed = value.parse::<usize>().map_err(|error| {
        FeffError::InvalidArgument(format!("failed to parse {flag} value `{value}`: {error}"))
    })?;

    if parsed == 0 {
        return Err(FeffError::InvalidArgument(format!("{flag} must be >= 1")));
    }

    Ok(parsed)
}

fn parse_non_negative_usize(flag: &str, value: &str) -> Result<usize> {
    value.parse::<usize>().map_err(|error| {
        FeffError::InvalidArgument(format!("failed to parse {flag} value `{value}`: {error}"))
    })
}

fn missing_option_value_error(flag: &str) -> FeffError {
    FeffError::InvalidArgument(format!("missing value for {flag}"))
}

fn print_usage() {
    eprintln!("Usage:");
    eprintln!(
        "  cargo run -- baseline <noscf|scf> [--mode <legacy|modern>] [--tests-root PATH] [--output-root PATH] [--version VERSION]"
    );
    eprintln!(
        "  cargo run -- workflow run [--mode <legacy|modern>] [--input PATH] [--working-root PATH] [--legacy-runner PATH]"
    );
    eprintln!(
        "  cargo run -- parity report [--tests-root PATH] [--working-root PATH] [--max-abs-delta FLOAT --max-rms-delta FLOAT] [--allow-regression ID ...] [--allow-regression-file PATH ...]"
    );
    eprintln!(
        "  cargo run -- benchmark report [--tests-root PATH] [--working-root PATH] [--legacy-runner PATH] [--iterations N] [--warmup-iterations N] [--json-output PATH]"
    );
    eprintln!();
    eprintln!("Variant aliases:");
    eprintln!("  scf: accepts `scf`, `withscf`, and `with-scf`");
    eprintln!();
    eprintln!("Baseline defaults:");
    eprintln!("  --mode        modern");
    eprintln!("  --tests-root  feff85exafs/tests");
    eprintln!("  --output-root docs/migration/baselines");
    eprintln!("  --version     v1");
    eprintln!();
    eprintln!("Workflow defaults:");
    eprintln!("  --mode          modern");
    eprintln!("  --input         feff.inp");
    eprintln!("  --working-root  target/workflow-run");
    eprintln!("  --legacy-runner feff85exafs/bin/f85e (when --mode legacy)");
    eprintln!();
    eprintln!("Parity defaults:");
    eprintln!("  --tests-root   feff85exafs/tests");
    eprintln!("  --working-root target/parity-report");
    eprintln!();
    eprintln!("Benchmark defaults:");
    eprintln!("  --tests-root         feff85exafs/tests");
    eprintln!("  --working-root       target/benchmark-report");
    eprintln!("  --legacy-runner      feff85exafs/bin/f85e");
    eprintln!("  --iterations         1");
    eprintln!("  --warmup-iterations  0");
    eprintln!("  --json-output        target/benchmark-report/benchmark-report.json");
}

#[cfg(test)]
mod tests {
    use super::*;
    use feff85exafs_core::parity::{
        ParityCaseReport, ParityDeltaSummary, ParityFileReport, ParityReport,
    };

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn fake_parity_report() -> ParityReport {
        ParityReport {
            tests_root: "feff85exafs/tests".to_string(),
            working_root: "target/parity-report".to_string(),
            case_count: 1,
            file_count: 2,
            cases: vec![ParityCaseReport {
                material: "Copper".to_string(),
                variant: "noSCF".to_string(),
                input_path: "feff85exafs/tests/Copper/baseline/noSCF/feff.inp".to_string(),
                file_reports: vec![
                    ParityFileReport {
                        id: "Copper/noSCF/chi.dat".to_string(),
                        file_name: "chi.dat".to_string(),
                        baseline_path: "baseline-chi".to_string(),
                        candidate_path: "candidate-chi".to_string(),
                        delta: ParityDeltaSummary {
                            baseline_row_count: 10,
                            candidate_row_count: 10,
                            compared_value_count: 20,
                            max_abs_delta: 0.2,
                            mean_abs_delta: 0.05,
                            rms_delta: 0.08,
                        },
                    },
                    ParityFileReport {
                        id: "Copper/noSCF/xmu.dat".to_string(),
                        file_name: "xmu.dat".to_string(),
                        baseline_path: "baseline-xmu".to_string(),
                        candidate_path: "candidate-xmu".to_string(),
                        delta: ParityDeltaSummary {
                            baseline_row_count: 10,
                            candidate_row_count: 10,
                            compared_value_count: 30,
                            max_abs_delta: 1.5,
                            mean_abs_delta: 0.3,
                            rms_delta: 0.9,
                        },
                    },
                ],
            }],
        }
    }

    #[test]
    fn parse_baseline_command_defaults_to_modern_mode() {
        let command = parse_baseline_command(&args(&["baseline", "noscf"]))
            .expect("baseline args should parse");
        assert_eq!(command.mode, RunMode::Modern);
    }

    #[test]
    fn parse_baseline_command_accepts_legacy_mode() {
        let command = parse_baseline_command(&args(&["baseline", "noscf", "--mode", "legacy"]))
            .expect("legacy mode should parse");
        assert_eq!(command.mode, RunMode::Legacy);
    }

    #[test]
    fn parse_baseline_command_rejects_invalid_mode_with_structured_error() {
        let err = parse_baseline_command(&args(&["baseline", "noscf", "--mode", "future"]))
            .expect_err("invalid mode should fail parsing");
        match err {
            FeffError::Validation(validation) => {
                assert_eq!(validation.len(), 1);
                assert_eq!(validation.issues()[0].field, "mode");
            }
            other => panic!("unexpected error type: {other}"),
        }
    }

    #[test]
    fn parse_workflow_run_command_uses_expected_defaults() {
        let command =
            parse_workflow_run_command(&args(&["workflow", "run"])).expect("workflow should parse");
        assert_eq!(command.mode, RunMode::Modern);
        assert_eq!(command.input_path, PathBuf::from("feff.inp"));
        assert_eq!(command.working_root, PathBuf::from("target/workflow-run"));
        assert!(command.legacy_runner.is_none());
    }

    #[test]
    fn parse_workflow_run_command_accepts_legacy_mode_and_custom_paths() {
        let command = parse_workflow_run_command(&args(&[
            "workflow",
            "run",
            "--mode",
            "legacy",
            "--input",
            "fixtures/copper.inp",
            "--working-root",
            "target/workflow/copper",
            "--legacy-runner",
            "/tmp/f85e",
        ]))
        .expect("workflow command with custom arguments should parse");

        assert_eq!(command.mode, RunMode::Legacy);
        assert_eq!(command.input_path, PathBuf::from("fixtures/copper.inp"));
        assert_eq!(
            command.working_root,
            PathBuf::from("target/workflow/copper")
        );
        assert_eq!(command.legacy_runner, Some(PathBuf::from("/tmp/f85e")));
    }

    #[test]
    fn parse_parity_report_command_uses_expected_defaults() {
        let command = parse_parity_report_command(&args(&["parity", "report"]))
            .expect("parity report args should parse");
        assert_eq!(command.tests_root, PathBuf::from("feff85exafs/tests"));
        assert_eq!(command.working_root, PathBuf::from("target/parity-report"));
        assert!(command.thresholds().is_none());
    }

    #[test]
    fn parse_parity_report_command_requires_both_threshold_flags() {
        let err =
            parse_parity_report_command(&args(&["parity", "report", "--max-abs-delta", "0.5"]))
                .expect_err("single threshold should fail parsing");

        match err {
            FeffError::InvalidArgument(message) => {
                assert!(message.contains("requires both"));
            }
            other => panic!("unexpected error type: {other}"),
        }
    }

    #[test]
    fn parse_parity_report_command_accepts_thresholds_and_allowlists() {
        let command = parse_parity_report_command(&args(&[
            "parity",
            "report",
            "--max-abs-delta",
            "1.0",
            "--max-rms-delta",
            "0.75",
            "--allow-regression",
            "Copper/noSCF/xmu.dat",
            "--allow-regression-file",
            "docs/migration/parity-approved-regressions.txt",
        ]))
        .expect("parity report args should parse");

        let thresholds = command
            .thresholds()
            .expect("thresholds should be present when flags are provided");
        assert_eq!(thresholds.max_abs_delta, 1.0);
        assert_eq!(thresholds.max_rms_delta, 0.75);
        assert!(
            command
                .approved_regression_ids
                .contains("Copper/noSCF/xmu.dat")
        );
        assert_eq!(
            command.approved_regression_files,
            vec![PathBuf::from(
                "docs/migration/parity-approved-regressions.txt"
            )]
        );
    }

    #[test]
    fn parse_benchmark_report_command_uses_expected_defaults() {
        let command = parse_benchmark_report_command(&args(&["benchmark", "report"]))
            .expect("benchmark report args should parse");
        assert_eq!(command.tests_root, PathBuf::from("feff85exafs/tests"));
        assert_eq!(
            command.working_root,
            PathBuf::from("target/benchmark-report")
        );
        assert_eq!(command.legacy_runner, PathBuf::from("feff85exafs/bin/f85e"));
        assert_eq!(command.iterations, 1);
        assert_eq!(command.warmup_iterations, 0);
        assert_eq!(
            command.json_output,
            PathBuf::from("target/benchmark-report/benchmark-report.json")
        );
    }

    #[test]
    fn parse_benchmark_report_command_accepts_custom_values() {
        let command = parse_benchmark_report_command(&args(&[
            "benchmark",
            "report",
            "--tests-root",
            "tests/custom",
            "--working-root",
            "target/bench",
            "--legacy-runner",
            "/tmp/f85e",
            "--iterations",
            "5",
            "--warmup-iterations",
            "2",
            "--json-output",
            "target/bench/report.json",
        ]))
        .expect("benchmark report args should parse");

        assert_eq!(command.tests_root, PathBuf::from("tests/custom"));
        assert_eq!(command.working_root, PathBuf::from("target/bench"));
        assert_eq!(command.legacy_runner, PathBuf::from("/tmp/f85e"));
        assert_eq!(command.iterations, 5);
        assert_eq!(command.warmup_iterations, 2);
        assert_eq!(
            command.json_output,
            PathBuf::from("target/bench/report.json")
        );
    }

    #[test]
    fn parse_benchmark_report_command_rejects_zero_iterations() {
        let err =
            parse_benchmark_report_command(&args(&["benchmark", "report", "--iterations", "0"]))
                .expect_err("zero iterations should fail parsing");

        match err {
            FeffError::InvalidArgument(message) => {
                assert!(message.contains("--iterations must be >= 1"));
            }
            other => panic!("unexpected error type: {other}"),
        }
    }

    #[test]
    fn evaluate_regressions_marks_approved_and_unapproved_findings() {
        let report = fake_parity_report();
        let thresholds = RegressionThresholds {
            max_abs_delta: 0.5,
            max_rms_delta: 0.5,
        };
        let approved = BTreeSet::from(["Copper/noSCF/xmu.dat".to_string()]);

        let findings = evaluate_regressions(&report, thresholds, &approved);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].id, "Copper/noSCF/xmu.dat");
        assert!(findings[0].approved);
    }
}
