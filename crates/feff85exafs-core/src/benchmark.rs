use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::workflow::{run_legacy_workflow, run_modern_workflow};
use feff85exafs_errors::{FeffError, Result, ValidationErrors};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchmarkReportRequest {
    pub tests_root: String,
    pub working_root: String,
    pub legacy_runner: String,
    pub iterations: usize,
    pub warmup_iterations: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BenchmarkTimingStats {
    pub min_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub median_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BenchmarkCaseReport {
    pub material: String,
    pub variant: String,
    pub input_path: String,
    pub rust_samples_ms: Vec<f64>,
    pub legacy_samples_ms: Vec<f64>,
    pub rust_stats: BenchmarkTimingStats,
    pub legacy_stats: BenchmarkTimingStats,
    pub rust_vs_legacy_ratio: Option<f64>,
    pub speedup_percent: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BenchmarkSummary {
    pub case_count: usize,
    pub iterations: usize,
    pub warmup_iterations: usize,
    pub rust_total_mean_ms: f64,
    pub legacy_total_mean_ms: f64,
    pub rust_vs_legacy_ratio: Option<f64>,
    pub speedup_percent: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BenchmarkReport {
    pub tests_root: String,
    pub working_root: String,
    pub legacy_runner: String,
    pub iterations: usize,
    pub warmup_iterations: usize,
    pub cases: Vec<BenchmarkCaseReport>,
    pub summary: BenchmarkSummary,
}

#[derive(Debug, Clone)]
struct BaselineCase {
    material: String,
    variant: String,
    input_path: PathBuf,
}

pub fn generate_benchmark_report(request: &BenchmarkReportRequest) -> Result<BenchmarkReport> {
    request.validate()?;

    let tests_root = Path::new(&request.tests_root);
    let working_root = Path::new(&request.working_root);
    let legacy_runner = request.legacy_runner.trim();

    if looks_like_path(legacy_runner) && !Path::new(legacy_runner).is_file() {
        return Err(validation_error(
            "legacy_runner",
            format!(
                "legacy runner executable was not found at `{}`",
                request.legacy_runner
            ),
        ));
    }

    fs::create_dir_all(working_root)?;

    let cases = discover_baseline_cases(tests_root)?;
    if cases.is_empty() {
        return Err(validation_error(
            "tests_root",
            format!(
                "no baseline corpus fixtures found under `{}`",
                path_string(tests_root)
            ),
        ));
    }

    let mut reports = Vec::with_capacity(cases.len());
    let mut rust_total_mean_ms = 0.0_f64;
    let mut legacy_total_mean_ms = 0.0_f64;

    for (case_index, case) in cases.iter().enumerate() {
        let case_working_root = working_root.join(format!(
            "{:03}-{}-{}",
            case_index + 1,
            slugify(&case.material),
            case.variant.to_ascii_lowercase()
        ));

        if case_working_root.exists() {
            fs::remove_dir_all(&case_working_root)?;
        }
        fs::create_dir_all(&case_working_root)?;

        for warmup_index in 0..request.warmup_iterations {
            run_modern_case(
                &case.input_path,
                &case_working_root.join(format!("warmup-{warmup_index:02}/modern")),
            )?;
            run_legacy_case(
                &case.input_path,
                legacy_runner,
                &case_working_root.join(format!("warmup-{warmup_index:02}/legacy")),
            )?;
        }

        let mut rust_samples_ms = Vec::with_capacity(request.iterations);
        let mut legacy_samples_ms = Vec::with_capacity(request.iterations);

        for iteration in 0..request.iterations {
            let rust_duration_ms = timed(|| {
                run_modern_case(
                    &case.input_path,
                    &case_working_root.join(format!("iter-{iteration:02}/modern")),
                )
            })?;
            rust_samples_ms.push(rust_duration_ms);

            let legacy_duration_ms = timed(|| {
                run_legacy_case(
                    &case.input_path,
                    legacy_runner,
                    &case_working_root.join(format!("iter-{iteration:02}/legacy")),
                )
            })?;
            legacy_samples_ms.push(legacy_duration_ms);
        }

        let rust_stats = summarize_samples(&rust_samples_ms)?;
        let legacy_stats = summarize_samples(&legacy_samples_ms)?;
        let ratio = safe_ratio(rust_stats.mean_ms, legacy_stats.mean_ms);
        let speedup_percent = ratio.map(|value| (1.0 - value) * 100.0);

        rust_total_mean_ms += rust_stats.mean_ms;
        legacy_total_mean_ms += legacy_stats.mean_ms;

        reports.push(BenchmarkCaseReport {
            material: case.material.clone(),
            variant: case.variant.clone(),
            input_path: path_string(&case.input_path),
            rust_samples_ms,
            legacy_samples_ms,
            rust_stats,
            legacy_stats,
            rust_vs_legacy_ratio: ratio,
            speedup_percent,
        });
    }

    let summary_ratio = safe_ratio(rust_total_mean_ms, legacy_total_mean_ms);
    let summary_speedup = summary_ratio.map(|value| (1.0 - value) * 100.0);

    Ok(BenchmarkReport {
        tests_root: request.tests_root.clone(),
        working_root: request.working_root.clone(),
        legacy_runner: request.legacy_runner.clone(),
        iterations: request.iterations,
        warmup_iterations: request.warmup_iterations,
        cases: reports,
        summary: BenchmarkSummary {
            case_count: cases.len(),
            iterations: request.iterations,
            warmup_iterations: request.warmup_iterations,
            rust_total_mean_ms,
            legacy_total_mean_ms,
            rust_vs_legacy_ratio: summary_ratio,
            speedup_percent: summary_speedup,
        },
    })
}

impl BenchmarkReportRequest {
    pub fn validate(&self) -> Result<()> {
        let mut errors = ValidationErrors::new();

        if self.tests_root.trim().is_empty() {
            errors.push("tests_root", "must not be blank");
        }
        if self.working_root.trim().is_empty() {
            errors.push("working_root", "must not be blank");
        }
        if self.legacy_runner.trim().is_empty() {
            errors.push("legacy_runner", "must not be blank");
        }
        if self.iterations == 0 {
            errors.push("iterations", "must be >= 1");
        }

        finish_validation(errors)
    }
}

fn discover_baseline_cases(tests_root: &Path) -> Result<Vec<BaselineCase>> {
    let mut cases = Vec::new();

    for entry in fs::read_dir(tests_root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }

        let material = entry.file_name().to_string_lossy().into_owned();
        for variant in ["noSCF", "withSCF"] {
            let baseline_dir = entry.path().join("baseline").join(variant);
            let input_path = baseline_dir.join("feff.inp");
            if !input_path.is_file() {
                continue;
            }

            cases.push(BaselineCase {
                material: material.clone(),
                variant: variant.to_string(),
                input_path,
            });
        }
    }

    cases.sort_by(|left, right| {
        left.material
            .cmp(&right.material)
            .then(left.variant.cmp(&right.variant))
    });
    Ok(cases)
}

fn run_modern_case(input_path: &Path, working_root: &Path) -> Result<()> {
    let output = run_modern_workflow(input_path, working_root)?;

    for artifact in [&output.ff2x_output.chi_dat, &output.ff2x_output.xmu_dat] {
        let artifact_path = Path::new(artifact);
        if !artifact_path.is_file() {
            return Err(validation_error(
                "modern_output",
                format!("expected modern output file `{artifact}`"),
            ));
        }
    }

    Ok(())
}

fn run_legacy_case(input_path: &Path, legacy_runner: &str, working_dir: &Path) -> Result<()> {
    let _ = run_legacy_workflow(input_path, working_dir, legacy_runner)?;
    Ok(())
}

fn timed<T>(operation: impl FnOnce() -> Result<T>) -> Result<f64> {
    let started_at = Instant::now();
    let _ = operation()?;
    Ok(started_at.elapsed().as_secs_f64() * 1000.0)
}

fn summarize_samples(samples: &[f64]) -> Result<BenchmarkTimingStats> {
    if samples.is_empty() {
        return Err(validation_error(
            "samples",
            "at least one benchmark sample is required",
        ));
    }

    let mut sorted = samples.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));

    let min_ms = sorted[0];
    let max_ms = sorted[sorted.len() - 1];
    let mean_ms = sorted.iter().sum::<f64>() / sorted.len() as f64;
    let median_ms = if sorted.len() % 2 == 1 {
        sorted[sorted.len() / 2]
    } else {
        let right = sorted.len() / 2;
        (sorted[right - 1] + sorted[right]) / 2.0
    };

    Ok(BenchmarkTimingStats {
        min_ms,
        max_ms,
        mean_ms,
        median_ms,
    })
}

fn safe_ratio(numerator: f64, denominator: f64) -> Option<f64> {
    if denominator > 0.0 {
        Some(numerator / denominator)
    } else {
        None
    }
}

fn looks_like_path(value: &str) -> bool {
    value.contains('/') || value.contains('\\')
}

fn finish_validation(errors: ValidationErrors) -> Result<()> {
    if errors.is_empty() {
        Ok(())
    } else {
        Err(FeffError::Validation(errors))
    }
}

fn validation_error(field: impl Into<String>, message: impl Into<String>) -> FeffError {
    let mut errors = ValidationErrors::new();
    errors.push(field, message);
    FeffError::Validation(errors)
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn slugify(value: &str) -> String {
    let mut slug = String::new();
    let mut previous_dash = false;

    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            previous_dash = false;
        } else if !previous_dash {
            slug.push('-');
            previous_dash = true;
        }
    }

    while slug.ends_with('-') {
        slug.pop();
    }

    if slug.is_empty() {
        "case".to_string()
    } else {
        slug
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir(name: &str) -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "feff85exafs-core-benchmark-{name}-{}-{timestamp}-{counter}",
            std::process::id()
        ))
    }

    #[test]
    fn request_validation_rejects_blank_paths_and_zero_iterations() {
        let err = BenchmarkReportRequest {
            tests_root: " ".to_string(),
            working_root: "\n".to_string(),
            legacy_runner: "\t".to_string(),
            iterations: 0,
            warmup_iterations: 0,
        }
        .validate()
        .expect_err("invalid benchmark request should fail validation");

        match err {
            FeffError::Validation(validation) => {
                let fields = validation
                    .issues()
                    .iter()
                    .map(|issue| issue.field.as_str())
                    .collect::<Vec<_>>();
                assert!(fields.contains(&"tests_root"));
                assert!(fields.contains(&"working_root"));
                assert!(fields.contains(&"legacy_runner"));
                assert!(fields.contains(&"iterations"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn summarize_samples_computes_expected_statistics() {
        let stats = summarize_samples(&[5.0, 2.0, 9.0, 4.0]).expect("sample summary should work");
        assert_eq!(stats.min_ms, 2.0);
        assert_eq!(stats.max_ms, 9.0);
        assert!((stats.mean_ms - 5.0).abs() < 1.0e-12);
        assert_eq!(stats.median_ms, 4.5);
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

        fn copy_executable(source: &Path, destination: &Path) -> io::Result<()> {
            fs::copy(source, destination)?;
            let mut permissions = fs::metadata(destination)?.permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(destination, permissions)?;
            Ok(())
        }

        fn create_fake_legacy_tree(root: &Path) -> io::Result<PathBuf> {
            let legacy_tree = root.join("legacy-tree");
            let bin_dir = legacy_tree.join("bin");
            let src_dir = legacy_tree.join("src");
            fs::create_dir_all(&bin_dir)?;
            fs::create_dir_all(&src_dir)?;

            let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
            copy_executable(
                &repo_root.join("feff85exafs/bin/f85e"),
                &bin_dir.join("f85e"),
            )?;

            let modules = [
                ("RDINP", "rdinp", "touch files.dat"),
                ("POT", "pot", "touch pot.pad"),
                ("XSPH", "xsph", "touch phase.pad"),
                ("PATH", "pathfinder", "touch paths.dat"),
                ("GENFMT", "genfmt", "touch feff0001.dat"),
                (
                    "FF2X",
                    "ff2x",
                    "printf '0.0 0.0\n' > chi.dat\nprintf '0.0 0.0\n' > xmu.dat",
                ),
            ];

            for (folder, executable, body) in modules {
                write_executable(
                    &src_dir.join(folder).join(executable),
                    format!("#!/bin/sh\nset -eu\n{body}\n").as_str(),
                )?;
            }

            Ok(bin_dir.join("f85e"))
        }

        #[test]
        fn generate_benchmark_report_runs_for_single_fixture_case() {
            let fixture_root =
                Path::new(env!("CARGO_MANIFEST_DIR")).join("../../feff85exafs/tests");
            let source_case = fixture_root.join("Copper/baseline/noSCF");

            let temp_root = temp_dir("single-case");
            let tests_root = temp_root.join("tests");
            let baseline_root = tests_root.join("Copper/baseline/noSCF");
            fs::create_dir_all(&baseline_root).expect("baseline fixture root should be created");
            fs::copy(source_case.join("feff.inp"), baseline_root.join("feff.inp"))
                .expect("should copy fixture feff.inp");

            let legacy_runner =
                create_fake_legacy_tree(&temp_root).expect("fake legacy tree should be created");
            let working_root = temp_root.join("work");

            let request = BenchmarkReportRequest {
                tests_root: path_string(&tests_root),
                working_root: path_string(&working_root),
                legacy_runner: path_string(&legacy_runner),
                iterations: 2,
                warmup_iterations: 1,
            };

            let report = generate_benchmark_report(&request)
                .expect("benchmark report should run for single fixture");
            assert_eq!(report.summary.case_count, 1);
            assert_eq!(report.cases.len(), 1);
            assert_eq!(report.cases[0].rust_samples_ms.len(), 2);
            assert_eq!(report.cases[0].legacy_samples_ms.len(), 2);
            assert!(report.summary.rust_total_mean_ms > 0.0);
            assert!(report.summary.legacy_total_mean_ms >= 0.0);

            if temp_root.exists() {
                fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
            }
        }
    }
}
