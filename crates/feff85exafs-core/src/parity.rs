use std::fs;
use std::path::{Path, PathBuf};

use crate::ff2x::{Ff2xInputData, run_ff2x};
use crate::genfmt::{GenfmtInputData, run_genfmt};
use crate::pathfinder::{PathfinderInputData, run_pathfinder};
use crate::pot::{PotInputData, run_pot};
use crate::rdinp::parse_rdinp;
use crate::xsph::{XsphInputData, run_xsph};
use feff85exafs_errors::{FeffError, Result, ValidationErrors};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParityReportRequest {
    pub tests_root: String,
    pub working_root: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ParityDeltaSummary {
    pub baseline_row_count: usize,
    pub candidate_row_count: usize,
    pub compared_value_count: usize,
    pub max_abs_delta: f64,
    pub mean_abs_delta: f64,
    pub rms_delta: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ParityFileReport {
    pub id: String,
    pub file_name: String,
    pub baseline_path: String,
    pub candidate_path: String,
    pub delta: ParityDeltaSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ParityCaseReport {
    pub material: String,
    pub variant: String,
    pub input_path: String,
    pub file_reports: Vec<ParityFileReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ParityReport {
    pub tests_root: String,
    pub working_root: String,
    pub case_count: usize,
    pub file_count: usize,
    pub cases: Vec<ParityCaseReport>,
}

#[derive(Debug, Clone)]
struct BaselineCase {
    material: String,
    variant: String,
    input_path: PathBuf,
    baseline_dir: PathBuf,
}

pub fn generate_parity_report(request: &ParityReportRequest) -> Result<ParityReport> {
    request.validate()?;

    let tests_root = Path::new(&request.tests_root);
    let working_root = Path::new(&request.working_root);
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
    let mut file_count = 0usize;

    for (case_index, case) in cases.iter().enumerate() {
        let case_working_root = working_root.join(format!(
            "{:03}-{}-{}",
            case_index + 1,
            slugify(&case.material),
            case.variant.to_ascii_lowercase()
        ));

        let ff2x_output = run_full_core_chain(&case.input_path, &case_working_root)?;
        let mut file_reports = Vec::new();

        let files = [
            ("chi.dat", PathBuf::from(&ff2x_output.chi_dat)),
            ("xmu.dat", PathBuf::from(&ff2x_output.xmu_dat)),
        ];

        for (file_name, candidate_path) in files {
            let baseline_path = case.baseline_dir.join(file_name);
            if !baseline_path.is_file() {
                return Err(validation_error(
                    format!("baseline.{}/{}/{}", case.material, case.variant, file_name),
                    format!("expected baseline file `{}`", path_string(&baseline_path)),
                ));
            }

            let delta = summarize_numeric_delta(&baseline_path, &candidate_path)?;
            file_reports.push(ParityFileReport {
                id: format!("{}/{}/{}", case.material, case.variant, file_name),
                file_name: file_name.to_string(),
                baseline_path: path_string(&baseline_path),
                candidate_path: path_string(&candidate_path),
                delta,
            });
        }

        file_count += file_reports.len();
        reports.push(ParityCaseReport {
            material: case.material.clone(),
            variant: case.variant.clone(),
            input_path: path_string(&case.input_path),
            file_reports,
        });
    }

    Ok(ParityReport {
        tests_root: request.tests_root.clone(),
        working_root: request.working_root.clone(),
        case_count: reports.len(),
        file_count,
        cases: reports,
    })
}

impl ParityReportRequest {
    pub fn validate(&self) -> Result<()> {
        let mut errors = ValidationErrors::new();
        if self.tests_root.trim().is_empty() {
            errors.push("tests_root", "must not be blank");
        }
        if self.working_root.trim().is_empty() {
            errors.push("working_root", "must not be blank");
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
                baseline_dir,
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

fn run_full_core_chain(
    input_path: &Path,
    working_root: &Path,
) -> Result<crate::ff2x::Ff2xOutputData> {
    let parsed = parse_rdinp(input_path)?;
    let pot_input = PotInputData::from_parsed_cards(&parsed)?;

    let pot_working_dir = working_root.join("pot");
    let xsph_working_dir = working_root.join("xsph");
    let path_working_dir = working_root.join("pathfinder");
    let genfmt_working_dir = working_root.join("genfmt");
    let ff2x_working_dir = working_root.join("ff2x");

    let pot_output = run_pot(&pot_input, &pot_working_dir)?;
    let xsph_input = XsphInputData::from_pot_stage(&pot_input, &pot_output)?;
    let xsph_output = run_xsph(&xsph_input, &xsph_working_dir)?;
    let pathfinder_input = PathfinderInputData::from_previous_stages(
        &pot_input,
        &pot_output,
        &xsph_input,
        &xsph_output,
    )?;
    let pathfinder_output = run_pathfinder(&pathfinder_input, &path_working_dir)?;
    let genfmt_input = GenfmtInputData::from_previous_stages(
        &pot_input,
        &pot_output,
        &xsph_input,
        &xsph_output,
        &pathfinder_input,
        &pathfinder_output,
    )?;
    let genfmt_output = run_genfmt(&genfmt_input, &genfmt_working_dir)?;
    let ff2x_input = Ff2xInputData::from_previous_stages(&genfmt_input, &genfmt_output)?;

    run_ff2x(&ff2x_input, &ff2x_working_dir)
}

fn summarize_numeric_delta(
    baseline_path: &Path,
    candidate_path: &Path,
) -> Result<ParityDeltaSummary> {
    let baseline_rows = parse_numeric_rows(baseline_path)?;
    let candidate_rows = parse_numeric_rows(candidate_path)?;

    let row_count = baseline_rows.len().min(candidate_rows.len());
    let mut compared_value_count = 0usize;
    let mut max_abs_delta = 0.0_f64;
    let mut sum_abs_delta = 0.0_f64;
    let mut sum_sq_delta = 0.0_f64;

    for row_index in 0..row_count {
        let baseline_row = &baseline_rows[row_index];
        let candidate_row = &candidate_rows[row_index];
        let column_count = baseline_row.len().min(candidate_row.len());
        for column_index in 0..column_count {
            let delta = (candidate_row[column_index] - baseline_row[column_index]).abs();
            max_abs_delta = max_abs_delta.max(delta);
            sum_abs_delta += delta;
            sum_sq_delta += delta * delta;
            compared_value_count += 1;
        }
    }

    if compared_value_count == 0 {
        return Err(validation_error(
            format!(
                "compare.{}",
                baseline_path
                    .file_name()
                    .map(|value| value.to_string_lossy().into_owned())
                    .unwrap_or_else(|| "unknown".to_string())
            ),
            "no comparable numeric values were found",
        ));
    }

    Ok(ParityDeltaSummary {
        baseline_row_count: baseline_rows.len(),
        candidate_row_count: candidate_rows.len(),
        compared_value_count,
        max_abs_delta,
        mean_abs_delta: sum_abs_delta / compared_value_count as f64,
        rms_delta: (sum_sq_delta / compared_value_count as f64).sqrt(),
    })
}

fn parse_numeric_rows(path: &Path) -> Result<Vec<Vec<f64>>> {
    let raw = fs::read_to_string(path)?;
    let mut rows = Vec::new();

    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let numeric_values = trimmed
            .split_whitespace()
            .filter_map(|value| value.parse::<f64>().ok())
            .collect::<Vec<_>>();
        if !numeric_values.is_empty() {
            rows.push(numeric_values);
        }
    }

    if rows.is_empty() {
        return Err(validation_error(
            path_string(path),
            "expected at least one numeric data row",
        ));
    }
    Ok(rows)
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

#[cfg(test)]
mod tests {
    use super::*;
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
            "feff85exafs-core-parity-{name}-{}-{timestamp}-{counter}",
            std::process::id()
        ))
    }

    #[test]
    fn request_validation_rejects_blank_paths() {
        let err = ParityReportRequest {
            tests_root: " ".to_string(),
            working_root: "\n".to_string(),
        }
        .validate()
        .expect_err("blank request fields should fail validation");

        match err {
            FeffError::Validation(validation) => {
                let fields = validation
                    .issues()
                    .iter()
                    .map(|issue| issue.field.as_str())
                    .collect::<Vec<_>>();
                assert!(fields.contains(&"tests_root"));
                assert!(fields.contains(&"working_root"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn summarize_numeric_delta_reports_expected_metrics() {
        let temp_root = temp_dir("numeric-delta");
        fs::create_dir_all(&temp_root).expect("temp root should be created");

        let baseline = temp_root.join("baseline.dat");
        let candidate = temp_root.join("candidate.dat");
        fs::write(&baseline, "# comment\n0 1 2\n1 2 3\n").expect("baseline should be written");
        fs::write(&candidate, "0 2 1\n1 3 2\n").expect("candidate should be written");

        let summary = summarize_numeric_delta(&baseline, &candidate)
            .expect("summary should succeed with numeric rows");
        assert_eq!(summary.baseline_row_count, 2);
        assert_eq!(summary.candidate_row_count, 2);
        assert_eq!(summary.compared_value_count, 6);
        assert!((summary.max_abs_delta - 1.0).abs() < 1.0e-12);
        assert!((summary.mean_abs_delta - (4.0 / 6.0)).abs() < 1.0e-12);

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }

    #[test]
    fn generate_parity_report_runs_for_single_fixture_case() {
        let fixture_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../feff85exafs/tests");
        let source_case = fixture_root.join("Copper/baseline/noSCF");
        let temp_root = temp_dir("single-case");
        let tests_root = temp_root.join("tests");
        let baseline_root = tests_root.join("Copper/baseline/noSCF");
        fs::create_dir_all(&baseline_root).expect("baseline fixture root should be created");

        for file_name in ["feff.inp", "chi.dat", "xmu.dat"] {
            fs::copy(source_case.join(file_name), baseline_root.join(file_name))
                .unwrap_or_else(|error| panic!("failed to copy fixture {file_name}: {error}"));
        }

        let working_root = temp_root.join("work");
        let request = ParityReportRequest {
            tests_root: path_string(&tests_root),
            working_root: path_string(&working_root),
        };
        let report =
            generate_parity_report(&request).expect("single case parity report should run");

        assert_eq!(report.case_count, 1);
        assert_eq!(report.file_count, 2);
        assert_eq!(report.cases.len(), 1);
        assert_eq!(report.cases[0].file_reports.len(), 2);

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }
}
