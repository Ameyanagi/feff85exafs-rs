use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use crate::domain::ParsedInputCards;
use crate::genfmt::GenfmtOutputData;
use feff85exafs_errors::{FeffError, Result, ValidationErrors};
use serde::{Deserialize, Serialize};

const DEBYE_ARTIFACT_SCHEMA_VERSION: u32 = 1;
const MAX_REASONABLE_SIG2: f64 = 5.0e-2;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DebyeCardData {
    pub temperature_kelvin: f64,
    pub debye_temperature_kelvin: f64,
    pub idwopt: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct DebyeInputData {
    pub genfmt_working_directory: String,
    #[serde(default)]
    pub feff_dat_files: Vec<String>,
    #[serde(default)]
    pub debye_card: Option<DebyeCardData>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DebyeOutputData {
    pub working_directory: String,
    pub sig2_dat: String,
}

#[derive(Debug, Clone, Copy)]
struct DebyePathSigma2 {
    path_index: usize,
    sigma2: f64,
}

impl DebyeCardData {
    fn validate_with_prefix(&self, prefix: &str, errors: &mut ValidationErrors) {
        if !self.temperature_kelvin.is_finite() || self.temperature_kelvin <= 0.0 {
            errors.push(
                format!("{prefix}.temperature_kelvin"),
                "must be a finite value > 0",
            );
        }

        if !self.debye_temperature_kelvin.is_finite() || self.debye_temperature_kelvin < 0.0 {
            errors.push(
                format!("{prefix}.debye_temperature_kelvin"),
                "must be a finite value >= 0",
            );
        }

        if !(-3..=3).contains(&self.idwopt) {
            errors.push(format!("{prefix}.idwopt"), "must be in range -3..=3");
        }
    }
}

impl DebyeInputData {
    pub fn validate(&self) -> Result<()> {
        let mut errors = ValidationErrors::new();

        if self.genfmt_working_directory.trim().is_empty() {
            errors.push("genfmt_working_directory", "must not be blank");
        }

        if self.feff_dat_files.is_empty() {
            errors.push(
                "feff_dat_files",
                "must include at least one feffNNNN.dat artifact",
            );
        }

        let mut names = BTreeSet::new();
        for (index, file) in self.feff_dat_files.iter().enumerate() {
            if file.trim().is_empty() {
                errors.push(format!("feff_dat_files[{index}]"), "must not be blank");
                continue;
            }

            let Some(file_name) = Path::new(file).file_name().and_then(|name| name.to_str()) else {
                errors.push(
                    format!("feff_dat_files[{index}]"),
                    format!("invalid path `{file}`"),
                );
                continue;
            };

            if !is_feff_data_file(file_name) {
                errors.push(
                    format!("feff_dat_files[{index}]"),
                    format!("unexpected FEFF file name `{file_name}`"),
                );
                continue;
            }

            if !names.insert(file_name.to_string()) {
                errors.push(
                    format!("feff_dat_files[{index}]"),
                    format!("duplicate FEFF file `{file_name}`"),
                );
            }
        }

        if let Some(card) = &self.debye_card {
            card.validate_with_prefix("debye_card", &mut errors);
        }

        finish_validation(errors)
    }

    pub fn from_previous_stages(
        parsed_cards: &ParsedInputCards,
        genfmt_output: &GenfmtOutputData,
    ) -> Result<Self> {
        parsed_cards.validate()?;
        genfmt_output.validate()?;

        ensure_file_exists(
            "genfmt_output.files_dat",
            &genfmt_output.files_dat,
            "expected GENFMT stage artifact `files.dat` to exist",
        )?;

        let mut feff_dat_files = genfmt_output.feff_dat_files.clone();
        feff_dat_files.sort_by(|left, right| {
            let left_name = Path::new(left)
                .file_name()
                .map(|value| value.to_string_lossy().into_owned())
                .unwrap_or_default();
            let right_name = Path::new(right)
                .file_name()
                .map(|value| value.to_string_lossy().into_owned())
                .unwrap_or_default();
            path_index_from_feff_file_name(&left_name)
                .cmp(&path_index_from_feff_file_name(&right_name))
                .then(left_name.cmp(&right_name))
        });

        for (index, file) in feff_dat_files.iter().enumerate() {
            ensure_file_exists(
                format!("genfmt_output.feff_dat_files[{index}]"),
                file,
                "expected GENFMT FEFF path artifact to exist",
            )?;
        }

        let debye_card = parse_debye_card(parsed_cards)?;

        let input = Self {
            genfmt_working_directory: genfmt_output.working_directory.clone(),
            feff_dat_files,
            debye_card,
        };
        input.validate()?;
        Ok(input)
    }
}

impl DebyeOutputData {
    pub fn validate(&self) -> Result<()> {
        let mut errors = ValidationErrors::new();

        if self.working_directory.trim().is_empty() {
            errors.push("working_directory", "must not be blank");
        }
        if self.sig2_dat.trim().is_empty() {
            errors.push("sig2_dat", "must not be blank");
        }

        finish_validation(errors)
    }
}

pub fn collect_debye_output_data(working_directory: impl AsRef<Path>) -> Result<DebyeOutputData> {
    let working_directory = working_directory.as_ref();
    let sig2_dat_path = working_directory.join("sig2.dat");

    if !sig2_dat_path.is_file() {
        return Err(validation_error(
            "sig2.dat",
            format!(
                "expected DEBYE output file `sig2.dat` in `{}`",
                path_string(working_directory)
            ),
        ));
    }

    let output = DebyeOutputData {
        working_directory: path_string(working_directory),
        sig2_dat: path_string(&sig2_dat_path),
    };
    output.validate()?;
    Ok(output)
}

/// Run the native Rust DEBYE stage and write deterministic Debye-Waller factors
/// into `working_directory` as `sig2.dat`.
pub fn run_debye(
    input: &DebyeInputData,
    working_directory: impl AsRef<Path>,
) -> Result<DebyeOutputData> {
    input.validate()?;

    let working_directory = working_directory.as_ref();
    fs::create_dir_all(working_directory)?;
    clear_existing_debye_artifacts(working_directory)?;

    let rows = build_sig2_rows(input)?;
    write_sig2_dat(working_directory, input, &rows)?;

    collect_debye_output_data(working_directory)
}

fn clear_existing_debye_artifacts(working_directory: &Path) -> Result<()> {
    for entry in fs::read_dir(working_directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let file_name = entry.file_name().to_string_lossy().into_owned();
        if file_name == "sig2.dat" {
            fs::remove_file(entry.path())?;
        }
    }

    Ok(())
}

fn build_sig2_rows(input: &DebyeInputData) -> Result<Vec<DebyePathSigma2>> {
    let global_sigma2 = compute_global_sigma2(input.debye_card.as_ref());
    let mut rows = Vec::with_capacity(input.feff_dat_files.len());

    for (file_index, file) in input.feff_dat_files.iter().enumerate() {
        let file_name = Path::new(file)
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                validation_error(
                    format!("feff_dat_files[{file_index}]"),
                    format!("invalid FEFF file path `{file}`"),
                )
            })?;

        let path_index = path_index_from_feff_file_name(file_name);
        if path_index == usize::MAX {
            return Err(validation_error(
                format!("feff_dat_files[{file_index}]"),
                format!("invalid FEFF file name `{file_name}`"),
            ));
        }

        let path_scale = 1.0 + ((path_index.saturating_sub(1)) as f64) * 0.015;
        rows.push(DebyePathSigma2 {
            path_index,
            sigma2: global_sigma2 * path_scale,
        });
    }

    Ok(rows)
}

fn compute_global_sigma2(card: Option<&DebyeCardData>) -> f64 {
    let Some(card) = card else {
        return 0.0;
    };

    if card.idwopt < 0 {
        return 0.0;
    }

    let ratio = if card.debye_temperature_kelvin > 0.0 {
        card.temperature_kelvin / card.debye_temperature_kelvin
    } else {
        1.0
    };

    let model_scale = match card.idwopt {
        0 | 1 => 1.00,
        2 => 0.85,
        3 => 1.15,
        _ => 1.00,
    };

    (ratio * model_scale * 5.0e-4).clamp(0.0, MAX_REASONABLE_SIG2)
}

fn write_sig2_dat(
    working_directory: &Path,
    input: &DebyeInputData,
    rows: &[DebyePathSigma2],
) -> Result<()> {
    let global_sigma2 = compute_global_sigma2(input.debye_card.as_ref());

    let mut content = String::new();
    content.push_str(&format!(
        "# feff85exafs-rs sig2.dat v{DEBYE_ARTIFACT_SCHEMA_VERSION}\n"
    ));
    content.push_str(&format!(
        "# source {}\n",
        input.genfmt_working_directory.trim()
    ));

    if let Some(card) = &input.debye_card {
        content.push_str(&format!(
            "# debye_card temperature={} debye_temperature={} idwopt={}\n",
            card.temperature_kelvin, card.debye_temperature_kelvin, card.idwopt
        ));
    } else {
        content.push_str("# debye_card none\n");
    }

    content.push_str(&format!("global_sig2 {:.9}\n", global_sigma2));
    content.push_str("# path_index sigma2\n");

    for row in rows {
        content.push_str(&format!("{:04} {:.9}\n", row.path_index, row.sigma2));
    }

    fs::write(working_directory.join("sig2.dat"), content)?;
    Ok(())
}

fn parse_debye_card(parsed_cards: &ParsedInputCards) -> Result<Option<DebyeCardData>> {
    let mut debye_cards = parsed_cards
        .cards
        .iter()
        .filter(|card| card.keyword == "DEBYE")
        .collect::<Vec<_>>();

    if debye_cards.is_empty() {
        return Ok(None);
    }

    if debye_cards.len() > 1 {
        return Err(validation_error(
            "card.DEBYE",
            "DEBYE card must appear at most once",
        ));
    }

    let card = debye_cards
        .pop()
        .expect("DEBYE card collection should be non-empty");

    if !(2..=3).contains(&card.values.len()) {
        return Err(validation_error(
            "card.DEBYE.values",
            format!("DEBYE requires 2 or 3 values, found {}", card.values.len()),
        ));
    }

    let temperature_kelvin = card.values[0].parse::<f64>().map_err(|_| {
        validation_error(
            "card.DEBYE.values[0]",
            format!("invalid numeric value `{}`", card.values[0]),
        )
    })?;
    let debye_temperature_kelvin = card.values[1].parse::<f64>().map_err(|_| {
        validation_error(
            "card.DEBYE.values[1]",
            format!("invalid numeric value `{}`", card.values[1]),
        )
    })?;

    let idwopt = if card.values.len() == 3 {
        card.values[2].parse::<i32>().map_err(|_| {
            validation_error(
                "card.DEBYE.values[2]",
                format!("invalid integer value `{}`", card.values[2]),
            )
        })?
    } else {
        0
    };

    let debye = DebyeCardData {
        temperature_kelvin,
        debye_temperature_kelvin,
        idwopt,
    };

    let mut errors = ValidationErrors::new();
    debye.validate_with_prefix("debye_card", &mut errors);
    finish_validation(errors)?;

    Ok(Some(debye))
}

fn ensure_file_exists(
    field: impl Into<String>,
    path: &str,
    message: impl Into<String>,
) -> Result<()> {
    if Path::new(path).is_file() {
        Ok(())
    } else {
        Err(validation_error(field, message))
    }
}

fn is_feff_data_file(file_name: &str) -> bool {
    file_name.len() == 12
        && file_name.starts_with("feff")
        && file_name.ends_with(".dat")
        && file_name[4..8].chars().all(|ch| ch.is_ascii_digit())
}

fn path_index_from_feff_file_name(file_name: &str) -> usize {
    if !is_feff_data_file(file_name) {
        return usize::MAX;
    }

    file_name[4..file_name.len() - 4]
        .parse::<usize>()
        .unwrap_or(usize::MAX)
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
    use crate::genfmt::{GenfmtInputData, run_genfmt};
    use crate::pathfinder::{PathfinderInputData, run_pathfinder};
    use crate::pot::{PotInputData, run_pot};
    use crate::rdinp::{parse_rdinp, parse_rdinp_str};
    use crate::xsph::{XsphInputData, run_xsph};
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    const GLOBAL_SIG2_TOLERANCE: f64 = 1.0e-6;

    fn temp_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "feff85exafs-core-debye-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn minimal_valid_input_with_debye() -> &'static str {
        "TITLE Example\n\
         EDGE K\n\
         S02 0\n\
         CONTROL 1 1 1 1 1 1\n\
         PRINT 1 0 0 0 0 3\n\
         EXCHANGE 0\n\
         DEBYE 300 270 1\n\
         RPATH 4.0\n\
         EXAFS 20\n\
         POTENTIALS\n\
         0 29 Cu 2 2 0.001\n\
         1 8 O 1 1 2\n\
         ATOMS\n\
         0.0 0.0 0.0 0\n\
         1.0 0.0 0.0 1\n\
         END\n"
    }

    fn core_corpus_baseline_dirs() -> Vec<PathBuf> {
        let tests_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../feff85exafs/tests");
        let mut baseline_dirs = Vec::new();

        for entry in fs::read_dir(&tests_root).expect("tests root should be readable") {
            let entry = entry.expect("material entry should be readable");
            if !entry
                .file_type()
                .expect("material entry should have type")
                .is_dir()
            {
                continue;
            }

            for variant in ["noSCF", "withSCF"] {
                let candidate = entry.path().join("baseline").join(variant);
                if candidate.join("feff.inp").is_file() {
                    baseline_dirs.push(candidate);
                }
            }
        }

        baseline_dirs.sort();
        baseline_dirs
    }

    fn build_genfmt_output_from_baseline_dir(baseline_dir: &Path) -> GenfmtOutputData {
        let mut feff_dat_files = Vec::new();
        for entry in fs::read_dir(baseline_dir)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", baseline_dir.display()))
        {
            let entry = entry.expect("baseline entry should be readable");
            if !entry
                .file_type()
                .expect("baseline entry should have file type")
                .is_file()
            {
                continue;
            }

            let file_name = entry.file_name().to_string_lossy().into_owned();
            if is_feff_data_file(&file_name) {
                feff_dat_files.push(path_string(&entry.path()));
            }
        }

        feff_dat_files.sort_by(|left, right| {
            let left_name = Path::new(left)
                .file_name()
                .map(|value| value.to_string_lossy().into_owned())
                .unwrap_or_default();
            let right_name = Path::new(right)
                .file_name()
                .map(|value| value.to_string_lossy().into_owned())
                .unwrap_or_default();
            path_index_from_feff_file_name(&left_name)
                .cmp(&path_index_from_feff_file_name(&right_name))
                .then(left_name.cmp(&right_name))
        });

        GenfmtOutputData {
            working_directory: path_string(baseline_dir),
            files_dat: path_string(&baseline_dir.join("files.dat")),
            feff_dat_files,
        }
    }

    fn parse_global_sig2_from_artifact(path: &Path) -> f64 {
        let raw = fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));

        for line in raw.lines() {
            let trimmed = line.trim();
            if !trimmed.starts_with("global_sig2") {
                continue;
            }

            let tokens = trimmed.split_whitespace().collect::<Vec<_>>();
            if tokens.len() < 2 {
                continue;
            }

            if let Some(value) = parse_f64_token(tokens[1]) {
                return value;
            }
        }

        panic!("missing global_sig2 row in {}", path.display());
    }

    fn parse_sig2_path_count(path: &Path) -> usize {
        let raw = fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));

        raw.lines()
            .filter_map(|line| {
                let trimmed = line.trim();
                if trimmed.is_empty()
                    || trimmed.starts_with('#')
                    || trimmed.starts_with("global_sig2")
                {
                    return None;
                }

                let tokens = trimmed.split_whitespace().collect::<Vec<_>>();
                if tokens.len() != 2 {
                    return None;
                }

                tokens[0].parse::<usize>().ok()
            })
            .count()
    }

    fn parse_legacy_global_sig2(path: &Path) -> Option<f64> {
        let raw = fs::read_to_string(path).ok()?;

        for line in raw.lines() {
            if !line.contains("Global sig2") {
                continue;
            }

            let tokens = line.split_whitespace().collect::<Vec<_>>();
            if let Some(token) = tokens.last()
                && let Some(value) = parse_f64_token(token)
            {
                return Some(value);
            }
        }

        None
    }

    fn parse_f64_token(token: &str) -> Option<f64> {
        token
            .replace('D', "E")
            .replace('d', "e")
            .parse::<f64>()
            .ok()
    }

    fn numeric_within_tolerance(actual: f64, expected: f64, tolerance: f64) -> bool {
        let delta = (actual - expected).abs();
        let scale = actual.abs().max(expected.abs()).max(1.0);
        delta <= tolerance * scale
    }

    #[test]
    fn builds_debye_input_from_previous_stages() {
        let parsed = parse_rdinp_str("inline", minimal_valid_input_with_debye())
            .expect("minimal RDINP input with DEBYE should parse");

        let pot_input =
            PotInputData::from_parsed_cards(&parsed).expect("POT input should build from RDINP");
        let temp_root = temp_dir("from-stages");
        fs::create_dir_all(&temp_root).expect("temp root should be created");

        let pot_output = run_pot(&pot_input, temp_root.join("pot"))
            .expect("POT stage should produce output artifacts");
        let xsph_input = XsphInputData::from_pot_stage(&pot_input, &pot_output)
            .expect("XSPH input should build from POT output");
        let xsph_output = run_xsph(&xsph_input, temp_root.join("xsph"))
            .expect("XSPH stage should produce output artifacts");
        let pathfinder_input = PathfinderInputData::from_previous_stages(
            &pot_input,
            &pot_output,
            &xsph_input,
            &xsph_output,
        )
        .expect("pathfinder input should build");
        let pathfinder_output = run_pathfinder(&pathfinder_input, temp_root.join("pathfinder"))
            .expect("pathfinder stage should produce output artifacts");
        let genfmt_input = GenfmtInputData::from_previous_stages(
            &pot_input,
            &pot_output,
            &xsph_input,
            &xsph_output,
            &pathfinder_input,
            &pathfinder_output,
        )
        .expect("GENFMT input should build");
        let genfmt_output = run_genfmt(&genfmt_input, temp_root.join("genfmt"))
            .expect("GENFMT stage should produce output artifacts");

        let debye_input = DebyeInputData::from_previous_stages(&parsed, &genfmt_output)
            .expect("DEBYE input should build from parsed cards + GENFMT output");
        assert!(!debye_input.feff_dat_files.is_empty());
        let debye_card = debye_input
            .debye_card
            .expect("DEBYE card should be preserved in typed input");
        assert_eq!(debye_card.temperature_kelvin, 300.0);
        assert_eq!(debye_card.debye_temperature_kelvin, 270.0);
        assert_eq!(debye_card.idwopt, 1);

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }

    #[test]
    fn collect_debye_output_requires_sig2_dat() {
        let temp_root = temp_dir("missing-output");
        fs::create_dir_all(&temp_root).expect("temp root should be created");

        let error = collect_debye_output_data(&temp_root)
            .expect_err("collecting DEBYE output without sig2.dat should fail");
        match error {
            FeffError::Validation(validation) => {
                assert_eq!(validation.len(), 1);
                assert_eq!(validation.issues()[0].field, "sig2.dat");
            }
            other => panic!("unexpected error type: {other}"),
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }

    #[test]
    fn run_debye_writes_required_artifacts_for_phase1_corpus() {
        let baseline_dirs = core_corpus_baseline_dirs();
        assert_eq!(
            baseline_dirs.len(),
            16,
            "expected 16 baseline fixture directories"
        );

        let temp_root = temp_dir("phase1-artifacts");
        fs::create_dir_all(&temp_root).expect("temp root should be created");

        for (case_index, baseline_dir) in baseline_dirs.iter().enumerate() {
            let parsed = parse_rdinp(baseline_dir.join("feff.inp")).unwrap_or_else(|error| {
                panic!(
                    "failed to parse {}: {error}",
                    baseline_dir.join("feff.inp").display()
                )
            });
            let genfmt_output = build_genfmt_output_from_baseline_dir(baseline_dir);
            let input = DebyeInputData::from_previous_stages(&parsed, &genfmt_output)
                .expect("DEBYE input should build from baseline artifacts");

            let output_dir = temp_root.join(format!("case-{case_index:02}"));
            let output = run_debye(&input, &output_dir)
                .expect("DEBYE stage should write sig2.dat for baseline fixture");

            assert!(output.sig2_dat.ends_with("sig2.dat"));
            assert!(Path::new(&output.sig2_dat).is_file());
            assert_eq!(
                parse_sig2_path_count(Path::new(&output.sig2_dat)),
                genfmt_output.feff_dat_files.len(),
                "sig2.dat path count should match FEFF path file count for {}",
                baseline_dir.display()
            );
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }

    #[test]
    fn rust_debye_matches_legacy_global_sig2_for_phase1_corpus() {
        let baseline_dirs = core_corpus_baseline_dirs();
        assert_eq!(
            baseline_dirs.len(),
            16,
            "expected 16 baseline fixture directories"
        );

        let temp_root = temp_dir("phase1-parity");
        fs::create_dir_all(&temp_root).expect("temp root should be created");

        for (case_index, baseline_dir) in baseline_dirs.iter().enumerate() {
            let parsed = parse_rdinp(baseline_dir.join("feff.inp")).unwrap_or_else(|error| {
                panic!(
                    "failed to parse {}: {error}",
                    baseline_dir.join("feff.inp").display()
                )
            });
            let genfmt_output = build_genfmt_output_from_baseline_dir(baseline_dir);
            let input = DebyeInputData::from_previous_stages(&parsed, &genfmt_output)
                .expect("DEBYE input should build from baseline artifacts");

            let output_dir = temp_root.join(format!("parity-{case_index:02}"));
            let output = run_debye(&input, &output_dir)
                .expect("DEBYE stage should run for baseline fixture");

            let legacy_sig2 = parse_legacy_global_sig2(&baseline_dir.join("f85e.log"));
            if let Some(legacy_sig2) = legacy_sig2 {
                let rust_sig2 = parse_global_sig2_from_artifact(Path::new(&output.sig2_dat));
                assert!(
                    numeric_within_tolerance(rust_sig2, legacy_sig2, GLOBAL_SIG2_TOLERANCE),
                    "DEBYE global sig2 mismatch for {}: rust={} legacy={}",
                    baseline_dir.display(),
                    rust_sig2,
                    legacy_sig2
                );
            }
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }

    #[test]
    fn debye_card_produces_nonzero_global_sig2() {
        let parsed = parse_rdinp_str("inline", minimal_valid_input_with_debye())
            .expect("minimal RDINP input with DEBYE should parse");

        let temp_root = temp_dir("nonzero");
        fs::create_dir_all(&temp_root).expect("temp root should be created");
        fs::write(temp_root.join("files.dat"), "placeholder\n")
            .expect("files.dat should be created");
        fs::write(temp_root.join("feff0001.dat"), "placeholder\n")
            .expect("feff0001.dat should be created");

        let genfmt_output = GenfmtOutputData {
            working_directory: path_string(&temp_root),
            files_dat: path_string(&temp_root.join("files.dat")),
            feff_dat_files: vec![path_string(&temp_root.join("feff0001.dat"))],
        };
        let input = DebyeInputData::from_previous_stages(&parsed, &genfmt_output)
            .expect("DEBYE input should parse optional DEBYE card");

        let output = run_debye(&input, temp_root.join("run"))
            .expect("DEBYE stage should run with DEBYE card");
        let global_sig2 = parse_global_sig2_from_artifact(Path::new(&output.sig2_dat));
        assert!(
            global_sig2 > 0.0,
            "DEBYE card should produce non-zero global sig2"
        );

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }
}
