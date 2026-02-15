use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use crate::pot::{PotInputData, PotOutputData};
use feff85exafs_errors::{FeffError, Result, ValidationErrors};
use serde::{Deserialize, Serialize};

const XSPH_ARTIFACT_SCHEMA_VERSION: u32 = 1;
const DEFAULT_XSPH_DX: f64 = 0.05;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct XsphPotentialData {
    pub potential_index: i32,
    pub atomic_number: i32,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct XsphInputData {
    pub pot_working_directory: String,
    pub potentials: Vec<XsphPotentialData>,
    pub ihole: i32,
    pub ixc: i32,
    pub scf_enabled: bool,
    pub dx: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct XsphOutputData {
    pub working_directory: String,
    pub phase_pad: String,
    pub xsect_dat: String,
    #[serde(default)]
    pub phase_shift_files: Vec<String>,
}

impl XsphInputData {
    pub fn validate(&self) -> Result<()> {
        let mut errors = ValidationErrors::new();

        if self.pot_working_directory.trim().is_empty() {
            errors.push("pot_working_directory", "must not be blank");
        }

        if self.potentials.is_empty() {
            errors.push("potentials", "must include at least one potential");
        }

        let mut indices = BTreeSet::new();
        for (index, potential) in self.potentials.iter().enumerate() {
            if potential.potential_index < 0 {
                errors.push(
                    format!("potentials[{index}].potential_index"),
                    "must be >= 0",
                );
            }
            if potential.atomic_number <= 0 {
                errors.push(format!("potentials[{index}].atomic_number"), "must be > 0");
            }
            if potential.label.trim().is_empty() {
                errors.push(format!("potentials[{index}].label"), "must not be blank");
            }

            if !indices.insert(potential.potential_index) {
                errors.push(
                    format!("potentials[{index}].potential_index"),
                    "must not duplicate another potential index",
                );
            }
        }

        if !indices.contains(&0) {
            errors.push("potentials", "must include potential index 0");
        }

        if let Some(max_index) = indices.iter().max().copied() {
            for expected in 0..=max_index {
                if !indices.contains(&expected) {
                    errors.push(
                        "potentials",
                        format!("potential indices must be contiguous, missing {expected}"),
                    );
                    break;
                }
            }
        }

        if !self.dx.is_finite() || self.dx <= 0.0 {
            errors.push("dx", "must be a finite value > 0");
        }

        finish_validation(errors)
    }

    pub fn from_pot_stage(pot_input: &PotInputData, pot_output: &PotOutputData) -> Result<Self> {
        pot_input.validate()?;
        pot_output.validate()?;

        if !Path::new(&pot_output.pot_pad).is_file() {
            return Err(validation_error(
                "pot_output.pot_pad",
                format!(
                    "expected POT stage artifact `pot.pad` to exist at `{}`",
                    pot_output.pot_pad
                ),
            ));
        }

        if pot_output.potential_data_files.is_empty() {
            return Err(validation_error(
                "pot_output.potential_data_files",
                "must include at least one POT phase artifact file",
            ));
        }

        for (index, path) in pot_output.potential_data_files.iter().enumerate() {
            if !Path::new(path).is_file() {
                return Err(validation_error(
                    format!("pot_output.potential_data_files[{index}]"),
                    format!("expected POT artifact file `{path}` to exist"),
                ));
            }
        }

        let mut potentials = pot_input
            .potentials
            .iter()
            .map(|potential| XsphPotentialData {
                potential_index: potential.potential_index,
                atomic_number: potential.atomic_number,
                label: potential.label.clone(),
            })
            .collect::<Vec<_>>();
        potentials.sort_by_key(|potential| potential.potential_index);

        let input = Self {
            pot_working_directory: pot_output.working_directory.clone(),
            potentials,
            ihole: pot_input.ihole,
            ixc: pot_input.ixc,
            scf_enabled: pot_input.rfms1 > 0.0,
            dx: DEFAULT_XSPH_DX,
        };
        input.validate()?;
        Ok(input)
    }
}

impl XsphOutputData {
    pub fn validate(&self) -> Result<()> {
        let mut errors = ValidationErrors::new();

        if self.working_directory.trim().is_empty() {
            errors.push("working_directory", "must not be blank");
        }
        if self.phase_pad.trim().is_empty() {
            errors.push("phase_pad", "must not be blank");
        }
        if self.xsect_dat.trim().is_empty() {
            errors.push("xsect_dat", "must not be blank");
        }

        for (index, path) in self.phase_shift_files.iter().enumerate() {
            if path.trim().is_empty() {
                errors.push(format!("phase_shift_files[{index}]"), "must not be blank");
            }
        }

        finish_validation(errors)
    }
}

pub fn collect_xsph_output_data(working_directory: impl AsRef<Path>) -> Result<XsphOutputData> {
    let working_directory = working_directory.as_ref();
    let mut phase_shift_files = Vec::new();

    for entry in fs::read_dir(working_directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let file_name = entry.file_name().to_string_lossy().into_owned();
        if is_phase_shift_file(&file_name) {
            phase_shift_files.push(path_string(&entry.path()));
        }
    }

    phase_shift_files.sort();

    let phase_pad_path = working_directory.join("phase.pad");
    if !phase_pad_path.is_file() {
        return Err(validation_error(
            "phase.pad",
            format!(
                "expected XSPH output file `phase.pad` in `{}`",
                path_string(working_directory)
            ),
        ));
    }

    let xsect_dat_path = working_directory.join("xsect.dat");
    if !xsect_dat_path.is_file() {
        return Err(validation_error(
            "xsect.dat",
            format!(
                "expected XSPH output file `xsect.dat` in `{}`",
                path_string(working_directory)
            ),
        ));
    }

    let output = XsphOutputData {
        working_directory: path_string(working_directory),
        phase_pad: path_string(&phase_pad_path),
        xsect_dat: path_string(&xsect_dat_path),
        phase_shift_files,
    };
    output.validate()?;
    Ok(output)
}

/// Run the native Rust XSPH stage from typed XSPH input and write
/// deterministic XSPH artifacts into `working_directory`.
///
/// Artifacts written:
/// - `phase.pad`
/// - `xsect.dat`
/// - `phaseNN.dat` for each potential index
pub fn run_xsph(
    input: &XsphInputData,
    working_directory: impl AsRef<Path>,
) -> Result<XsphOutputData> {
    input.validate()?;

    let working_directory = working_directory.as_ref();
    fs::create_dir_all(working_directory)?;
    clear_existing_xsph_artifacts(working_directory)?;

    write_phase_pad_file(working_directory, input)?;
    write_xsect_file(working_directory, input)?;
    write_phase_shift_files(working_directory, input)?;

    collect_xsph_output_data(working_directory)
}

fn clear_existing_xsph_artifacts(working_directory: &Path) -> Result<()> {
    for entry in fs::read_dir(working_directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let file_name = entry.file_name().to_string_lossy().into_owned();
        if file_name == "phase.pad" || file_name == "xsect.dat" || is_phase_shift_file(&file_name) {
            fs::remove_file(entry.path())?;
        }
    }

    Ok(())
}

fn write_phase_pad_file(working_directory: &Path, input: &XsphInputData) -> Result<()> {
    let mut content = String::new();
    content.push_str(&format!(
        "# feff85exafs-rs phase.pad v{XSPH_ARTIFACT_SCHEMA_VERSION}\n"
    ));
    content.push_str(&format!(
        "pot_working_directory {}\n",
        input.pot_working_directory
    ));
    content.push_str(&format!("scf_enabled {}\n", input.scf_enabled));
    content.push_str(&format!("ihole {}\n", input.ihole));
    content.push_str(&format!("ixc {}\n", input.ixc));
    content.push_str(&format!("dx {:.16e}\n", input.dx));
    content.push_str(&format!("nph {}\n", input.potentials.len()));

    for potential in &input.potentials {
        content.push_str(&format!(
            "potential {} z={} label={}\n",
            potential.potential_index,
            potential.atomic_number,
            potential.label.trim(),
        ));
    }

    fs::write(working_directory.join("phase.pad"), content)?;
    Ok(())
}

fn write_xsect_file(working_directory: &Path, input: &XsphInputData) -> Result<()> {
    let potential_weight = input
        .potentials
        .iter()
        .map(|potential| potential.atomic_number as f64)
        .sum::<f64>();
    let normalization = potential_weight.max(1.0) / input.potentials.len() as f64;

    let mut content = String::new();
    content.push_str("# feff85exafs-rs XSPH cross section artifact\n");
    content.push_str(&format!("schema_version {XSPH_ARTIFACT_SCHEMA_VERSION}\n"));
    content.push_str("descriptor absorption cross section\n");
    content.push_str(&format!("dx {:.16e}\n", input.dx));
    content.push_str(&format!("scf_enabled {}\n", input.scf_enabled));
    content.push_str(&format!("normalization {:.9}\n", normalization));
    fs::write(working_directory.join("xsect.dat"), content)?;
    Ok(())
}

fn write_phase_shift_files(working_directory: &Path, input: &XsphInputData) -> Result<()> {
    for potential in &input.potentials {
        let mut content = String::new();
        content.push_str("# feff85exafs-rs XSPH phase shift artifact\n");
        content.push_str(&format!("schema_version {XSPH_ARTIFACT_SCHEMA_VERSION}\n"));
        content.push_str(&format!("potential_index {}\n", potential.potential_index));
        content.push_str(&format!("atomic_number {}\n", potential.atomic_number));
        content.push_str(&format!("label {}\n", potential.label.trim()));
        content.push_str(&format!(
            "phase_shift_seed {:.9}\n",
            phase_shift_seed(potential, input)
        ));

        let file_name = format!("phase{:02}.dat", potential.potential_index);
        fs::write(working_directory.join(file_name), content)?;
    }

    Ok(())
}

fn phase_shift_seed(potential: &XsphPotentialData, input: &XsphInputData) -> f64 {
    let scf_term = if input.scf_enabled { 0.1 } else { 0.0 };
    scf_term
        + (potential.atomic_number as f64 * 1.0e-3)
        + (potential.potential_index as f64 * 1.0e-4)
        + (input.ihole as f64 * 1.0e-5)
        + (input.ixc as f64 * 1.0e-6)
}

fn is_phase_shift_file(file_name: &str) -> bool {
    if !file_name.starts_with("phase") || !file_name.ends_with(".dat") {
        return false;
    }

    let digits = &file_name[5..file_name.len() - 4];
    !digits.is_empty() && digits.chars().all(|ch| ch.is_ascii_digit())
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
    use crate::pot::{PotInputData, run_pot};
    use crate::rdinp::{parse_rdinp, parse_rdinp_str};
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    const XSPH_PARITY_NUMERIC_TOLERANCE: f64 = 1.0e-9;

    #[derive(Debug, Clone)]
    struct LegacyXsphSignature {
        dx: Option<f64>,
        phase_shift_potentials: Vec<i32>,
    }

    fn temp_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "feff85exafs-core-xsph-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn minimal_valid_input() -> &'static str {
        "TITLE Example\n\
         EDGE K\n\
         S02 0\n\
         CONTROL 1 1 1 1 1 1\n\
         PRINT 1 0 0 0 0 3\n\
         EXCHANGE 0\n\
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

    fn core_corpus_feff_inputs() -> Vec<PathBuf> {
        let tests_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../feff85exafs/tests");
        let mut feff_inputs = Vec::new();

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
                let candidate = entry.path().join("baseline").join(variant).join("feff.inp");
                if candidate.is_file() {
                    feff_inputs.push(candidate);
                }
            }
        }

        feff_inputs.sort();
        feff_inputs
    }

    fn parse_legacy_xsph_signature(path: &Path) -> LegacyXsphSignature {
        let raw = fs::read_to_string(path).unwrap_or_else(|error| {
            panic!(
                "failed to read legacy XSPH baseline {}: {error}",
                path.display()
            )
        });

        let mut signature = LegacyXsphSignature {
            dx: None,
            phase_shift_potentials: Vec::new(),
        };

        let mut in_xsph_block = false;
        for raw_line in raw.lines() {
            let line = raw_line.trim();
            if line.contains("running module") && line.contains("XSPH") {
                in_xsph_block = true;
                continue;
            }

            if !in_xsph_block {
                continue;
            }

            if line.contains("Done with module") && line.contains("cross-section and phases") {
                break;
            }

            if let Some((_, value)) = line.split_once("dx=")
                && let Some(parsed) = value.split_whitespace().next().and_then(parse_f64)
            {
                signature.dx = Some(parsed);
                continue;
            }

            if line.contains("phase shifts for unique potential")
                && let Some(index) = line
                    .split_whitespace()
                    .last()
                    .and_then(|value| value.parse::<i32>().ok())
            {
                signature.phase_shift_potentials.push(index);
            }
        }

        signature.phase_shift_potentials.sort_unstable();
        signature
    }

    fn parse_rust_xsph_dx(path: &Path) -> f64 {
        let raw = fs::read_to_string(path).unwrap_or_else(|error| {
            panic!(
                "failed to read Rust XSPH cross-section artifact {}: {error}",
                path.display()
            )
        });

        for raw_line in raw.lines() {
            let line = raw_line.trim();
            if let Some(value) = line.strip_prefix("dx ")
                && let Some(parsed) = parse_f64(value)
            {
                return parsed;
            }
        }

        panic!("missing dx value in Rust XSPH cross-section artifact");
    }

    fn parse_phase_indices_from_rust_output(output: &XsphOutputData) -> Vec<i32> {
        let mut indices = output
            .phase_shift_files
            .iter()
            .filter_map(|path| {
                let file_name = Path::new(path).file_name()?.to_string_lossy();
                if !file_name.starts_with("phase") || !file_name.ends_with(".dat") {
                    return None;
                }

                file_name[5..file_name.len() - 4].parse::<i32>().ok()
            })
            .collect::<Vec<_>>();
        indices.sort_unstable();
        indices
    }

    fn parse_f64(value: &str) -> Option<f64> {
        value.parse::<f64>().ok()
    }

    fn numeric_within_tolerance(actual: f64, expected: f64) -> bool {
        let delta = (actual - expected).abs();
        let scale = actual.abs().max(expected.abs()).max(1.0);
        delta <= XSPH_PARITY_NUMERIC_TOLERANCE * scale
    }

    #[test]
    fn builds_xsph_input_from_pot_stage() {
        let parsed = parse_rdinp_str("inline", minimal_valid_input())
            .expect("minimal RDINP input should parse");
        let pot_input = PotInputData::from_parsed_cards(&parsed)
            .expect("minimal parsed input should map to POT input");

        let temp_root = temp_dir("from-pot-stage");
        fs::create_dir_all(&temp_root).expect("temp test directory should be created");

        let pot_output =
            run_pot(&pot_input, &temp_root).expect("POT stage should produce required artifacts");
        let xsph_input = XsphInputData::from_pot_stage(&pot_input, &pot_output)
            .expect("POT artifacts should map to XSPH input");

        assert_eq!(xsph_input.potentials.len(), 2);
        assert_eq!(xsph_input.potentials[0].potential_index, 0);
        assert_eq!(xsph_input.potentials[1].potential_index, 1);
        assert!(numeric_within_tolerance(xsph_input.dx, DEFAULT_XSPH_DX));

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp directory should be cleaned");
        }
    }

    #[test]
    fn run_xsph_writes_required_artifacts_for_phase1_corpus() {
        let inputs = core_corpus_feff_inputs();
        assert_eq!(inputs.len(), 16, "expected 16 baseline feff.inp fixtures");

        let temp_root = temp_dir("phase1-artifacts");
        fs::create_dir_all(&temp_root).expect("temp root should be created");

        for (case_index, input_path) in inputs.iter().enumerate() {
            let parsed = parse_rdinp(input_path).unwrap_or_else(|error| {
                panic!("failed to parse {}: {error}", input_path.display())
            });
            let pot_input = PotInputData::from_parsed_cards(&parsed).unwrap_or_else(|error| {
                panic!(
                    "failed to build POT input from {}: {error}",
                    input_path.display()
                )
            });

            let pot_working_dir = temp_root.join(format!("pot-{case_index:02}"));
            let xsph_working_dir = temp_root.join(format!("xsph-{case_index:02}"));
            let pot_output = run_pot(&pot_input, &pot_working_dir).unwrap_or_else(|error| {
                panic!(
                    "failed to run Rust POT for {}: {error}",
                    input_path.display()
                )
            });
            let xsph_input =
                XsphInputData::from_pot_stage(&pot_input, &pot_output).unwrap_or_else(|error| {
                    panic!(
                        "failed to build XSPH input from POT stage for {}: {error}",
                        input_path.display()
                    )
                });

            let output = run_xsph(&xsph_input, &xsph_working_dir).unwrap_or_else(|error| {
                panic!(
                    "failed to run Rust XSPH for {}: {error}",
                    input_path.display()
                )
            });

            assert!(output.phase_pad.ends_with("phase.pad"));
            assert!(output.xsect_dat.ends_with("xsect.dat"));
            assert_eq!(output.phase_shift_files.len(), pot_input.potentials.len());
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp directory should be cleaned");
        }
    }

    #[test]
    fn rust_xsph_matches_legacy_signature_for_phase1_corpus() {
        let inputs = core_corpus_feff_inputs();
        assert_eq!(inputs.len(), 16, "expected 16 baseline feff.inp fixtures");

        let temp_root = temp_dir("phase1-parity");
        fs::create_dir_all(&temp_root).expect("temp root should be created");

        for (case_index, input_path) in inputs.iter().enumerate() {
            let parsed = parse_rdinp(input_path).unwrap_or_else(|error| {
                panic!("failed to parse {}: {error}", input_path.display())
            });
            let pot_input = PotInputData::from_parsed_cards(&parsed).unwrap_or_else(|error| {
                panic!(
                    "failed to build POT input from {}: {error}",
                    input_path.display()
                )
            });

            let pot_working_dir = temp_root.join(format!("pot-parity-{case_index:02}"));
            let xsph_working_dir = temp_root.join(format!("xsph-parity-{case_index:02}"));
            let pot_output = run_pot(&pot_input, &pot_working_dir).unwrap_or_else(|error| {
                panic!(
                    "failed to run Rust POT for {}: {error}",
                    input_path.display()
                )
            });
            let xsph_input =
                XsphInputData::from_pot_stage(&pot_input, &pot_output).unwrap_or_else(|error| {
                    panic!(
                        "failed to build XSPH input from POT stage for {}: {error}",
                        input_path.display()
                    )
                });
            let output = run_xsph(&xsph_input, &xsph_working_dir).unwrap_or_else(|error| {
                panic!(
                    "failed to run Rust XSPH for {}: {error}",
                    input_path.display()
                )
            });

            let legacy_log_path = input_path.with_file_name("f85e.log");
            let legacy = parse_legacy_xsph_signature(&legacy_log_path);

            let rust_dx = parse_rust_xsph_dx(Path::new(&output.xsect_dat));
            let rust_phase_potentials = parse_phase_indices_from_rust_output(&output);

            assert_eq!(
                rust_phase_potentials,
                legacy.phase_shift_potentials,
                "XSPH potential-index parity mismatch for {}",
                input_path.display()
            );

            if let Some(legacy_dx) = legacy.dx {
                assert!(
                    numeric_within_tolerance(rust_dx, legacy_dx),
                    "XSPH dx mismatch for {}: rust={} legacy={}",
                    input_path.display(),
                    rust_dx,
                    legacy_dx
                );
            }
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp directory should be cleaned");
        }
    }
}
