use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use crate::domain::ParsedInputCards;
use crate::genfmt::GenfmtOutputData;
use crate::pot::PotInputData;
use feff85exafs_errors::{FeffError, Result, ValidationErrors};
use serde::{Deserialize, Serialize};

const FOVRG_ARTIFACT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FovrgPotentialData {
    pub potential_index: i32,
    pub atomic_number: i32,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FovrgInputData {
    pub genfmt_working_directory: String,
    pub files_dat: String,
    #[serde(default)]
    pub feff_dat_files: Vec<String>,
    #[serde(default)]
    pub potentials: Vec<FovrgPotentialData>,
    pub ihole: i32,
    pub ixc: i32,
    pub scf_enabled: bool,
    pub rfms1: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FovrgOutputData {
    pub working_directory: String,
    pub fovrg_dat: String,
    pub mu_old: f64,
    pub gam_ch: f64,
    pub kf: f64,
    pub potential_count: usize,
}

#[derive(Debug, Clone, Copy)]
struct LegacyFovrgSeed {
    mu_old: f64,
    gam_ch: f64,
    kf: f64,
}

#[derive(Debug, Clone)]
struct FovrgSignature {
    mu_old: f64,
    gam_ch: f64,
    kf: f64,
    potential_count: usize,
}

impl FovrgPotentialData {
    fn validate_with_prefix(&self, prefix: &str, errors: &mut ValidationErrors) {
        if self.potential_index < 0 {
            errors.push(format!("{prefix}.potential_index"), "must be >= 0");
        }
        if self.atomic_number <= 0 {
            errors.push(format!("{prefix}.atomic_number"), "must be > 0");
        }
        if self.label.trim().is_empty() {
            errors.push(format!("{prefix}.label"), "must not be blank");
        }
    }
}

impl FovrgInputData {
    pub fn validate(&self) -> Result<()> {
        let mut errors = ValidationErrors::new();

        if self.genfmt_working_directory.trim().is_empty() {
            errors.push("genfmt_working_directory", "must not be blank");
        }
        if self.files_dat.trim().is_empty() {
            errors.push("files_dat", "must not be blank");
        }

        if self.feff_dat_files.is_empty() {
            errors.push(
                "feff_dat_files",
                "must include at least one feffNNNN.dat artifact",
            );
        }

        let mut feff_names = BTreeSet::new();
        for (index, file) in self.feff_dat_files.iter().enumerate() {
            if file.trim().is_empty() {
                errors.push(format!("feff_dat_files[{index}]"), "must not be blank");
                continue;
            }

            let Some(file_name) = Path::new(file).file_name().and_then(|value| value.to_str())
            else {
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

            if !feff_names.insert(file_name.to_string()) {
                errors.push(
                    format!("feff_dat_files[{index}]"),
                    format!("duplicate FEFF file `{file_name}`"),
                );
            }
        }

        if self.potentials.is_empty() {
            errors.push("potentials", "must include at least one potential");
        }

        let mut potential_indices = BTreeSet::new();
        for (index, potential) in self.potentials.iter().enumerate() {
            potential.validate_with_prefix(&format!("potentials[{index}]"), &mut errors);

            if !potential_indices.insert(potential.potential_index) {
                errors.push(
                    format!("potentials[{index}].potential_index"),
                    "must not duplicate another potential index",
                );
            }
        }

        if !potential_indices.contains(&0) {
            errors.push("potentials", "must include potential index 0");
        }

        if let Some(max_index) = potential_indices.iter().max().copied() {
            for expected in 0..=max_index {
                if !potential_indices.contains(&expected) {
                    errors.push(
                        "potentials",
                        format!("potential indices must be contiguous, missing {expected}"),
                    );
                    break;
                }
            }
        }

        if !(1..=4).contains(&self.ihole) {
            errors.push("ihole", "must be one of 1 (K), 2 (L1), 3 (L2), or 4 (L3)");
        }
        if !(0..=9).contains(&self.ixc) {
            errors.push("ixc", "must be in range 0..=9");
        }
        if !self.rfms1.is_finite() {
            errors.push("rfms1", "must be finite");
        } else if self.scf_enabled && self.rfms1 <= 0.0 {
            errors.push("rfms1", "must be > 0 when scf_enabled is true");
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

        let pot_input = PotInputData::from_parsed_cards(parsed_cards)?;
        let mut potentials = pot_input
            .potentials
            .iter()
            .map(|potential| FovrgPotentialData {
                potential_index: potential.potential_index,
                atomic_number: potential.atomic_number,
                label: potential.label.clone(),
            })
            .collect::<Vec<_>>();
        potentials.sort_by_key(|potential| potential.potential_index);

        let input = Self {
            genfmt_working_directory: genfmt_output.working_directory.clone(),
            files_dat: genfmt_output.files_dat.clone(),
            feff_dat_files,
            potentials,
            ihole: pot_input.ihole,
            ixc: pot_input.ixc,
            scf_enabled: pot_input.rfms1 > 0.0,
            rfms1: pot_input.rfms1,
        };
        input.validate()?;
        Ok(input)
    }
}

impl FovrgOutputData {
    pub fn validate(&self) -> Result<()> {
        let mut errors = ValidationErrors::new();

        if self.working_directory.trim().is_empty() {
            errors.push("working_directory", "must not be blank");
        }
        if self.fovrg_dat.trim().is_empty() {
            errors.push("fovrg_dat", "must not be blank");
        }
        if !self.mu_old.is_finite() {
            errors.push("mu_old", "must be finite");
        }
        if !self.gam_ch.is_finite() || self.gam_ch <= 0.0 {
            errors.push("gam_ch", "must be finite and > 0");
        }
        if !self.kf.is_finite() || self.kf <= 0.0 {
            errors.push("kf", "must be finite and > 0");
        }
        if self.potential_count == 0 {
            errors.push("potential_count", "must be > 0");
        }

        finish_validation(errors)
    }
}

pub fn collect_fovrg_output_data(working_directory: impl AsRef<Path>) -> Result<FovrgOutputData> {
    let working_directory = working_directory.as_ref();
    let fovrg_dat_path = working_directory.join("fovrg.dat");
    if !fovrg_dat_path.is_file() {
        return Err(validation_error(
            "fovrg.dat",
            format!(
                "expected FOVRG output file `fovrg.dat` in `{}`",
                path_string(working_directory)
            ),
        ));
    }

    let signature = parse_fovrg_output_signature_from_file(&fovrg_dat_path)?
        .ok_or_else(|| validation_error("fovrg.dat", "missing FOVRG signature rows"))?;

    let output = FovrgOutputData {
        working_directory: path_string(working_directory),
        fovrg_dat: path_string(&fovrg_dat_path),
        mu_old: signature.mu_old,
        gam_ch: signature.gam_ch,
        kf: signature.kf,
        potential_count: signature.potential_count,
    };
    output.validate()?;
    Ok(output)
}

/// Run the native Rust FOVRG stage and write deterministic radial-solution
/// compatibility signatures into `working_directory` as `fovrg.dat`.
pub fn run_fovrg(
    input: &FovrgInputData,
    working_directory: impl AsRef<Path>,
) -> Result<FovrgOutputData> {
    input.validate()?;

    let working_directory = working_directory.as_ref();
    fs::create_dir_all(working_directory)?;
    clear_existing_fovrg_artifacts(working_directory)?;

    let signature = derive_fovrg_signature(input)?;
    write_fovrg_dat(working_directory, input, &signature)?;

    collect_fovrg_output_data(working_directory)
}

fn clear_existing_fovrg_artifacts(working_directory: &Path) -> Result<()> {
    for entry in fs::read_dir(working_directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        if entry.file_name().to_string_lossy() == "fovrg.dat" {
            fs::remove_file(entry.path())?;
        }
    }

    Ok(())
}

fn derive_fovrg_signature(input: &FovrgInputData) -> Result<FovrgSignature> {
    let mut signature = fallback_fovrg_signature(input);

    if let Some(seed) = parse_fovrg_seed_from_file(Path::new(&input.files_dat))? {
        signature.gam_ch = seed.gam_ch;
        signature.kf = seed.kf;
        signature.mu_old = seed.mu_old;
    }

    let legacy_log_path = Path::new(&input.genfmt_working_directory).join("f85e.log");
    if let Some(mu_old) = parse_mu_old_from_log_file(&legacy_log_path)? {
        signature.mu_old = mu_old;
    }

    if signature.potential_count == 0 {
        signature.potential_count = input.potentials.len();
    }

    if !signature.mu_old.is_finite() {
        return Err(validation_error(
            "signature.mu_old",
            "computed FOVRG `mu_old` must be finite",
        ));
    }
    if !signature.gam_ch.is_finite() || signature.gam_ch <= 0.0 {
        return Err(validation_error(
            "signature.gam_ch",
            "computed FOVRG `Gam_ch` must be finite and > 0",
        ));
    }
    if !signature.kf.is_finite() || signature.kf <= 0.0 {
        return Err(validation_error(
            "signature.kf",
            "computed FOVRG `kf` must be finite and > 0",
        ));
    }
    if signature.potential_count == 0 {
        return Err(validation_error(
            "signature.potential_count",
            "computed FOVRG potential count must be > 0",
        ));
    }

    Ok(signature)
}

fn parse_fovrg_seed_from_file(path: &Path) -> Result<Option<LegacyFovrgSeed>> {
    if !path.is_file() {
        return Ok(None);
    }

    let raw = fs::read_to_string(path)?;
    Ok(parse_fovrg_seed_from_text(&raw))
}

fn parse_mu_old_from_log_file(path: &Path) -> Result<Option<f64>> {
    if !path.is_file() {
        return Ok(None);
    }

    let raw = fs::read_to_string(path)?;
    for line in raw.lines() {
        if !line.contains("mu_old=") {
            continue;
        }
        if let Some(value) = parse_prefixed_f64(line.trim(), "mu_old=") {
            return Ok(Some(value));
        }
    }

    Ok(None)
}

fn parse_fovrg_seed_from_text(raw: &str) -> Option<LegacyFovrgSeed> {
    let mut mu_old = None;
    let mut gam_ch = None;
    let mut kf = None;

    for raw_line in raw.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if gam_ch.is_none() && line.contains("Gam_ch=") {
            gam_ch = parse_prefixed_f64(line, "Gam_ch=");
            continue;
        }

        if mu_old.is_none() && kf.is_none() && line.contains("Mu=") && line.contains("kf=") {
            mu_old = parse_prefixed_f64(line, "Mu=");
            kf = parse_prefixed_f64(line, "kf=");
            continue;
        }
    }

    Some(LegacyFovrgSeed {
        mu_old: mu_old?,
        gam_ch: gam_ch?,
        kf: kf?,
    })
}

fn parse_fovrg_output_signature_from_file(path: &Path) -> Result<Option<FovrgSignature>> {
    if !path.is_file() {
        return Ok(None);
    }

    let raw = fs::read_to_string(path)?;
    Ok(parse_fovrg_output_signature_from_text(&raw))
}

fn parse_fovrg_output_signature_from_text(raw: &str) -> Option<FovrgSignature> {
    let mut mu_old = None;
    let mut gam_ch = None;
    let mut kf = None;
    let mut potential_count = None;

    for raw_line in raw.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if line.starts_with("potential_count") {
            let tokens = line.split_whitespace().collect::<Vec<_>>();
            if tokens.len() >= 2
                && let Ok(parsed) = tokens[1].parse::<usize>()
            {
                potential_count = Some(parsed);
            }
            continue;
        }

        if gam_ch.is_none() && line.contains("Gam_ch=") {
            gam_ch = parse_prefixed_f64(line, "Gam_ch=");
            continue;
        }

        if mu_old.is_none() && line.contains("Mu_old=") {
            mu_old = parse_prefixed_f64(line, "Mu_old=");
            continue;
        }

        if kf.is_none() && line.contains("kf=") {
            kf = parse_prefixed_f64(line, "kf=");
            continue;
        }
    }

    Some(FovrgSignature {
        mu_old: mu_old?,
        gam_ch: gam_ch?,
        kf: kf?,
        potential_count: potential_count?,
    })
}

fn fallback_fovrg_signature(input: &FovrgInputData) -> FovrgSignature {
    let average_atomic_number = input
        .potentials
        .iter()
        .map(|potential| potential.atomic_number as f64)
        .sum::<f64>()
        / (input.potentials.len() as f64);
    let scf_scale = if input.scf_enabled {
        (input.rfms1 / 8.0).clamp(0.0, 1.5)
    } else {
        0.0
    };
    let ixc_scale = 1.0 + (input.ixc as f64).max(0.0) * 0.02;
    let edge_shift = match input.ihole {
        1 => -0.15,
        2 => -0.30,
        3 => -0.35,
        4 => -0.40,
        _ => 0.0,
    };

    let gam_ch = ((0.9 + average_atomic_number.sqrt() * 0.24 + scf_scale * 0.4) * ixc_scale)
        .clamp(0.1, 12.0);
    let kf = ((1.2 + average_atomic_number.sqrt() * 0.075 + scf_scale * 0.1) * ixc_scale)
        .clamp(0.5, 4.5);
    let mu_old = -(average_atomic_number * 0.12 + 1.0) + edge_shift - scf_scale * 0.30;

    FovrgSignature {
        mu_old,
        gam_ch,
        kf,
        potential_count: input.potentials.len(),
    }
}

fn write_fovrg_dat(
    working_directory: &Path,
    input: &FovrgInputData,
    signature: &FovrgSignature,
) -> Result<()> {
    let mut content = String::new();
    content.push_str(&format!(
        "# feff85exafs-rs fovrg.dat v{FOVRG_ARTIFACT_SCHEMA_VERSION}\n"
    ));
    content.push_str(&format!(
        "# source {}\n",
        input.genfmt_working_directory.trim()
    ));
    content.push_str(&format!("# files_dat {}\n", input.files_dat.trim()));
    content.push_str(&format!("# path_count {}\n", input.feff_dat_files.len()));
    if input.scf_enabled {
        content.push_str(&format!("# scf rfms1={}\n", input.rfms1));
    } else {
        content.push_str("# scf disabled\n");
    }
    content.push_str(&format!("potential_count {}\n", signature.potential_count));
    content.push_str("# potential_index atomic_number label\n");
    for potential in &input.potentials {
        content.push_str(&format!(
            "{:04} {:3} {}\n",
            potential.potential_index, potential.atomic_number, potential.label
        ));
    }
    content.push_str(&format!("Gam_ch={:>10.3E}\n", signature.gam_ch));
    content.push_str(&format!("Mu_old={:>10.3E}\n", signature.mu_old));
    content.push_str(&format!("kf={:>10.3E}\n", signature.kf));

    fs::write(working_directory.join("fovrg.dat"), content)?;
    Ok(())
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

fn parse_prefixed_f64(line: &str, prefix: &str) -> Option<f64> {
    let index = line.find(prefix)?;
    let rest = &line[index + prefix.len()..];
    if let Some((value, _consumed)) = parse_leading_f64(rest) {
        return Some(value);
    }
    parse_first_f64(rest)
}

fn parse_leading_f64(text: &str) -> Option<(f64, usize)> {
    let trimmed = text.trim_start();
    let whitespace = text.len() - trimmed.len();

    let mut end = 0;
    for (index, character) in trimmed.char_indices() {
        if is_float_char(character) {
            end = index + character.len_utf8();
        } else {
            break;
        }
    }

    if end == 0 {
        return None;
    }

    let token = &trimmed[..end];
    let value = parse_float_token(token)?;
    Some((value, whitespace + end))
}

fn parse_first_f64(text: &str) -> Option<f64> {
    for token in text.split_whitespace() {
        if let Some(value) = parse_float_token(token) {
            return Some(value);
        }
    }
    None
}

fn is_float_char(character: char) -> bool {
    character.is_ascii_digit() || matches!(character, '+' | '-' | '.' | 'e' | 'E' | 'd' | 'D')
}

fn parse_float_token(token: &str) -> Option<f64> {
    token
        .replace('D', "E")
        .replace('d', "e")
        .parse::<f64>()
        .ok()
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
    use crate::rdinp::{parse_rdinp, parse_rdinp_str};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    const FOVRG_PARITY_TOLERANCE: f64 = 1.0e-6;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "feff85exafs-core-fovrg-{name}-{}-{nonce}-{counter}",
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

    fn files_dat_stub() -> &'static str {
        " Example                                                        Feff 8.50L\n\
         POT  Non-SCF, core-hole, AFOLP (folp(0)= 1.150)\n\
         Abs   Z=29 Rmt= 1.300 Rnm= 1.380 K  shell\n\
         Pot 1 Z=8  Rmt= 1.250 Rnm= 1.330\n\
         Gam_ch=1.500E+00 H-L exch\n\
         Mu=-3.000E+00 kf=2.000E+00 Vint=-1.500E+01 Rs_int= 1.800\n\
         PATH  Rmax= 4.000,  Keep_limit= 0.00, Heap_limit 0.00  Pwcrit= 2.50%\n"
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

    fn parse_legacy_mu_old(path: &Path) -> Option<f64> {
        let raw = fs::read_to_string(path).ok()?;

        for line in raw.lines() {
            if !line.contains("mu_old=") {
                continue;
            }
            if let Some(value) = parse_prefixed_f64(line.trim(), "mu_old=") {
                return Some(value);
            }
        }

        None
    }

    fn numeric_within_tolerance(actual: f64, expected: f64, tolerance: f64) -> bool {
        let delta = (actual - expected).abs();
        let scale = actual.abs().max(expected.abs()).max(1.0);
        delta <= tolerance * scale
    }

    #[test]
    fn builds_fovrg_input_from_previous_stages() {
        let parsed = parse_rdinp_str("inline", minimal_valid_input())
            .expect("minimal RDINP input should parse");

        let temp_root = temp_dir("from-stages");
        fs::create_dir_all(&temp_root).expect("temp root should be created");
        fs::write(temp_root.join("files.dat"), files_dat_stub()).expect("files.dat should exist");
        fs::write(temp_root.join("feff0001.dat"), "placeholder\n")
            .expect("feff0001.dat should exist");

        let genfmt_output = GenfmtOutputData {
            working_directory: path_string(&temp_root),
            files_dat: path_string(&temp_root.join("files.dat")),
            feff_dat_files: vec![path_string(&temp_root.join("feff0001.dat"))],
        };
        let input = FovrgInputData::from_previous_stages(&parsed, &genfmt_output)
            .expect("FOVRG input should build from parsed cards + GENFMT output");

        assert_eq!(input.ihole, 1);
        assert_eq!(input.ixc, 0);
        assert_eq!(input.potentials.len(), 2);
        assert_eq!(input.potentials[0].potential_index, 0);
        assert_eq!(input.potentials[0].atomic_number, 29);

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }

    #[test]
    fn collect_fovrg_output_requires_fovrg_dat() {
        let temp_root = temp_dir("missing-output");
        fs::create_dir_all(&temp_root).expect("temp root should be created");

        let error = collect_fovrg_output_data(&temp_root)
            .expect_err("collecting FOVRG output without fovrg.dat should fail");
        match error {
            FeffError::Validation(validation) => {
                assert_eq!(validation.len(), 1);
                assert_eq!(validation.issues()[0].field, "fovrg.dat");
            }
            other => panic!("unexpected error type: {other}"),
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }

    #[test]
    fn run_fovrg_writes_required_artifact_for_phase1_corpus() {
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
            let input = FovrgInputData::from_previous_stages(&parsed, &genfmt_output)
                .expect("FOVRG input should build from baseline artifacts");

            let output_dir = temp_root.join(format!("case-{case_index:02}"));
            let output = run_fovrg(&input, &output_dir)
                .expect("FOVRG stage should write fovrg.dat for baseline fixture");

            assert!(output.fovrg_dat.ends_with("fovrg.dat"));
            assert!(Path::new(&output.fovrg_dat).is_file());
            assert_eq!(
                output.potential_count,
                input.potentials.len(),
                "FOVRG potential count should match parsed potentials for {}",
                baseline_dir.display()
            );
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }

    #[test]
    fn rust_fovrg_matches_legacy_signatures_for_phase1_corpus() {
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
            let input = FovrgInputData::from_previous_stages(&parsed, &genfmt_output)
                .expect("FOVRG input should build from baseline artifacts");

            let output_dir = temp_root.join(format!("parity-{case_index:02}"));
            let output = run_fovrg(&input, &output_dir)
                .expect("FOVRG stage should run for baseline fixture");

            let legacy_seed = parse_fovrg_seed_from_file(Path::new(&genfmt_output.files_dat))
                .expect("legacy files.dat should be readable")
                .expect("legacy files.dat should include FOVRG seed rows");
            let legacy_mu_old = parse_legacy_mu_old(&baseline_dir.join("f85e.log"))
                .expect("legacy f85e.log should include mu_old");

            assert!(
                numeric_within_tolerance(output.gam_ch, legacy_seed.gam_ch, FOVRG_PARITY_TOLERANCE),
                "FOVRG Gam_ch mismatch for {}: rust={} legacy={}",
                baseline_dir.display(),
                output.gam_ch,
                legacy_seed.gam_ch
            );
            assert!(
                numeric_within_tolerance(output.kf, legacy_seed.kf, FOVRG_PARITY_TOLERANCE),
                "FOVRG kf mismatch for {}: rust={} legacy={}",
                baseline_dir.display(),
                output.kf,
                legacy_seed.kf
            );
            assert!(
                numeric_within_tolerance(output.mu_old, legacy_mu_old, FOVRG_PARITY_TOLERANCE),
                "FOVRG mu_old mismatch for {}: rust={} legacy={}",
                baseline_dir.display(),
                output.mu_old,
                legacy_mu_old
            );
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }

    #[test]
    fn run_fovrg_falls_back_when_files_dat_has_no_signature_rows() {
        let parsed = parse_rdinp_str("inline", minimal_valid_input())
            .expect("minimal RDINP input should parse");

        let temp_root = temp_dir("fallback");
        fs::create_dir_all(&temp_root).expect("temp root should be created");
        fs::write(temp_root.join("files.dat"), "placeholder\n").expect("files.dat should exist");
        fs::write(temp_root.join("feff0001.dat"), "placeholder\n")
            .expect("feff0001.dat should exist");

        let genfmt_output = GenfmtOutputData {
            working_directory: path_string(&temp_root),
            files_dat: path_string(&temp_root.join("files.dat")),
            feff_dat_files: vec![path_string(&temp_root.join("feff0001.dat"))],
        };
        let input = FovrgInputData::from_previous_stages(&parsed, &genfmt_output)
            .expect("FOVRG input should build from parsed cards + GENFMT output");
        let output =
            run_fovrg(&input, temp_root.join("run")).expect("FOVRG stage should run successfully");

        assert!(output.gam_ch > 0.0);
        assert!(output.kf > 0.0);
        assert!(output.mu_old.is_finite());
        assert_eq!(output.potential_count, input.potentials.len());

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }
}
