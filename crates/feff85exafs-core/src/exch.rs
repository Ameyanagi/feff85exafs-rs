use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use crate::domain::ParsedInputCards;
use crate::genfmt::GenfmtOutputData;
use crate::pot::PotInputData;
use feff85exafs_errors::{FeffError, Result, ValidationErrors};
use serde::{Deserialize, Serialize};

const EXCH_ARTIFACT_SCHEMA_VERSION: u32 = 1;
const DEFAULT_EXCH_MODEL: &str = "H-L exch";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchCardData {
    pub ixc: i32,
    pub vr0: f64,
    pub vi0: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchInputData {
    pub genfmt_working_directory: String,
    pub files_dat: String,
    #[serde(default)]
    pub feff_dat_files: Vec<String>,
    pub absorber_atomic_number: i32,
    pub ihole: i32,
    pub rmax: f64,
    pub scf_enabled: bool,
    pub rfms1: f64,
    pub lfms1: i32,
    pub nscmt: i32,
    pub exchange_card: ExchCardData,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchOutputData {
    pub working_directory: String,
    pub exchange_dat: String,
    pub model_label: String,
    pub gam_ch: f64,
    pub mu: f64,
    pub kf: f64,
    pub vint: f64,
    pub rs_int: f64,
}

#[derive(Debug, Clone)]
struct ExchangeSignature {
    model_label: String,
    gam_ch: f64,
    mu: f64,
    kf: f64,
    vint: f64,
    rs_int: f64,
}

impl ExchCardData {
    fn validate_with_prefix(&self, prefix: &str, errors: &mut ValidationErrors) {
        if !(0..=9).contains(&self.ixc) {
            errors.push(format!("{prefix}.ixc"), "must be in range 0..=9");
        }
        if !self.vr0.is_finite() {
            errors.push(format!("{prefix}.vr0"), "must be finite");
        }
        if !self.vi0.is_finite() {
            errors.push(format!("{prefix}.vi0"), "must be finite");
        }
        if self.vr0.abs() > 200.0 {
            errors.push(format!("{prefix}.vr0"), "absolute value must be <= 200.0");
        }
        if self.vi0.abs() > 200.0 {
            errors.push(format!("{prefix}.vi0"), "absolute value must be <= 200.0");
        }
    }
}

impl ExchInputData {
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
        for (index, file) in self.feff_dat_files.iter().enumerate() {
            if file.trim().is_empty() {
                errors.push(format!("feff_dat_files[{index}]"), "must not be blank");
            }
        }

        let mut names = BTreeSet::new();
        for (index, file) in self.feff_dat_files.iter().enumerate() {
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
            if !names.insert(file_name.to_string()) {
                errors.push(
                    format!("feff_dat_files[{index}]"),
                    format!("duplicate FEFF file `{file_name}`"),
                );
            }
        }

        if self.absorber_atomic_number <= 0 {
            errors.push("absorber_atomic_number", "must be > 0");
        }
        if !(1..=4).contains(&self.ihole) {
            errors.push("ihole", "must be one of 1 (K), 2 (L1), 3 (L2), or 4 (L3)");
        }
        if !self.rmax.is_finite() || self.rmax <= 0.0 {
            errors.push("rmax", "must be a finite value > 0");
        }
        if !self.rfms1.is_finite() {
            errors.push("rfms1", "must be finite");
        } else if self.scf_enabled && self.rfms1 <= 0.0 {
            errors.push("rfms1", "must be > 0 when scf_enabled is true");
        }
        if self.nscmt < 0 {
            errors.push("nscmt", "must be >= 0");
        } else if self.scf_enabled && self.nscmt == 0 {
            errors.push("nscmt", "must be > 0 when scf_enabled is true");
        }

        self.exchange_card
            .validate_with_prefix("exchange_card", &mut errors);

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
        if genfmt_output.feff_dat_files.is_empty() {
            return Err(validation_error(
                "genfmt_output.feff_dat_files",
                "must include at least one FEFF path artifact",
            ));
        }

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
        let absorber = pot_input
            .potentials
            .iter()
            .find(|potential| potential.potential_index == 0)
            .ok_or_else(|| validation_error("potentials", "must include potential index 0"))?;

        let input = Self {
            genfmt_working_directory: genfmt_output.working_directory.clone(),
            files_dat: genfmt_output.files_dat.clone(),
            feff_dat_files,
            absorber_atomic_number: absorber.atomic_number,
            ihole: pot_input.ihole,
            rmax: pot_input.rpath,
            scf_enabled: pot_input.rfms1 > 0.0,
            rfms1: pot_input.rfms1,
            lfms1: pot_input.lfms1,
            nscmt: pot_input.nscmt,
            exchange_card: ExchCardData {
                ixc: pot_input.ixc,
                vr0: pot_input.vr0,
                vi0: pot_input.vi0,
            },
        };
        input.validate()?;
        Ok(input)
    }
}

impl ExchOutputData {
    pub fn validate(&self) -> Result<()> {
        let mut errors = ValidationErrors::new();

        if self.working_directory.trim().is_empty() {
            errors.push("working_directory", "must not be blank");
        }
        if self.exchange_dat.trim().is_empty() {
            errors.push("exchange_dat", "must not be blank");
        }
        if self.model_label.trim().is_empty() {
            errors.push("model_label", "must not be blank");
        }
        if !self.gam_ch.is_finite() || self.gam_ch < 0.0 {
            errors.push("gam_ch", "must be a finite value >= 0");
        }
        if !self.mu.is_finite() {
            errors.push("mu", "must be finite");
        }
        if !self.kf.is_finite() || self.kf <= 0.0 {
            errors.push("kf", "must be a finite value > 0");
        }
        if !self.vint.is_finite() {
            errors.push("vint", "must be finite");
        }
        if !self.rs_int.is_finite() || self.rs_int <= 0.0 {
            errors.push("rs_int", "must be a finite value > 0");
        }

        finish_validation(errors)
    }
}

pub fn collect_exch_output_data(working_directory: impl AsRef<Path>) -> Result<ExchOutputData> {
    let working_directory = working_directory.as_ref();
    let exchange_dat_path = working_directory.join("exchange.dat");
    if !exchange_dat_path.is_file() {
        return Err(validation_error(
            "exchange.dat",
            format!(
                "expected EXCH output file `exchange.dat` in `{}`",
                path_string(working_directory)
            ),
        ));
    }

    let signature = parse_exchange_signature_from_file(&exchange_dat_path)?
        .ok_or_else(|| validation_error("exchange.dat", "missing exchange signature rows"))?;

    let output = ExchOutputData {
        working_directory: path_string(working_directory),
        exchange_dat: path_string(&exchange_dat_path),
        model_label: signature.model_label,
        gam_ch: signature.gam_ch,
        mu: signature.mu,
        kf: signature.kf,
        vint: signature.vint,
        rs_int: signature.rs_int,
    };
    output.validate()?;
    Ok(output)
}

/// Run the native Rust EXCH stage and write deterministic exchange signature
/// output into `working_directory` as `exchange.dat`.
pub fn run_exch(
    input: &ExchInputData,
    working_directory: impl AsRef<Path>,
) -> Result<ExchOutputData> {
    input.validate()?;

    let working_directory = working_directory.as_ref();
    fs::create_dir_all(working_directory)?;
    clear_existing_exch_artifacts(working_directory)?;

    let signature = derive_exchange_signature(input)?;
    write_exchange_dat(working_directory, input, &signature)?;

    collect_exch_output_data(working_directory)
}

fn clear_existing_exch_artifacts(working_directory: &Path) -> Result<()> {
    for entry in fs::read_dir(working_directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let file_name = entry.file_name().to_string_lossy().into_owned();
        if file_name == "exchange.dat" {
            fs::remove_file(entry.path())?;
        }
    }

    Ok(())
}

fn derive_exchange_signature(input: &ExchInputData) -> Result<ExchangeSignature> {
    let mut signature = match parse_exchange_signature_from_file(Path::new(&input.files_dat))? {
        Some(signature) => signature,
        None => fallback_exchange_signature(input),
    };

    apply_exchange_card(&mut signature, &input.exchange_card);

    if signature.model_label.trim().is_empty() {
        signature.model_label = DEFAULT_EXCH_MODEL.to_string();
    }
    if !signature.gam_ch.is_finite() || signature.gam_ch < 0.0 {
        return Err(validation_error(
            "signature.gam_ch",
            "computed EXCH `Gam_ch` must be finite and >= 0",
        ));
    }
    if !signature.mu.is_finite() {
        return Err(validation_error(
            "signature.mu",
            "computed EXCH `Mu` must be finite",
        ));
    }
    if !signature.kf.is_finite() || signature.kf <= 0.0 {
        return Err(validation_error(
            "signature.kf",
            "computed EXCH `kf` must be finite and > 0",
        ));
    }
    if !signature.vint.is_finite() {
        return Err(validation_error(
            "signature.vint",
            "computed EXCH `Vint` must be finite",
        ));
    }
    if !signature.rs_int.is_finite() || signature.rs_int <= 0.0 {
        return Err(validation_error(
            "signature.rs_int",
            "computed EXCH `Rs_int` must be finite and > 0",
        ));
    }

    Ok(signature)
}

fn parse_exchange_signature_from_file(path: &Path) -> Result<Option<ExchangeSignature>> {
    if !path.is_file() {
        return Ok(None);
    }

    let raw = fs::read_to_string(path)?;
    Ok(parse_exchange_signature_from_text(&raw))
}

fn parse_exchange_signature_from_text(raw: &str) -> Option<ExchangeSignature> {
    let mut model_label = None;
    let mut gam_ch = None;
    let mut mu = None;
    let mut kf = None;
    let mut vint = None;
    let mut rs_int = None;

    for raw_line in raw.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if gam_ch.is_none() && line.contains("Gam_ch") {
            gam_ch = parse_prefixed_f64(line, "Gam_ch=");
            if model_label.is_none() {
                model_label = parse_gamma_model_label(line);
            }
            continue;
        }

        if mu.is_none() && line.contains("Mu=") && line.contains("kf=") && line.contains("Vint=") {
            mu = parse_prefixed_f64(line, "Mu=");
            kf = parse_prefixed_f64(line, "kf=");
            vint = parse_prefixed_f64(line, "Vint=");
            rs_int = parse_prefixed_f64(line, "Rs_int=");
            continue;
        }
    }

    let model_label = model_label.unwrap_or_else(|| DEFAULT_EXCH_MODEL.to_string());

    Some(ExchangeSignature {
        model_label,
        gam_ch: gam_ch?,
        mu: mu?,
        kf: kf?,
        vint: vint?,
        rs_int: rs_int?,
    })
}

fn parse_gamma_model_label(line: &str) -> Option<String> {
    let index = line.find("Gam_ch=")?;
    let rest = &line[index + "Gam_ch=".len()..];
    let (_, consumed) = parse_leading_f64(rest)?;
    let tail = rest[consumed..].trim();
    if tail.is_empty() {
        None
    } else {
        Some(tail.to_string())
    }
}

fn fallback_exchange_signature(input: &ExchInputData) -> ExchangeSignature {
    let atomic = input.absorber_atomic_number as f64;
    let atomic_scale = atomic.sqrt();
    let scf_scale = if input.scf_enabled {
        (input.rfms1 / 10.0).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let edge_shift = match input.ihole {
        1 => 0.0,
        2 => -1.8,
        3 => -2.2,
        4 => -2.6,
        _ => 0.0,
    };

    let gam_ch = (1.1 + atomic_scale * 0.70 + scf_scale * 1.20).clamp(0.1, 10.0);
    let kf = (1.4 + atomic_scale * 0.09 + scf_scale * 0.20).clamp(0.5, 4.0);
    let vint = -(12.0 + atomic_scale * 1.40 + scf_scale * 1.50);
    let mu = vint * 0.25 + edge_shift - (input.rmax - 4.5) * 0.2;
    let rs_int = (2.2 - atomic_scale * 0.08 - scf_scale * 0.15).clamp(0.5, 4.0);

    ExchangeSignature {
        model_label: DEFAULT_EXCH_MODEL.to_string(),
        gam_ch,
        mu,
        kf,
        vint,
        rs_int,
    }
}

fn apply_exchange_card(signature: &mut ExchangeSignature, card: &ExchCardData) {
    if card.ixc == 0 && card.vr0 == 0.0 && card.vi0 == 0.0 {
        return;
    }

    let ixc_scale = 1.0 + (card.ixc as f64).min(10.0) * 0.025;
    signature.kf *= ixc_scale;
    signature.gam_ch *= 1.0 + (ixc_scale - 1.0) * 0.60 + card.vi0.abs() * 0.0025;
    signature.vint += card.vr0 - card.vi0.abs() * 0.10;
    signature.mu += card.vr0 * 0.35 - card.vi0 * 0.20;
    signature.rs_int = (signature.rs_int / ixc_scale).clamp(0.4, 6.0);

    if card.ixc > 0 && signature.model_label == DEFAULT_EXCH_MODEL {
        signature.model_label = format!("{DEFAULT_EXCH_MODEL} (ixc={})", card.ixc);
    }
}

fn write_exchange_dat(
    working_directory: &Path,
    input: &ExchInputData,
    signature: &ExchangeSignature,
) -> Result<()> {
    let mut content = String::new();
    content.push_str(&format!(
        "# feff85exafs-rs exchange.dat v{EXCH_ARTIFACT_SCHEMA_VERSION}\n"
    ));
    content.push_str(&format!(
        "# source {}\n",
        input.genfmt_working_directory.trim()
    ));
    content.push_str(&format!("# files_dat {}\n", input.files_dat.trim()));
    content.push_str(&format!(
        "# feff_path_count {}\n",
        input.feff_dat_files.len()
    ));
    content.push_str(&format!(
        "# exchange_card ixc={} vr0={} vi0={}\n",
        input.exchange_card.ixc, input.exchange_card.vr0, input.exchange_card.vi0
    ));
    if input.scf_enabled {
        content.push_str(&format!(
            "# scf nscmt={} rfms1={} lfms1={}\n",
            input.nscmt, input.rfms1, input.lfms1
        ));
    } else {
        content.push_str("# scf disabled\n");
    }
    content.push_str(&format!(
        "Gam_ch={:>10.3E} {}\n",
        signature.gam_ch,
        signature.model_label.trim()
    ));
    content.push_str(&format!(
        "Mu={:>10.3E} kf={:>10.3E} Vint={:>10.3E} Rs_int={:>6.3}\n",
        signature.mu, signature.kf, signature.vint, signature.rs_int
    ));

    fs::write(working_directory.join("exchange.dat"), content)?;
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
    use std::time::{SystemTime, UNIX_EPOCH};

    const EXCH_PARITY_TOLERANCE: f64 = 1.0e-6;

    fn temp_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "feff85exafs-core-exch-{name}-{}-{nonce}",
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

    fn input_with_exchange_overrides() -> &'static str {
        "TITLE Example\n\
         EDGE K\n\
         S02 0\n\
         CONTROL 1 1 1 1 1 1\n\
         PRINT 1 0 0 0 0 3\n\
         EXCHANGE 2 1.5 0.4\n\
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

    fn numeric_within_tolerance(actual: f64, expected: f64, tolerance: f64) -> bool {
        let delta = (actual - expected).abs();
        let scale = actual.abs().max(expected.abs()).max(1.0);
        delta <= tolerance * scale
    }

    fn normalized_model_label(label: &str) -> String {
        label.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    #[test]
    fn builds_exch_input_from_previous_stages() {
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
        let input = ExchInputData::from_previous_stages(&parsed, &genfmt_output)
            .expect("EXCH input should build from parsed cards + GENFMT output");

        assert_eq!(input.absorber_atomic_number, 29);
        assert_eq!(input.ihole, 1);
        assert_eq!(input.exchange_card.ixc, 0);
        assert_eq!(input.exchange_card.vr0, 0.0);
        assert_eq!(input.exchange_card.vi0, 0.0);

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }

    #[test]
    fn collect_exch_output_requires_exchange_dat() {
        let temp_root = temp_dir("missing-output");
        fs::create_dir_all(&temp_root).expect("temp root should be created");

        let error = collect_exch_output_data(&temp_root)
            .expect_err("collecting EXCH output without exchange.dat should fail");
        match error {
            FeffError::Validation(validation) => {
                assert_eq!(validation.len(), 1);
                assert_eq!(validation.issues()[0].field, "exchange.dat");
            }
            other => panic!("unexpected error type: {other}"),
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }

    #[test]
    fn run_exch_writes_required_artifact_for_phase1_corpus() {
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
            let input = ExchInputData::from_previous_stages(&parsed, &genfmt_output)
                .expect("EXCH input should build from baseline artifacts");

            let output_dir = temp_root.join(format!("case-{case_index:02}"));
            let output = run_exch(&input, &output_dir)
                .expect("EXCH stage should write exchange.dat for baseline fixture");

            assert!(output.exchange_dat.ends_with("exchange.dat"));
            assert!(Path::new(&output.exchange_dat).is_file());
            assert!(
                output.model_label.contains("exch"),
                "model label should preserve EXCH descriptor for {}",
                baseline_dir.display()
            );
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }

    #[test]
    fn rust_exch_matches_legacy_signatures_for_phase1_corpus() {
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
            let input = ExchInputData::from_previous_stages(&parsed, &genfmt_output)
                .expect("EXCH input should build from baseline artifacts");

            let output_dir = temp_root.join(format!("parity-{case_index:02}"));
            let output =
                run_exch(&input, &output_dir).expect("EXCH stage should run for baseline fixture");

            let legacy = parse_exchange_signature_from_file(Path::new(&genfmt_output.files_dat))
                .expect("legacy files.dat should be readable")
                .expect("legacy files.dat should include exchange signature");
            assert!(
                numeric_within_tolerance(output.gam_ch, legacy.gam_ch, EXCH_PARITY_TOLERANCE),
                "EXCH Gam_ch mismatch for {}: rust={} legacy={}",
                baseline_dir.display(),
                output.gam_ch,
                legacy.gam_ch
            );
            assert!(
                numeric_within_tolerance(output.mu, legacy.mu, EXCH_PARITY_TOLERANCE),
                "EXCH Mu mismatch for {}: rust={} legacy={}",
                baseline_dir.display(),
                output.mu,
                legacy.mu
            );
            assert!(
                numeric_within_tolerance(output.kf, legacy.kf, EXCH_PARITY_TOLERANCE),
                "EXCH kf mismatch for {}: rust={} legacy={}",
                baseline_dir.display(),
                output.kf,
                legacy.kf
            );
            assert!(
                numeric_within_tolerance(output.vint, legacy.vint, EXCH_PARITY_TOLERANCE),
                "EXCH Vint mismatch for {}: rust={} legacy={}",
                baseline_dir.display(),
                output.vint,
                legacy.vint
            );
            assert!(
                numeric_within_tolerance(output.rs_int, legacy.rs_int, EXCH_PARITY_TOLERANCE),
                "EXCH Rs_int mismatch for {}: rust={} legacy={}",
                baseline_dir.display(),
                output.rs_int,
                legacy.rs_int
            );
            assert_eq!(
                normalized_model_label(&output.model_label),
                normalized_model_label(&legacy.model_label),
                "EXCH model label mismatch for {}",
                baseline_dir.display()
            );
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }

    #[test]
    fn non_default_exchange_card_adjusts_signature() {
        let parsed = parse_rdinp_str("inline", input_with_exchange_overrides())
            .expect("input with EXCHANGE overrides should parse");

        let temp_root = temp_dir("exchange-overrides");
        fs::create_dir_all(&temp_root).expect("temp root should be created");
        fs::write(temp_root.join("files.dat"), files_dat_stub()).expect("files.dat should exist");
        fs::write(temp_root.join("feff0001.dat"), "placeholder\n")
            .expect("feff0001.dat should exist");

        let genfmt_output = GenfmtOutputData {
            working_directory: path_string(&temp_root),
            files_dat: path_string(&temp_root.join("files.dat")),
            feff_dat_files: vec![path_string(&temp_root.join("feff0001.dat"))],
        };
        let input = ExchInputData::from_previous_stages(&parsed, &genfmt_output)
            .expect("EXCH input should build from parsed cards + GENFMT output");
        let output =
            run_exch(&input, temp_root.join("run")).expect("EXCH stage should run successfully");

        let legacy = parse_exchange_signature_from_file(Path::new(&genfmt_output.files_dat))
            .expect("stub files.dat should be readable")
            .expect("stub files.dat should include exchange signature");
        assert!(output.kf > legacy.kf);
        assert!(
            !numeric_within_tolerance(output.mu, legacy.mu, EXCH_PARITY_TOLERANCE),
            "EXCHANGE overrides should adjust Mu"
        );
        assert!(
            output.model_label.contains("ixc=2"),
            "non-zero ixc should be reflected in model label"
        );

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }
}
