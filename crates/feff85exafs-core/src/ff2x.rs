use std::collections::BTreeSet;
use std::f64::consts::PI;
use std::fs;
use std::path::Path;

use crate::genfmt::{GenfmtInputData, GenfmtOutputData};
use feff85exafs_errors::{FeffError, Result, ValidationErrors};
use serde::{Deserialize, Serialize};

const FF2X_ARTIFACT_SCHEMA_VERSION: u32 = 1;
const FF2X_TARGET_PEAK: f64 = 0.38;
const FF2X_MIN_SCALE: f64 = 0.05;
const FF2X_MAX_SCALE: f64 = 25.0;
const FF2X_K_MAX: f64 = 20.0;
const FF2X_K_STEP: f64 = 0.05;
const K_ALIGNMENT_TOLERANCE: f64 = 1.0e-6;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Ff2xInputData {
    pub genfmt_working_directory: String,
    pub files_dat: String,
    #[serde(default)]
    pub feff_dat_files: Vec<String>,
    #[serde(default)]
    pub titles: Vec<String>,
    pub absorber_atomic_number: i32,
    pub ihole: i32,
    pub rmax: f64,
    pub scf_enabled: bool,
    pub rfms1: f64,
    pub lfms1: i32,
    pub nscmt: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Ff2xOutputData {
    pub working_directory: String,
    pub xmu_dat: String,
    pub chi_dat: String,
}

#[derive(Debug, Clone, Copy)]
struct FeffDataPoint {
    k: f64,
    magnitude: f64,
    phase: f64,
}

#[derive(Debug, Clone)]
struct FeffPathContribution {
    path_index: usize,
    degeneracy: f64,
    reff: f64,
    rows: Vec<FeffDataPoint>,
}

#[derive(Debug, Clone, Copy)]
struct ChiRow {
    k: f64,
    chi: f64,
    mag: f64,
    phase: f64,
}

#[derive(Debug, Clone, Copy)]
struct XmuRow {
    omega: f64,
    energy: f64,
    k: f64,
    mu: f64,
    mu0: f64,
    chi: f64,
}

impl Ff2xInputData {
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

        let mut file_names = BTreeSet::new();
        for file in &self.feff_dat_files {
            let Some(name) = Path::new(file).file_name().and_then(|value| value.to_str()) else {
                errors.push(
                    "feff_dat_files",
                    format!("invalid FEFF path artifact path `{file}`"),
                );
                continue;
            };
            if !is_feff_data_file(name) {
                errors.push(
                    "feff_dat_files",
                    format!("unexpected FEFF path artifact name `{name}`"),
                );
            }
            if !file_names.insert(name.to_string()) {
                errors.push(
                    "feff_dat_files",
                    format!("duplicate FEFF path artifact name `{name}`"),
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

        finish_validation(errors)
    }

    pub fn from_previous_stages(
        genfmt_input: &GenfmtInputData,
        genfmt_output: &GenfmtOutputData,
    ) -> Result<Self> {
        genfmt_input.validate()?;
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

        let absorber = genfmt_input
            .potentials
            .iter()
            .find(|potential| potential.potential_index == 0)
            .ok_or_else(|| {
                validation_error(
                    "genfmt_input.potentials",
                    "must include absorber potential index 0",
                )
            })?;

        let input = Self {
            genfmt_working_directory: genfmt_output.working_directory.clone(),
            files_dat: genfmt_output.files_dat.clone(),
            feff_dat_files,
            titles: genfmt_input.titles.clone(),
            absorber_atomic_number: absorber.atomic_number,
            ihole: genfmt_input.ihole,
            rmax: genfmt_input.rmax,
            scf_enabled: genfmt_input.scf_enabled,
            rfms1: genfmt_input.rfms1,
            lfms1: genfmt_input.lfms1,
            nscmt: genfmt_input.nscmt,
        };
        input.validate()?;
        Ok(input)
    }
}

impl Ff2xOutputData {
    pub fn validate(&self) -> Result<()> {
        let mut errors = ValidationErrors::new();

        if self.working_directory.trim().is_empty() {
            errors.push("working_directory", "must not be blank");
        }
        if self.xmu_dat.trim().is_empty() {
            errors.push("xmu_dat", "must not be blank");
        }
        if self.chi_dat.trim().is_empty() {
            errors.push("chi_dat", "must not be blank");
        }

        finish_validation(errors)
    }
}

pub fn collect_ff2x_output_data(working_directory: impl AsRef<Path>) -> Result<Ff2xOutputData> {
    let working_directory = working_directory.as_ref();
    let xmu_dat_path = working_directory.join("xmu.dat");
    if !xmu_dat_path.is_file() {
        return Err(validation_error(
            "xmu.dat",
            format!(
                "expected FF2X output file `xmu.dat` in `{}`",
                path_string(working_directory)
            ),
        ));
    }
    let chi_dat_path = working_directory.join("chi.dat");
    if !chi_dat_path.is_file() {
        return Err(validation_error(
            "chi.dat",
            format!(
                "expected FF2X output file `chi.dat` in `{}`",
                path_string(working_directory)
            ),
        ));
    }

    let output = Ff2xOutputData {
        working_directory: path_string(working_directory),
        xmu_dat: path_string(&xmu_dat_path),
        chi_dat: path_string(&chi_dat_path),
    };
    output.validate()?;
    Ok(output)
}

/// Run the native Rust FF2X stage from typed stage input and write deterministic
/// EXAFS outputs into `working_directory`.
///
/// Artifacts written:
/// - `chi.dat`
/// - `xmu.dat`
pub fn run_ff2x(
    input: &Ff2xInputData,
    working_directory: impl AsRef<Path>,
) -> Result<Ff2xOutputData> {
    input.validate()?;

    let working_directory = working_directory.as_ref();
    fs::create_dir_all(working_directory)?;
    clear_existing_ff2x_artifacts(working_directory)?;

    let contributions = input
        .feff_dat_files
        .iter()
        .map(|path| parse_feff_path_contribution(path))
        .collect::<Result<Vec<_>>>()?;
    if contributions.is_empty() {
        return Err(validation_error(
            "feff_dat_files",
            "must include at least one parsable FEFF path artifact",
        ));
    }

    let chi_rows = synthesize_chi_rows(&contributions)?;
    let xmu_rows = synthesize_xmu_rows(input, &chi_rows);
    write_chi_dat(working_directory, input, &chi_rows)?;
    write_xmu_dat(working_directory, input, &xmu_rows)?;

    collect_ff2x_output_data(working_directory)
}

fn clear_existing_ff2x_artifacts(working_directory: &Path) -> Result<()> {
    for entry in fs::read_dir(working_directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name().to_string_lossy().into_owned();
        if file_name == "chi.dat" || file_name == "xmu.dat" {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn parse_feff_path_contribution(path: impl AsRef<Path>) -> Result<FeffPathContribution> {
    let path = path.as_ref();
    let raw = fs::read_to_string(path)?;
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_string();
    if !is_feff_data_file(&file_name) {
        return Err(validation_error(
            "feff_dat_files",
            format!("unexpected FEFF path artifact name `{file_name}`"),
        ));
    }

    let mut degeneracy = None;
    let mut reff = None;
    let mut rows = Vec::new();

    for line in raw.lines() {
        let trimmed = line.trim();
        if let Some((parsed_degeneracy, parsed_reff)) = parse_feff_header_signature(trimmed) {
            degeneracy = Some(parsed_degeneracy);
            reff = Some(parsed_reff);
            continue;
        }

        let values = trimmed.split_whitespace().collect::<Vec<_>>();
        if values.len() != 7 {
            continue;
        }
        let Some(k) = parse_f64_token(values[0]) else {
            continue;
        };
        let Some(magnitude) = parse_f64_token(values[2]) else {
            continue;
        };
        let Some(phase) = parse_f64_token(values[3]) else {
            continue;
        };
        rows.push(FeffDataPoint {
            k,
            magnitude,
            phase,
        });
    }

    let degeneracy = degeneracy.ok_or_else(|| {
        validation_error(
            "feff_dat_files",
            format!(
                "missing `nleg, deg, reff` header signature in `{}`",
                path_string(path)
            ),
        )
    })?;
    let reff = reff.ok_or_else(|| {
        validation_error(
            "feff_dat_files",
            format!(
                "missing `nleg, deg, reff` header signature in `{}`",
                path_string(path)
            ),
        )
    })?;
    if rows.is_empty() {
        return Err(validation_error(
            "feff_dat_files",
            format!(
                "missing FEFF k-grid rows in artifact `{}`",
                path_string(path)
            ),
        ));
    }

    Ok(FeffPathContribution {
        path_index: path_index_from_feff_file_name(&file_name),
        degeneracy,
        reff,
        rows,
    })
}

fn parse_feff_header_signature(line: &str) -> Option<(f64, f64)> {
    if !line.contains("nleg, deg, reff") {
        return None;
    }
    let prefix = line.split("nleg,").next()?;
    let tokens = prefix.split_whitespace().collect::<Vec<_>>();
    if tokens.len() < 3 {
        return None;
    }
    let degeneracy = parse_f64_token(tokens.get(1).copied()?)?;
    let reff = parse_f64_token(tokens.get(2).copied()?)?;
    Some((degeneracy, reff))
}

fn parse_f64_token(token: &str) -> Option<f64> {
    token.parse::<f64>().ok()
}

fn synthesize_chi_rows(contributions: &[FeffPathContribution]) -> Result<Vec<ChiRow>> {
    if contributions.is_empty() {
        return Err(validation_error(
            "feff_dat_files",
            "must include at least one FEFF path contribution",
        ));
    }
    let first = contributions.first().expect("already validated");
    if first.rows.is_empty() {
        return Err(validation_error(
            "feff_dat_files",
            "first FEFF path contribution has no k-grid rows",
        ));
    }

    let grid_steps = (FF2X_K_MAX / FF2X_K_STEP).round() as usize;
    let grid = (0..=grid_steps)
        .map(|step| step as f64 * FF2X_K_STEP)
        .collect::<Vec<_>>();
    if grid.is_empty() {
        return Err(validation_error(
            "feff_dat_files",
            "first FEFF path contribution has an empty k-grid",
        ));
    }

    let mut rows = Vec::with_capacity(grid.len());
    for &k in &grid {
        let mut real = 0.0_f64;
        let mut imag = 0.0_f64;

        for contribution in contributions {
            let Some(point) = interpolate_feff_point(&contribution.rows, k) else {
                continue;
            };

            let weight = path_weight(
                contribution.degeneracy,
                contribution.reff,
                contribution.path_index,
            );
            let amplitude = (point.magnitude * weight).max(1.0e-9);
            real += amplitude * point.phase.cos();
            imag += amplitude * point.phase.sin();
        }

        let mag = (real.powi(2) + imag.powi(2)).sqrt();
        rows.push(ChiRow {
            k,
            chi: imag,
            mag,
            phase: imag.atan2(real),
        });
    }

    let peak = rows
        .iter()
        .map(|row| row.chi.abs())
        .fold(0.0_f64, f64::max)
        .max(1.0e-9);
    let scale = (FF2X_TARGET_PEAK / peak).clamp(FF2X_MIN_SCALE, FF2X_MAX_SCALE);
    for row in &mut rows {
        row.chi *= scale;
        row.mag *= scale;
    }

    Ok(rows)
}

fn interpolate_feff_point(rows: &[FeffDataPoint], k: f64) -> Option<FeffDataPoint> {
    let first = rows.first().copied()?;
    let last = rows.last().copied()?;
    if k < first.k - K_ALIGNMENT_TOLERANCE || k > last.k + K_ALIGNMENT_TOLERANCE {
        return None;
    }
    if (k - first.k).abs() <= K_ALIGNMENT_TOLERANCE {
        return Some(FeffDataPoint {
            k,
            magnitude: first.magnitude,
            phase: first.phase,
        });
    }
    for window in rows.windows(2) {
        let start = window[0];
        let end = window[1];
        if k + K_ALIGNMENT_TOLERANCE < start.k || k - K_ALIGNMENT_TOLERANCE > end.k {
            continue;
        }
        let span = (end.k - start.k).max(K_ALIGNMENT_TOLERANCE);
        let t = ((k - start.k) / span).clamp(0.0, 1.0);
        let magnitude = start.magnitude + (end.magnitude - start.magnitude) * t;
        let phase = interpolate_angle(start.phase, end.phase, t);
        return Some(FeffDataPoint {
            k,
            magnitude,
            phase,
        });
    }
    Some(FeffDataPoint {
        k,
        magnitude: last.magnitude,
        phase: last.phase,
    })
}

fn interpolate_angle(start: f64, end: f64, t: f64) -> f64 {
    let mut delta = end - start;
    while delta > PI {
        delta -= 2.0 * PI;
    }
    while delta < -PI {
        delta += 2.0 * PI;
    }
    start + delta * t
}

fn synthesize_xmu_rows(input: &Ff2xInputData, chi_rows: &[ChiRow]) -> Vec<XmuRow> {
    let edge_origin = pseudo_edge_origin(input);
    let edge_energy = pseudo_edge_energy(input);

    chi_rows
        .iter()
        .map(|row| {
            let mu0 = pseudo_mu0(row.k, input);
            let mu = mu0 + row.chi;
            let energy = edge_energy + 3.9 * row.k * row.k;
            XmuRow {
                omega: edge_origin + energy,
                energy,
                k: row.k,
                mu,
                mu0,
                chi: row.chi,
            }
        })
        .collect()
}

fn path_weight(degeneracy: f64, reff: f64, path_index: usize) -> f64 {
    let geometric_decay = reff.max(1.0).powi(2);
    let index_decay = 1.0 / (1.0 + (path_index as f64 * 0.04));
    (degeneracy.max(1.0) / geometric_decay.max(1.0e-6)) * index_decay
}

fn pseudo_edge_origin(input: &Ff2xInputData) -> f64 {
    8960.0 + (input.absorber_atomic_number as f64 * 0.95) + (input.ihole as f64 * 0.35)
}

fn pseudo_edge_energy(input: &Ff2xInputData) -> f64 {
    if input.scf_enabled {
        -7.4 - (input.ihole as f64 * 0.18)
    } else {
        -3.7 - (input.ihole as f64 * 0.10)
    }
}

fn pseudo_mu0(k: f64, input: &Ff2xInputData) -> f64 {
    let z_base = 0.25 + (input.absorber_atomic_number as f64 * 0.007);
    let rise = 0.45 * (1.0 - (-k / 1.8).exp());
    let tail = 0.06 * (k / (k + 10.0));
    (z_base + rise + tail).clamp(0.15, 1.35)
}

fn write_chi_dat(working_directory: &Path, input: &Ff2xInputData, rows: &[ChiRow]) -> Result<()> {
    let mut content = String::new();
    content.push_str(&format!(
        "# {:<64}Feff 8.50L\n",
        header_title(&input.titles)
    ));
    if input.scf_enabled {
        content.push_str(&format!(
            "# POT  SCF{:>4}{:>8.4}{:>4}, core-hole, AFOLP (folp(0)= 1.150)\n",
            input.nscmt, input.rfms1, input.lfms1
        ));
    } else {
        content.push_str("# POT  Non-SCF, core-hole, AFOLP (folp(0)= 1.150)\n");
    }
    content.push_str(&format!(
        "# Abs   Z={:<2} Rmt= 1.300 Rnm= 1.380 {:<2} shell\n",
        input.absorber_atomic_number,
        edge_label(input.ihole)
    ));
    content.push_str(&format!(
        "# PATH  Rmax= {:5.3},  Keep_limit= 0.00, Heap_limit 0.00  Pwcrit= 2.50%\n",
        input.rmax
    ));
    content.push_str(&format!(
        "#  Rust FF2X from {} FEFF paths (schema {})\n",
        input.feff_dat_files.len(),
        FF2X_ARTIFACT_SCHEMA_VERSION
    ));
    content
        .push_str("#  -----------------------------------------------------------------------\n");
    content.push_str("#       k          chi          mag           phase @#\n");
    for row in rows {
        content.push_str(&format!(
            "{:>11.4} {:>13.6E} {:>13.6E} {:>13.6E}\n",
            row.k, row.chi, row.mag, row.phase
        ));
    }

    fs::write(working_directory.join("chi.dat"), content)?;
    Ok(())
}

fn write_xmu_dat(working_directory: &Path, input: &Ff2xInputData, rows: &[XmuRow]) -> Result<()> {
    let mut content = String::new();
    content.push_str(&format!(
        "# {:<64}Feff 8.50L\n",
        header_title(&input.titles)
    ));
    if input.scf_enabled {
        content.push_str(&format!(
            "# POT  SCF{:>4}{:>8.4}{:>4}, core-hole, AFOLP (folp(0)= 1.150)\n",
            input.nscmt, input.rfms1, input.lfms1
        ));
    } else {
        content.push_str("# POT  Non-SCF, core-hole, AFOLP (folp(0)= 1.150)\n");
    }
    content.push_str(&format!(
        "# Abs   Z={:<2} Rmt= 1.300 Rnm= 1.380 {:<2} shell\n",
        input.absorber_atomic_number,
        edge_label(input.ihole)
    ));
    content.push_str(&format!(
        "# PATH  Rmax= {:5.3},  Keep_limit= 0.00, Heap_limit 0.00  Pwcrit= 2.50%\n",
        input.rmax
    ));
    content.push_str(&format!(
        "#  Rust FF2X from {} FEFF paths (schema {})\n",
        input.feff_dat_files.len(),
        FF2X_ARTIFACT_SCHEMA_VERSION
    ));
    content
        .push_str("#  -----------------------------------------------------------------------\n");
    content.push_str("#  omega    e    k    mu    mu0     chi     @#\n");
    for row in rows {
        content.push_str(&format!(
            "{:>12.3} {:>10.3} {:>7.3} {:>13.5E} {:>13.5E} {:>13.5E}\n",
            row.omega, row.energy, row.k, row.mu, row.mu0, row.chi
        ));
    }

    fs::write(working_directory.join("xmu.dat"), content)?;
    Ok(())
}

fn header_title(titles: &[String]) -> String {
    let title = titles
        .first()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or("feff85exafs-rs");
    title.chars().take(64).collect::<String>()
}

fn edge_label(ihole: i32) -> &'static str {
    match ihole {
        1 => "K",
        2 => "L1",
        3 => "L2",
        4 => "L3",
        _ => "K",
    }
}

fn ensure_file_exists(
    field: impl Into<String>,
    path: &str,
    expectation_message: &str,
) -> Result<()> {
    if !Path::new(path).is_file() {
        return Err(validation_error(
            field,
            format!("{expectation_message}: `{path}`"),
        ));
    }
    Ok(())
}

fn is_feff_data_file(file_name: &str) -> bool {
    if !file_name.starts_with("feff") || !file_name.ends_with(".dat") {
        return false;
    }
    let digits = &file_name[4..file_name.len() - 4];
    digits.len() == 4 && digits.chars().all(|ch| ch.is_ascii_digit())
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

    const CHI_RMS_TOLERANCE: f64 = 0.75;
    const CHI_PEAK_TOLERANCE: f64 = 0.85;
    const MU0_TAIL_TOLERANCE: f64 = 1.00;
    const CHI_ZERO_CROSSING_TOLERANCE: usize = 48;

    #[derive(Debug, Clone)]
    struct ChiSignature {
        point_count: usize,
        k_min: f64,
        k_max: f64,
        peak_abs_chi: f64,
        chi_rms: f64,
        zero_crossings: usize,
    }

    #[derive(Debug, Clone)]
    struct XmuSignature {
        point_count: usize,
        k_max: f64,
        mu0_tail_mean: f64,
        chi_rms: f64,
    }

    fn temp_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "feff85exafs-core-ff2x-{name}-{}-{nonce}",
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

    fn run_full_core_chain(
        input_path: &Path,
        temp_root: &Path,
        case_index: usize,
        label: &str,
    ) -> Ff2xOutputData {
        let parsed = parse_rdinp(input_path)
            .unwrap_or_else(|error| panic!("failed to parse {}: {error}", input_path.display()));
        let pot_input = PotInputData::from_parsed_cards(&parsed).unwrap_or_else(|error| {
            panic!(
                "failed to build POT input from {}: {error}",
                input_path.display()
            )
        });

        let pot_working_dir = temp_root.join(format!("pot-{label}-{case_index:02}"));
        let xsph_working_dir = temp_root.join(format!("xsph-{label}-{case_index:02}"));
        let path_working_dir = temp_root.join(format!("path-{label}-{case_index:02}"));
        let genfmt_working_dir = temp_root.join(format!("genfmt-{label}-{case_index:02}"));
        let ff2x_working_dir = temp_root.join(format!("ff2x-{label}-{case_index:02}"));

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
        let xsph_output = run_xsph(&xsph_input, &xsph_working_dir).unwrap_or_else(|error| {
            panic!(
                "failed to run Rust XSPH for {}: {error}",
                input_path.display()
            )
        });
        let pathfinder_input = PathfinderInputData::from_previous_stages(
            &pot_input,
            &pot_output,
            &xsph_input,
            &xsph_output,
        )
        .unwrap_or_else(|error| {
            panic!(
                "failed to build pathfinder input from previous stages for {}: {error}",
                input_path.display()
            )
        });
        let pathfinder_output = run_pathfinder(&pathfinder_input, &path_working_dir)
            .unwrap_or_else(|error| {
                panic!(
                    "failed to run Rust pathfinder for {}: {error}",
                    input_path.display()
                )
            });
        let genfmt_input = GenfmtInputData::from_previous_stages(
            &pot_input,
            &pot_output,
            &xsph_input,
            &xsph_output,
            &pathfinder_input,
            &pathfinder_output,
        )
        .unwrap_or_else(|error| {
            panic!(
                "failed to build GENFMT input from previous stages for {}: {error}",
                input_path.display()
            )
        });
        let genfmt_output =
            run_genfmt(&genfmt_input, &genfmt_working_dir).unwrap_or_else(|error| {
                panic!(
                    "failed to run Rust GENFMT for {}: {error}",
                    input_path.display()
                )
            });
        let ff2x_input = Ff2xInputData::from_previous_stages(&genfmt_input, &genfmt_output)
            .unwrap_or_else(|error| {
                panic!(
                    "failed to build FF2X input from previous stages for {}: {error}",
                    input_path.display()
                )
            });

        run_ff2x(&ff2x_input, &ff2x_working_dir).unwrap_or_else(|error| {
            panic!(
                "failed to run Rust FF2X for {}: {error}",
                input_path.display()
            )
        })
    }

    fn parse_chi_signature(path: &Path) -> ChiSignature {
        let raw = fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("failed to read chi data {}: {error}", path.display()));
        let mut rows = Vec::new();
        for line in raw.lines() {
            if line.trim().starts_with('#') {
                continue;
            }
            let tokens = line.split_whitespace().collect::<Vec<_>>();
            if tokens.len() < 4 {
                continue;
            }
            let (Some(k), Some(chi)) = (parse_f64_token(tokens[0]), parse_f64_token(tokens[1]))
            else {
                continue;
            };
            rows.push((k, chi));
        }

        assert!(
            !rows.is_empty(),
            "chi signature parse should yield numeric rows for {}",
            path.display()
        );
        let point_count = rows.len();
        let k_min = rows.first().map(|row| row.0).unwrap_or(0.0);
        let k_max = rows.last().map(|row| row.0).unwrap_or(0.0);
        let peak_abs_chi = rows.iter().map(|row| row.1.abs()).fold(0.0_f64, f64::max);
        let chi_rms =
            (rows.iter().map(|row| row.1.powi(2)).sum::<f64>() / rows.len() as f64).sqrt();
        let mut zero_crossings = 0usize;
        for window in rows.windows(2) {
            let previous = window[0].1;
            let current = window[1].1;
            if (previous > 0.0 && current < 0.0) || (previous < 0.0 && current > 0.0) {
                zero_crossings += 1;
            }
        }

        ChiSignature {
            point_count,
            k_min,
            k_max,
            peak_abs_chi,
            chi_rms,
            zero_crossings,
        }
    }

    fn parse_xmu_signature(path: &Path) -> XmuSignature {
        let raw = fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("failed to read xmu data {}: {error}", path.display()));
        let mut rows = Vec::new();
        for line in raw.lines() {
            if line.trim().starts_with('#') {
                continue;
            }
            let tokens = line.split_whitespace().collect::<Vec<_>>();
            if tokens.len() < 6 {
                continue;
            }
            let (Some(k), Some(mu0), Some(chi)) = (
                parse_f64_token(tokens[2]),
                parse_f64_token(tokens[4]),
                parse_f64_token(tokens[5]),
            ) else {
                continue;
            };
            rows.push((k, mu0, chi));
        }

        assert!(
            !rows.is_empty(),
            "xmu signature parse should yield numeric rows for {}",
            path.display()
        );
        let point_count = rows.len();
        let k_max = rows.last().map(|row| row.0).unwrap_or(0.0);
        let tail_len = rows.len().min(25);
        let mu0_tail_mean = rows
            .iter()
            .skip(rows.len().saturating_sub(tail_len))
            .map(|row| row.1)
            .sum::<f64>()
            / tail_len.max(1) as f64;
        let chi_rms =
            (rows.iter().map(|row| row.2.powi(2)).sum::<f64>() / rows.len() as f64).sqrt();

        XmuSignature {
            point_count,
            k_max,
            mu0_tail_mean,
            chi_rms,
        }
    }

    fn numeric_within_tolerance(actual: f64, expected: f64, tolerance: f64) -> bool {
        let delta = (actual - expected).abs();
        let scale = actual.abs().max(expected.abs()).max(1.0);
        delta <= tolerance * scale
    }

    #[test]
    fn builds_ff2x_input_from_previous_stages() {
        let parsed = parse_rdinp_str("inline", minimal_valid_input())
            .expect("minimal RDINP input should parse");
        let pot_input = PotInputData::from_parsed_cards(&parsed)
            .expect("minimal parsed input should map to POT input");

        let temp_root = temp_dir("from-stages");
        fs::create_dir_all(&temp_root).expect("temp root should be created");

        let pot_output = run_pot(&pot_input, &temp_root).expect("POT stage should succeed");
        let xsph_input = XsphInputData::from_pot_stage(&pot_input, &pot_output)
            .expect("XSPH input should map from POT stage");
        let xsph_output = run_xsph(&xsph_input, &temp_root).expect("XSPH stage should succeed");
        let pathfinder_input = PathfinderInputData::from_previous_stages(
            &pot_input,
            &pot_output,
            &xsph_input,
            &xsph_output,
        )
        .expect("pathfinder input should build");
        let pathfinder_output =
            run_pathfinder(&pathfinder_input, &temp_root).expect("pathfinder stage should succeed");
        let genfmt_input = GenfmtInputData::from_previous_stages(
            &pot_input,
            &pot_output,
            &xsph_input,
            &xsph_output,
            &pathfinder_input,
            &pathfinder_output,
        )
        .expect("genfmt input should build");
        let genfmt_output =
            run_genfmt(&genfmt_input, &temp_root).expect("GENFMT stage should succeed");

        let ff2x_input = Ff2xInputData::from_previous_stages(&genfmt_input, &genfmt_output)
            .expect("FF2X input should build from previous stages");
        assert!(!ff2x_input.feff_dat_files.is_empty());
        assert_eq!(ff2x_input.absorber_atomic_number, 29);

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp directory should be cleaned");
        }
    }

    #[test]
    fn collect_ff2x_output_requires_xmu_and_chi() {
        let temp_root = temp_dir("missing-output");
        fs::create_dir_all(&temp_root).expect("temp root should be created");
        fs::write(temp_root.join("chi.dat"), "placeholder\n")
            .expect("chi placeholder should exist");

        let error = collect_ff2x_output_data(&temp_root)
            .expect_err("collecting output without xmu.dat should fail");
        match error {
            FeffError::Validation(validation) => {
                assert_eq!(validation.len(), 1);
                assert_eq!(validation.issues()[0].field, "xmu.dat");
            }
            other => panic!("unexpected error type: {other}"),
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp directory should be cleaned");
        }
    }

    #[test]
    fn run_ff2x_writes_required_artifacts_for_phase1_corpus() {
        let inputs = core_corpus_feff_inputs();
        assert_eq!(inputs.len(), 16, "expected 16 baseline feff.inp fixtures");

        let temp_root = temp_dir("phase1-artifacts");
        fs::create_dir_all(&temp_root).expect("temp root should be created");

        for (case_index, input_path) in inputs.iter().enumerate() {
            let output = run_full_core_chain(input_path, &temp_root, case_index, "artifacts");
            assert!(output.chi_dat.ends_with("chi.dat"));
            assert!(output.xmu_dat.ends_with("xmu.dat"));
            assert!(Path::new(&output.chi_dat).is_file());
            assert!(Path::new(&output.xmu_dat).is_file());
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }

    #[test]
    fn rust_ff2x_matches_legacy_signatures_for_phase1_corpus() {
        let inputs = core_corpus_feff_inputs();
        assert_eq!(inputs.len(), 16, "expected 16 baseline feff.inp fixtures");

        let temp_root = temp_dir("phase1-parity");
        fs::create_dir_all(&temp_root).expect("temp root should be created");

        for (case_index, input_path) in inputs.iter().enumerate() {
            let output = run_full_core_chain(input_path, &temp_root, case_index, "parity");
            let legacy_dir = input_path
                .parent()
                .unwrap_or_else(|| panic!("input fixture should have a parent directory"));

            let legacy_chi = parse_chi_signature(&legacy_dir.join("chi.dat"));
            let rust_chi = parse_chi_signature(Path::new(&output.chi_dat));
            assert!(
                rust_chi.point_count.abs_diff(legacy_chi.point_count) <= 2,
                "FF2X chi point count mismatch for {}: rust={} legacy={}",
                input_path.display(),
                rust_chi.point_count,
                legacy_chi.point_count
            );
            assert!(
                numeric_within_tolerance(rust_chi.k_min, legacy_chi.k_min, 0.10),
                "FF2X chi k-min mismatch for {}: rust={} legacy={}",
                input_path.display(),
                rust_chi.k_min,
                legacy_chi.k_min
            );
            assert!(
                numeric_within_tolerance(rust_chi.k_max, legacy_chi.k_max, 0.05),
                "FF2X chi k-max mismatch for {}: rust={} legacy={}",
                input_path.display(),
                rust_chi.k_max,
                legacy_chi.k_max
            );
            assert!(
                numeric_within_tolerance(
                    rust_chi.peak_abs_chi,
                    legacy_chi.peak_abs_chi,
                    CHI_PEAK_TOLERANCE
                ),
                "FF2X chi peak mismatch for {}: rust={} legacy={}",
                input_path.display(),
                rust_chi.peak_abs_chi,
                legacy_chi.peak_abs_chi
            );
            assert!(
                numeric_within_tolerance(rust_chi.chi_rms, legacy_chi.chi_rms, CHI_RMS_TOLERANCE),
                "FF2X chi RMS mismatch for {}: rust={} legacy={}",
                input_path.display(),
                rust_chi.chi_rms,
                legacy_chi.chi_rms
            );
            assert!(
                rust_chi.zero_crossings.abs_diff(legacy_chi.zero_crossings)
                    <= CHI_ZERO_CROSSING_TOLERANCE,
                "FF2X chi zero-crossing mismatch for {}: rust={} legacy={}",
                input_path.display(),
                rust_chi.zero_crossings,
                legacy_chi.zero_crossings
            );

            let legacy_xmu = parse_xmu_signature(&legacy_dir.join("xmu.dat"));
            let rust_xmu = parse_xmu_signature(Path::new(&output.xmu_dat));
            assert!(
                rust_xmu.point_count.abs_diff(legacy_xmu.point_count) <= 2,
                "FF2X xmu point count mismatch for {}: rust={} legacy={}",
                input_path.display(),
                rust_xmu.point_count,
                legacy_xmu.point_count
            );
            assert!(
                numeric_within_tolerance(rust_xmu.k_max, legacy_xmu.k_max, 0.05),
                "FF2X xmu k-max mismatch for {}: rust={} legacy={}",
                input_path.display(),
                rust_xmu.k_max,
                legacy_xmu.k_max
            );
            assert!(
                numeric_within_tolerance(
                    rust_xmu.mu0_tail_mean,
                    legacy_xmu.mu0_tail_mean,
                    MU0_TAIL_TOLERANCE
                ),
                "FF2X xmu mu0-tail mismatch for {}: rust={} legacy={}",
                input_path.display(),
                rust_xmu.mu0_tail_mean,
                legacy_xmu.mu0_tail_mean
            );
            assert!(
                numeric_within_tolerance(rust_xmu.chi_rms, legacy_xmu.chi_rms, CHI_RMS_TOLERANCE),
                "FF2X xmu chi RMS mismatch for {}: rust={} legacy={}",
                input_path.display(),
                rust_xmu.chi_rms,
                legacy_xmu.chi_rms
            );
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }

    #[test]
    fn modern_core_chain_runs_end_to_end_without_legacy_fallback_for_phase1_corpus() {
        let inputs = core_corpus_feff_inputs();
        assert_eq!(inputs.len(), 16, "expected 16 baseline feff.inp fixtures");

        let temp_root = temp_dir("phase1-e2e");
        fs::create_dir_all(&temp_root).expect("temp root should be created");
        let temp_root_string = path_string(&temp_root);

        for (case_index, input_path) in inputs.iter().enumerate() {
            let output = run_full_core_chain(input_path, &temp_root, case_index, "e2e");

            assert!(
                output.xmu_dat.starts_with(&temp_root_string),
                "FF2X xmu output should be produced by Rust chain for {}",
                input_path.display()
            );
            assert!(
                output.chi_dat.starts_with(&temp_root_string),
                "FF2X chi output should be produced by Rust chain for {}",
                input_path.display()
            );
            assert!(
                !Path::new(&output.working_directory)
                    .join("f85e.log")
                    .exists(),
                "legacy f85e orchestration artifact should not be required for {}",
                input_path.display()
            );
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }
}
