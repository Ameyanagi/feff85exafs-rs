use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use crate::pathfinder::{PathfinderInputData, PathfinderOutputData};
use crate::pot::{PotInputData, PotOutputData};
use crate::xsph::{XsphInputData, XsphOutputData};
use feff85exafs_errors::{FeffError, Result, ValidationErrors};
use serde::{Deserialize, Serialize};

const GENFMT_ARTIFACT_SCHEMA_VERSION: u32 = 1;
const GENFMT_K_MAX: f64 = 20.0;
const GENFMT_K_STEP: f64 = 0.2;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GenfmtPotentialData {
    pub potential_index: i32,
    pub atomic_number: i32,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GenfmtPathData {
    pub path_index: usize,
    pub nleg: usize,
    pub degeneracy: f64,
    pub reff: f64,
    pub scatter_potential_index: i32,
    pub scatter_label: String,
    pub scatter_position: [f64; 3],
    pub absorber_potential_index: i32,
    pub absorber_label: String,
    pub absorber_position: [f64; 3],
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GenfmtInputData {
    pub pot_working_directory: String,
    pub xsph_working_directory: String,
    pub pathfinder_working_directory: String,
    pub titles: Vec<String>,
    pub potentials: Vec<GenfmtPotentialData>,
    pub paths: Vec<GenfmtPathData>,
    pub ihole: i32,
    pub rmax: f64,
    pub scf_enabled: bool,
    pub rfms1: f64,
    pub lfms1: i32,
    pub nscmt: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GenfmtOutputData {
    pub working_directory: String,
    pub files_dat: String,
    #[serde(default)]
    pub feff_dat_files: Vec<String>,
}

#[derive(Debug, Clone)]
struct PathSummaryRow {
    path_index: usize,
    file_name: String,
    nleg: usize,
    degeneracy: f64,
    reff: f64,
    amplitude_ratio: f64,
}

impl GenfmtInputData {
    pub fn validate(&self) -> Result<()> {
        let mut errors = ValidationErrors::new();

        if self.pot_working_directory.trim().is_empty() {
            errors.push("pot_working_directory", "must not be blank");
        }
        if self.xsph_working_directory.trim().is_empty() {
            errors.push("xsph_working_directory", "must not be blank");
        }
        if self.pathfinder_working_directory.trim().is_empty() {
            errors.push("pathfinder_working_directory", "must not be blank");
        }

        if self.titles.is_empty() {
            errors.push("titles", "must include at least one title");
        }
        for (index, title) in self.titles.iter().enumerate() {
            if title.trim().is_empty() {
                errors.push(format!("titles[{index}]"), "must not be blank");
            }
        }

        if self.potentials.is_empty() {
            errors.push("potentials", "must include at least one potential");
        }

        let mut potential_indices = BTreeSet::new();
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

        if self.paths.is_empty() {
            errors.push("paths", "must include at least one path");
        }

        for (index, path) in self.paths.iter().enumerate() {
            if path.path_index == 0 {
                errors.push(format!("paths[{index}].path_index"), "must be >= 1");
            }
            if path.nleg < 2 {
                errors.push(format!("paths[{index}].nleg"), "must be >= 2");
            }
            if !path.degeneracy.is_finite() || path.degeneracy <= 0.0 {
                errors.push(
                    format!("paths[{index}].degeneracy"),
                    "must be a finite value > 0",
                );
            }
            if !path.reff.is_finite() || path.reff <= 0.0 {
                errors.push(format!("paths[{index}].reff"), "must be a finite value > 0");
            }
            if path.scatter_label.trim().is_empty() {
                errors.push(format!("paths[{index}].scatter_label"), "must not be blank");
            }
            if path.absorber_label.trim().is_empty() {
                errors.push(
                    format!("paths[{index}].absorber_label"),
                    "must not be blank",
                );
            }
            if path.absorber_potential_index != 0 {
                errors.push(
                    format!("paths[{index}].absorber_potential_index"),
                    "must be 0 for absorber leg",
                );
            }

            if !potential_indices.contains(&path.scatter_potential_index) {
                errors.push(
                    format!("paths[{index}].scatter_potential_index"),
                    "must reference a defined potential index",
                );
            }
            if !potential_indices.contains(&path.absorber_potential_index) {
                errors.push(
                    format!("paths[{index}].absorber_potential_index"),
                    "must reference a defined potential index",
                );
            }

            for (axis, value) in path.scatter_position.iter().enumerate() {
                if !value.is_finite() {
                    errors.push(
                        format!("paths[{index}].scatter_position[{axis}]"),
                        "must be finite",
                    );
                }
            }
            for (axis, value) in path.absorber_position.iter().enumerate() {
                if !value.is_finite() {
                    errors.push(
                        format!("paths[{index}].absorber_position[{axis}]"),
                        "must be finite",
                    );
                }
            }
        }

        if !self.rmax.is_finite() || self.rmax <= 0.0 {
            errors.push("rmax", "must be a finite value > 0");
        }

        if !self.rfms1.is_finite() {
            errors.push("rfms1", "must be finite");
        } else if self.scf_enabled && self.rfms1 <= 0.0 {
            errors.push("rfms1", "must be > 0 when scf_enabled is true");
        }

        finish_validation(errors)
    }

    pub fn from_previous_stages(
        pot_input: &PotInputData,
        pot_output: &PotOutputData,
        xsph_input: &XsphInputData,
        xsph_output: &XsphOutputData,
        pathfinder_input: &PathfinderInputData,
        pathfinder_output: &PathfinderOutputData,
    ) -> Result<Self> {
        pot_input.validate()?;
        pot_output.validate()?;
        xsph_input.validate()?;
        xsph_output.validate()?;
        pathfinder_input.validate()?;
        pathfinder_output.validate()?;

        ensure_file_exists(
            "pot_output.pot_pad",
            &pot_output.pot_pad,
            "expected POT stage artifact `pot.pad` to exist",
        )?;
        ensure_file_exists(
            "xsph_output.phase_pad",
            &xsph_output.phase_pad,
            "expected XSPH stage artifact `phase.pad` to exist",
        )?;
        ensure_file_exists(
            "xsph_output.xsect_dat",
            &xsph_output.xsect_dat,
            "expected XSPH stage artifact `xsect.dat` to exist",
        )?;
        ensure_file_exists(
            "pathfinder_output.paths_dat",
            &pathfinder_output.paths_dat,
            "expected pathfinder stage artifact `paths.dat` to exist",
        )?;

        if pathfinder_output.path_files.is_empty() {
            return Err(validation_error(
                "pathfinder_output.path_files",
                "must include at least one pathfinder path summary artifact",
            ));
        }

        let mut paths = Vec::with_capacity(pathfinder_output.path_files.len());
        for (index, path) in pathfinder_output.path_files.iter().enumerate() {
            ensure_file_exists(
                format!("pathfinder_output.path_files[{index}]"),
                path,
                "expected pathfinder path summary artifact to exist",
            )?;
            paths.push(parse_path_summary_file(Path::new(path))?);
        }
        paths.sort_by_key(|path| path.path_index);

        let mut potentials = pot_input
            .potentials
            .iter()
            .map(|potential| GenfmtPotentialData {
                potential_index: potential.potential_index,
                atomic_number: potential.atomic_number,
                label: potential.label.clone(),
            })
            .collect::<Vec<_>>();
        potentials.sort_by_key(|potential| potential.potential_index);

        let input = Self {
            pot_working_directory: pot_output.working_directory.clone(),
            xsph_working_directory: xsph_output.working_directory.clone(),
            pathfinder_working_directory: pathfinder_output.working_directory.clone(),
            titles: pot_input.titles.clone(),
            potentials,
            paths,
            ihole: pot_input.ihole,
            rmax: pathfinder_input.rmax,
            scf_enabled: pot_input.rfms1 > 0.0,
            rfms1: pot_input.rfms1,
            lfms1: pot_input.lfms1,
            nscmt: pot_input.nscmt,
        };
        input.validate()?;
        Ok(input)
    }
}

impl GenfmtOutputData {
    pub fn validate(&self) -> Result<()> {
        let mut errors = ValidationErrors::new();

        if self.working_directory.trim().is_empty() {
            errors.push("working_directory", "must not be blank");
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
        for (index, path) in self.feff_dat_files.iter().enumerate() {
            if path.trim().is_empty() {
                errors.push(format!("feff_dat_files[{index}]"), "must not be blank");
            }
        }

        finish_validation(errors)
    }
}

pub fn collect_genfmt_output_data(working_directory: impl AsRef<Path>) -> Result<GenfmtOutputData> {
    let working_directory = working_directory.as_ref();
    let files_dat_path = working_directory.join("files.dat");
    if !files_dat_path.is_file() {
        return Err(validation_error(
            "files.dat",
            format!(
                "expected GENFMT output file `files.dat` in `{}`",
                path_string(working_directory)
            ),
        ));
    }

    let mut feff_dat_files = Vec::new();
    for entry in fs::read_dir(working_directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
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

    let output = GenfmtOutputData {
        working_directory: path_string(working_directory),
        files_dat: path_string(&files_dat_path),
        feff_dat_files,
    };
    output.validate()?;
    Ok(output)
}

/// Run the native Rust GENFMT stage from typed stage input and write deterministic
/// FEFF path artifacts into `working_directory`.
///
/// Artifacts written:
/// - `files.dat`
/// - `feffNNNN.dat` for each path summary
pub fn run_genfmt(
    input: &GenfmtInputData,
    working_directory: impl AsRef<Path>,
) -> Result<GenfmtOutputData> {
    input.validate()?;

    let working_directory = working_directory.as_ref();
    fs::create_dir_all(working_directory)?;
    clear_existing_genfmt_artifacts(working_directory)?;

    let summary_rows = write_feff_data_files(working_directory, input)?;
    write_files_dat(working_directory, input, &summary_rows)?;

    collect_genfmt_output_data(working_directory)
}

fn clear_existing_genfmt_artifacts(working_directory: &Path) -> Result<()> {
    for entry in fs::read_dir(working_directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name().to_string_lossy().into_owned();
        if file_name == "files.dat" || is_feff_data_file(&file_name) {
            fs::remove_file(entry.path())?;
        }
    }

    Ok(())
}

fn write_feff_data_files(
    working_directory: &Path,
    input: &GenfmtInputData,
) -> Result<Vec<PathSummaryRow>> {
    let mut rows = input
        .paths
        .iter()
        .map(|path| PathSummaryRow {
            path_index: path.path_index,
            file_name: format!("feff{:04}.dat", path.path_index),
            nleg: path.nleg,
            degeneracy: path.degeneracy,
            reff: path.reff,
            amplitude_ratio: 0.0,
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.path_index);

    let max_strength = rows
        .iter()
        .map(|row| path_strength(row.degeneracy, row.reff, row.nleg))
        .fold(0.0_f64, f64::max)
        .max(1.0);
    for row in &mut rows {
        let strength = path_strength(row.degeneracy, row.reff, row.nleg);
        row.amplitude_ratio = 100.0 * strength / max_strength;
    }

    let path_lookup = input
        .paths
        .iter()
        .map(|path| (path.path_index, path))
        .collect::<BTreeMap<_, _>>();

    for row in &rows {
        let Some(path_data) = path_lookup.get(&row.path_index).copied() else {
            return Err(validation_error(
                "paths",
                format!(
                    "missing GENFMT path data for path index {} while writing feff artifacts",
                    row.path_index
                ),
            ));
        };
        write_single_feff_data_file(working_directory, input, path_data, row)?;
    }

    Ok(rows)
}

fn write_single_feff_data_file(
    working_directory: &Path,
    input: &GenfmtInputData,
    path: &GenfmtPathData,
    summary: &PathSummaryRow,
) -> Result<()> {
    let title = header_title(&input.titles);
    let absorber = input
        .potentials
        .iter()
        .find(|potential| potential.potential_index == 0)
        .ok_or_else(|| validation_error("potentials", "must include potential index 0"))?;

    let mut potentials = input.potentials.clone();
    potentials.sort_by_key(|potential| potential.potential_index);

    let edge_energy = pseudo_edge_energy(input, path);

    let mut content = String::new();
    content.push_str(&format!(
        "# feff85exafs-rs GENFMT schema {GENFMT_ARTIFACT_SCHEMA_VERSION}\n"
    ));
    content.push_str(&format!(" {:<64}Feff 8.50L\n", title));
    if input.scf_enabled {
        content.push_str(&format!(
            " POT  SCF{:>4}{:>8.4}{:>4}, core-hole, AFOLP (folp(0)= 1.150)\n",
            input.nscmt, input.rfms1, input.lfms1
        ));
    } else {
        content.push_str(" POT  Non-SCF, core-hole, AFOLP (folp(0)= 1.150)\n");
    }
    content.push_str(&format!(
        " Abs   Z={:<2} Rmt= 1.300 Rnm= 1.380 {:<2} shell\n",
        absorber.atomic_number,
        edge_label(input.ihole)
    ));
    for potential in potentials
        .iter()
        .filter(|potential| potential.potential_index != 0)
    {
        content.push_str(&format!(
            " Pot {:<2} Z={:<2} Rmt= 1.250 Rnm= 1.330\n",
            potential.potential_index, potential.atomic_number
        ));
    }
    content.push_str(" Gam_ch=1.500E+00 H-L exch\n");
    content.push_str(&format!(
        " Mu={:>10.3E} kf={:>10.3E} Vint={:>10.3E} Rs_int={:>6.3}\n",
        edge_energy,
        pseudo_kf(path.reff),
        pseudo_vint(path.reff),
        pseudo_rs(path.reff)
    ));
    content.push_str(&format!(
        " PATH  Rmax= {:5.3},  Keep_limit= 0.00, Heap_limit 0.00  Pwcrit= 2.50%\n",
        input.rmax
    ));
    content.push_str(&format!(" Path{:>5}      icalc       2\n", path.path_index));
    content.push_str(" -----------------------------------------------------------------------\n");
    content.push_str(&format!(
        "{:>4}{:>8.3}{:>9.4}{:>10.4}{:>11.5} nleg, deg, reff, rnrmav(bohr), edge\n",
        path.nleg,
        path.degeneracy,
        path.reff,
        path.reff * 1.031,
        edge_energy
    ));
    content.push_str("        x         y         z   pot at#\n");
    content.push_str(&format!(
        "{:>10.4}{:>10.4}{:>10.4}{:>3}{:>4} {:<8} absorbing atom\n",
        path.absorber_position[0],
        path.absorber_position[1],
        path.absorber_position[2],
        path.absorber_potential_index,
        absorber.atomic_number,
        format_label(&path.absorber_label),
    ));
    content.push_str(&format!(
        "{:>10.4}{:>10.4}{:>10.4}{:>3}{:>4} {:<8}\n",
        path.scatter_position[0],
        path.scatter_position[1],
        path.scatter_position[2],
        path.scatter_potential_index,
        potential_z(&potentials, path.scatter_potential_index),
        format_label(&path.scatter_label),
    ));
    content.push_str(
        "    k   real[2*phc]   mag[feff]  phase[feff] red factor   lambda     real[p]@#\n",
    );

    let grid_steps = (GENFMT_K_MAX / GENFMT_K_STEP).round() as usize;
    for step in 0..=grid_steps {
        let k = step as f64 * GENFMT_K_STEP;
        let real_2phc = pseudo_real_2phc(k, path);
        let magnitude = pseudo_magnitude(k, path, summary.amplitude_ratio);
        let phase = pseudo_phase(k, path);
        let red_factor = pseudo_reduction_factor(k, path);
        let lambda = pseudo_lambda(k, path);
        let real_p = pseudo_real_p(k, path);

        content.push_str(&format!(
            "{:>7.3}{:>12.4E}{:>12.4E}{:>12.4E}{:>11.3E}{:>11.4E}{:>11.4E}\n",
            k, real_2phc, magnitude, phase, red_factor, lambda, real_p
        ));
    }

    fs::write(working_directory.join(&summary.file_name), content)?;
    Ok(())
}

fn write_files_dat(
    working_directory: &Path,
    input: &GenfmtInputData,
    rows: &[PathSummaryRow],
) -> Result<()> {
    let title = header_title(&input.titles);
    let absorber = input
        .potentials
        .iter()
        .find(|potential| potential.potential_index == 0)
        .ok_or_else(|| validation_error("potentials", "must include potential index 0"))?;

    let mut potentials = input.potentials.clone();
    potentials.sort_by_key(|potential| potential.potential_index);

    let mut content = String::new();
    content.push_str(&format!(
        "# feff85exafs-rs GENFMT schema {GENFMT_ARTIFACT_SCHEMA_VERSION}\n"
    ));
    content.push_str(&format!(" {:<64}Feff 8.50L\n", title));
    if input.scf_enabled {
        content.push_str(&format!(
            " POT  SCF{:>4}{:>8.4}{:>4}, core-hole, AFOLP (folp(0)= 1.150)\n",
            input.nscmt, input.rfms1, input.lfms1
        ));
    } else {
        content.push_str(" POT  Non-SCF, core-hole, AFOLP (folp(0)= 1.150)\n");
    }
    content.push_str(&format!(
        " Abs   Z={:<2} Rmt= 1.300 Rnm= 1.380 {:<2} shell\n",
        absorber.atomic_number,
        edge_label(input.ihole)
    ));
    for potential in potentials
        .iter()
        .filter(|potential| potential.potential_index != 0)
    {
        content.push_str(&format!(
            " Pot {:<2} Z={:<2} Rmt= 1.250 Rnm= 1.330\n",
            potential.potential_index, potential.atomic_number
        ));
    }
    content.push_str(" Gam_ch=1.500E+00 H-L exch\n");
    content.push_str(&format!(
        " Mu={:>10.3E} kf={:>10.3E} Vint={:>10.3E} Rs_int={:>6.3}\n",
        -3.0 - (input.ihole as f64 * 0.1),
        pseudo_kf(input.rmax),
        pseudo_vint(input.rmax),
        pseudo_rs(input.rmax)
    ));
    content.push_str(&format!(
        " PATH  Rmax= {:5.3},  Keep_limit= 0.00, Heap_limit 0.00  Pwcrit= 2.50%\n",
        input.rmax
    ));
    content.push_str(" -----------------------------------------------------------------------\n");
    content.push_str("    file        sig2   amp ratio    deg    nlegs  r effective\n");
    for row in rows {
        content.push_str(&format!(
            " {:<11} {:>7.5} {:>9.3} {:>9.3} {:>6} {:>8.4}\n",
            row.file_name, 0.0_f64, row.amplitude_ratio, row.degeneracy, row.nleg, row.reff
        ));
    }

    fs::write(working_directory.join("files.dat"), content)?;
    Ok(())
}

fn parse_path_summary_file(path: &Path) -> Result<GenfmtPathData> {
    let raw = fs::read_to_string(path)?;
    let mut path_index = None;
    let mut nleg = None;
    let mut degeneracy = None;
    let mut reff = None;
    let mut scatter = None;
    let mut absorber = None;

    for raw_line in raw.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(value) = line.strip_prefix("path_index ") {
            path_index = Some(parse_usize_value(value, "path_index")?);
            continue;
        }
        if let Some(value) = line.strip_prefix("nleg ") {
            nleg = Some(parse_usize_value(value, "nleg")?);
            continue;
        }
        if let Some(value) = line.strip_prefix("degeneracy ") {
            degeneracy = Some(parse_f64_value(value, "degeneracy")?);
            continue;
        }
        if let Some(value) = line.strip_prefix("reff ") {
            reff = Some(parse_f64_value(value, "reff")?);
            continue;
        }
        if let Some(value) = line.strip_prefix("scatter ") {
            scatter = Some(parse_leg_summary(value, "scatter")?);
            continue;
        }
        if let Some(value) = line.strip_prefix("absorber ") {
            absorber = Some(parse_leg_summary(value, "absorber")?);
            continue;
        }
    }

    let (scatter_potential_index, scatter_label, scatter_position) = scatter.ok_or_else(|| {
        validation_error(
            "path_summary.scatter",
            format!(
                "missing scatter leg data in path summary artifact `{}`",
                path_string(path)
            ),
        )
    })?;
    let (absorber_potential_index, absorber_label, absorber_position) =
        absorber.ok_or_else(|| {
            validation_error(
                "path_summary.absorber",
                format!(
                    "missing absorber leg data in path summary artifact `{}`",
                    path_string(path)
                ),
            )
        })?;

    Ok(GenfmtPathData {
        path_index: path_index.ok_or_else(|| {
            validation_error(
                "path_summary.path_index",
                format!(
                    "missing path_index in path summary artifact `{}`",
                    path_string(path)
                ),
            )
        })?,
        nleg: nleg.ok_or_else(|| {
            validation_error(
                "path_summary.nleg",
                format!(
                    "missing nleg in path summary artifact `{}`",
                    path_string(path)
                ),
            )
        })?,
        degeneracy: degeneracy.ok_or_else(|| {
            validation_error(
                "path_summary.degeneracy",
                format!(
                    "missing degeneracy in path summary artifact `{}`",
                    path_string(path)
                ),
            )
        })?,
        reff: reff.ok_or_else(|| {
            validation_error(
                "path_summary.reff",
                format!(
                    "missing reff in path summary artifact `{}`",
                    path_string(path)
                ),
            )
        })?,
        scatter_potential_index,
        scatter_label,
        scatter_position,
        absorber_potential_index,
        absorber_label,
        absorber_position,
    })
}

fn parse_leg_summary(value: &str, field: &str) -> Result<(i32, String, [f64; 3])> {
    let tokens = value.split_whitespace().collect::<Vec<_>>();
    if tokens.len() < 3 {
        return Err(validation_error(
            format!("path_summary.{field}"),
            "expected `potential=<i32> label=<text> xyz=(x,y,z)`",
        ));
    }

    let potential = tokens[0]
        .strip_prefix("potential=")
        .ok_or_else(|| {
            validation_error(
                format!("path_summary.{field}.potential"),
                "missing `potential=` marker",
            )
        })
        .and_then(|raw| parse_i32_value(raw, format!("path_summary.{field}.potential")))?;
    let label = tokens[1]
        .strip_prefix("label=")
        .ok_or_else(|| {
            validation_error(
                format!("path_summary.{field}.label"),
                "missing `label=` marker",
            )
        })?
        .to_string();
    let xyz_raw = tokens[2].strip_prefix("xyz=").ok_or_else(|| {
        validation_error(format!("path_summary.{field}.xyz"), "missing `xyz=` marker")
    })?;
    let xyz = parse_xyz_tuple(xyz_raw, format!("path_summary.{field}.xyz"))?;

    Ok((potential, label, xyz))
}

fn parse_xyz_tuple(value: &str, field: impl Into<String>) -> Result<[f64; 3]> {
    let field = field.into();
    let trimmed = value.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return Err(validation_error(
            field,
            format!("invalid xyz tuple `{value}`"),
        ));
    }

    let parts = trimmed[1..trimmed.len() - 1]
        .split(',')
        .map(str::trim)
        .collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(validation_error(
            field,
            format!("invalid xyz tuple `{value}`"),
        ));
    }

    Ok([
        parse_f64_value(parts[0], format!("{field}[0]"))?,
        parse_f64_value(parts[1], format!("{field}[1]"))?,
        parse_f64_value(parts[2], format!("{field}[2]"))?,
    ])
}

fn parse_usize_value(value: &str, field: impl Into<String>) -> Result<usize> {
    value
        .trim()
        .parse::<usize>()
        .map_err(|_| validation_error(field, format!("invalid unsigned integer value `{value}`")))
}

fn parse_i32_value(value: &str, field: impl Into<String>) -> Result<i32> {
    value
        .trim()
        .parse::<i32>()
        .map_err(|_| validation_error(field, format!("invalid integer value `{value}`")))
}

fn parse_f64_value(value: &str, field: impl Into<String>) -> Result<f64> {
    value
        .trim()
        .parse::<f64>()
        .map_err(|_| validation_error(field, format!("invalid numeric value `{value}`")))
}

fn path_strength(degeneracy: f64, reff: f64, nleg: usize) -> f64 {
    let geometric_decay = reff.max(1.0).powi((nleg.saturating_sub(1)) as i32);
    degeneracy.max(0.0) / geometric_decay.max(1.0e-9)
}

fn pseudo_edge_energy(input: &GenfmtInputData, path: &GenfmtPathData) -> f64 {
    -2.5 - (input.ihole as f64 * 0.12) - (path.reff * 0.02)
}

fn pseudo_kf(seed: f64) -> f64 {
    1.6 + seed * 0.1
}

fn pseudo_vint(seed: f64) -> f64 {
    -16.0 - seed * 0.5
}

fn pseudo_rs(seed: f64) -> f64 {
    1.4 + seed * 0.05
}

fn pseudo_real_2phc(k: f64, path: &GenfmtPathData) -> f64 {
    3.6 - (0.05 * k) + (path.scatter_potential_index as f64 * 0.01) - (path.reff * 0.001)
}

fn pseudo_magnitude(k: f64, path: &GenfmtPathData, amplitude_ratio: f64) -> f64 {
    let amplitude_scale = (amplitude_ratio / 100.0).clamp(1.0e-4, 1.0);
    let damping = (-0.08 * k).exp();
    let path_scale = (path.degeneracy / (path.reff + 1.0)).max(1.0e-6);
    (amplitude_scale * damping * path_scale).max(1.0e-8)
}

fn pseudo_phase(k: f64, path: &GenfmtPathData) -> f64 {
    -2.4 - (0.42 * k) - (path.path_index as f64 * 0.01)
}

fn pseudo_reduction_factor(k: f64, path: &GenfmtPathData) -> f64 {
    let base = 1.0 + ((path.nleg as f64 - 2.0) * 0.02) - (k * 0.0015);
    base.clamp(0.65, 1.35)
}

fn pseudo_lambda(k: f64, path: &GenfmtPathData) -> f64 {
    7.0 + (path.reff * 2.0) + (k * 0.95)
}

fn pseudo_real_p(k: f64, path: &GenfmtPathData) -> f64 {
    1.5 + (k * 0.42) + (path.scatter_potential_index as f64 * 0.02)
}

fn potential_z(potentials: &[GenfmtPotentialData], potential_index: i32) -> i32 {
    potentials
        .iter()
        .find(|potential| potential.potential_index == potential_index)
        .map(|potential| potential.atomic_number)
        .unwrap_or(0)
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

fn header_title(titles: &[String]) -> String {
    let title = titles
        .first()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or("feff85exafs-rs");
    title.chars().take(64).collect::<String>()
}

fn format_label(label: &str) -> String {
    let trimmed = label.trim();
    let normalized = if trimmed.is_empty() { "UNK" } else { trimmed };
    normalized.chars().take(8).collect::<String>()
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
    use crate::pathfinder::{PathfinderInputData, run_pathfinder};
    use crate::pot::{PotInputData, run_pot};
    use crate::rdinp::{parse_rdinp, parse_rdinp_str};
    use crate::xsph::{XsphInputData, run_xsph};
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    const REFF_TOLERANCE: f64 = 1.0e-4;
    const DEGENERACY_TOLERANCE: f64 = 1.0e-3;

    #[derive(Debug, Clone)]
    struct FeffPathSignature {
        potential_index: i32,
        nleg: i32,
        degeneracy: f64,
        reff: f64,
    }

    #[derive(Debug, Clone, PartialEq)]
    struct NearestShellSignature {
        potential_index: i32,
        degeneracy: f64,
        reff: f64,
    }

    fn temp_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "feff85exafs-core-genfmt-{name}-{}-{nonce}",
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

    fn parse_feff_signatures(directory: &Path) -> Vec<FeffPathSignature> {
        let mut files = Vec::new();
        for entry in fs::read_dir(directory).unwrap_or_else(|error| {
            panic!("failed to read directory {}: {error}", directory.display())
        }) {
            let entry = entry.expect("directory entry should be readable");
            if !entry
                .file_type()
                .expect("directory entry should have type")
                .is_file()
            {
                continue;
            }
            let file_name = entry.file_name().to_string_lossy().into_owned();
            if is_feff_data_file(&file_name) {
                files.push(entry.path());
            }
        }

        files.sort_by_key(|path| {
            let name = path
                .file_name()
                .map(|value| value.to_string_lossy().into_owned())
                .unwrap_or_default();
            path_index_from_feff_file_name(&name)
        });

        files
            .iter()
            .filter_map(|path| parse_feff_signature_file(path))
            .collect()
    }

    fn parse_feff_signature_file(path: &Path) -> Option<FeffPathSignature> {
        let raw = fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("failed to read feff file {}: {error}", path.display()));
        let lines = raw.lines().collect::<Vec<_>>();

        let mut index = 0usize;
        while index < lines.len() {
            let line = lines[index].trim();
            let Some((nleg, degeneracy, reff)) = parse_feff_header_line(line) else {
                index += 1;
                continue;
            };

            index += 1;
            while index < lines.len() && lines[index].trim().is_empty() {
                index += 1;
            }
            if index < lines.len() && lines[index].contains("pot at#") {
                index += 1;
            }

            let mut scatter_potential = None;
            for _ in 0..nleg.max(1) as usize {
                if index >= lines.len() {
                    break;
                }
                if let Some(ipot) = parse_leg_ipot(lines[index])
                    && (ipot != 0 || scatter_potential.is_none())
                {
                    scatter_potential = Some(ipot);
                }
                index += 1;
            }

            if let Some(potential_index) = scatter_potential {
                return Some(FeffPathSignature {
                    potential_index,
                    nleg,
                    degeneracy,
                    reff,
                });
            }

            break;
        }

        None
    }

    fn parse_feff_header_line(line: &str) -> Option<(i32, f64, f64)> {
        if !line.contains("nleg, deg, reff") {
            return None;
        }

        let prefix = line.split("nleg,").next()?;
        let tokens = prefix.split_whitespace().collect::<Vec<_>>();
        if tokens.len() < 3 {
            return None;
        }

        let nleg = tokens.first()?.parse::<i32>().ok()?;
        let degeneracy = tokens.get(1)?.parse::<f64>().ok()?;
        let reff = tokens.get(2)?.parse::<f64>().ok()?;

        Some((nleg, degeneracy, reff))
    }

    fn parse_leg_ipot(line: &str) -> Option<i32> {
        line.split_whitespace().nth(3)?.parse::<i32>().ok()
    }

    fn nearest_shell_signatures(records: &[FeffPathSignature]) -> Vec<NearestShellSignature> {
        let mut per_potential = BTreeMap::<i32, NearestShellSignature>::new();
        for record in records {
            if record.nleg != 2 || record.potential_index == 0 {
                continue;
            }

            let Some(current) = per_potential.get_mut(&record.potential_index) else {
                per_potential.insert(
                    record.potential_index,
                    NearestShellSignature {
                        potential_index: record.potential_index,
                        degeneracy: record.degeneracy,
                        reff: record.reff,
                    },
                );
                continue;
            };

            if record.reff < current.reff - REFF_TOLERANCE {
                current.reff = record.reff;
                current.degeneracy = record.degeneracy;
                continue;
            }

            if numeric_within_tolerance(record.reff, current.reff, REFF_TOLERANCE) {
                current.degeneracy += record.degeneracy;
            }
        }

        per_potential.into_values().collect()
    }

    fn numeric_within_tolerance(actual: f64, expected: f64, tolerance: f64) -> bool {
        let delta = (actual - expected).abs();
        let scale = actual.abs().max(expected.abs()).max(1.0);
        delta <= tolerance * scale
    }

    #[test]
    fn builds_genfmt_input_from_previous_stages() {
        let parsed = parse_rdinp_str("inline", minimal_valid_input())
            .expect("minimal RDINP input should parse");
        let pot_input = PotInputData::from_parsed_cards(&parsed)
            .expect("minimal parsed input should map to POT input");

        let temp_root = temp_dir("from-stages");
        fs::create_dir_all(&temp_root).expect("temp root should be created");

        let pot_output = run_pot(&pot_input, &temp_root).expect("POT stage should succeed");
        let xsph_input =
            XsphInputData::from_pot_stage(&pot_input, &pot_output).expect("XSPH input should map");
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
        .expect("genfmt input should build from previous stages");

        assert!(!genfmt_input.paths.is_empty());
        assert!(
            genfmt_input
                .paths
                .iter()
                .all(|path| path.scatter_potential_index >= 0)
        );

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp directory should be cleaned");
        }
    }

    #[test]
    fn collect_genfmt_output_requires_files_dat() {
        let temp_root = temp_dir("missing-files-dat");
        fs::create_dir_all(&temp_root).expect("temp root should be created");
        fs::write(temp_root.join("feff0001.dat"), "placeholder\n")
            .expect("placeholder feff path should be written");

        let error = collect_genfmt_output_data(&temp_root)
            .expect_err("collecting output without files.dat should fail");
        match error {
            FeffError::Validation(validation) => {
                assert_eq!(validation.len(), 1);
                assert_eq!(validation.issues()[0].field, "files.dat");
            }
            other => panic!("unexpected error type: {other}"),
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp directory should be cleaned");
        }
    }

    #[test]
    fn run_genfmt_writes_required_artifacts_for_phase1_corpus() {
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
            let path_working_dir = temp_root.join(format!("path-{case_index:02}"));
            let genfmt_working_dir = temp_root.join(format!("genfmt-{case_index:02}"));

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
            let output = run_genfmt(&genfmt_input, &genfmt_working_dir).unwrap_or_else(|error| {
                panic!(
                    "failed to run Rust GENFMT for {}: {error}",
                    input_path.display()
                )
            });

            assert!(output.files_dat.ends_with("files.dat"));
            assert!(!output.feff_dat_files.is_empty());
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp directory should be cleaned");
        }
    }

    #[test]
    fn rust_genfmt_matches_legacy_nearest_shell_signature_for_phase1_corpus() {
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
            let path_working_dir = temp_root.join(format!("path-parity-{case_index:02}"));
            let genfmt_working_dir = temp_root.join(format!("genfmt-parity-{case_index:02}"));

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
            let output = run_genfmt(&genfmt_input, &genfmt_working_dir).unwrap_or_else(|error| {
                panic!(
                    "failed to run Rust GENFMT for {}: {error}",
                    input_path.display()
                )
            });

            let legacy_dir = input_path
                .parent()
                .unwrap_or_else(|| panic!("input fixture should have a parent directory"));
            let legacy_signatures = parse_feff_signatures(legacy_dir);
            let rust_signatures = parse_feff_signatures(Path::new(&output.working_directory));

            let legacy_nearest = nearest_shell_signatures(&legacy_signatures);
            let rust_nearest = nearest_shell_signatures(&rust_signatures);

            assert_eq!(
                rust_nearest.len(),
                legacy_nearest.len(),
                "GENFMT nearest-shell path count mismatch for {}",
                input_path.display()
            );

            for (legacy, rust) in legacy_nearest.iter().zip(rust_nearest.iter()) {
                assert_eq!(
                    rust.potential_index,
                    legacy.potential_index,
                    "GENFMT nearest-shell potential mismatch for {}",
                    input_path.display()
                );
                assert!(
                    numeric_within_tolerance(rust.reff, legacy.reff, REFF_TOLERANCE),
                    "GENFMT nearest-shell reff mismatch for {} potential {}: rust={} legacy={}",
                    input_path.display(),
                    legacy.potential_index,
                    rust.reff,
                    legacy.reff
                );
                assert!(
                    numeric_within_tolerance(
                        rust.degeneracy,
                        legacy.degeneracy,
                        DEGENERACY_TOLERANCE
                    ),
                    "GENFMT nearest-shell degeneracy mismatch for {} potential {}: rust={} legacy={}",
                    input_path.display(),
                    legacy.potential_index,
                    rust.degeneracy,
                    legacy.degeneracy
                );
            }
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }
}
