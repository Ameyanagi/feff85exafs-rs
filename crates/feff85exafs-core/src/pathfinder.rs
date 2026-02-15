use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use crate::pot::{PotInputData, PotOutputData};
use crate::xsph::{XsphInputData, XsphOutputData};
use feff85exafs_errors::{FeffError, Result, ValidationErrors};
use serde::{Deserialize, Serialize};

const PATHFINDER_ARTIFACT_SCHEMA_VERSION: u32 = 1;
const REFF_BUCKET_SCALE: f64 = 1.0e4;
const REFF_BUCKET_EPSILON: f64 = 1.0e-9;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PathfinderAtomData {
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub potential_index: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PathfinderPotentialData {
    pub potential_index: i32,
    pub atomic_number: i32,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PathfinderInputData {
    pub pot_working_directory: String,
    pub xsph_working_directory: String,
    pub atoms: Vec<PathfinderAtomData>,
    pub potentials: Vec<PathfinderPotentialData>,
    pub rmax: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PathfinderOutputData {
    pub working_directory: String,
    pub paths_dat: String,
    #[serde(default)]
    pub path_files: Vec<String>,
}

#[derive(Debug, Clone)]
struct GeneratedPath {
    path_index: usize,
    nleg: usize,
    degeneracy: f64,
    reff: f64,
    scatter_leg: PathLeg,
    absorber_leg: PathLeg,
}

#[derive(Debug, Clone)]
struct PathLeg {
    x: f64,
    y: f64,
    z: f64,
    potential_index: i32,
    label: String,
    rleg: f64,
    beta: f64,
    eta: f64,
}

#[derive(Debug, Clone)]
struct PathGroup {
    potential_index: i32,
    label: String,
    reff: f64,
    scatter_position: [f64; 3],
    degeneracy: usize,
}

impl PathfinderInputData {
    pub fn validate(&self) -> Result<()> {
        let mut errors = ValidationErrors::new();

        if self.pot_working_directory.trim().is_empty() {
            errors.push("pot_working_directory", "must not be blank");
        }
        if self.xsph_working_directory.trim().is_empty() {
            errors.push("xsph_working_directory", "must not be blank");
        }
        if self.atoms.is_empty() {
            errors.push("atoms", "must include at least one atom");
        }
        if self.potentials.is_empty() {
            errors.push("potentials", "must include at least one potential");
        }
        if !self.rmax.is_finite() || self.rmax <= 0.0 {
            errors.push("rmax", "must be a finite value > 0");
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

        for (index, atom) in self.atoms.iter().enumerate() {
            if atom.potential_index < 0 {
                errors.push(format!("atoms[{index}].potential_index"), "must be >= 0");
                continue;
            }
            if !potential_indices.contains(&atom.potential_index) {
                errors.push(
                    format!("atoms[{index}].potential_index"),
                    "must reference a defined potential index",
                );
            }
        }

        if !self.atoms.iter().any(|atom| atom.potential_index == 0) {
            errors.push("atoms", "must include an absorber atom (potential index 0)");
        }
        if !self.atoms.iter().any(|atom| atom.potential_index != 0) {
            errors.push("atoms", "must include at least one scattering atom");
        }

        finish_validation(errors)
    }

    pub fn from_previous_stages(
        pot_input: &PotInputData,
        pot_output: &PotOutputData,
        xsph_input: &XsphInputData,
        xsph_output: &XsphOutputData,
    ) -> Result<Self> {
        pot_input.validate()?;
        pot_output.validate()?;
        xsph_input.validate()?;
        xsph_output.validate()?;

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

        if xsph_output.phase_shift_files.is_empty() {
            return Err(validation_error(
                "xsph_output.phase_shift_files",
                "must include at least one XSPH phase-shift artifact",
            ));
        }
        for (index, phase_file) in xsph_output.phase_shift_files.iter().enumerate() {
            ensure_file_exists(
                format!("xsph_output.phase_shift_files[{index}]"),
                phase_file,
                "expected XSPH phase-shift artifact to exist",
            )?;
        }

        let atoms = pot_input
            .atoms
            .iter()
            .map(|atom| PathfinderAtomData {
                x: atom.x,
                y: atom.y,
                z: atom.z,
                potential_index: atom.potential_index,
            })
            .collect::<Vec<_>>();

        let mut potentials = pot_input
            .potentials
            .iter()
            .map(|potential| PathfinderPotentialData {
                potential_index: potential.potential_index,
                atomic_number: potential.atomic_number,
                label: potential.label.clone(),
            })
            .collect::<Vec<_>>();
        potentials.sort_by_key(|potential| potential.potential_index);

        let absorber = atoms
            .iter()
            .find(|atom| atom.potential_index == 0)
            .ok_or_else(|| {
                validation_error("atoms", "must include absorber atom with potential index 0")
            })?;

        let _max_scatter_reff = atoms
            .iter()
            .filter(|atom| atom.potential_index != 0)
            .map(|atom| atom_distance(absorber, atom))
            .fold(None, |acc: Option<f64>, reff| {
                Some(acc.map_or(reff, |current| current.max(reff)))
            })
            .ok_or_else(|| {
                validation_error("atoms", "must include at least one scattering atom")
            })?;

        let input = Self {
            pot_working_directory: pot_output.working_directory.clone(),
            xsph_working_directory: xsph_output.working_directory.clone(),
            atoms,
            potentials,
            rmax: round_reff(pot_input.rpath),
        };
        input.validate()?;
        Ok(input)
    }
}

impl PathfinderOutputData {
    pub fn validate(&self) -> Result<()> {
        let mut errors = ValidationErrors::new();

        if self.working_directory.trim().is_empty() {
            errors.push("working_directory", "must not be blank");
        }
        if self.paths_dat.trim().is_empty() {
            errors.push("paths_dat", "must not be blank");
        }
        for (index, path) in self.path_files.iter().enumerate() {
            if path.trim().is_empty() {
                errors.push(format!("path_files[{index}]"), "must not be blank");
            }
        }

        finish_validation(errors)
    }
}

pub fn collect_pathfinder_output_data(
    working_directory: impl AsRef<Path>,
) -> Result<PathfinderOutputData> {
    let working_directory = working_directory.as_ref();
    let paths_dat_path = working_directory.join("paths.dat");
    if !paths_dat_path.is_file() {
        return Err(validation_error(
            "paths.dat",
            format!(
                "expected pathfinder output file `paths.dat` in `{}`",
                path_string(working_directory)
            ),
        ));
    }

    let mut path_files = Vec::new();
    for entry in fs::read_dir(working_directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name().to_string_lossy().into_owned();
        if is_path_data_file(&file_name) {
            path_files.push(path_string(&entry.path()));
        }
    }
    path_files.sort();

    let output = PathfinderOutputData {
        working_directory: path_string(working_directory),
        paths_dat: path_string(&paths_dat_path),
        path_files,
    };
    output.validate()?;
    Ok(output)
}

/// Run the native Rust pathfinder stage from typed pathfinder input and
/// write deterministic path artifacts into `working_directory`.
///
/// Artifacts written:
/// - `paths.dat`
/// - `pathNNNN.dat` for each generated path summary
pub fn run_pathfinder(
    input: &PathfinderInputData,
    working_directory: impl AsRef<Path>,
) -> Result<PathfinderOutputData> {
    input.validate()?;

    let working_directory = working_directory.as_ref();
    fs::create_dir_all(working_directory)?;
    clear_existing_pathfinder_artifacts(working_directory)?;

    let generated_paths = build_paths(input)?;
    write_paths_dat(working_directory, input, &generated_paths)?;
    write_path_summary_files(working_directory, &generated_paths)?;

    collect_pathfinder_output_data(working_directory)
}

fn clear_existing_pathfinder_artifacts(working_directory: &Path) -> Result<()> {
    for entry in fs::read_dir(working_directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name().to_string_lossy().into_owned();
        if file_name == "paths.dat" || is_path_data_file(&file_name) {
            fs::remove_file(entry.path())?;
        }
    }

    Ok(())
}

fn build_paths(input: &PathfinderInputData) -> Result<Vec<GeneratedPath>> {
    let absorber = input
        .atoms
        .iter()
        .find(|atom| atom.potential_index == 0)
        .ok_or_else(|| {
            validation_error("atoms", "must include absorber atom with potential index 0")
        })?;

    let absorber_label = input
        .potentials
        .iter()
        .find(|potential| potential.potential_index == 0)
        .map(|potential| potential.label.clone())
        .unwrap_or_else(|| "Abs".to_string());

    let mut potential_labels = BTreeMap::new();
    for potential in &input.potentials {
        potential_labels.insert(potential.potential_index, potential.label.clone());
    }

    let mut groups = BTreeMap::<(i32, i64), PathGroup>::new();
    for atom in &input.atoms {
        if atom.potential_index == 0 {
            continue;
        }

        let reff = atom_distance(absorber, atom);
        if reff <= REFF_BUCKET_EPSILON || reff > input.rmax + REFF_BUCKET_EPSILON {
            continue;
        }

        let bucket = reff_bucket(reff);
        let bucketed_reff = bucket as f64 / REFF_BUCKET_SCALE;
        let label = potential_labels
            .get(&atom.potential_index)
            .cloned()
            .unwrap_or_else(|| format!("ipot{}", atom.potential_index));

        let key = (atom.potential_index, bucket);
        let entry = groups.entry(key).or_insert_with(|| PathGroup {
            potential_index: atom.potential_index,
            label,
            reff: bucketed_reff,
            scatter_position: [atom.x, atom.y, atom.z],
            degeneracy: 0,
        });

        entry.degeneracy += 1;
        if compare_positions([atom.x, atom.y, atom.z], entry.scatter_position) == Ordering::Less {
            entry.scatter_position = [atom.x, atom.y, atom.z];
        }
    }

    let mut groups = groups.into_values().collect::<Vec<_>>();
    groups.sort_by(|left, right| {
        left.reff
            .total_cmp(&right.reff)
            .then(left.potential_index.cmp(&right.potential_index))
            .then(compare_positions(
                left.scatter_position,
                right.scatter_position,
            ))
    });

    if groups.is_empty() {
        return Err(validation_error(
            "atoms",
            "no scattering paths were generated from input atoms and rmax",
        ));
    }

    let mut generated = Vec::with_capacity(groups.len());
    for (index, group) in groups.into_iter().enumerate() {
        generated.push(GeneratedPath {
            path_index: index + 1,
            nleg: 2,
            degeneracy: group.degeneracy as f64,
            reff: group.reff,
            scatter_leg: PathLeg {
                x: group.scatter_position[0],
                y: group.scatter_position[1],
                z: group.scatter_position[2],
                potential_index: group.potential_index,
                label: group.label,
                rleg: group.reff,
                beta: 180.0,
                eta: 0.0,
            },
            absorber_leg: PathLeg {
                x: absorber.x,
                y: absorber.y,
                z: absorber.z,
                potential_index: 0,
                label: absorber_label.clone(),
                rleg: group.reff,
                beta: 180.0,
                eta: 0.0,
            },
        });
    }

    Ok(generated)
}

fn write_paths_dat(
    working_directory: &Path,
    input: &PathfinderInputData,
    paths: &[GeneratedPath],
) -> Result<()> {
    let mut content = String::new();
    content.push_str(&format!(
        "PATH  Rmax= {:5.3},  Keep_limit= 0.00, Heap_limit 0.00  Pwcrit= 2.50%\n",
        input.rmax
    ));
    content.push_str(" -----------------------------------------------------------------------\n");

    for path in paths {
        content.push_str(&format!(
            "{:>6}{:>5}{:>8.3}  index, nleg, degeneracy, r= {:>7.4}\n",
            path.path_index, path.nleg, path.degeneracy, path.reff
        ));
        content.push_str(
            "      x           y           z     ipot  label      rleg      beta        eta\n",
        );
        write_path_leg_line(&mut content, &path.scatter_leg);
        write_path_leg_line(&mut content, &path.absorber_leg);
    }

    fs::write(working_directory.join("paths.dat"), content)?;
    Ok(())
}

fn write_path_leg_line(content: &mut String, leg: &PathLeg) {
    content.push_str(&format!(
        "{:>12.6}{:>12.6}{:>12.6}{:>5} {:<8}{:>10.4}{:>10.4}{:>10.4}\n",
        leg.x,
        leg.y,
        leg.z,
        leg.potential_index,
        format_label(&leg.label),
        leg.rleg,
        leg.beta,
        leg.eta,
    ));
}

fn write_path_summary_files(working_directory: &Path, paths: &[GeneratedPath]) -> Result<()> {
    for path in paths {
        let file_name = format!("path{:04}.dat", path.path_index);
        let mut content = String::new();
        content.push_str("# feff85exafs-rs PATH summary artifact\n");
        content.push_str(&format!(
            "schema_version {PATHFINDER_ARTIFACT_SCHEMA_VERSION}\n"
        ));
        content.push_str(&format!("path_index {}\n", path.path_index));
        content.push_str(&format!("nleg {}\n", path.nleg));
        content.push_str(&format!("degeneracy {:.9}\n", path.degeneracy));
        content.push_str(&format!("reff {:.9}\n", path.reff));
        content.push_str(&format!(
            "scatter potential={} label={} xyz=({:.9},{:.9},{:.9})\n",
            path.scatter_leg.potential_index,
            path.scatter_leg.label.trim(),
            path.scatter_leg.x,
            path.scatter_leg.y,
            path.scatter_leg.z
        ));
        content.push_str(&format!(
            "absorber potential={} label={} xyz=({:.9},{:.9},{:.9})\n",
            path.absorber_leg.potential_index,
            path.absorber_leg.label.trim(),
            path.absorber_leg.x,
            path.absorber_leg.y,
            path.absorber_leg.z
        ));

        fs::write(working_directory.join(file_name), content)?;
    }

    Ok(())
}

fn format_label(label: &str) -> String {
    let trimmed = label.trim();
    let normalized = if trimmed.is_empty() { "UNK" } else { trimmed };
    let short = normalized.chars().take(6).collect::<String>();
    format!("'{short:<6}'")
}

fn reff_bucket(reff: f64) -> i64 {
    (reff * REFF_BUCKET_SCALE).round() as i64
}

fn round_reff(reff: f64) -> f64 {
    reff_bucket(reff) as f64 / REFF_BUCKET_SCALE
}

fn atom_distance(left: &PathfinderAtomData, right: &PathfinderAtomData) -> f64 {
    let dx = right.x - left.x;
    let dy = right.y - left.y;
    let dz = right.z - left.z;
    (dx * dx + dy * dy + dz * dz).sqrt()
}

fn compare_positions(left: [f64; 3], right: [f64; 3]) -> Ordering {
    left[0]
        .total_cmp(&right[0])
        .then(left[1].total_cmp(&right[1]))
        .then(left[2].total_cmp(&right[2]))
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

fn is_path_data_file(file_name: &str) -> bool {
    if !file_name.starts_with("path") || !file_name.ends_with(".dat") {
        return false;
    }
    let digits = &file_name[4..file_name.len() - 4];
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
    use crate::xsph::{XsphInputData, run_xsph};
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    const REFF_TOLERANCE: f64 = 1.0e-4;
    const DEGENERACY_TOLERANCE: f64 = 1.0e-3;

    #[derive(Debug, Clone)]
    struct PathRecordSignature {
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
            "feff85exafs-core-pathfinder-{name}-{}-{nonce}",
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

    fn parse_paths_signatures(path: &Path) -> Vec<PathRecordSignature> {
        let raw = fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("failed to read paths.dat {}: {error}", path.display()));
        let lines = raw.lines().collect::<Vec<_>>();

        let mut signatures = Vec::new();
        let mut index = 0usize;
        while index < lines.len() {
            let line = lines[index].trim();
            let Some((nleg, degeneracy, reff)) = parse_path_header_line(line) else {
                index += 1;
                continue;
            };

            index += 1;
            while index < lines.len() && lines[index].trim().is_empty() {
                index += 1;
            }
            if index < lines.len() && lines[index].contains("ipot") {
                index += 1;
            }

            let mut scatter_potential = None;
            for leg in 0..nleg.max(0) as usize {
                if index >= lines.len() {
                    break;
                }
                if let Some(ipot) = parse_leg_ipot(lines[index])
                    && (leg == 0 || scatter_potential.is_none())
                {
                    scatter_potential = Some(ipot);
                }
                index += 1;
            }

            if let Some(potential_index) = scatter_potential {
                signatures.push(PathRecordSignature {
                    potential_index,
                    nleg,
                    degeneracy,
                    reff,
                });
            }
        }

        signatures
    }

    fn parse_path_header_line(line: &str) -> Option<(i32, f64, f64)> {
        if !line.contains("index, nleg, degeneracy") {
            return None;
        }

        let prefix = line.split("index,").next()?;
        let tokens = prefix.split_whitespace().collect::<Vec<_>>();
        if tokens.len() < 3 {
            return None;
        }

        let nleg = tokens.get(1)?.parse::<i32>().ok()?;
        let degeneracy = tokens.get(2)?.parse::<f64>().ok()?;
        let reff = line
            .split("r=")
            .nth(1)?
            .split_whitespace()
            .next()?
            .parse::<f64>()
            .ok()?;

        Some((nleg, degeneracy, reff))
    }

    fn parse_leg_ipot(line: &str) -> Option<i32> {
        line.split_whitespace().nth(3)?.parse::<i32>().ok()
    }

    fn nearest_shell_signatures(records: &[PathRecordSignature]) -> Vec<NearestShellSignature> {
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
    fn builds_pathfinder_input_from_previous_stages() {
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
        .expect("pathfinder input should build from previous stages");
        assert!(pathfinder_input.rmax > 0.0);
        assert!(
            pathfinder_input
                .atoms
                .iter()
                .any(|atom| atom.potential_index == 0)
        );
        assert!(
            pathfinder_input
                .atoms
                .iter()
                .any(|atom| atom.potential_index != 0)
        );

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp directory should be cleaned");
        }
    }

    #[test]
    fn collect_pathfinder_output_requires_paths_dat() {
        let temp_root = temp_dir("missing-paths-dat");
        fs::create_dir_all(&temp_root).expect("temp root should be created");
        fs::write(temp_root.join("path0001.dat"), "placeholder\n")
            .expect("path summary should be written");

        let error = collect_pathfinder_output_data(&temp_root)
            .expect_err("collecting output without paths.dat should fail");
        match error {
            FeffError::Validation(validation) => {
                assert_eq!(validation.len(), 1);
                assert_eq!(validation.issues()[0].field, "paths.dat");
            }
            other => panic!("unexpected error type: {other}"),
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp directory should be cleaned");
        }
    }

    #[test]
    fn run_pathfinder_writes_required_artifacts_for_phase1_corpus() {
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

            let path_input = PathfinderInputData::from_previous_stages(
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
            let output = run_pathfinder(&path_input, &path_working_dir).unwrap_or_else(|error| {
                panic!(
                    "failed to run Rust pathfinder for {}: {error}",
                    input_path.display()
                )
            });

            assert!(output.paths_dat.ends_with("paths.dat"));
            assert!(!output.path_files.is_empty());
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
    }

    #[test]
    fn rust_pathfinder_matches_legacy_nearest_shell_signature_for_phase1_corpus() {
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
            let path_input = PathfinderInputData::from_previous_stages(
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
            let path_output =
                run_pathfinder(&path_input, &path_working_dir).unwrap_or_else(|error| {
                    panic!(
                        "failed to run Rust pathfinder for {}: {error}",
                        input_path.display()
                    )
                });

            let legacy_paths = parse_paths_signatures(&input_path.with_file_name("paths.dat"));
            let rust_paths = parse_paths_signatures(Path::new(&path_output.paths_dat));

            let legacy_nearest = nearest_shell_signatures(&legacy_paths);
            let rust_nearest = nearest_shell_signatures(&rust_paths);

            assert_eq!(
                rust_nearest.len(),
                legacy_nearest.len(),
                "nearest-shell path count mismatch for {}",
                input_path.display()
            );

            for (legacy, rust) in legacy_nearest.iter().zip(rust_nearest.iter()) {
                assert_eq!(
                    rust.potential_index,
                    legacy.potential_index,
                    "nearest-shell potential mismatch for {}",
                    input_path.display()
                );
                assert!(
                    numeric_within_tolerance(rust.reff, legacy.reff, REFF_TOLERANCE),
                    "nearest-shell reff mismatch for {} potential {}: rust={} legacy={}",
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
                    "nearest-shell degeneracy mismatch for {} potential {}: rust={} legacy={}",
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
