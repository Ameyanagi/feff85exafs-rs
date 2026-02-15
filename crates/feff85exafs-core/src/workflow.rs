use std::fs;
use std::path::Path;
use std::process::Command;

use crate::debye::{DebyeInputData, DebyeOutputData, run_debye};
use crate::exch::{ExchInputData, ExchOutputData, run_exch};
use crate::ff2x::{Ff2xInputData, Ff2xOutputData, run_ff2x};
use crate::fovrg::{FovrgInputData, FovrgOutputData, run_fovrg};
use crate::genfmt::{GenfmtInputData, GenfmtOutputData, run_genfmt};
use crate::pathfinder::{PathfinderInputData, PathfinderOutputData, run_pathfinder};
use crate::pot::{PotInputData, PotOutputData, run_pot};
use crate::rdinp::parse_rdinp;
use crate::xsph::{XsphInputData, XsphOutputData, run_xsph};
use feff85exafs_errors::{FeffError, Result, ValidationErrors};

#[derive(Debug, Clone, PartialEq)]
pub struct ModernWorkflowOutput {
    pub pot_output: PotOutputData,
    pub xsph_output: XsphOutputData,
    pub pathfinder_output: PathfinderOutputData,
    pub genfmt_output: GenfmtOutputData,
    pub ff2x_output: Ff2xOutputData,
    pub debye_output: DebyeOutputData,
    pub exch_output: ExchOutputData,
    pub fovrg_output: FovrgOutputData,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyWorkflowOutput {
    pub working_directory: String,
    pub chi_dat: String,
    pub xmu_dat: String,
}

pub fn run_modern_workflow(
    input_path: impl AsRef<Path>,
    working_root: impl AsRef<Path>,
) -> Result<ModernWorkflowOutput> {
    let input_path = input_path.as_ref();
    ensure_input_file(input_path)?;

    let working_root = working_root.as_ref();
    reset_dir(working_root)?;

    let parsed = parse_rdinp(input_path)?;
    let pot_input = PotInputData::from_parsed_cards(&parsed)?;

    let pot_working_dir = working_root.join("pot");
    let xsph_working_dir = working_root.join("xsph");
    let pathfinder_working_dir = working_root.join("pathfinder");
    let genfmt_working_dir = working_root.join("genfmt");
    let ff2x_working_dir = working_root.join("ff2x");
    let debye_working_dir = working_root.join("debye");
    let exch_working_dir = working_root.join("exch");
    let fovrg_working_dir = working_root.join("fovrg");

    let pot_output = run_pot(&pot_input, &pot_working_dir)?;
    let xsph_input = XsphInputData::from_pot_stage(&pot_input, &pot_output)?;
    let xsph_output = run_xsph(&xsph_input, &xsph_working_dir)?;
    let pathfinder_input = PathfinderInputData::from_previous_stages(
        &pot_input,
        &pot_output,
        &xsph_input,
        &xsph_output,
    )?;
    let pathfinder_output = run_pathfinder(&pathfinder_input, &pathfinder_working_dir)?;
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
    let ff2x_output = run_ff2x(&ff2x_input, &ff2x_working_dir)?;
    let debye_input = DebyeInputData::from_previous_stages(&parsed, &genfmt_output)?;
    let debye_output = run_debye(&debye_input, &debye_working_dir)?;
    let exch_input = ExchInputData::from_previous_stages(&parsed, &genfmt_output)?;
    let exch_output = run_exch(&exch_input, &exch_working_dir)?;
    let fovrg_input = FovrgInputData::from_previous_stages(&parsed, &genfmt_output)?;
    let fovrg_output = run_fovrg(&fovrg_input, &fovrg_working_dir)?;

    Ok(ModernWorkflowOutput {
        pot_output,
        xsph_output,
        pathfinder_output,
        genfmt_output,
        ff2x_output,
        debye_output,
        exch_output,
        fovrg_output,
    })
}

pub fn run_legacy_workflow(
    input_path: impl AsRef<Path>,
    working_root: impl AsRef<Path>,
    legacy_runner: &str,
) -> Result<LegacyWorkflowOutput> {
    let input_path = input_path.as_ref();
    ensure_input_file(input_path)?;

    if legacy_runner.trim().is_empty() {
        return Err(validation_error("legacy_runner", "must not be blank"));
    }

    if looks_like_path(legacy_runner) && !Path::new(legacy_runner).is_file() {
        return Err(validation_error(
            "legacy_runner",
            format!("legacy runner executable was not found at `{legacy_runner}`"),
        ));
    }

    let working_root = working_root.as_ref();
    reset_dir(working_root)?;
    fs::copy(input_path, working_root.join("feff.inp"))?;

    let output = Command::new(legacy_runner)
        .current_dir(working_root)
        .output()
        .map_err(|error| {
            FeffError::InvalidArgument(format!(
                "failed to execute legacy runner `{legacy_runner}`: {error}"
            ))
        })?;

    if !output.status.success() {
        return Err(FeffError::InvalidArgument(format!(
            "legacy runner `{legacy_runner}` failed for `{}` with status {}\nstdout:\n{}\nstderr:\n{}",
            path_string(input_path),
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        )));
    }

    let chi_dat_path = working_root.join("chi.dat");
    if !chi_dat_path.is_file() {
        return Err(validation_error(
            "legacy_output.chi_dat",
            format!(
                "expected legacy output file `{}`",
                path_string(&chi_dat_path)
            ),
        ));
    }

    let xmu_dat_path = working_root.join("xmu.dat");
    if !xmu_dat_path.is_file() {
        return Err(validation_error(
            "legacy_output.xmu_dat",
            format!(
                "expected legacy output file `{}`",
                path_string(&xmu_dat_path)
            ),
        ));
    }

    Ok(LegacyWorkflowOutput {
        working_directory: path_string(working_root),
        chi_dat: path_string(&chi_dat_path),
        xmu_dat: path_string(&xmu_dat_path),
    })
}

fn ensure_input_file(path: &Path) -> Result<()> {
    if path.is_file() {
        Ok(())
    } else {
        Err(validation_error(
            "input_path",
            format!("input file was not found at `{}`", path_string(path)),
        ))
    }
}

fn reset_dir(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)?;
    Ok(())
}

fn looks_like_path(value: &str) -> bool {
    value.contains('/')
        || value.contains('\\')
        || value.starts_with('.')
        || value.starts_with('~')
        || value.ends_with(".exe")
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
    use std::path::PathBuf;
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
            "feff85exafs-core-workflow-{name}-{}-{timestamp}-{counter}",
            std::process::id()
        ))
    }

    #[test]
    fn modern_workflow_runs_core_and_non_core_stages_for_fixture() {
        let fixture_input = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../feff85exafs/tests/Copper/baseline/noSCF/feff.inp");
        let temp_root = temp_dir("modern");
        let output = run_modern_workflow(&fixture_input, temp_root.join("run"))
            .expect("modern workflow should run for fixture");

        assert!(Path::new(&output.ff2x_output.chi_dat).is_file());
        assert!(Path::new(&output.ff2x_output.xmu_dat).is_file());
        assert!(Path::new(&output.debye_output.sig2_dat).is_file());
        assert!(Path::new(&output.exch_output.exchange_dat).is_file());
        assert!(Path::new(&output.fovrg_output.fovrg_dat).is_file());

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
        }
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

        #[test]
        fn legacy_workflow_runs_with_explicit_runner() {
            let fixture_input = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../feff85exafs/tests/Copper/baseline/noSCF/feff.inp");
            let temp_root = temp_dir("legacy");
            let runner = temp_root.join("fake-legacy-runner.sh");

            write_executable(
                &runner,
                "#!/bin/sh\nset -eu\nprintf '0.0 0.0\\n' > chi.dat\nprintf '0.0 0.0\\n' > xmu.dat\n",
            )
            .expect("legacy runner should be created");

            let output = run_legacy_workflow(
                &fixture_input,
                temp_root.join("run"),
                &runner.to_string_lossy(),
            )
            .expect("legacy workflow should run with explicit runner");

            assert!(Path::new(&output.chi_dat).is_file());
            assert!(Path::new(&output.xmu_dat).is_file());

            if temp_root.exists() {
                fs::remove_dir_all(&temp_root).expect("temp root should be cleaned");
            }
        }
    }
}
