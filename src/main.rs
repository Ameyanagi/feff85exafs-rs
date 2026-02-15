use std::env;
use std::path::PathBuf;
use std::process;

use feff85exafs_core::baseline::{generate_noscf_manifests, generate_withscf_manifests};
use feff85exafs_core::domain::RunMode;
use feff85exafs_core::mode::{parse_run_mode_or_default, run_mode_value};
use feff85exafs_errors::{FeffError, Result};

#[derive(Debug, Clone, Copy)]
enum BaselineVariant {
    NoScf,
    WithScf,
}

#[derive(Debug, Clone)]
struct BaselineCommand {
    variant: BaselineVariant,
    mode: RunMode,
    tests_root: PathBuf,
    output_root: PathBuf,
    version: String,
}

impl BaselineVariant {
    fn parse(value: &str) -> Option<Self> {
        let normalized = value.to_ascii_lowercase();
        match normalized.as_str() {
            "noscf" => Some(Self::NoScf),
            "scf" | "withscf" | "with-scf" => Some(Self::WithScf),
            _ => None,
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Self::NoScf => "noSCF",
            Self::WithScf => "withSCF",
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() || args.iter().any(|arg| arg == "-h" || arg == "--help") {
        print_usage();
        return;
    }

    match run(args) {
        Ok(()) => {}
        Err(error) => {
            eprintln!("error: {error}");
            eprintln!();
            print_usage();
            process::exit(1);
        }
    }
}

fn run(args: Vec<String>) -> Result<()> {
    let command = parse_baseline_command(&args)?;

    let summary = match command.variant {
        BaselineVariant::NoScf => {
            generate_noscf_manifests(&command.tests_root, &command.output_root, &command.version)?
        }
        BaselineVariant::WithScf => {
            generate_withscf_manifests(&command.tests_root, &command.output_root, &command.version)?
        }
    };

    println!("Using run mode: {}", run_mode_value(command.mode));
    println!(
        "Generated {} {} manifests in {}",
        summary.case_count,
        command.variant.label(),
        summary.version_dir.display()
    );
    println!("Manifest files:");
    for manifest in summary.manifest_paths {
        println!("  {}", manifest.display());
    }
    Ok(())
}

fn parse_baseline_command(args: &[String]) -> Result<BaselineCommand> {
    if args.first().map(String::as_str) != Some("baseline") {
        return Err(FeffError::InvalidArgument(
            "only `baseline` is currently supported".to_string(),
        ));
    }
    let variant = args
        .get(1)
        .ok_or_else(|| {
            FeffError::InvalidArgument(
                "baseline variant must be one of: `noscf`, `scf`".to_string(),
            )
        })
        .and_then(|value| {
            BaselineVariant::parse(value).ok_or_else(|| {
                FeffError::InvalidArgument(format!(
                    "unsupported baseline variant `{value}` (expected `noscf` or `scf`)"
                ))
            })
        })?;

    let mut tests_root = PathBuf::from("feff85exafs/tests");
    let mut output_root = PathBuf::from("docs/migration/baselines");
    let mut version = "v1".to_string();
    let mut mode_arg: Option<&str> = None;

    let mut idx = 2;
    while idx < args.len() {
        match args[idx].as_str() {
            "--mode" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--mode"))?;
                mode_arg = Some(value.as_str());
            }
            "--tests-root" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--tests-root"))?;
                tests_root = PathBuf::from(value);
            }
            "--output-root" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--output-root"))?;
                output_root = PathBuf::from(value);
            }
            "--version" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| missing_option_value_error("--version"))?;
                version = value.clone();
            }
            unknown => {
                return Err(FeffError::InvalidArgument(format!(
                    "unknown argument `{unknown}`"
                )));
            }
        }
        idx += 1;
    }

    if version.trim().is_empty() {
        return Err(FeffError::InvalidArgument(
            "version cannot be empty".to_string(),
        ));
    }

    let mode = parse_run_mode_or_default(mode_arg)?;

    Ok(BaselineCommand {
        variant,
        mode,
        tests_root,
        output_root,
        version,
    })
}

fn missing_option_value_error(flag: &str) -> FeffError {
    FeffError::InvalidArgument(format!("missing value for {flag}"))
}

fn print_usage() {
    eprintln!("Usage:");
    eprintln!(
        "  cargo run -- baseline <noscf|scf> [--mode <legacy|modern>] [--tests-root PATH] [--output-root PATH] [--version VERSION]"
    );
    eprintln!();
    eprintln!("Variant aliases:");
    eprintln!("  scf: accepts `scf`, `withscf`, and `with-scf`");
    eprintln!();
    eprintln!("Defaults:");
    eprintln!("  --mode       modern");
    eprintln!("  --tests-root  feff85exafs/tests");
    eprintln!("  --output-root docs/migration/baselines");
    eprintln!("  --version     v1");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn parse_baseline_command_defaults_to_modern_mode() {
        let command = parse_baseline_command(&args(&["baseline", "noscf"]))
            .expect("baseline args should parse");
        assert_eq!(command.mode, RunMode::Modern);
    }

    #[test]
    fn parse_baseline_command_accepts_legacy_mode() {
        let command = parse_baseline_command(&args(&["baseline", "noscf", "--mode", "legacy"]))
            .expect("legacy mode should parse");
        assert_eq!(command.mode, RunMode::Legacy);
    }

    #[test]
    fn parse_baseline_command_rejects_invalid_mode_with_structured_error() {
        let err = parse_baseline_command(&args(&["baseline", "noscf", "--mode", "future"]))
            .expect_err("invalid mode should fail parsing");
        match err {
            FeffError::Validation(validation) => {
                assert_eq!(validation.len(), 1);
                assert_eq!(validation.issues()[0].field, "mode");
            }
            other => panic!("unexpected error type: {other}"),
        }
    }
}
