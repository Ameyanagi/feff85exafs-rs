use std::env;
use std::path::PathBuf;
use std::process;

use feff85exafs_core::baseline::{generate_noscf_manifests, generate_withscf_manifests};
use feff85exafs_errors::{FeffError, Result};

enum BaselineVariant {
    NoScf,
    WithScf,
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

    let mut idx = 2;
    while idx < args.len() {
        match args[idx].as_str() {
            "--tests-root" => {
                idx += 1;
                let value = args.get(idx).ok_or_else(|| {
                    FeffError::InvalidArgument("missing value for --tests-root".to_string())
                })?;
                tests_root = PathBuf::from(value);
            }
            "--output-root" => {
                idx += 1;
                let value = args.get(idx).ok_or_else(|| {
                    FeffError::InvalidArgument("missing value for --output-root".to_string())
                })?;
                output_root = PathBuf::from(value);
            }
            "--version" => {
                idx += 1;
                let value = args.get(idx).ok_or_else(|| {
                    FeffError::InvalidArgument("missing value for --version".to_string())
                })?;
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

    let summary = match variant {
        BaselineVariant::NoScf => generate_noscf_manifests(&tests_root, &output_root, &version)?,
        BaselineVariant::WithScf => {
            generate_withscf_manifests(&tests_root, &output_root, &version)?
        }
    };

    println!(
        "Generated {} {} manifests in {}",
        summary.case_count,
        variant.label(),
        summary.version_dir.display()
    );
    println!("Manifest files:");
    for manifest in summary.manifest_paths {
        println!("  {}", manifest.display());
    }
    Ok(())
}

fn print_usage() {
    eprintln!("Usage:");
    eprintln!(
        "  cargo run -- baseline <noscf|scf> [--tests-root PATH] [--output-root PATH] [--version VERSION]"
    );
    eprintln!();
    eprintln!("Variant aliases:");
    eprintln!("  scf: accepts `scf`, `withscf`, and `with-scf`");
    eprintln!();
    eprintln!("Defaults:");
    eprintln!("  --tests-root  feff85exafs/tests");
    eprintln!("  --output-root docs/migration/baselines");
    eprintln!("  --version     v1");
}
