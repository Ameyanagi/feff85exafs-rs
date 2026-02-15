use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use crate::domain::RunMode;
use crate::legacy::validate_legacy_baseline_file_names;
use feff85exafs_errors::Result;
use serde::Serialize;
use sha2::{Digest, Sha256};

const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct GenerationSummary {
    pub version_dir: PathBuf,
    pub case_count: usize,
    pub manifest_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone)]
struct CaseInput {
    material: String,
    baseline_dir: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BaselineVariant {
    NoScf,
    WithScf,
}

impl BaselineVariant {
    fn value(self) -> &'static str {
        match self {
            Self::NoScf => "noSCF",
            Self::WithScf => "withSCF",
        }
    }

    fn manifest_suffix(self) -> &'static str {
        match self {
            Self::NoScf => "noscf",
            Self::WithScf => "withscf",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct FileDigest {
    path: String,
    size: u64,
    sha256: String,
}

#[derive(Debug, Clone, Serialize)]
struct CaseManifest {
    schema_version: u32,
    material: String,
    variant: String,
    source: String,
    file_count: usize,
    total_bytes: u64,
    files: Vec<FileDigest>,
}

#[derive(Debug, Clone, Serialize)]
struct CorpusIndex {
    schema_version: u32,
    variant: String,
    source_root: String,
    version: String,
    case_count: usize,
    cases: Vec<IndexEntry>,
}

#[derive(Debug, Clone, Serialize)]
struct IndexEntry {
    material: String,
    manifest: String,
    source: String,
    file_count: usize,
    total_bytes: u64,
}

pub fn generate_noscf_manifests(
    tests_root: &Path,
    output_root: &Path,
    version: &str,
) -> Result<GenerationSummary> {
    generate_noscf_manifests_for_mode(tests_root, output_root, version, RunMode::Modern)
}

pub fn generate_noscf_manifests_for_mode(
    tests_root: &Path,
    output_root: &Path,
    version: &str,
    mode: RunMode,
) -> Result<GenerationSummary> {
    generate_manifests(
        tests_root,
        output_root,
        version,
        BaselineVariant::NoScf,
        mode,
    )
}

pub fn generate_withscf_manifests(
    tests_root: &Path,
    output_root: &Path,
    version: &str,
) -> Result<GenerationSummary> {
    generate_withscf_manifests_for_mode(tests_root, output_root, version, RunMode::Modern)
}

pub fn generate_withscf_manifests_for_mode(
    tests_root: &Path,
    output_root: &Path,
    version: &str,
    mode: RunMode,
) -> Result<GenerationSummary> {
    verify_withscf_has_same_materials(tests_root)?;
    generate_manifests(
        tests_root,
        output_root,
        version,
        BaselineVariant::WithScf,
        mode,
    )
}

fn generate_manifests(
    tests_root: &Path,
    output_root: &Path,
    version: &str,
    variant: BaselineVariant,
    mode: RunMode,
) -> Result<GenerationSummary> {
    let cases = discover_cases(tests_root, variant)?;
    let version_dir = output_root.join(variant.value()).join(version);

    if version_dir.exists() {
        fs::remove_dir_all(&version_dir)?;
    }
    fs::create_dir_all(&version_dir)?;

    let mut manifests = Vec::with_capacity(cases.len());
    let mut index_entries = Vec::with_capacity(cases.len());

    for (idx, case) in cases.iter().enumerate() {
        let files = collect_file_digests(&case.baseline_dir)?;
        ensure_legacy_file_contract(mode, &files)?;
        let total_bytes = files.iter().map(|file| file.size).sum();
        let source = path_string(
            case.baseline_dir
                .strip_prefix(tests_root)
                .unwrap_or(case.baseline_dir.as_path()),
        );
        let manifest_name = manifest_file_name(idx + 1, &case.material, variant);
        let manifest_path = version_dir.join(&manifest_name);

        let manifest = CaseManifest {
            schema_version: SCHEMA_VERSION,
            material: case.material.clone(),
            variant: variant.value().to_string(),
            source: source.clone(),
            file_count: files.len(),
            total_bytes,
            files,
        };

        write_json(&manifest_path, &manifest)?;
        manifests.push(manifest_path.clone());
        index_entries.push(IndexEntry {
            material: case.material.clone(),
            manifest: manifest_name,
            source,
            file_count: manifest.file_count,
            total_bytes: manifest.total_bytes,
        });
    }

    let index = CorpusIndex {
        schema_version: SCHEMA_VERSION,
        variant: variant.value().to_string(),
        source_root: path_string(tests_root),
        version: version.to_string(),
        case_count: index_entries.len(),
        cases: index_entries,
    };
    write_json(&version_dir.join("index.json"), &index)?;

    Ok(GenerationSummary {
        version_dir,
        case_count: manifests.len(),
        manifest_paths: manifests,
    })
}

fn ensure_legacy_file_contract(mode: RunMode, files: &[FileDigest]) -> Result<()> {
    if mode != RunMode::Legacy {
        return Ok(());
    }

    let file_names = files
        .iter()
        .map(|file| file.path.clone())
        .collect::<Vec<_>>();
    validate_legacy_baseline_file_names(&file_names).map_err(|message| {
        io::Error::other(format!("legacy mode baseline validation failed: {message}")).into()
    })
}

fn verify_withscf_has_same_materials(tests_root: &Path) -> Result<()> {
    let noscf_cases = discover_cases(tests_root, BaselineVariant::NoScf)?;
    let withscf_cases = discover_cases(tests_root, BaselineVariant::WithScf)?;

    let noscf_materials: BTreeSet<String> =
        noscf_cases.into_iter().map(|case| case.material).collect();
    let withscf_materials: BTreeSet<String> = withscf_cases
        .into_iter()
        .map(|case| case.material)
        .collect();

    let missing_withscf: Vec<String> = noscf_materials
        .difference(&withscf_materials)
        .cloned()
        .collect();
    let missing_noscf: Vec<String> = withscf_materials
        .difference(&noscf_materials)
        .cloned()
        .collect();

    if missing_withscf.is_empty() && missing_noscf.is_empty() {
        return Ok(());
    }

    let mut problems = Vec::new();
    if !missing_withscf.is_empty() {
        problems.push(format!(
            "missing withSCF baselines for: {}",
            missing_withscf.join(", ")
        ));
    }
    if !missing_noscf.is_empty() {
        problems.push(format!(
            "unexpected withSCF-only baselines for: {}",
            missing_noscf.join(", ")
        ));
    }

    Err(io::Error::other(format!(
        "SCF corpus must match noSCF materials: {}",
        problems.join("; ")
    ))
    .into())
}

fn discover_cases(tests_root: &Path, variant: BaselineVariant) -> io::Result<Vec<CaseInput>> {
    let mut cases = Vec::new();

    for entry in fs::read_dir(tests_root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let material_path = entry.path();
        let material = entry.file_name().to_string_lossy().into_owned();
        let baseline_dir = material_path.join("baseline").join(variant.value());
        if baseline_dir.is_dir() {
            cases.push(CaseInput {
                material,
                baseline_dir,
            });
        }
    }

    cases.sort_by(|left, right| {
        left.material
            .to_ascii_lowercase()
            .cmp(&right.material.to_ascii_lowercase())
            .then_with(|| left.material.cmp(&right.material))
    });

    Ok(cases)
}

fn collect_file_digests(root: &Path) -> io::Result<Vec<FileDigest>> {
    let mut stack = vec![root.to_path_buf()];
    let mut files = Vec::new();

    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file() {
                files.push(path);
            }
        }
    }

    files.sort();

    let mut digests = Vec::with_capacity(files.len());
    for file in files {
        let rel = file
            .strip_prefix(root)
            .map_err(|_| io::Error::other("failed to build a relative baseline path"))?;
        digests.push(FileDigest {
            path: path_string(rel),
            size: file.metadata()?.len(),
            sha256: sha256_hex(&file)?,
        });
    }
    Ok(digests)
}

fn sha256_hex(path: &Path) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    let mut content = serde_json::to_string_pretty(value)
        .map_err(|err| io::Error::other(format!("failed to serialize JSON: {err}")))?;
    content.push('\n');
    fs::write(path, content)?;
    Ok(())
}

fn manifest_file_name(index: usize, material: &str, variant: BaselineVariant) -> String {
    format!(
        "{index:03}-{}-{}.json",
        slugify(material),
        variant.manifest_suffix()
    )
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn slugify(input: &str) -> String {
    let mut slug = String::new();
    let mut previous_dash = false;

    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            previous_dash = false;
        } else if !previous_dash {
            slug.push('-');
            previous_dash = true;
        }
    }

    while slug.starts_with('-') {
        slug.remove(0);
    }
    while slug.ends_with('-') {
        slug.pop();
    }

    if slug.is_empty() {
        "case".to_string()
    } else {
        slug
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::io::Write;
    use std::process;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new() -> io::Result<Self> {
            let counter = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let unique = format!(
                "feff85exafs-rs-baseline-{}-{}-{counter}",
                process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system time before unix epoch")
                    .as_nanos()
            );
            let path = env::temp_dir().join(unique);
            fs::create_dir_all(&path)?;
            Ok(Self { path })
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn write_file(path: &Path, content: &str) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = File::create(path)?;
        file.write_all(content.as_bytes())?;
        Ok(())
    }

    #[test]
    fn slugify_collapses_symbol_runs() {
        assert_eq!(slugify("LCO-perp"), "lco-perp");
        assert_eq!(slugify("NiO test_case"), "nio-test-case");
        assert_eq!(slugify("___"), "case");
    }

    #[test]
    fn discover_cases_filters_by_variant() {
        let tmp = TempDir::new().expect("create temp dir");
        let tests_root = tmp.path.join("tests");

        fs::create_dir_all(tests_root.join("Alpha/baseline/noSCF")).expect("make Alpha noSCF");
        fs::create_dir_all(tests_root.join("Beta/baseline/withSCF")).expect("make Beta withSCF");

        let no_scf_cases =
            discover_cases(&tests_root, BaselineVariant::NoScf).expect("discover noSCF cases");
        assert_eq!(no_scf_cases.len(), 1);
        assert_eq!(no_scf_cases[0].material, "Alpha");

        let with_scf_cases =
            discover_cases(&tests_root, BaselineVariant::WithScf).expect("discover withSCF cases");
        assert_eq!(with_scf_cases.len(), 1);
        assert_eq!(with_scf_cases[0].material, "Beta");
    }

    #[test]
    fn generate_noscf_manifests_is_deterministic() {
        let tmp = TempDir::new().expect("create temp dir");
        let tests_root = tmp.path.join("tests");
        let output_root = tmp.path.join("artifacts");

        write_file(
            &tests_root.join("Beta/baseline/noSCF/feff.inp"),
            "beta baseline",
        )
        .expect("write Beta file");
        write_file(
            &tests_root.join("alpha/baseline/noSCF/files.dat"),
            "alpha files",
        )
        .expect("write alpha file");
        write_file(
            &tests_root.join("alpha/baseline/noSCF/sub/xmu.dat"),
            "alpha xmu",
        )
        .expect("write alpha nested file");

        let first = generate_noscf_manifests(&tests_root, &output_root, "v1")
            .expect("generate first run manifests");
        let index_path = first.version_dir.join("index.json");
        let first_index = fs::read_to_string(&index_path).expect("read first index");

        assert_eq!(first.case_count, 2);
        assert_eq!(
            first
                .manifest_paths
                .iter()
                .map(|path| {
                    path.file_name()
                        .expect("manifest file name")
                        .to_string_lossy()
                        .into_owned()
                })
                .collect::<Vec<_>>(),
            vec!["001-alpha-noscf.json", "002-beta-noscf.json"]
        );

        let second = generate_noscf_manifests(&tests_root, &output_root, "v1")
            .expect("generate second run manifests");
        let second_index =
            fs::read_to_string(second.version_dir.join("index.json")).expect("read second index");

        assert_eq!(first_index, second_index);
    }

    #[test]
    fn generate_withscf_manifests_is_deterministic() {
        let tmp = TempDir::new().expect("create temp dir");
        let tests_root = tmp.path.join("tests");
        let output_root = tmp.path.join("artifacts");

        write_file(
            &tests_root.join("Beta/baseline/noSCF/feff.inp"),
            "beta noSCF baseline",
        )
        .expect("write Beta noSCF file");
        write_file(
            &tests_root.join("Beta/baseline/withSCF/feff.inp"),
            "beta withSCF baseline",
        )
        .expect("write Beta withSCF file");
        write_file(
            &tests_root.join("alpha/baseline/noSCF/files.dat"),
            "alpha noSCF files",
        )
        .expect("write alpha noSCF file");
        write_file(
            &tests_root.join("alpha/baseline/withSCF/files.dat"),
            "alpha withSCF files",
        )
        .expect("write alpha withSCF file");
        write_file(
            &tests_root.join("alpha/baseline/withSCF/sub/xmu.dat"),
            "alpha withSCF xmu",
        )
        .expect("write alpha withSCF nested file");

        let first = generate_withscf_manifests(&tests_root, &output_root, "v1")
            .expect("generate first withSCF run manifests");
        let first_index = fs::read_to_string(first.version_dir.join("index.json"))
            .expect("read first withSCF index");

        assert_eq!(first.case_count, 2);
        assert_eq!(
            first
                .manifest_paths
                .iter()
                .map(|path| {
                    path.file_name()
                        .expect("manifest file name")
                        .to_string_lossy()
                        .into_owned()
                })
                .collect::<Vec<_>>(),
            vec!["001-alpha-withscf.json", "002-beta-withscf.json"]
        );

        let second = generate_withscf_manifests(&tests_root, &output_root, "v1")
            .expect("generate second withSCF run manifests");
        let second_index = fs::read_to_string(second.version_dir.join("index.json"))
            .expect("read second withSCF index");

        assert_eq!(first_index, second_index);
    }

    #[test]
    fn generate_withscf_manifests_requires_matching_material_sets() {
        let tmp = TempDir::new().expect("create temp dir");
        let tests_root = tmp.path.join("tests");
        let output_root = tmp.path.join("artifacts");

        write_file(
            &tests_root.join("alpha/baseline/noSCF/feff.inp"),
            "alpha noSCF baseline",
        )
        .expect("write alpha noSCF file");
        write_file(
            &tests_root.join("beta/baseline/withSCF/feff.inp"),
            "beta withSCF baseline",
        )
        .expect("write beta withSCF file");

        let err = generate_withscf_manifests(&tests_root, &output_root, "v1")
            .expect_err("withSCF generation should fail when case sets differ");
        assert!(
            err.to_string()
                .contains("SCF corpus must match noSCF materials"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn legacy_mode_requires_historical_baseline_file_names() {
        let tmp = TempDir::new().expect("create temp dir");
        let tests_root = tmp.path.join("tests");
        let output_root = tmp.path.join("artifacts");

        write_file(
            &tests_root.join("alpha/baseline/noSCF/feff.inp"),
            "alpha noSCF baseline",
        )
        .expect("write alpha noSCF file");

        let err =
            generate_noscf_manifests_for_mode(&tests_root, &output_root, "v1", RunMode::Legacy)
                .expect_err("legacy mode should fail when expected names are missing");
        assert!(
            err.to_string()
                .contains("legacy mode baseline validation failed"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn legacy_mode_accepts_historical_baseline_file_names() {
        let tmp = TempDir::new().expect("create temp dir");
        let tests_root = tmp.path.join("tests");
        let output_root = tmp.path.join("artifacts");

        write_file(
            &tests_root.join("alpha/baseline/noSCF/feff.inp"),
            "alpha noSCF baseline",
        )
        .expect("write feff input");
        write_file(
            &tests_root.join("alpha/baseline/noSCF/files.dat"),
            "files index",
        )
        .expect("write files.dat");
        write_file(
            &tests_root.join("alpha/baseline/noSCF/paths.dat"),
            "paths index",
        )
        .expect("write paths.dat");
        write_file(
            &tests_root.join("alpha/baseline/noSCF/xmu.dat"),
            "xmu output",
        )
        .expect("write xmu.dat");
        write_file(
            &tests_root.join("alpha/baseline/noSCF/chi.dat"),
            "chi output",
        )
        .expect("write chi.dat");
        write_file(
            &tests_root.join("alpha/baseline/noSCF/f85e.log"),
            "legacy log",
        )
        .expect("write f85e.log");
        write_file(
            &tests_root.join("alpha/baseline/noSCF/feff0001.dat"),
            "path data",
        )
        .expect("write feff dat");

        let summary =
            generate_noscf_manifests_for_mode(&tests_root, &output_root, "v1", RunMode::Legacy)
                .expect("legacy mode should accept expected output names");
        assert_eq!(summary.case_count, 1);
    }
}
