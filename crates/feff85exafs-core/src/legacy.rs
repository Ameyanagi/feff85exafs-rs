use crate::domain::PipelineStage;

pub const LEGACY_STAGE_ORDER: [PipelineStage; 6] = [
    PipelineStage::Rdinp,
    PipelineStage::Pot,
    PipelineStage::Xsph,
    PipelineStage::Pathfinder,
    PipelineStage::Genfmt,
    PipelineStage::Ff2x,
];

const LEGACY_REQUIRED_FILES: [&str; 6] = [
    "feff.inp",
    "files.dat",
    "paths.dat",
    "xmu.dat",
    "chi.dat",
    "f85e.log",
];

pub fn legacy_stage_order() -> &'static [PipelineStage] {
    &LEGACY_STAGE_ORDER
}

pub fn legacy_stage_name(stage: PipelineStage) -> &'static str {
    match stage {
        PipelineStage::Rdinp => "rdinp",
        PipelineStage::Pot => "pot",
        PipelineStage::Xsph => "xsph",
        PipelineStage::Pathfinder => "pathfinder",
        PipelineStage::Genfmt => "genfmt",
        PipelineStage::Ff2x => "ff2x",
        PipelineStage::Debye => "debye",
        PipelineStage::Exch => "exch",
        PipelineStage::Fovrg => "fovrg",
    }
}

pub fn validate_legacy_baseline_file_names(file_names: &[String]) -> Result<(), String> {
    let mut missing = Vec::new();
    for required_name in LEGACY_REQUIRED_FILES {
        if !file_names.iter().any(|name| name == required_name) {
            missing.push(required_name.to_string());
        }
    }

    if !file_names.iter().any(|name| is_legacy_feff_path_file(name)) {
        missing.push("feffNNNN.dat".to_string());
    }

    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "missing legacy baseline outputs: {}",
            missing.join(", ")
        ))
    }
}

fn is_legacy_feff_path_file(path: &str) -> bool {
    let Some(file_name) = std::path::Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
    else {
        return false;
    };

    file_name.len() == 12
        && file_name.starts_with("feff")
        && file_name.ends_with(".dat")
        && file_name[4..8].chars().all(|ch| ch.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_stage_order_matches_historical_sequence() {
        assert_eq!(
            legacy_stage_order(),
            &[
                PipelineStage::Rdinp,
                PipelineStage::Pot,
                PipelineStage::Xsph,
                PipelineStage::Pathfinder,
                PipelineStage::Genfmt,
                PipelineStage::Ff2x,
            ]
        );
    }

    #[test]
    fn legacy_baseline_file_validation_requires_core_legacy_names() {
        let err = validate_legacy_baseline_file_names(&["feff.inp".to_string()])
            .expect_err("missing required files should fail legacy validation");
        assert!(err.contains("f85e.log"));
        assert!(err.contains("feffNNNN.dat"));
    }

    #[test]
    fn legacy_baseline_file_validation_accepts_expected_names() {
        let names = vec![
            "feff.inp".to_string(),
            "files.dat".to_string(),
            "paths.dat".to_string(),
            "xmu.dat".to_string(),
            "chi.dat".to_string(),
            "f85e.log".to_string(),
            "feff0001.dat".to_string(),
        ];
        validate_legacy_baseline_file_names(&names)
            .expect("valid legacy names should pass validation");
    }
}
