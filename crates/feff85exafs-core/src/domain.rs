use std::collections::BTreeSet;

use feff85exafs_errors::{FeffError, Result, ValidationErrors};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunMode {
    Legacy,
    Modern,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScfStrategy {
    NoScf,
    WithScf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunConfig {
    pub material: String,
    pub mode: RunMode,
    pub scf_strategy: ScfStrategy,
    pub working_directory: String,
    #[serde(default)]
    pub keep_intermediates: bool,
}

impl RunConfig {
    pub fn validate(&self) -> Result<()> {
        let mut errors = ValidationErrors::new();
        validate_non_empty(&self.material, "material", &mut errors);
        validate_non_empty(&self.working_directory, "working_directory", &mut errors);
        finish_validation(errors)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParsedInputCards {
    pub source: String,
    pub cards: Vec<ParsedCard>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParsedCard {
    pub keyword: String,
    #[serde(default)]
    pub values: Vec<String>,
}

impl ParsedInputCards {
    pub fn validate(&self) -> Result<()> {
        let mut errors = ValidationErrors::new();
        validate_non_empty(&self.source, "source", &mut errors);

        if self.cards.is_empty() {
            errors.push("cards", "must include at least one parsed card");
        }

        for (card_index, card) in self.cards.iter().enumerate() {
            let keyword_field = format!("cards[{card_index}].keyword");
            if !is_valid_card_keyword(&card.keyword) {
                errors.push(
                    keyword_field,
                    "must start with a letter and contain only A-Z, 0-9, and '_'",
                );
            }

            for (value_index, value) in card.values.iter().enumerate() {
                if value.trim().is_empty() {
                    errors.push(
                        format!("cards[{card_index}].values[{value_index}]"),
                        "must not be blank",
                    );
                }
            }
        }

        finish_validation(errors)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputMetadata {
    pub run_id: String,
    pub mode: RunMode,
    pub stages: Vec<StageOutput>,
    #[serde(default)]
    pub produced_files: Vec<String>,
    pub completed_at_utc: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum PipelineStage {
    Rdinp,
    Pot,
    Xsph,
    Pathfinder,
    Genfmt,
    Ff2x,
    Debye,
    Exch,
    Fovrg,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StageOutput {
    pub stage: PipelineStage,
    pub output_directory: String,
    pub artifact_count: usize,
}

impl OutputMetadata {
    pub fn validate(&self) -> Result<()> {
        let mut errors = ValidationErrors::new();
        validate_non_empty(&self.run_id, "run_id", &mut errors);
        validate_non_empty(&self.completed_at_utc, "completed_at_utc", &mut errors);
        if !self.completed_at_utc.contains('T') {
            errors.push(
                "completed_at_utc",
                "must be an RFC3339-like UTC timestamp containing 'T'",
            );
        }

        if self.stages.is_empty() {
            errors.push("stages", "must include metadata for at least one stage");
        }

        let mut seen_stages = BTreeSet::new();
        for (stage_index, stage) in self.stages.iter().enumerate() {
            if !seen_stages.insert(stage.stage) {
                errors.push(
                    format!("stages[{stage_index}].stage"),
                    "must not contain duplicate stage entries",
                );
            }

            validate_non_empty(
                &stage.output_directory,
                format!("stages[{stage_index}].output_directory").as_str(),
                &mut errors,
            );

            if stage.artifact_count == 0 {
                errors.push(
                    format!("stages[{stage_index}].artifact_count"),
                    "must be greater than 0",
                );
            }
        }

        for (path_index, path) in self.produced_files.iter().enumerate() {
            if path.trim().is_empty() {
                errors.push(format!("produced_files[{path_index}]"), "must not be blank");
            }
        }

        finish_validation(errors)
    }
}

fn finish_validation(errors: ValidationErrors) -> Result<()> {
    if errors.is_empty() {
        Ok(())
    } else {
        Err(FeffError::Validation(errors))
    }
}

fn validate_non_empty(value: &str, field: &str, errors: &mut ValidationErrors) {
    if value.trim().is_empty() {
        errors.push(field, "must not be blank");
    }
}

fn is_valid_card_keyword(keyword: &str) -> bool {
    if keyword.is_empty() {
        return false;
    }

    let mut chars = keyword.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() => {}
        _ => return false,
    }

    keyword
        .chars()
        .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_run_config() -> RunConfig {
        RunConfig {
            material: "Copper".to_string(),
            mode: RunMode::Modern,
            scf_strategy: ScfStrategy::NoScf,
            working_directory: "runs/copper".to_string(),
            keep_intermediates: true,
        }
    }

    fn sample_cards() -> ParsedInputCards {
        ParsedInputCards {
            source: "materials/copper/feff.inp".to_string(),
            cards: vec![
                ParsedCard {
                    keyword: "TITLE".to_string(),
                    values: vec!["Copper example".to_string()],
                },
                ParsedCard {
                    keyword: "SCF".to_string(),
                    values: vec!["7.0".to_string(), "0".to_string()],
                },
            ],
        }
    }

    fn sample_output_metadata() -> OutputMetadata {
        OutputMetadata {
            run_id: "run-20260216-0001".to_string(),
            mode: RunMode::Modern,
            stages: vec![
                StageOutput {
                    stage: PipelineStage::Rdinp,
                    output_directory: "runs/copper/rdinp".to_string(),
                    artifact_count: 2,
                },
                StageOutput {
                    stage: PipelineStage::Pot,
                    output_directory: "runs/copper/pot".to_string(),
                    artifact_count: 3,
                },
            ],
            produced_files: vec![
                "runs/copper/feff.inp".to_string(),
                "runs/copper/xmu.dat".to_string(),
            ],
            completed_at_utc: "2026-02-16T04:30:00Z".to_string(),
        }
    }

    #[test]
    fn run_config_validation_accepts_valid_config() {
        let config = sample_run_config();
        config.validate().expect("run config should be valid");
    }

    #[test]
    fn run_config_validation_rejects_blank_fields() {
        let mut config = sample_run_config();
        config.material = "   ".to_string();
        config.working_directory = "".to_string();

        let err = config
            .validate()
            .expect_err("run config with blank fields should be invalid");
        match err {
            FeffError::Validation(validation) => {
                assert_eq!(validation.len(), 2);
            }
            other => panic!("unexpected error type: {other}"),
        }
    }

    #[test]
    fn parsed_input_cards_validation_rejects_invalid_content() {
        let cards = ParsedInputCards {
            source: "".to_string(),
            cards: vec![ParsedCard {
                keyword: "bad keyword".to_string(),
                values: vec!["".to_string()],
            }],
        };

        let err = cards
            .validate()
            .expect_err("parsed cards with invalid data should be rejected");
        match err {
            FeffError::Validation(validation) => {
                assert_eq!(validation.len(), 3);
                assert!(
                    validation
                        .issues()
                        .iter()
                        .any(|issue| issue.field == "cards[0].keyword")
                );
            }
            other => panic!("unexpected error type: {other}"),
        }
    }

    #[test]
    fn output_metadata_validation_rejects_duplicate_stages() {
        let mut metadata = sample_output_metadata();
        metadata.completed_at_utc = "2026-02-16".to_string();
        metadata.stages.push(StageOutput {
            stage: PipelineStage::Pot,
            output_directory: "".to_string(),
            artifact_count: 0,
        });
        metadata.produced_files.push(" ".to_string());

        let err = metadata
            .validate()
            .expect_err("output metadata with duplicate stages should be invalid");
        match err {
            FeffError::Validation(validation) => {
                assert_eq!(validation.len(), 5);
                assert!(validation.issues().iter().any(|issue| {
                    issue.field == "stages[2].stage"
                        && issue.message.contains("duplicate stage entries")
                }));
            }
            other => panic!("unexpected error type: {other}"),
        }
    }

    #[test]
    fn domain_models_support_serde_round_trip() {
        let config = sample_run_config();
        let cards = sample_cards();
        let metadata = sample_output_metadata();

        let config_json = serde_json::to_string(&config).expect("serialize run config");
        let cards_json = serde_json::to_string(&cards).expect("serialize parsed cards");
        let metadata_json = serde_json::to_string(&metadata).expect("serialize output metadata");

        let config_round_trip: RunConfig =
            serde_json::from_str(&config_json).expect("deserialize run config");
        let cards_round_trip: ParsedInputCards =
            serde_json::from_str(&cards_json).expect("deserialize parsed cards");
        let metadata_round_trip: OutputMetadata =
            serde_json::from_str(&metadata_json).expect("deserialize output metadata");

        assert_eq!(config, config_round_trip);
        assert_eq!(cards, cards_round_trip);
        assert_eq!(metadata, metadata_round_trip);
    }
}
