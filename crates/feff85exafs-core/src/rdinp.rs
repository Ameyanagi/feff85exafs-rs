use std::fs;
use std::path::Path;

use crate::domain::{ParsedCard, ParsedInputCards};
use feff85exafs_errors::{FeffError, Result, ValidationErrors};

const SUPPORTED_EDGES: &[&str] = &["K", "L1", "L2", "L3"];

#[derive(Debug, Clone, Copy, Default)]
enum Section {
    #[default]
    None,
    Potentials,
    Atoms,
}

#[derive(Debug, Default)]
struct CardCounters {
    title: usize,
    edge: usize,
    s02: usize,
    control: usize,
    print: usize,
    exchange: usize,
    scf: usize,
    rpath: usize,
    exafs: usize,
    polarization: usize,
    ellipticity: usize,
    potentials: usize,
    atoms: usize,
    end: usize,
}

#[derive(Debug, Default)]
struct ParseState {
    section: Section,
    counters: CardCounters,
    potential_rows: usize,
    atom_rows: usize,
    cards: Vec<ParsedCard>,
}

/// Parse an RDINP-style `feff.inp` file from disk into typed cards.
pub fn parse_rdinp(path: impl AsRef<Path>) -> Result<ParsedInputCards> {
    let path = path.as_ref();
    let raw = fs::read_to_string(path)?;
    let source = path.to_string_lossy().replace('\\', "/");
    parse_rdinp_str(&source, &raw)
}

/// Parse RDINP-style `feff.inp` content from an in-memory string.
pub fn parse_rdinp_str(source: &str, raw: &str) -> Result<ParsedInputCards> {
    let mut state = ParseState::default();

    for (line_index, raw_line) in raw.lines().enumerate() {
        let line_number = line_index + 1;
        let trimmed_start = raw_line.trim_start();
        if trimmed_start.is_empty() || trimmed_start.starts_with('*') {
            continue;
        }

        let uncommented = strip_inline_comment(raw_line).trim();
        if uncommented.is_empty() {
            continue;
        }

        let values = uncommented.split_whitespace().collect::<Vec<_>>();
        if values.is_empty() {
            continue;
        }

        let keyword = values[0].to_ascii_uppercase();
        if is_card_keyword(&keyword) {
            parse_card_line(line_number, &keyword, &values[1..], &mut state)?;
        } else {
            parse_section_row(line_number, &values, &mut state)?;
        }
    }

    validate_required_cards(&state)?;

    let parsed = ParsedInputCards {
        source: source.to_string(),
        cards: state.cards,
    };
    parsed.validate()?;
    Ok(parsed)
}

fn parse_card_line(
    line_number: usize,
    keyword: &str,
    values: &[&str],
    state: &mut ParseState,
) -> Result<()> {
    match keyword {
        "TITLE" => {
            if values.is_empty() {
                return Err(validation_error(
                    format!("line[{line_number}].values"),
                    "TITLE requires at least one value",
                ));
            }
            state.counters.title += 1;
            state.section = Section::None;
            push_card(keyword, values, state);
        }
        "EDGE" => {
            expect_exact_values(keyword, line_number, values, 1)?;
            let edge = values[0].to_ascii_uppercase();
            if !SUPPORTED_EDGES.contains(&edge.as_str()) {
                return Err(validation_error(
                    format!("line[{line_number}].values[0]"),
                    format!(
                        "unsupported EDGE value `{edge}` (supported: {})",
                        SUPPORTED_EDGES.join(", ")
                    ),
                ));
            }
            state.counters.edge += 1;
            state.section = Section::None;
            push_card(keyword, &[edge.as_str()], state);
        }
        "S02" => {
            expect_exact_values(keyword, line_number, values, 1)?;
            let s02 = parse_f64(values[0], line_number, 0)?;
            if s02 < 0.0 {
                return Err(validation_error(
                    format!("line[{line_number}].values[0]"),
                    "S02 must be >= 0",
                ));
            }
            state.counters.s02 += 1;
            state.section = Section::None;
            push_card(keyword, values, state);
        }
        "CONTROL" => {
            expect_exact_values(keyword, line_number, values, 6)?;
            for (index, value) in values.iter().enumerate() {
                let flag = parse_i64(value, line_number, index)?;
                if !(0..=1).contains(&flag) {
                    return Err(validation_error(
                        format!("line[{line_number}].values[{index}]"),
                        "CONTROL values must be 0 or 1",
                    ));
                }
            }
            state.counters.control += 1;
            state.section = Section::None;
            push_card(keyword, values, state);
        }
        "PRINT" => {
            expect_exact_values(keyword, line_number, values, 6)?;
            for (index, value) in values.iter().enumerate() {
                let flag = parse_i64(value, line_number, index)?;
                if flag < 0 {
                    return Err(validation_error(
                        format!("line[{line_number}].values[{index}]"),
                        "PRINT values must be >= 0",
                    ));
                }
            }
            state.counters.print += 1;
            state.section = Section::None;
            push_card(keyword, values, state);
        }
        "EXCHANGE" => {
            expect_values_in_range(keyword, line_number, values, 1, 3)?;
            let ixc = parse_i64(values[0], line_number, 0)?;
            if ixc < 0 {
                return Err(validation_error(
                    format!("line[{line_number}].values[0]"),
                    "EXCHANGE ixc must be >= 0",
                ));
            }
            for (index, value) in values.iter().enumerate().skip(1) {
                let _ = parse_f64(value, line_number, index)?;
            }
            state.counters.exchange += 1;
            state.section = Section::None;
            push_card(keyword, values, state);
        }
        "SCF" => {
            expect_values_in_range(keyword, line_number, values, 1, 4)?;
            let radius = parse_f64(values[0], line_number, 0)?;
            if radius <= 0.0 {
                return Err(validation_error(
                    format!("line[{line_number}].values[0]"),
                    "SCF radius must be > 0",
                ));
            }
            for (index, value) in values.iter().enumerate().skip(1) {
                let _ = parse_f64(value, line_number, index)?;
            }
            state.counters.scf += 1;
            state.section = Section::None;
            push_card(keyword, values, state);
        }
        "RPATH" => {
            expect_exact_values(keyword, line_number, values, 1)?;
            let radius = parse_f64(values[0], line_number, 0)?;
            if radius <= 0.0 {
                return Err(validation_error(
                    format!("line[{line_number}].values[0]"),
                    "RPATH must be > 0",
                ));
            }
            state.counters.rpath += 1;
            state.section = Section::None;
            push_card(keyword, values, state);
        }
        "EXAFS" => {
            expect_exact_values(keyword, line_number, values, 1)?;
            let kmax = parse_i64(values[0], line_number, 0)?;
            if kmax <= 0 {
                return Err(validation_error(
                    format!("line[{line_number}].values[0]"),
                    "EXAFS value must be > 0",
                ));
            }
            state.counters.exafs += 1;
            state.section = Section::None;
            push_card(keyword, values, state);
        }
        "POLARIZATION" => {
            expect_exact_values(keyword, line_number, values, 3)?;
            for (index, value) in values.iter().enumerate() {
                let _ = parse_f64(value, line_number, index)?;
            }
            state.counters.polarization += 1;
            state.section = Section::None;
            push_card(keyword, values, state);
        }
        "ELLIPTICITY" => {
            expect_exact_values(keyword, line_number, values, 4)?;
            for (index, value) in values.iter().enumerate() {
                let _ = parse_f64(value, line_number, index)?;
            }
            state.counters.ellipticity += 1;
            state.section = Section::None;
            push_card(keyword, values, state);
        }
        "POTENTIALS" => {
            if !values.is_empty() {
                return Err(validation_error(
                    format!("line[{line_number}].values"),
                    "POTENTIALS does not accept inline values",
                ));
            }
            state.counters.potentials += 1;
            state.section = Section::Potentials;
            push_card(keyword, values, state);
        }
        "ATOMS" => {
            if !values.is_empty() {
                return Err(validation_error(
                    format!("line[{line_number}].values"),
                    "ATOMS does not accept inline values",
                ));
            }
            state.counters.atoms += 1;
            state.section = Section::Atoms;
            push_card(keyword, values, state);
        }
        "END" => {
            if !values.is_empty() {
                return Err(validation_error(
                    format!("line[{line_number}].values"),
                    "END does not accept inline values",
                ));
            }
            state.counters.end += 1;
            state.section = Section::None;
            push_card(keyword, values, state);
        }
        _ => {
            return Err(validation_error(
                format!("line[{line_number}].keyword"),
                format!("unsupported card `{keyword}`"),
            ));
        }
    }

    Ok(())
}

fn parse_section_row(line_number: usize, values: &[&str], state: &mut ParseState) -> Result<()> {
    match state.section {
        Section::Potentials => {
            parse_potential_row(line_number, values)?;
            state.potential_rows += 1;
            push_card("POTENTIAL", values, state);
            Ok(())
        }
        Section::Atoms => {
            parse_atom_row(line_number, values)?;
            state.atom_rows += 1;
            push_card("ATOM", values, state);
            Ok(())
        }
        Section::None => Err(validation_error(
            format!("line[{line_number}]"),
            "data row encountered outside a section card",
        )),
    }
}

fn parse_potential_row(line_number: usize, values: &[&str]) -> Result<()> {
    if values.len() < 3 {
        return Err(validation_error(
            format!("line[{line_number}]"),
            "POTENTIAL row must include at least `ipot`, `Z`, and element symbol",
        ));
    }

    let ipot = parse_i64(values[0], line_number, 0)?;
    if ipot < 0 {
        return Err(validation_error(
            format!("line[{line_number}].values[0]"),
            "potential `ipot` must be >= 0",
        ));
    }

    let z = parse_i64(values[1], line_number, 1)?;
    if z <= 0 {
        return Err(validation_error(
            format!("line[{line_number}].values[1]"),
            "potential atomic number `Z` must be > 0",
        ));
    }

    let element = values[2];
    if !is_valid_element_symbol(element) {
        return Err(validation_error(
            format!("line[{line_number}].values[2]"),
            "element symbol must be alphabetic",
        ));
    }

    for (index, value) in values.iter().enumerate().skip(3) {
        let _ = parse_f64(value, line_number, index)?;
    }

    Ok(())
}

fn parse_atom_row(line_number: usize, values: &[&str]) -> Result<()> {
    if values.len() < 4 {
        return Err(validation_error(
            format!("line[{line_number}]"),
            "ATOM row must include at least `x`, `y`, `z`, and `ipot`",
        ));
    }

    for (index, value) in values.iter().enumerate().take(3) {
        let _ = parse_f64(value, line_number, index)?;
    }
    let ipot = parse_i64(values[3], line_number, 3)?;
    if ipot < 0 {
        return Err(validation_error(
            format!("line[{line_number}].values[3]"),
            "atom `ipot` must be >= 0",
        ));
    }
    Ok(())
}

fn validate_required_cards(state: &ParseState) -> Result<()> {
    let counters = &state.counters;

    require_card_present(counters.title, "TITLE")?;
    require_singleton(counters.edge, "EDGE")?;
    require_singleton(counters.s02, "S02")?;
    require_singleton(counters.control, "CONTROL")?;
    require_singleton(counters.print, "PRINT")?;
    require_singleton(counters.exchange, "EXCHANGE")?;
    require_singleton(counters.rpath, "RPATH")?;
    require_singleton(counters.exafs, "EXAFS")?;
    require_singleton(counters.potentials, "POTENTIALS")?;
    require_singleton(counters.atoms, "ATOMS")?;
    require_max_one(counters.scf, "SCF")?;
    require_max_one(counters.polarization, "POLARIZATION")?;
    require_max_one(counters.ellipticity, "ELLIPTICITY")?;
    require_max_one(counters.end, "END")?;

    if counters.ellipticity > 0 && counters.polarization == 0 {
        return Err(validation_error(
            "card.ELLIPTICITY",
            "ELLIPTICITY requires POLARIZATION",
        ));
    }

    if state.potential_rows == 0 {
        return Err(validation_error(
            "card.POTENTIALS",
            "POTENTIALS section must include at least one potential row",
        ));
    }

    if state.atom_rows == 0 {
        return Err(validation_error(
            "card.ATOMS",
            "ATOMS section must include at least one atom row",
        ));
    }

    Ok(())
}

fn require_card_present(count: usize, keyword: &str) -> Result<()> {
    if count == 0 {
        return Err(validation_error(
            format!("card.{keyword}"),
            format!("missing required card `{keyword}`"),
        ));
    }
    Ok(())
}

fn require_singleton(count: usize, keyword: &str) -> Result<()> {
    if count == 0 {
        return Err(validation_error(
            format!("card.{keyword}"),
            format!("missing required card `{keyword}`"),
        ));
    }
    if count > 1 {
        return Err(validation_error(
            format!("card.{keyword}"),
            format!("card `{keyword}` must appear at most once"),
        ));
    }
    Ok(())
}

fn require_max_one(count: usize, keyword: &str) -> Result<()> {
    if count > 1 {
        return Err(validation_error(
            format!("card.{keyword}"),
            format!("card `{keyword}` must appear at most once"),
        ));
    }
    Ok(())
}

fn expect_exact_values(
    keyword: &str,
    line_number: usize,
    values: &[&str],
    expected: usize,
) -> Result<()> {
    if values.len() != expected {
        return Err(validation_error(
            format!("line[{line_number}].values"),
            format!(
                "{keyword} requires exactly {expected} value(s), found {}",
                values.len()
            ),
        ));
    }
    Ok(())
}

fn expect_values_in_range(
    keyword: &str,
    line_number: usize,
    values: &[&str],
    min: usize,
    max: usize,
) -> Result<()> {
    if values.len() < min || values.len() > max {
        return Err(validation_error(
            format!("line[{line_number}].values"),
            format!(
                "{keyword} requires between {min} and {max} value(s), found {}",
                values.len()
            ),
        ));
    }
    Ok(())
}

fn parse_i64(value: &str, line_number: usize, value_index: usize) -> Result<i64> {
    value.parse::<i64>().map_err(|_| {
        validation_error(
            format!("line[{line_number}].values[{value_index}]"),
            format!("invalid integer value `{value}`"),
        )
    })
}

fn parse_f64(value: &str, line_number: usize, value_index: usize) -> Result<f64> {
    value.parse::<f64>().map_err(|_| {
        validation_error(
            format!("line[{line_number}].values[{value_index}]"),
            format!("invalid numeric value `{value}`"),
        )
    })
}

fn strip_inline_comment(line: &str) -> &str {
    match line.find('*') {
        Some(index) => &line[..index],
        None => line,
    }
}

fn is_card_keyword(token: &str) -> bool {
    token
        .chars()
        .next()
        .map(|ch| ch.is_ascii_alphabetic())
        .unwrap_or(false)
}

fn is_valid_element_symbol(value: &str) -> bool {
    !value.is_empty() && value.chars().all(|ch| ch.is_ascii_alphabetic())
}

fn push_card(keyword: &str, values: &[&str], state: &mut ParseState) {
    state.cards.push(ParsedCard {
        keyword: keyword.to_string(),
        values: values.iter().map(|value| (*value).to_string()).collect(),
    });
}

fn validation_error(field: impl Into<String>, message: impl Into<String>) -> FeffError {
    let mut errors = ValidationErrors::new();
    errors.push(field, message);
    FeffError::Validation(errors)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

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

    #[test]
    fn parses_phase1_corpus_feff_inputs() {
        let inputs = core_corpus_feff_inputs();
        assert_eq!(inputs.len(), 16, "expected 16 baseline feff.inp fixtures");

        for input in inputs {
            let parsed = parse_rdinp(&input).unwrap_or_else(|error| {
                panic!("failed to parse {}: {error}", input.display());
            });
            assert!(parsed.cards.iter().any(|card| card.keyword == "POTENTIALS"));
            assert!(parsed.cards.iter().any(|card| card.keyword == "ATOMS"));
            assert!(parsed.cards.iter().any(|card| card.keyword == "POTENTIAL"));
            assert!(parsed.cards.iter().any(|card| card.keyword == "ATOM"));
            assert!(
                parsed
                    .cards
                    .iter()
                    .filter(|card| card.keyword == "TITLE")
                    .count()
                    >= 1
            );
        }
    }

    #[test]
    fn rejects_unsupported_card_keyword() {
        let input = format!("{}FOOBAR 1\n", minimal_valid_input());
        let err = parse_rdinp_str("inline", &input).expect_err("unknown card should fail");
        match err {
            FeffError::Validation(validation) => {
                assert_eq!(validation.len(), 1);
                assert!(validation.issues()[0].field.starts_with("line["));
                assert!(
                    validation.issues()[0]
                        .message
                        .contains("unsupported card `FOOBAR`")
                );
            }
            other => panic!("unexpected error type: {other}"),
        }
    }

    #[test]
    fn rejects_unsupported_edge_value() {
        let input = minimal_valid_input().replace("EDGE K", "EDGE M4");
        let err = parse_rdinp_str("inline", &input).expect_err("unsupported edge should fail");
        match err {
            FeffError::Validation(validation) => {
                assert_eq!(validation.len(), 1);
                assert_eq!(validation.issues()[0].field, "line[2].values[0]");
            }
            other => panic!("unexpected error type: {other}"),
        }
    }

    #[test]
    fn rejects_malformed_numeric_value() {
        let input = minimal_valid_input().replace("RPATH 4.0", "RPATH nope");
        let err = parse_rdinp_str("inline", &input).expect_err("malformed RPATH should fail");
        match err {
            FeffError::Validation(validation) => {
                assert_eq!(validation.len(), 1);
                assert_eq!(validation.issues()[0].field, "line[7].values[0]");
                assert!(
                    validation.issues()[0]
                        .message
                        .contains("invalid numeric value")
                );
            }
            other => panic!("unexpected error type: {other}"),
        }
    }

    #[test]
    fn rejects_missing_required_cards() {
        let input = "TITLE Example\nEDGE K\nS02 0\n";
        let err = parse_rdinp_str("inline", input).expect_err("missing cards should fail");
        match err {
            FeffError::Validation(validation) => {
                assert_eq!(validation.len(), 1);
                assert_eq!(validation.issues()[0].field, "card.CONTROL");
            }
            other => panic!("unexpected error type: {other}"),
        }
    }

    #[test]
    fn rejects_ellipticity_without_polarization() {
        let input = format!(
            "{}ELLIPTICITY 1 0 0 1\n",
            minimal_valid_input().replace("END\n", "")
        );
        let err = parse_rdinp_str("inline", &input)
            .expect_err("ellipticity without polarization should fail");
        match err {
            FeffError::Validation(validation) => {
                assert_eq!(validation.len(), 1);
                assert_eq!(validation.issues()[0].field, "card.ELLIPTICITY");
            }
            other => panic!("unexpected error type: {other}"),
        }
    }
}
