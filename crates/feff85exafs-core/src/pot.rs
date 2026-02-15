use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use crate::domain::ParsedInputCards;
use feff85exafs_errors::{FeffError, Result, ValidationErrors};
use serde::{Deserialize, Serialize};

const DEFAULT_VFEFF: &str = "Feff 8.50 for exafs";
const DEFAULT_VF85E: &str = "0.1";
const POT_ARTIFACT_SCHEMA_VERSION: u32 = 1;
const EDGE_LABELS: [&str; 30] = [
    "NO", "K", "L1", "L2", "L3", "M1", "M2", "M3", "M4", "M5", "N1", "N2", "N3", "N4", "N5", "N6",
    "N7", "O1", "O2", "O3", "O4", "O5", "O6", "O7", "P1", "P2", "P3", "P4", "P5", "R1",
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PotAtomData {
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub potential_index: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PotPotentialData {
    pub potential_index: i32,
    pub atomic_number: i32,
    pub label: String,
    pub lmaxsc: i32,
    pub lmaxph: i32,
    pub stoichiometry: f64,
    pub spin: f64,
    pub overlap_fraction: f64,
    pub ionization: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PotInputData {
    pub vfeff: String,
    pub vf85e: String,
    pub titles: Vec<String>,
    pub atoms: Vec<PotAtomData>,
    pub potentials: Vec<PotPotentialData>,
    pub ihole: i32,
    pub rfms1: f64,
    pub lfms1: i32,
    pub nscmt: i32,
    pub ca1: f64,
    pub nmix: i32,
    pub ecv: f64,
    pub icoul: i32,
    pub ipol: i32,
    pub evec: [f64; 3],
    pub elpty: f64,
    pub xivec: [f64; 3],
    pub ispin: i32,
    pub spvec: [f64; 3],
    pub angks: f64,
    pub ptz0: [f64; 6],
    pub ptz1: [f64; 6],
    pub ptz2: [f64; 6],
    pub gamach: f64,
    pub ixc: i32,
    pub vr0: f64,
    pub vi0: f64,
    pub ixc0: i32,
    pub iafolp: i32,
    pub rgrd: f64,
    pub iunf: i32,
    pub inters: i32,
    pub totvol: f64,
    pub jumprm: i32,
    pub nohole: i32,
    pub iplsmn: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PotOutputData {
    pub working_directory: String,
    pub pot_pad: String,
    #[serde(default)]
    pub potential_data_files: Vec<String>,
    pub misc_dat: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct LegacyPotInputFile {
    vfeff: String,
    vf85e: String,
    ntitle: usize,
    titles: Vec<String>,
    natt: usize,
    x: Vec<f64>,
    y: Vec<f64>,
    z: Vec<f64>,
    iphatx: Vec<i32>,
    nph: usize,
    iz: Vec<i32>,
    potlbl: Vec<String>,
    lmaxsc: Vec<i32>,
    lmaxph: Vec<i32>,
    xnatph: Vec<f64>,
    spinph: Vec<f64>,
    ihole: i32,
    rfms1: f64,
    lfms1: i32,
    nscmt: i32,
    ca1: f64,
    nmix: i32,
    ecv: f64,
    icoul: i32,
    ipol: i32,
    evec: [f64; 3],
    elpty: f64,
    xivec: [f64; 3],
    ispin: i32,
    spvec: [f64; 3],
    angks: f64,
    ptz0: [f64; 6],
    ptz1: [f64; 6],
    ptz2: [f64; 6],
    gamach: f64,
    ixc: i32,
    #[serde(rename = "vro", alias = "vr0")]
    vr0: f64,
    #[serde(rename = "vio", alias = "vi0")]
    vi0: f64,
    ixc0: i32,
    iafolp: i32,
    folp: Vec<f64>,
    xion: Vec<f64>,
    rgrd: f64,
    iunf: i32,
    inters: i32,
    totvol: f64,
    jumprm: i32,
    nohole: i32,
    #[serde(default)]
    iplsmn: i32,
}

impl PotInputData {
    pub fn validate(&self) -> Result<()> {
        let mut errors = ValidationErrors::new();

        if self.vfeff.trim().is_empty() {
            errors.push("vfeff", "must not be blank");
        }
        if self.vf85e.trim().is_empty() {
            errors.push("vf85e", "must not be blank");
        }

        if self.titles.is_empty() {
            errors.push("titles", "must include at least one title");
        }
        for (index, title) in self.titles.iter().enumerate() {
            if title.trim().is_empty() {
                errors.push(format!("titles[{index}]"), "must not be blank");
            }
        }

        if self.atoms.is_empty() {
            errors.push("atoms", "must include at least one atom");
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

        for (index, atom) in self.atoms.iter().enumerate() {
            if atom.potential_index < 0 {
                errors.push(format!("atoms[{index}].potential_index"), "must be >= 0");
            } else if !potential_indices.contains(&atom.potential_index) {
                errors.push(
                    format!("atoms[{index}].potential_index"),
                    "must reference a defined potential index",
                );
            }
        }

        if !matches!(self.ipol, 0 | 1) {
            errors.push("ipol", "must be 0 or 1");
        }
        if !matches!(self.ispin, 0 | 1) {
            errors.push("ispin", "must be 0 or 1");
        }

        finish_validation(errors)
    }

    pub fn from_parsed_cards(parsed: &ParsedInputCards) -> Result<Self> {
        parsed.validate()?;

        let mut input = Self::defaults();

        let mut titles = Vec::new();
        let mut edge_value = None;
        let mut exchange_values = None;
        let mut scf_values = None;

        for (card_index, card) in parsed.cards.iter().enumerate() {
            match card.keyword.as_str() {
                "TITLE" => {
                    titles.push(card.values.join(" "));
                }
                "EDGE" => {
                    let Some(value) = card.values.first() else {
                        return Err(validation_error(
                            format!("cards[{card_index}].values"),
                            "EDGE card must include one value",
                        ));
                    };
                    edge_value = Some(value.clone());
                }
                "SCF" => {
                    scf_values = Some(card.values.clone());
                }
                "EXCHANGE" => {
                    exchange_values = Some(card.values.clone());
                }
                "POLARIZATION" => {
                    if card.values.len() != 3 {
                        return Err(validation_error(
                            format!("cards[{card_index}].values"),
                            "POLARIZATION must include exactly three values",
                        ));
                    }
                    input.ipol = 1;
                    for (value_index, value) in card.values.iter().enumerate() {
                        input.evec[value_index] = parse_f64_value(
                            value,
                            format!("cards[{card_index}].values[{value_index}]"),
                        )?;
                    }
                }
                "ELLIPTICITY" => {
                    if card.values.len() != 4 {
                        return Err(validation_error(
                            format!("cards[{card_index}].values"),
                            "ELLIPTICITY must include exactly four values",
                        ));
                    }
                    input.elpty =
                        parse_f64_value(&card.values[0], format!("cards[{card_index}].values[0]"))?;
                    for (value_index, value) in card.values.iter().enumerate().skip(1) {
                        input.xivec[value_index - 1] = parse_f64_value(
                            value,
                            format!("cards[{card_index}].values[{value_index}]"),
                        )?;
                    }
                }
                "POTENTIAL" => {
                    if card.values.len() < 3 {
                        return Err(validation_error(
                            format!("cards[{card_index}].values"),
                            "POTENTIAL requires at least three values",
                        ));
                    }

                    let ipot =
                        parse_i32_value(&card.values[0], format!("cards[{card_index}].values[0]"))?;
                    let z =
                        parse_i32_value(&card.values[1], format!("cards[{card_index}].values[1]"))?;
                    let lmaxsc = card
                        .values
                        .get(3)
                        .map(|value| {
                            parse_i32_value(value, format!("cards[{card_index}].values[3]"))
                        })
                        .transpose()?
                        .unwrap_or(0);
                    let lmaxph = card
                        .values
                        .get(4)
                        .map(|value| {
                            parse_i32_value(value, format!("cards[{card_index}].values[4]"))
                        })
                        .transpose()?
                        .unwrap_or(0);
                    let xnatph = card
                        .values
                        .get(5)
                        .map(|value| {
                            parse_f64_value(value, format!("cards[{card_index}].values[5]"))
                        })
                        .transpose()?
                        .unwrap_or(0.0);
                    let spin = card
                        .values
                        .get(6)
                        .map(|value| {
                            parse_f64_value(value, format!("cards[{card_index}].values[6]"))
                        })
                        .transpose()?
                        .unwrap_or(0.0);

                    input.potentials.push(PotPotentialData {
                        potential_index: ipot,
                        atomic_number: z,
                        label: card.values[2].clone(),
                        lmaxsc,
                        lmaxph,
                        stoichiometry: xnatph,
                        spin,
                        overlap_fraction: 1.0,
                        ionization: 0.0,
                    });
                }
                "ATOM" => {
                    if card.values.len() < 4 {
                        return Err(validation_error(
                            format!("cards[{card_index}].values"),
                            "ATOM requires at least four values",
                        ));
                    }

                    input.atoms.push(PotAtomData {
                        x: parse_f64_value(
                            &card.values[0],
                            format!("cards[{card_index}].values[0]"),
                        )?,
                        y: parse_f64_value(
                            &card.values[1],
                            format!("cards[{card_index}].values[1]"),
                        )?,
                        z: parse_f64_value(
                            &card.values[2],
                            format!("cards[{card_index}].values[2]"),
                        )?,
                        potential_index: parse_i32_value(
                            &card.values[3],
                            format!("cards[{card_index}].values[3]"),
                        )?,
                    });
                }
                _ => {}
            }
        }

        if titles.is_empty() {
            return Err(validation_error(
                "cards.TITLE",
                "at least one TITLE card is required for POT input translation",
            ));
        }
        input.titles = titles;

        let edge = edge_value.ok_or_else(|| {
            validation_error(
                "cards.EDGE",
                "EDGE card is required for POT input translation",
            )
        })?;
        input.ihole = edge_value_to_ihole(&edge).ok_or_else(|| {
            validation_error(
                "cards.EDGE",
                format!("unsupported EDGE value `{edge}` for POT input translation"),
            )
        })?;

        let exchange_values = exchange_values.ok_or_else(|| {
            validation_error(
                "cards.EXCHANGE",
                "EXCHANGE card is required for POT input translation",
            )
        })?;
        input.ixc = parse_i32_value(&exchange_values[0], "cards.EXCHANGE.values[0]")?;
        if let Some(value) = exchange_values.get(1) {
            input.vr0 = parse_f64_value(value, "cards.EXCHANGE.values[1]")?;
        }
        if let Some(value) = exchange_values.get(2) {
            input.vi0 = parse_f64_value(value, "cards.EXCHANGE.values[2]")?;
        }

        if let Some(values) = scf_values {
            if let Some(value) = values.first() {
                input.rfms1 = parse_f64_value(value, "cards.SCF.values[0]")?;
            }
            if let Some(value) = values.get(1) {
                input.lfms1 = parse_i32_value(value, "cards.SCF.values[1]")?;
            }
            if let Some(value) = values.get(2) {
                input.nscmt = parse_i32_value(value, "cards.SCF.values[2]")?;
            }
            if let Some(value) = values.get(3) {
                input.ca1 = parse_f64_value(value, "cards.SCF.values[3]")?;
            }
        }

        input
            .potentials
            .sort_by_key(|potential| potential.potential_index);

        input.validate()?;
        Ok(input)
    }

    fn defaults() -> Self {
        Self {
            vfeff: DEFAULT_VFEFF.to_string(),
            vf85e: DEFAULT_VF85E.to_string(),
            titles: Vec::new(),
            atoms: Vec::new(),
            potentials: Vec::new(),
            ihole: 0,
            rfms1: -1.0,
            lfms1: 0,
            nscmt: 30,
            ca1: 0.0,
            nmix: 0,
            ecv: -40.0,
            icoul: 0,
            ipol: 0,
            evec: [0.0, 0.0, 0.0],
            elpty: 0.0,
            xivec: [0.0, 0.0, 0.0],
            ispin: 0,
            spvec: [0.0, 0.0, 0.0],
            angks: 0.0,
            ptz0: [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            ptz1: [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            ptz2: [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            gamach: 0.0,
            ixc: 0,
            vr0: 0.0,
            vi0: 0.0,
            ixc0: -1,
            iafolp: 0,
            rgrd: 0.05,
            iunf: 0,
            inters: 0,
            totvol: 0.0,
            jumprm: 0,
            nohole: -1,
            iplsmn: 0,
        }
    }
}

impl PotOutputData {
    pub fn validate(&self) -> Result<()> {
        let mut errors = ValidationErrors::new();

        if self.working_directory.trim().is_empty() {
            errors.push("working_directory", "must not be blank");
        }
        if self.pot_pad.trim().is_empty() {
            errors.push("pot_pad", "must not be blank");
        }

        for (index, path) in self.potential_data_files.iter().enumerate() {
            if path.trim().is_empty() {
                errors.push(
                    format!("potential_data_files[{index}]"),
                    "must not be blank",
                );
            }
        }

        if let Some(path) = &self.misc_dat
            && path.trim().is_empty()
        {
            errors.push("misc_dat", "must not be blank when present");
        }

        finish_validation(errors)
    }
}

impl LegacyPotInputFile {
    fn into_typed(self) -> Result<PotInputData> {
        ensure_min_len("titles", self.titles.len(), self.ntitle)?;
        ensure_min_len("x", self.x.len(), self.natt)?;
        ensure_min_len("y", self.y.len(), self.natt)?;
        ensure_min_len("z", self.z.len(), self.natt)?;
        ensure_min_len("iphatx", self.iphatx.len(), self.natt)?;

        let potential_count = self
            .nph
            .checked_add(1)
            .ok_or_else(|| validation_error("nph", "nph is too large"))?;
        ensure_min_len("iz", self.iz.len(), potential_count)?;
        ensure_min_len("potlbl", self.potlbl.len(), potential_count)?;
        ensure_min_len("lmaxsc", self.lmaxsc.len(), potential_count)?;
        ensure_min_len("lmaxph", self.lmaxph.len(), potential_count)?;
        ensure_min_len("xnatph", self.xnatph.len(), potential_count)?;
        ensure_min_len("spinph", self.spinph.len(), potential_count)?;
        ensure_min_len("folp", self.folp.len(), potential_count)?;
        ensure_min_len("xion", self.xion.len(), potential_count)?;

        let mut atoms = Vec::with_capacity(self.natt);
        for index in 0..self.natt {
            atoms.push(PotAtomData {
                x: self.x[index],
                y: self.y[index],
                z: self.z[index],
                potential_index: self.iphatx[index],
            });
        }

        let mut potentials = Vec::with_capacity(potential_count);
        for index in 0..potential_count {
            potentials.push(PotPotentialData {
                potential_index: index as i32,
                atomic_number: self.iz[index],
                label: self.potlbl[index].trim().to_string(),
                lmaxsc: self.lmaxsc[index],
                lmaxph: self.lmaxph[index],
                stoichiometry: self.xnatph[index],
                spin: self.spinph[index],
                overlap_fraction: self.folp[index],
                ionization: self.xion[index],
            });
        }

        let typed = PotInputData {
            vfeff: self.vfeff,
            vf85e: self.vf85e,
            titles: self.titles.into_iter().take(self.ntitle).collect(),
            atoms,
            potentials,
            ihole: self.ihole,
            rfms1: self.rfms1,
            lfms1: self.lfms1,
            nscmt: self.nscmt,
            ca1: self.ca1,
            nmix: self.nmix,
            ecv: self.ecv,
            icoul: self.icoul,
            ipol: self.ipol,
            evec: self.evec,
            elpty: self.elpty,
            xivec: self.xivec,
            ispin: self.ispin,
            spvec: self.spvec,
            angks: self.angks,
            ptz0: self.ptz0,
            ptz1: self.ptz1,
            ptz2: self.ptz2,
            gamach: self.gamach,
            ixc: self.ixc,
            vr0: self.vr0,
            vi0: self.vi0,
            ixc0: self.ixc0,
            iafolp: self.iafolp,
            rgrd: self.rgrd,
            iunf: self.iunf,
            inters: self.inters,
            totvol: self.totvol,
            jumprm: self.jumprm,
            nohole: self.nohole,
            iplsmn: self.iplsmn,
        };
        typed.validate()?;
        Ok(typed)
    }

    fn from_typed(input: &PotInputData) -> Result<Self> {
        input.validate()?;

        let max_index = input
            .potentials
            .iter()
            .map(|potential| potential.potential_index)
            .max()
            .ok_or_else(|| validation_error("potentials", "must include at least one potential"))?;
        let potential_count = usize::try_from(max_index)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| validation_error("potentials", "invalid potential indices"))?;

        let mut iz = vec![0_i32; potential_count];
        let mut potlbl = vec![String::new(); potential_count];
        let mut lmaxsc = vec![0_i32; potential_count];
        let mut lmaxph = vec![0_i32; potential_count];
        let mut xnatph = vec![0.0_f64; potential_count];
        let mut spinph = vec![0.0_f64; potential_count];
        let mut folp = vec![1.0_f64; potential_count];
        let mut xion = vec![0.0_f64; potential_count];

        for potential in &input.potentials {
            let index = usize::try_from(potential.potential_index).map_err(|_| {
                validation_error(
                    "potentials.potential_index",
                    "potential index must be >= 0 for serialization",
                )
            })?;
            iz[index] = potential.atomic_number;
            potlbl[index] = potential.label.clone();
            lmaxsc[index] = potential.lmaxsc;
            lmaxph[index] = potential.lmaxph;
            xnatph[index] = potential.stoichiometry;
            spinph[index] = potential.spin;
            folp[index] = potential.overlap_fraction;
            xion[index] = potential.ionization;
        }

        let mut x = Vec::with_capacity(input.atoms.len());
        let mut y = Vec::with_capacity(input.atoms.len());
        let mut z = Vec::with_capacity(input.atoms.len());
        let mut iphatx = Vec::with_capacity(input.atoms.len());
        for atom in &input.atoms {
            x.push(atom.x);
            y.push(atom.y);
            z.push(atom.z);
            iphatx.push(atom.potential_index);
        }

        Ok(Self {
            vfeff: input.vfeff.clone(),
            vf85e: input.vf85e.clone(),
            ntitle: input.titles.len(),
            titles: input.titles.clone(),
            natt: input.atoms.len(),
            x,
            y,
            z,
            iphatx,
            nph: usize::try_from(max_index)
                .map_err(|_| validation_error("potentials", "invalid max potential index"))?,
            iz,
            potlbl,
            lmaxsc,
            lmaxph,
            xnatph,
            spinph,
            ihole: input.ihole,
            rfms1: input.rfms1,
            lfms1: input.lfms1,
            nscmt: input.nscmt,
            ca1: input.ca1,
            nmix: input.nmix,
            ecv: input.ecv,
            icoul: input.icoul,
            ipol: input.ipol,
            evec: input.evec,
            elpty: input.elpty,
            xivec: input.xivec,
            ispin: input.ispin,
            spvec: input.spvec,
            angks: input.angks,
            ptz0: input.ptz0,
            ptz1: input.ptz1,
            ptz2: input.ptz2,
            gamach: input.gamach,
            ixc: input.ixc,
            vr0: input.vr0,
            vi0: input.vi0,
            ixc0: input.ixc0,
            iafolp: input.iafolp,
            folp,
            xion,
            rgrd: input.rgrd,
            iunf: input.iunf,
            inters: input.inters,
            totvol: input.totvol,
            jumprm: input.jumprm,
            nohole: input.nohole,
            iplsmn: input.iplsmn,
        })
    }
}

pub fn read_pot_input_json(path: impl AsRef<Path>) -> Result<PotInputData> {
    let path = path.as_ref();
    let raw = fs::read_to_string(path)?;
    let parsed: LegacyPotInputFile = serde_json::from_str(&raw).map_err(|error| {
        FeffError::InvalidArgument(format!(
            "failed to parse POT input JSON from `{}`: {error}",
            path_string(path)
        ))
    })?;
    parsed.into_typed()
}

pub fn write_pot_input_json(path: impl AsRef<Path>, input: &PotInputData) -> Result<()> {
    let path = path.as_ref();
    let serialized = LegacyPotInputFile::from_typed(input)?;
    let json = serde_json::to_string_pretty(&serialized).map_err(|error| {
        FeffError::InvalidArgument(format!(
            "failed to serialize POT input JSON for `{}`: {error}",
            path_string(path)
        ))
    })?;
    fs::write(path, format!("{json}\n"))?;
    Ok(())
}

pub fn collect_pot_output_data(working_directory: impl AsRef<Path>) -> Result<PotOutputData> {
    let working_directory = working_directory.as_ref();
    let mut potential_files = Vec::new();

    for entry in fs::read_dir(working_directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let file_name = entry.file_name().to_string_lossy().into_owned();
        if is_potential_data_file(&file_name) {
            potential_files.push(path_string(&entry.path()));
        }
    }

    potential_files.sort();

    let pot_pad_path = working_directory.join("pot.pad");
    if !pot_pad_path.is_file() {
        return Err(validation_error(
            "pot.pad",
            format!(
                "expected POT output file `pot.pad` in `{}`",
                path_string(working_directory)
            ),
        ));
    }

    let misc_path = working_directory.join("misc.dat");
    let output = PotOutputData {
        working_directory: path_string(working_directory),
        pot_pad: path_string(&pot_pad_path),
        potential_data_files: potential_files,
        misc_dat: misc_path.is_file().then(|| path_string(&misc_path)),
    };
    output.validate()?;
    Ok(output)
}

pub fn read_pot_output_json(path: impl AsRef<Path>) -> Result<PotOutputData> {
    let path = path.as_ref();
    let raw = fs::read_to_string(path)?;
    let output = serde_json::from_str::<PotOutputData>(&raw).map_err(|error| {
        FeffError::InvalidArgument(format!(
            "failed to parse POT output JSON from `{}`: {error}",
            path_string(path)
        ))
    })?;
    output.validate()?;
    Ok(output)
}

pub fn write_pot_output_json(path: impl AsRef<Path>, output: &PotOutputData) -> Result<()> {
    output.validate()?;
    let path = path.as_ref();
    let json = serde_json::to_string_pretty(output).map_err(|error| {
        FeffError::InvalidArgument(format!(
            "failed to serialize POT output JSON for `{}`: {error}",
            path_string(path)
        ))
    })?;
    fs::write(path, format!("{json}\n"))?;
    Ok(())
}

/// Run the native Rust POT stage for typed POT input and write
/// deterministic POT artifacts into `working_directory`.
///
/// Artifacts written:
/// - `pot.pad`
/// - `potNN.dat` for each potential index
/// - `misc.dat`
pub fn run_pot(input: &PotInputData, working_directory: impl AsRef<Path>) -> Result<PotOutputData> {
    input.validate()?;
    let working_directory = working_directory.as_ref();
    fs::create_dir_all(working_directory)?;
    clear_existing_pot_artifacts(working_directory)?;

    write_pot_pad_file(working_directory, input)?;
    write_potential_data_files(working_directory, input)?;
    write_misc_file(working_directory, input)?;

    collect_pot_output_data(working_directory)
}

fn clear_existing_pot_artifacts(working_directory: &Path) -> Result<()> {
    for entry in fs::read_dir(working_directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if file_name == "pot.pad" || file_name == "misc.dat" || is_potential_data_file(&file_name) {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn write_pot_pad_file(working_directory: &Path, input: &PotInputData) -> Result<()> {
    let mut potentials = input.potentials.clone();
    potentials.sort_by_key(|potential| potential.potential_index);

    let mut content = String::new();
    content.push_str(&format!(
        "# feff85exafs-rs pot.pad v{POT_ARTIFACT_SCHEMA_VERSION}\n"
    ));
    content.push_str(&format!("vfeff {}\n", input.vfeff.trim()));
    content.push_str(&format!("vf85e {}\n", input.vf85e.trim()));
    content.push_str(&format!("scf_enabled {}\n", input.rfms1 > 0.0));
    content.push_str(&format!("ihole {}\n", input.ihole));
    content.push_str(&format!("ixc {}\n", input.ixc));
    content.push_str(&format!(
        "scf rfms1={:.9} lfms1={} nscmt={} ca1={:.9}\n",
        input.rfms1, input.lfms1, input.nscmt, input.ca1
    ));
    content.push_str(&format!(
        "counts nat={} nph={}\n",
        input.atoms.len(),
        potentials.len()
    ));

    for (index, title) in input.titles.iter().enumerate() {
        content.push_str(&format!("title[{index}] {}\n", title.trim()));
    }

    for potential in &potentials {
        let (atom_count, mean_radius, max_radius) =
            atom_radius_stats(&input.atoms, potential.potential_index);
        content.push_str(&format!(
            "potential {} z={} label={} lmaxsc={} lmaxph={} stoich={:.9} spin={:.9} atoms={} mean_r={:.9} max_r={:.9}\n",
            potential.potential_index,
            potential.atomic_number,
            potential.label.trim(),
            potential.lmaxsc,
            potential.lmaxph,
            potential.stoichiometry,
            potential.spin,
            atom_count,
            mean_radius,
            max_radius
        ));
    }

    fs::write(working_directory.join("pot.pad"), content)?;
    Ok(())
}

fn write_potential_data_files(working_directory: &Path, input: &PotInputData) -> Result<()> {
    let mut potentials = input.potentials.clone();
    potentials.sort_by_key(|potential| potential.potential_index);

    for potential in &potentials {
        let file_name = format!("pot{:02}.dat", potential.potential_index);
        let mut content = String::new();
        content.push_str("# feff85exafs-rs POT potential artifact\n");
        content.push_str(&format!("potential_index {}\n", potential.potential_index));
        content.push_str(&format!("atomic_number {}\n", potential.atomic_number));
        content.push_str(&format!("label {}\n", potential.label.trim()));
        content.push_str(&format!("lmaxsc {}\n", potential.lmaxsc));
        content.push_str(&format!("lmaxph {}\n", potential.lmaxph));
        content.push_str(&format!("stoichiometry {:.9}\n", potential.stoichiometry));
        content.push_str(&format!("spin {:.9}\n", potential.spin));
        content.push_str(&format!(
            "overlap_fraction {:.9}\n",
            potential.overlap_fraction
        ));
        content.push_str(&format!("ionization {:.9}\n", potential.ionization));

        let mut atom_count = 0_usize;
        for atom in &input.atoms {
            if atom.potential_index != potential.potential_index {
                continue;
            }
            atom_count += 1;
            let radius = atom_radius(atom);
            content.push_str(&format!(
                "atom {:.9} {:.9} {:.9} {:.9}\n",
                atom.x, atom.y, atom.z, radius
            ));
        }
        content.push_str(&format!("atom_count {atom_count}\n"));
        fs::write(working_directory.join(file_name), content)?;
    }

    Ok(())
}

fn write_misc_file(working_directory: &Path, input: &PotInputData) -> Result<()> {
    let mut content = String::new();
    content.push_str("# feff85exafs-rs POT misc artifact\n");
    content.push_str(&format!("schema_version {POT_ARTIFACT_SCHEMA_VERSION}\n"));
    content.push_str(&format!("nat {}\n", input.atoms.len()));
    content.push_str(&format!("nph {}\n", input.potentials.len()));
    content.push_str(&format!("scf_enabled {}\n", input.rfms1 > 0.0));
    content.push_str(&format!("ihole {}\n", input.ihole));
    content.push_str(&format!("ixc {}\n", input.ixc));
    fs::write(working_directory.join("misc.dat"), content)?;
    Ok(())
}

fn atom_radius_stats(atoms: &[PotAtomData], potential_index: i32) -> (usize, f64, f64) {
    let mut count = 0_usize;
    let mut radius_sum = 0.0_f64;
    let mut max_radius = 0.0_f64;

    for atom in atoms {
        if atom.potential_index != potential_index {
            continue;
        }
        let radius = atom_radius(atom);
        count += 1;
        radius_sum += radius;
        if radius > max_radius {
            max_radius = radius;
        }
    }

    let mean = if count == 0 {
        0.0
    } else {
        radius_sum / count as f64
    };
    (count, mean, max_radius)
}

fn atom_radius(atom: &PotAtomData) -> f64 {
    (atom.x * atom.x + atom.y * atom.y + atom.z * atom.z).sqrt()
}

fn ensure_min_len(field: &str, actual: usize, required: usize) -> Result<()> {
    if actual < required {
        return Err(validation_error(
            field,
            format!("must contain at least {required} value(s), found {actual}"),
        ));
    }
    Ok(())
}

fn edge_value_to_ihole(value: &str) -> Option<i32> {
    let normalized = value.trim().to_ascii_uppercase();
    if let Ok(number) = normalized.parse::<i32>()
        && (0..=29).contains(&number)
    {
        return Some(number);
    }

    EDGE_LABELS
        .iter()
        .position(|label| *label == normalized)
        .map(|position| position as i32)
}

fn parse_i32_value(value: &str, field: impl Into<String>) -> Result<i32> {
    let field = field.into();
    if let Ok(parsed) = value.parse::<i32>() {
        return Ok(parsed);
    }

    let parsed = value.parse::<f64>().map_err(|_| {
        validation_error(
            field.clone(),
            format!("invalid integer-compatible value `{value}`"),
        )
    })?;
    let rounded = parsed.round();
    if (parsed - rounded).abs() > 1.0e-9 {
        return Err(validation_error(
            field,
            format!("invalid integer-compatible value `{value}`"),
        ));
    }

    if rounded < i32::MIN as f64 || rounded > i32::MAX as f64 {
        return Err(validation_error(
            field,
            format!("integer value `{value}` is out of range"),
        ));
    }

    Ok(rounded as i32)
}

fn parse_f64_value(value: &str, field: impl Into<String>) -> Result<f64> {
    value.parse::<f64>().map_err(|_| {
        validation_error(
            field,
            format!("invalid numeric value `{value}` for POT translation"),
        )
    })
}

fn is_potential_data_file(file_name: &str) -> bool {
    if !file_name.starts_with("pot") || !file_name.ends_with(".dat") {
        return false;
    }

    let digits = &file_name[3..file_name.len() - 4];
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
    use crate::rdinp::{parse_rdinp, parse_rdinp_str};
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    const POT_PARITY_NUMERIC_TOLERANCE: f64 = 1.0e-6;

    #[derive(Debug, Clone)]
    struct LegacyPotSignature {
        scf_enabled: bool,
        scf_nscmt: Option<i32>,
        scf_rfms1: Option<f64>,
        scf_lfms1: Option<i32>,
        potentials: Vec<(i32, i32)>,
    }

    fn temp_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "feff85exafs-core-pot-{name}-{}-{nonce}",
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

    fn parse_legacy_pot_signature(path: &Path) -> LegacyPotSignature {
        let raw = fs::read_to_string(path).unwrap_or_else(|error| {
            panic!(
                "failed to read legacy POT baseline {}: {error}",
                path.display()
            )
        });

        let mut signature = LegacyPotSignature {
            scf_enabled: false,
            scf_nscmt: None,
            scf_rfms1: None,
            scf_lfms1: None,
            potentials: Vec::new(),
        };

        for raw_line in raw.lines() {
            let line = raw_line.trim();
            if line.starts_with("POT  SCF") {
                signature.scf_enabled = true;
                let tokens = line.split_whitespace().collect::<Vec<_>>();
                if tokens.len() >= 5 {
                    signature.scf_nscmt = tokens[2].parse::<i32>().ok();
                    signature.scf_rfms1 = tokens[3].parse::<f64>().ok();
                    signature.scf_lfms1 = tokens[4].trim_end_matches(',').parse::<i32>().ok();
                }
                continue;
            }

            if line.starts_with("POT  Non-SCF") {
                signature.scf_enabled = false;
                continue;
            }

            if line.starts_with("Abs") {
                if let Some(z) = parse_marker_i32(line, "Z=") {
                    signature.potentials.push((0, z));
                }
                continue;
            }

            if line.starts_with("Pot ") {
                let tokens = line.split_whitespace().collect::<Vec<_>>();
                if tokens.len() >= 3
                    && let Ok(index) = tokens[1].parse::<i32>()
                    && let Some(z) = parse_marker_i32(line, "Z=")
                {
                    signature.potentials.push((index, z));
                }
            }
        }

        signature.potentials.sort_by_key(|(index, _)| *index);
        signature
    }

    fn parse_marker_i32(line: &str, marker: &str) -> Option<i32> {
        let start = line.find(marker)? + marker.len();
        let value = line[start..]
            .trim_start()
            .chars()
            .take_while(|ch| ch.is_ascii_digit() || *ch == '+' || *ch == '-')
            .collect::<String>();
        if value.is_empty() {
            None
        } else {
            value.parse::<i32>().ok()
        }
    }

    fn numeric_within_tolerance(actual: f64, expected: f64) -> bool {
        let delta = (actual - expected).abs();
        let scale = actual.abs().max(expected.abs()).max(1.0);
        delta <= POT_PARITY_NUMERIC_TOLERANCE * scale
    }

    #[test]
    fn parses_and_roundtrips_legacy_pot_input_json_fixture() {
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../feff85exafs/wrappers/fortran/libpotph.json");

        let parsed = read_pot_input_json(&fixture).expect("fixture should parse as POT input");
        assert_eq!(parsed.titles.len(), 1);
        assert_eq!(parsed.atoms.len(), 177);
        assert_eq!(parsed.potentials.len(), 2);
        assert_eq!(parsed.ihole, 1);

        let temp_root = temp_dir("input-roundtrip");
        fs::create_dir_all(&temp_root).expect("temp test directory should be created");
        let roundtrip_path = temp_root.join("libpotph.roundtrip.json");

        write_pot_input_json(&roundtrip_path, &parsed)
            .expect("typed POT input should serialize to JSON");
        let reparsed = read_pot_input_json(&roundtrip_path)
            .expect("serialized POT input should parse back to typed model");
        assert_eq!(parsed, reparsed);

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp directory should be cleaned");
        }
    }

    #[test]
    fn builds_pot_input_from_rdinp_cards() {
        let parsed = parse_rdinp_str("inline", minimal_valid_input())
            .expect("minimal RDINP input should parse");
        let pot_input = PotInputData::from_parsed_cards(&parsed)
            .expect("parsed cards should translate to POT input");

        assert_eq!(pot_input.titles, vec!["Example".to_string()]);
        assert_eq!(pot_input.ihole, 1);
        assert_eq!(pot_input.ixc, 0);
        assert_eq!(pot_input.potentials.len(), 2);
        assert_eq!(pot_input.atoms.len(), 2);
        assert_eq!(pot_input.potentials[0].potential_index, 0);
        assert_eq!(pot_input.potentials[1].potential_index, 1);

        let temp_root = temp_dir("from-rdinp");
        fs::create_dir_all(&temp_root).expect("temp test directory should be created");
        let json_path = temp_root.join("libpotph.json");
        write_pot_input_json(&json_path, &pot_input)
            .expect("translated POT input should serialize to JSON");

        let parsed_back =
            read_pot_input_json(&json_path).expect("serialized POT input should parse from JSON");
        assert_eq!(pot_input, parsed_back);

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp directory should be cleaned");
        }
    }

    #[test]
    fn collects_and_roundtrips_pot_output_data() {
        let temp_root = temp_dir("output");
        fs::create_dir_all(&temp_root).expect("temp test directory should be created");

        fs::write(temp_root.join("pot.pad"), "placeholder\n").expect("pot.pad should be written");
        fs::write(temp_root.join("pot00.dat"), "pot00\n").expect("pot00 should be written");
        fs::write(temp_root.join("pot02.dat"), "pot02\n").expect("pot02 should be written");
        fs::write(temp_root.join("misc.dat"), "misc\n").expect("misc should be written");
        fs::write(temp_root.join("ignore.txt"), "ignore\n")
            .expect("irrelevant file should be written");

        let output = collect_pot_output_data(&temp_root)
            .expect("POT output files should translate into typed output");
        assert_eq!(output.potential_data_files.len(), 2);
        assert!(output.potential_data_files[0].ends_with("pot00.dat"));
        assert!(output.potential_data_files[1].ends_with("pot02.dat"));
        assert!(
            output
                .misc_dat
                .as_deref()
                .is_some_and(|path| path.ends_with("misc.dat"))
        );

        let json_path = temp_root.join("pot-output.json");
        write_pot_output_json(&json_path, &output)
            .expect("typed POT output should serialize to JSON");
        let parsed = read_pot_output_json(&json_path)
            .expect("POT output JSON should parse into typed output");
        assert_eq!(output, parsed);

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp directory should be cleaned");
        }
    }

    #[test]
    fn collect_pot_output_data_requires_pot_pad() {
        let temp_root = temp_dir("missing-pot-pad");
        fs::create_dir_all(&temp_root).expect("temp test directory should be created");
        fs::write(temp_root.join("pot00.dat"), "pot00\n").expect("pot00 should be written");

        let err = collect_pot_output_data(&temp_root)
            .expect_err("missing pot.pad should fail typed output translation");
        match err {
            FeffError::Validation(validation) => {
                assert_eq!(validation.len(), 1);
                assert_eq!(validation.issues()[0].field, "pot.pad");
            }
            other => panic!("unexpected error type: {other}"),
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp directory should be cleaned");
        }
    }

    #[test]
    fn run_pot_writes_required_artifacts_for_phase1_corpus() {
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
            let working_dir = temp_root.join(format!("case-{case_index:02}"));
            let output = run_pot(&pot_input, &working_dir).unwrap_or_else(|error| {
                panic!(
                    "failed to run Rust POT for {}: {error}",
                    input_path.display()
                )
            });

            assert!(output.pot_pad.ends_with("pot.pad"));
            assert_eq!(
                output.potential_data_files.len(),
                pot_input.potentials.len()
            );
            assert!(
                output
                    .misc_dat
                    .as_deref()
                    .is_some_and(|path| path.ends_with("misc.dat"))
            );
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp directory should be cleaned");
        }
    }

    #[test]
    fn rust_pot_matches_legacy_pot_signature_for_phase1_corpus() {
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
            let working_dir = temp_root.join(format!("parity-{case_index:02}"));
            let output = run_pot(&pot_input, &working_dir).unwrap_or_else(|error| {
                panic!(
                    "failed to run Rust POT for {}: {error}",
                    input_path.display()
                )
            });

            let legacy_files_path = input_path.with_file_name("files.dat");
            let legacy = parse_legacy_pot_signature(&legacy_files_path);

            let rust_scf_enabled = pot_input.rfms1 > 0.0;
            assert_eq!(
                rust_scf_enabled,
                legacy.scf_enabled,
                "SCF mode mismatch for {}",
                input_path.display()
            );

            if legacy.scf_enabled {
                let expected_nscmt = legacy.scf_nscmt.unwrap_or_else(|| {
                    panic!(
                        "missing SCF nscmt in legacy signature {}",
                        legacy_files_path.display()
                    )
                });
                let expected_rfms1 = legacy.scf_rfms1.unwrap_or_else(|| {
                    panic!(
                        "missing SCF rfms1 in legacy signature {}",
                        legacy_files_path.display()
                    )
                });
                let expected_lfms1 = legacy.scf_lfms1.unwrap_or_else(|| {
                    panic!(
                        "missing SCF lfms1 in legacy signature {}",
                        legacy_files_path.display()
                    )
                });
                assert_eq!(pot_input.nscmt, expected_nscmt);
                assert_eq!(pot_input.lfms1, expected_lfms1);
                assert!(
                    numeric_within_tolerance(pot_input.rfms1, expected_rfms1),
                    "rfms1 mismatch for {}: rust={} legacy={}",
                    input_path.display(),
                    pot_input.rfms1,
                    expected_rfms1
                );
            }

            let mut rust_potentials = pot_input
                .potentials
                .iter()
                .map(|potential| (potential.potential_index, potential.atomic_number))
                .collect::<Vec<_>>();
            rust_potentials.sort_by_key(|(index, _)| *index);

            assert_eq!(
                rust_potentials,
                legacy.potentials,
                "potential index/Z parity mismatch for {}",
                input_path.display()
            );
            assert_eq!(
                output.potential_data_files.len(),
                legacy.potentials.len(),
                "potential artifact count mismatch for {}",
                input_path.display()
            );
        }

        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).expect("temp directory should be cleaned");
        }
    }
}
