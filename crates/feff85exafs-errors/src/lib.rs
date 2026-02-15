use std::error::Error;
use std::fmt::{self, Display};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationIssue {
    pub field: String,
    pub message: String,
}

impl ValidationIssue {
    pub fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ValidationErrors {
    issues: Vec<ValidationIssue>,
}

impl ValidationErrors {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, field: impl Into<String>, message: impl Into<String>) {
        self.issues.push(ValidationIssue::new(field, message));
    }

    pub fn is_empty(&self) -> bool {
        self.issues.is_empty()
    }

    pub fn len(&self) -> usize {
        self.issues.len()
    }

    pub fn issues(&self) -> &[ValidationIssue] {
        &self.issues
    }
}

impl From<Vec<ValidationIssue>> for ValidationErrors {
    fn from(issues: Vec<ValidationIssue>) -> Self {
        Self { issues }
    }
}

impl Display for ValidationErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.issues.is_empty() {
            return write!(f, "validation failed");
        }

        let summary = self
            .issues
            .iter()
            .map(|issue| format!("{}: {}", issue.field, issue.message))
            .collect::<Vec<_>>()
            .join("; ");
        write!(f, "validation failed: {summary}")
    }
}

#[derive(Debug)]
pub enum FeffError {
    InvalidArgument(String),
    Validation(ValidationErrors),
    Io(std::io::Error),
}

pub type Result<T> = std::result::Result<T, FeffError>;

impl Display for FeffError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument(message) => write!(f, "{message}"),
            Self::Validation(validation) => write!(f, "{validation}"),
            Self::Io(error) => write!(f, "{error}"),
        }
    }
}

impl Error for FeffError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidArgument(_) => None,
            Self::Validation(_) => None,
            Self::Io(error) => Some(error),
        }
    }
}

impl From<std::io::Error> for FeffError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ValidationErrors> for FeffError {
    fn from(validation: ValidationErrors) -> Self {
        Self::Validation(validation)
    }
}
