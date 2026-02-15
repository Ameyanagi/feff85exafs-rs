use std::error::Error;
use std::fmt::{self, Display};

#[derive(Debug)]
pub enum FeffError {
    InvalidArgument(String),
    Io(std::io::Error),
}

pub type Result<T> = std::result::Result<T, FeffError>;

impl Display for FeffError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument(message) => write!(f, "{message}"),
            Self::Io(error) => write!(f, "{error}"),
        }
    }
}

impl Error for FeffError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidArgument(_) => None,
            Self::Io(error) => Some(error),
        }
    }
}

impl From<std::io::Error> for FeffError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}
