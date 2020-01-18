use std::fmt;

use crate::base::Id;

#[derive(Debug)]
pub enum SyntaxError {
    ExpectedApplication,
    UnknownFunction,
}

#[derive(Debug)]
pub enum Error {
    Lua(rlua::Error),
    Io(std::io::Error),
    Readline(rustyline::error::ReadlineError),
    Regex(regex::Error),
    ApplicationOrder,
    FileNotLoaded(String),
    InvalidTarget(String),
    MissingId(Id),
    OutputWithoutId,
    Parser(String),
    SymbolNotFound(String),
    Syntax(SyntaxError, String),
}

impl From<rlua::Error> for Error {
    fn from(err: rlua::Error) -> Error {
        Error::Lua(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<rustyline::error::ReadlineError> for Error {
    fn from(err: rustyline::error::ReadlineError) -> Error {
        Error::Readline(err)
    }
}

impl From<regex::Error> for Error {
    fn from(err: regex::Error) -> Error {
        Error::Regex(err)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Lua(ref err) => write!(f, "{}", err),
            Error::Io(ref err) => write!(f, "{}", err),
            Error::Readline(ref err) => write!(f, "{}", err),
            Error::Regex(ref err) => write!(f, "{}", err),
            Error::ApplicationOrder => write!(f, "Invalid application order"),
            Error::FileNotLoaded(ref path) => write!(f, "File not loaded: {}", path),
            Error::InvalidTarget(ref target) => write!(f, "Invalid target: {}", target),
            Error::MissingId(ref id) => write!(f, "Missing ID: {:?}", id),
            Error::OutputWithoutId => write!(f, "Output without ID"),
            Error::Parser(ref err) => write!(f, "Parser error:\n{}", err),
            Error::SymbolNotFound(ref symbol) => write!(f, "Symbol not found: {}", symbol),
            Error::Syntax(ref kind, ref message) => {
                write!(f, "Syntax error: {:?} in {}", kind, message)
            }
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
