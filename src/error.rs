use std::fmt;

#[derive(Debug)]
pub enum SyntaxError {
    ExpectedApplication,
    UnknownFunction,
}

#[derive(Debug)]
pub enum Error {
    Lua(rlua::Error),
    Io(std::io::Error),
    Regex(regex::Error),
    ExpectedReturnedId(String),
    FileNotLoaded(String),
    MissingId(String),
    Parser(String),
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
            Error::Regex(ref err) => write!(f, "{}", err),
            Error::ExpectedReturnedId(ref err) => write!(f, "Expected returned ID in {}", err),
            Error::FileNotLoaded(ref path) => write!(f, "File not loaded: {}", path),
            Error::MissingId(ref id) => write!(f, "Missing symbol: {}", id),
            Error::Parser(ref err) => write!(f, "Parser error:\n{}", err),
            Error::Syntax(ref kind, ref message) => {
                write!(f, "Syntax error: {:?} in {}", kind, message)
            }
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
