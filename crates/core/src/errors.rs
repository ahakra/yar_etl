use thiserror::Error;


#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("IO error: {0}")]
    Io(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Connection error: {0}")]
    Connection(String),
}

// Create a type alias for Result
pub type AdapterResult<T> = std::result::Result<T, AdapterError>;