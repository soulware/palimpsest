use thiserror::Error;

#[derive(Debug, Error)]
pub enum IamError {
    #[error("http transport: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("building http request: {0}")]
    HttpBuild(String),

    #[error("signing request: {0}")]
    Signing(String),

    #[error("parsing response: {0}")]
    Parse(String),

    #[error("iam error {code}: {message}")]
    Service { code: String, message: String },

    #[error("unexpected response (status {status}): {body}")]
    Unexpected { status: u16, body: String },
}
