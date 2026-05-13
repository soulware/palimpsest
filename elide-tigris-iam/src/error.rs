use thiserror::Error;

#[derive(Debug, Error)]
pub enum IamError {
    // reqwest::Error's Display only prints the outer message (e.g.
    // "error sending request for url (...)") and drops the cause —
    // dns-failure vs connect-refused vs tls vs http/2 negotiation all
    // look identical to an operator unless we walk .source() ourselves.
    #[error("http transport: {}", FormatChain(.0))]
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

struct FormatChain<'a>(&'a reqwest::Error);

impl std::fmt::Display for FormatChain<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)?;
        let mut src: Option<&dyn std::error::Error> = std::error::Error::source(self.0);
        while let Some(e) = src {
            write!(f, ": {e}")?;
            src = e.source();
        }
        Ok(())
    }
}
