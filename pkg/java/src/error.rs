#[derive(Debug, Clone)]
pub struct GlueSQLError {
    pub message: String,
}

impl GlueSQLError {
    pub fn new(message: String) -> Self {
        GlueSQLError { message }
    }
}

impl std::fmt::Display for GlueSQLError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GlueSQLError: {}", self.message)
    }
}

impl std::error::Error for GlueSQLError {}