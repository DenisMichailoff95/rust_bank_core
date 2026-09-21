#[derive(Debug, Clone)]
pub struct YdbConfig {
    pub connection_string: String,
    pub credentials: String,
}

impl YdbConfig {
    pub fn from_env() -> Self {
        Self {
            connection_string: std::env::var("YDB_CONNECTION_STRING")
                .unwrap_or_else(|_| "grpc://localhost:2136?database=local".into()),
            credentials: std::env::var("YDB_TOKEN").unwrap_or_default(),
        }
    }
}