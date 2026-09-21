#[derive(Debug, Clone)]
pub struct YdbConfig {
    pub connection_string: String,
    pub credentials: String,
}

impl YdbConfig {
    pub fn from_env() -> Self {
        Self {
            // Для локального YDB-контейнера (ydbplatform/local-ydb) по умолчанию:
            //   - gRPC порт: 2136
            //   - database: /local
            //   - TLS отключён (используем grpc://, а не grpcs://)
            //   - аутентификация не требуется (анонимный режим)
            connection_string: std::env::var("YDB_CONNECTION_STRING")
                .unwrap_or_else(|_| "grpc://localhost:2136?database=/local".into()),
            credentials: std::env::var("YDB_TOKEN").unwrap_or_default(),
        }
    }
}