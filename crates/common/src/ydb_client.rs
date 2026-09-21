use crate::config::YdbConfig;
use ydb::{ClientBuilder, YdbResult};

/// Создаёт клиент YDB.
///
/// Поддерживаются два режима:
/// 1. **Анонимный** (для локального контейнера `ydbplatform/local-ydb`) — если
///    `config.credentials` пустой, `with_credentials` НЕ вызывается вообще.
/// 2. **С токеном** (для продакшена / YDB Cloud) — используется
///    `AccessTokenCredentials`.
///
/// ВАЖНО: для локального контейнера используйте `grpc://` (без TLS),
/// а не `grpcs://`. Иначе SDK попытается установить TLS-соединение,
/// а контейнер его не поддерживает.
pub async fn create_ydb_client(config: &YdbConfig) -> YdbResult<ydb::Client> {
    let builder = ClientBuilder::new_from_connection_string(&config.connection_string)?;

    let builder = if config.credentials.is_empty() {
        // Анонимный режим. Не вызываем with_credentials вообще,
        // иначе ydb SDK может попытаться интерпретировать пустую строку
        // как токен и упасть с ошибкой.
        builder
    } else {
        builder.with_credentials(ydb::AccessTokenCredentials::from(
            config.credentials.as_str(),
        ))
    };

    let client = builder.client()?;
    client.wait().await?;
    Ok(client)
}