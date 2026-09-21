use crate::config::YdbConfig;
use ydb::{ClientBuilder, YdbResult};

pub async fn create_ydb_client(config: &YdbConfig) -> YdbResult<ydb::Client> {
    let builder = ClientBuilder::new_from_connection_string(&config.connection_string)?;

    let builder = if config.credentials.is_empty() {
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