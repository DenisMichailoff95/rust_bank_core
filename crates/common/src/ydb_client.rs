use crate::config::YdbConfig;
use ydb::{ClientBuilder, StaticToken, YdbResult};

pub async fn create_ydb_client(config: &YdbConfig) -> YdbResult<ydb::Client> {
    let client = ClientBuilder::new_from_connection_string(&config.connection_string)?
        .with_credentials(StaticToken::from(&config.credentials))
        .client()?;

    client.wait().await?;
    Ok(client)
}