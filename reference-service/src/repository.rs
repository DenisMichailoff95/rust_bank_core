use crate::cache::{Currency, OperationType};
use common::config::YdbConfig;
use common::ydb_client::create_ydb_client;
use ydb::{Client, Query, YdbResult};

#[derive(Debug, Clone)]
pub struct ReferenceRepository {
    pub client: Client,
}

impl ReferenceRepository {
    pub async fn new(config: &YdbConfig) -> YdbResult<Self> {
        let client = create_ydb_client(config).await?;
        Ok(Self { client })
    }

    pub async fn load_currencies(&self) -> YdbResult<Vec<Currency>> {
        let result = self
            .client
            .table_client()
            .retry_transaction(|mut t| async move {
                let res = t
                    .query(Query::from(
                        "SELECT currency_code, numeric_code, name, decimal_places, is_base FROM currencies",
                    ))
                    .await?;
                Ok(res)
            })
            .await?;

        let mut currencies = Vec::new();
        for row in result.into_iter() {
            let code: String = row.get("currency_code")?.try_into()?;
            let numeric: i64 = row
                .get("numeric_code")
                .ok()
                .and_then(|v| v.try_into().ok())
                .unwrap_or(0);
            let name: String = row.get("name")?.try_into()?;
            let decimals: i64 = row.get("decimal_places")?.try_into()?;
            let is_base: bool = row.get("is_base")?.try_into()?;

            currencies.push(Currency {
                currency_code: code,
                numeric_code: numeric as u32,
                name,
                decimal_places: decimals as u32,
                is_base,
            });
        }
        Ok(currencies)
    }

    pub async fn load_operation_types(&self) -> YdbResult<Vec<OperationType>> {
        let result = self
            .client
            .table_client()
            .retry_transaction(|mut t| async move {
                let res = t
                    .query(Query::from(
                        "SELECT operation_code, name, direction FROM operation_types",
                    ))
                    .await?;
                Ok(res)
            })
            .await?;

        let mut ops = Vec::new();
        for row in result.into_iter() {
            let code: String = row.get("operation_code")?.try_into()?;
            let name: String = row.get("name")?.try_into()?;
            let direction: String = row.get("direction")?.try_into()?;

            ops.push(OperationType {
                operation_code: code,
                name,
                direction,
            });
        }
        Ok(ops)
    }
}