use crate::cache::{Currency, OperationType};
use common::config::YdbConfig;
use common::ydb_client::create_ydb_client;
use ydb::{Client, Query, YdbOrCustomerError};

type RepoResult<T> = Result<T, YdbOrCustomerError>;

pub struct ReferenceRepository {
    pub client: Client,
}

impl ReferenceRepository {
    pub async fn new(config: &YdbConfig) -> RepoResult<Self> {
        let client = create_ydb_client(config)
            .await
            .map_err(YdbOrCustomerError::from)?;
        Ok(Self { client })
    }

    pub async fn load_currencies(&self) -> RepoResult<Vec<Currency>> {
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
        for mut row in result.into_only_result()?.rows() {
            let code: String = row.remove_field_by_name("currency_code")?.try_into()?;
            let numeric: u16 = row
                .remove_field_by_name("numeric_code")
                .ok()
                .and_then(|v| v.try_into().ok())
                .unwrap_or(0);
            let name: String = row.remove_field_by_name("name")?.try_into()?;
            let decimals: u8 = row
                .remove_field_by_name("decimal_places")
                .ok()
                .and_then(|v| v.try_into().ok())
                .unwrap_or(2);
            let is_base: bool = row.remove_field_by_name("is_base")?.try_into()?;

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

    pub async fn load_operation_types(&self) -> RepoResult<Vec<OperationType>> {
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
        for mut row in result.into_only_result()?.rows() {
            let code: String = row.remove_field_by_name("operation_code")?.try_into()?;
            let name: String = row.remove_field_by_name("name")?.try_into()?;
            let direction: String = row.remove_field_by_name("direction")?.try_into()?;

            ops.push(OperationType {
                operation_code: code,
                name,
                direction,
            });
        }
        Ok(ops)
    }
}