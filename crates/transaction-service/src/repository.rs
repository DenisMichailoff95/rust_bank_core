use common::config::YdbConfig;
use common::ydb_client::create_ydb_client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use ydb::{ydb_params, Client, Query, YdbError, YdbOrCustomerError};

type RepoResult<T> = Result<T, YdbOrCustomerError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionRecord {
    pub account_id: String,
    pub transaction_id: String,
    pub operation_code: String,
    pub amount: String,
    pub currency_code: String,
    pub status: String,
    pub created_at: i64,
    pub description: Option<String>,
}

pub struct TransactionRepository {
    pub client: Client,
}

/// Хелпер для бизнес-ошибок. Возвращает `YdbOrCustomerError`,
/// потому что `retry_transaction` ожидает именно этот тип от замыкания.
fn custom_error(msg: impl Into<String>) -> YdbOrCustomerError {
    YdbError::Custom(msg.into()).into()
}

impl TransactionRepository {
    pub async fn new(config: &YdbConfig) -> RepoResult<Self> {
        let client = create_ydb_client(config)
            .await
            .map_err(YdbOrCustomerError::from)?;
        Ok(Self { client })
    }

    pub async fn post_transaction(
        &self,
        account_id: &str,
        operation_code: &str,
        amount: Decimal,
        currency_code: &str,
        description: Option<&str>,
        direction: &str,
        event_payload: &str,
    ) -> RepoResult<(String, String)> {
        let aid = account_id.to_string();
        let op_code = operation_code.to_string();
        let amount_str = amount.to_string();
        let ccy = currency_code.to_string();
        let desc = description.map(|s| s.to_string()).unwrap_or_default();
        let tx_id = uuid::Uuid::new_v4().to_string();
        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event_payload.to_string();
        let now = chrono::Utc::now().timestamp();
        let dir = direction.to_string();

        self.client
            .table_client()
            .retry_transaction(|mut t| {
                let aid = aid.clone();
                let op_code = op_code.clone();
                let amount_str = amount_str.clone();
                let ccy = ccy.clone();
                let desc = desc.clone();
                let tx_id = tx_id.clone();
                let event_id = event_id.clone();
                let payload = payload.clone();
                let dir = dir.clone();

                async move {
                    let res = t
                        .query(
                            Query::from(
                                "SELECT balance, currency_code FROM accounts WHERE account_id = $account_id",
                            ).with_params(ydb_params!("$account_id" => aid.clone())),
                        )
                        .await?;

                    let mut row = res.into_only_row()?;
                    let balance_str: String =
                        row.remove_field_by_name("balance")?.try_into()?;
                    let acc_ccy: String =
                        row.remove_field_by_name("currency_code")?.try_into()?;

                    if acc_ccy != ccy {
                        return Err(custom_error(format!(
                            "Currency mismatch: account {} vs tx {}",
                            acc_ccy, ccy
                        )));
                    }

                    let current = Decimal::from_str(&balance_str)
                        .map_err(|e| custom_error(format!("Invalid balance: {e}")))?;

                    let delta = Decimal::from_str(&amount_str).unwrap();
                    let new_balance = match dir.as_str() {
                        "credit" => current + delta,
                        "debit" => {
                            if current < delta {
                                return Err(custom_error("Insufficient funds"));
                            }
                            current - delta
                        }
                        _ => {
                            return Err(custom_error(format!("Unknown direction: {dir}")));
                        }
                    };

                    t.query(
                        Query::from(
                            "UPDATE accounts SET balance = $balance WHERE account_id = $account_id",
                        ).with_params(ydb_params!(
                            "$balance" => new_balance.to_string(),
                            "$account_id" => aid.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $transaction_id, $operation_code, $amount, $currency_code, $status, $description)",
                        ).with_params(ydb_params!(
                            "$account_id" => aid.clone(),
                            "$created_at" => now,
                            "$transaction_id" => tx_id.clone(),
                            "$operation_code" => op_code.clone(),
                            "$amount" => amount_str.clone(),
                            "$currency_code" => ccy.clone(),
                            "$status" => "completed",
                            "$description" => desc.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'transaction', $transaction_id, 'TransactionPosted', $payload, 'PENDING', $created_at, 0)",
                        ).with_params(ydb_params!(
                            "$event_id" => event_id.clone(),
                            "$transaction_id" => tx_id.clone(),
                            "$payload" => payload.clone(),
                            "$created_at" => now
                        )),
                    )
                        .await?;

                    Ok((tx_id, new_balance.to_string()))
                }
            })
            .await
    }

    pub async fn get_transaction(
        &self,
        account_id: &str,
        transaction_id: &str,
    ) -> RepoResult<Option<TransactionRecord>> {
        let aid = account_id.to_string();
        let tid = transaction_id.to_string();
        let result = self
            .client
            .table_client()
            .retry_transaction(|mut t| {
                let aid = aid.clone();
                let tid = tid.clone();
                async move {
                    let res = t
                        .query(
                            Query::from(
                                "SELECT account_id, transaction_id, operation_code, amount, currency_code, status, created_at, description \
                                 FROM transactions WHERE account_id = $account_id AND transaction_id = $transaction_id",
                            ).with_params(ydb_params!(
                                "$account_id" => aid,
                                "$transaction_id" => tid
                            )),
                        )
                        .await?;
                    Ok(res)
                }
            })
            .await?;

        let rows: Vec<_> = result.into_only_result()?.rows().collect();
        if rows.is_empty() {
            return Ok(None);
        }
        let mut row = rows.into_iter().next().unwrap();
        Ok(Some(TransactionRecord {
            account_id: row.remove_field_by_name("account_id")?.try_into()?,
            transaction_id: row.remove_field_by_name("transaction_id")?.try_into()?,
            operation_code: row.remove_field_by_name("operation_code")?.try_into()?,
            amount: row.remove_field_by_name("amount")?.try_into()?,
            currency_code: row.remove_field_by_name("currency_code")?.try_into()?,
            status: row.remove_field_by_name("status")?.try_into()?,
            created_at: row.remove_field_by_name("created_at")?.try_into()?,
            description: row
                .remove_field_by_name("description")
                .ok()
                .and_then(|v| v.try_into().ok()),
        }))
    }

    pub async fn list_account_transactions(
        &self,
        account_id: &str,
        limit: i32,
    ) -> RepoResult<Vec<TransactionRecord>> {
        let aid = account_id.to_string();
        let lim = limit.max(1).min(1000) as i64;
        let result = self
            .client
            .table_client()
            .retry_transaction(|mut t| {
                let aid = aid.clone();
                async move {
                    let res = t
                        .query(
                            Query::from(
                                "SELECT account_id, transaction_id, operation_code, amount, currency_code, status, created_at, description \
                                 FROM transactions WHERE account_id = $account_id ORDER BY created_at DESC LIMIT $limit",
                            ).with_params(ydb_params!(
                                "$account_id" => aid,
                                "$limit" => lim
                            )),
                        )
                        .await?;
                    Ok(res)
                }
            })
            .await?;

        let mut records = Vec::new();
        for mut row in result.into_only_result()?.rows() {
            records.push(TransactionRecord {
                account_id: row.remove_field_by_name("account_id")?.try_into()?,
                transaction_id: row.remove_field_by_name("transaction_id")?.try_into()?,
                operation_code: row.remove_field_by_name("operation_code")?.try_into()?,
                amount: row.remove_field_by_name("amount")?.try_into()?,
                currency_code: row.remove_field_by_name("currency_code")?.try_into()?,
                status: row.remove_field_by_name("status")?.try_into()?,
                created_at: row.remove_field_by_name("created_at")?.try_into()?,
                description: row
                    .remove_field_by_name("description")
                    .ok()
                    .and_then(|v| v.try_into().ok()),
            });
        }
        Ok(records)
    }
}