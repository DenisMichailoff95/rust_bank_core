use common::config::YdbConfig;
use common::ydb_client::create_ydb_client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use ydb::{Client, Query, YdbResult};

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

#[derive(Debug, Clone)]
pub struct TransactionRepository {
    pub client: Client,
}

impl TransactionRepository {
    pub async fn new(config: &YdbConfig) -> YdbResult<Self> {
        let client = create_ydb_client(config).await?;
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
    ) -> YdbResult<(String, String)> {
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
                            )
                                .param("$account_id", aid.clone()),
                        )
                        .await?;

                    let row = res.into_only_row()?;
                    let balance_str: String = row.get("balance")?.try_into()?;
                    let acc_ccy: String = row.get("currency_code")?.try_into()?;

                    if acc_ccy != ccy {
                        return Err(ydb::YdbOrCustomerError::from(
                            ydb::YdbStatusError {
                                message: format!(
                                    "Currency mismatch: account {} vs tx {}",
                                    acc_ccy, ccy
                                ),
                                ..Default::default()
                            },
                        ));
                    }

                    let current = Decimal::from_str(&balance_str).map_err(|e| {
                        ydb::YdbOrCustomerError::from(ydb::YdbStatusError {
                            message: format!("Invalid balance: {}", e),
                            ..Default::default()
                        })
                    })?;

                    let delta = Decimal::from_str(&amount_str).unwrap();
                    let new_balance = match dir.as_str() {
                        "credit" => current + delta,
                        "debit" => current - delta,
                        _ => {
                            return Err(ydb::YdbOrCustomerError::from(
                                ydb::YdbStatusError {
                                    message: format!("Unknown direction: {}", dir),
                                    ..Default::default()
                                },
                            ));
                        }
                    };

                    t.query(
                        Query::from(
                            "UPDATE accounts SET balance = $balance WHERE account_id = $account_id",
                        )
                            .param("$balance", new_balance.to_string())
                            .param("$account_id", aid.clone()),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $transaction_id, $operation_code, $amount, $currency_code, $status, $description)",
                        )
                            .param("$account_id", aid.clone())
                            .param("$created_at", now)
                            .param("$transaction_id", tx_id.clone())
                            .param("$operation_code", op_code.clone())
                            .param("$amount", amount_str.clone())
                            .param("$currency_code", ccy.clone())
                            .param("$status", "completed")
                            .param("$description", desc.clone()),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'transaction', $transaction_id, 'TransactionPosted', $payload, 'PENDING', $created_at, 0)",
                        )
                            .param("$event_id", event_id.clone())
                            .param("$transaction_id", tx_id.clone())
                            .param("$payload", payload.clone())
                            .param("$created_at", now),
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
    ) -> YdbResult<Option<TransactionRecord>> {
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
                            )
                                .param("$account_id", aid)
                                .param("$transaction_id", tid),
                        )
                        .await?;
                    Ok(res)
                }
            })
            .await?;

        let rows: Vec<_> = result.into_iter().collect();
        if rows.is_empty() {
            return Ok(None);
        }

        let row = &rows[0];
        Ok(Some(TransactionRecord {
            account_id: row.get("account_id")?.try_into()?,
            transaction_id: row.get("transaction_id")?.try_into()?,
            operation_code: row.get("operation_code")?.try_into()?,
            amount: row.get("amount")?.try_into()?,
            currency_code: row.get("currency_code")?.try_into()?,
            status: row.get("status")?.try_into()?,
            created_at: row.get("created_at")?.try_into()?,
            description: row.get("description").ok().and_then(|v| v.try_into().ok()),
        }))
    }

    pub async fn list_account_transactions(
        &self,
        account_id: &str,
        limit: i32,
    ) -> YdbResult<Vec<TransactionRecord>> {
        let aid = account_id.to_string();
        let lim = limit.max(1).min(1000) as u64;
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
                            )
                                .param("$account_id", aid)
                                .param("$limit", lim as i64),
                        )
                        .await?;
                    Ok(res)
                }
            })
            .await?;

        let mut records = Vec::new();
        for row in result.into_iter() {
            records.push(TransactionRecord {
                account_id: row.get("account_id")?.try_into()?,
                transaction_id: row.get("transaction_id")?.try_into()?,
                operation_code: row.get("operation_code")?.try_into()?,
                amount: row.get("amount")?.try_into()?,
                currency_code: row.get("currency_code")?.try_into()?,
                status: row.get("status")?.try_into()?,
                created_at: row.get("created_at")?.try_into()?,
                description: row.get("description").ok().and_then(|v| v.try_into().ok()),
            });
        }
        Ok(records)
    }
}