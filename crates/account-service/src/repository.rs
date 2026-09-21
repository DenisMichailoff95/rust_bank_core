use common::config::YdbConfig;
use common::ydb_client::create_ydb_client;
use serde::{Deserialize, Serialize};
use ydb::{ydb_params, Client, Query, YdbOrCustomerError};

type RepoResult<T> = Result<T, YdbOrCustomerError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountRecord {
    pub account_id: String,
    pub client_id: String,
    pub currency_code: String,
    pub account_number: String,
    pub account_type: String,
    pub balance: String,
    pub status: String,
    pub opened_at: i64,
}

pub struct AccountRepository {
    pub client: Client,
}

impl AccountRepository {
    pub async fn new(config: &YdbConfig) -> RepoResult<Self> {
        let client = create_ydb_client(config)
            .await
            .map_err(YdbOrCustomerError::from)?;
        Ok(Self { client })
    }

    pub async fn create_account(
        &self,
        record: &AccountRecord,
        event_payload: &str,
    ) -> RepoResult<()> {
        let account_id = record.account_id.clone();
        let client_id = record.client_id.clone();
        let currency_code = record.currency_code.clone();
        let account_number = record.account_number.clone();
        let account_type = record.account_type.clone();
        let balance = record.balance.clone();
        let status = record.status.clone();
        let opened_at = record.opened_at;
        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event_payload.to_string();

        self.client
            .table_client()
            .retry_transaction(|mut t| {
                let account_id = account_id.clone();
                let client_id = client_id.clone();
                let currency_code = currency_code.clone();
                let account_number = account_number.clone();
                let account_type = account_type.clone();
                let balance = balance.clone();
                let status = status.clone();
                let event_id = event_id.clone();
                let payload = payload.clone();

                async move {
                    t.query(
                        Query::from(
                            "UPSERT INTO accounts (account_id, client_id, currency_code, account_number, account_type, balance, status, opened_at) \
                             VALUES ($account_id, $client_id, $currency_code, $account_number, $account_type, $balance, $status, $opened_at)",
                        ).with_params(ydb_params!(
                            "$account_id" => account_id.clone(),
                            "$client_id" => client_id.clone(),
                            "$currency_code" => currency_code,
                            "$account_number" => account_number,
                            "$account_type" => account_type,
                            "$balance" => balance,
                            "$status" => status,
                            "$opened_at" => opened_at
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'account', $account_id, 'AccountCreated', $payload, 'PENDING', $created_at, 0)",
                        ).with_params(ydb_params!(
                            "$event_id" => event_id,
                            "$account_id" => account_id,
                            "$payload" => payload,
                            "$created_at" => opened_at
                        )),
                    )
                        .await?;

                    Ok(())
                }
            })
            .await
    }

    pub async fn get_account(&self, account_id: &str) -> RepoResult<Option<AccountRecord>> {
        let aid = account_id.to_string();
        let result = self
            .client
            .table_client()
            .retry_transaction(|mut t| {
                let aid = aid.clone();
                async move {
                    let res = t
                        .query(
                            Query::from(
                                "SELECT account_id, client_id, currency_code, account_number, account_type, balance, status, opened_at \
                                 FROM accounts WHERE account_id = $account_id",
                            ).with_params(ydb_params!("$account_id" => aid)),
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

        Ok(Some(AccountRecord {
            account_id: row.remove_field_by_name("account_id")?.try_into()?,
            client_id: row.remove_field_by_name("client_id")?.try_into()?,
            currency_code: row.remove_field_by_name("currency_code")?.try_into()?,
            account_number: row.remove_field_by_name("account_number")?.try_into()?,
            account_type: row.remove_field_by_name("account_type")?.try_into()?,
            balance: row.remove_field_by_name("balance")?.try_into()?,
            status: row.remove_field_by_name("status")?.try_into()?,
            opened_at: row.remove_field_by_name("opened_at")?.try_into()?,
        }))
    }

    pub async fn get_balance(&self, account_id: &str) -> RepoResult<Option<(String, String)>> {
        let aid = account_id.to_string();
        let result = self
            .client
            .table_client()
            .retry_transaction(|mut t| {
                let aid = aid.clone();
                async move {
                    let res = t
                        .query(
                            Query::from(
                                "SELECT balance, currency_code FROM accounts WHERE account_id = $account_id",
                            ).with_params(ydb_params!("$account_id" => aid)),
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
        let balance: String = row.remove_field_by_name("balance")?.try_into()?;
        let currency_code: String =
            row.remove_field_by_name("currency_code")?.try_into()?;
        Ok(Some((balance, currency_code)))
    }

    pub async fn list_client_accounts(&self, client_id: &str) -> RepoResult<Vec<AccountRecord>> {
        let cid = client_id.to_string();
        let result = self
            .client
            .table_client()
            .retry_transaction(|mut t| {
                let cid = cid.clone();
                async move {
                    let res = t
                        .query(
                            Query::from(
                                "SELECT account_id, client_id, currency_code, account_number, account_type, balance, status, opened_at \
                                 FROM accounts WHERE client_id = $client_id",
                            ).with_params(ydb_params!("$client_id" => cid)),
                        )
                        .await?;
                    Ok(res)
                }
            })
            .await?;

        let mut records = Vec::new();
        for mut row in result.into_only_result()?.rows() {
            records.push(AccountRecord {
                account_id: row.remove_field_by_name("account_id")?.try_into()?,
                client_id: row.remove_field_by_name("client_id")?.try_into()?,
                currency_code: row.remove_field_by_name("currency_code")?.try_into()?,
                account_number: row.remove_field_by_name("account_number")?.try_into()?,
                account_type: row.remove_field_by_name("account_type")?.try_into()?,
                balance: row.remove_field_by_name("balance")?.try_into()?,
                status: row.remove_field_by_name("status")?.try_into()?,
                opened_at: row.remove_field_by_name("opened_at")?.try_into()?,
            });
        }
        Ok(records)
    }

    pub async fn update_status(
        &self,
        account_id: &str,
        new_status: &str,
        event_payload: &str,
    ) -> RepoResult<String> {
        let aid = account_id.to_string();
        let ns = new_status.to_string();
        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event_payload.to_string();
        let now = chrono::Utc::now().timestamp();

        self.client
            .table_client()
            .retry_transaction(|mut t| {
                let aid = aid.clone();
                let ns = ns.clone();
                let event_id = event_id.clone();
                let payload = payload.clone();

                async move {
                    let res = t
                        .query(
                            Query::from(
                                "SELECT status FROM accounts WHERE account_id = $account_id",
                            ).with_params(ydb_params!("$account_id" => aid.clone())),
                        )
                        .await?;
                    let old_status: String = res
                        .into_only_row()?
                        .remove_field_by_name("status")?
                        .try_into()?;

                    t.query(
                        Query::from(
                            "UPDATE accounts SET status = $status WHERE account_id = $account_id",
                        ).with_params(ydb_params!(
                            "$status" => ns.clone(),
                            "$account_id" => aid.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'account', $account_id, 'AccountStatusChanged', $payload, 'PENDING', $created_at, 0)",
                        ).with_params(ydb_params!(
                            "$event_id" => event_id.clone(),
                            "$account_id" => aid.clone(),
                            "$payload" => payload.clone(),
                            "$created_at" => now
                        )),
                    )
                        .await?;

                    Ok(old_status)
                }
            })
            .await
    }
}