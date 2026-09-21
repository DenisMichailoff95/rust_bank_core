use common::config::YdbConfig;
use common::ydb_client::create_ydb_client;
use serde::{Deserialize, Serialize};
use ydb::{ydb_params, Client, Query, YdbOrCustomerError};

type RepoResult<T> = Result<T, YdbOrCustomerError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientRecord {
    pub client_id: String,
    pub last_name: String,
    pub first_name: String,
    pub middle_name: Option<String>,
    pub birth_date: Option<String>,
    pub passport_series: Option<String>,
    pub passport_number: Option<String>,
    pub status: String,
    pub created_at: i64,
}

pub struct ClientRepository {
    pub client: Client,
}

impl ClientRepository {
    pub async fn new(config: &YdbConfig) -> RepoResult<Self> {
        let client = create_ydb_client(config)
            .await
            .map_err(YdbOrCustomerError::from)?;
        Ok(Self { client })
    }

    pub async fn create_client(
        &self,
        record: &ClientRecord,
        event_payload: &str,
    ) -> RepoResult<()> {
        let client_id = record.client_id.clone();
        let last_name = record.last_name.clone();
        let first_name = record.first_name.clone();
        let middle_name = record.middle_name.clone().unwrap_or_default();
        let birth_date = record.birth_date.clone().unwrap_or_default();
        let passport_series = record.passport_series.clone().unwrap_or_default();
        let passport_number = record.passport_number.clone().unwrap_or_default();
        let status = record.status.clone();
        let created_at = record.created_at;
        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event_payload.to_string();

        self.client
            .table_client()
            .retry_transaction(|mut t| {
                let client_id = client_id.clone();
                let last_name = last_name.clone();
                let first_name = first_name.clone();
                let middle_name = middle_name.clone();
                let birth_date = birth_date.clone();
                let passport_series = passport_series.clone();
                let passport_number = passport_number.clone();
                let status = status.clone();
                let event_id = event_id.clone();
                let payload = payload.clone();

                async move {
                    t.query(
                        Query::from(
                            "UPSERT INTO clients (client_id, last_name, first_name, middle_name, birth_date, passport_series, passport_number, status, created_at) \
                             VALUES ($client_id, $last_name, $first_name, $middle_name, $birth_date, $passport_series, $passport_number, $status, $created_at)",
                        ).with_params(ydb_params!(
                            "$client_id" => client_id.clone(),
                            "$last_name" => last_name,
                            "$first_name" => first_name,
                            "$middle_name" => middle_name,
                            "$birth_date" => birth_date,
                            "$passport_series" => passport_series,
                            "$passport_number" => passport_number,
                            "$status" => status,
                            "$created_at" => created_at
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'client', $client_id, 'ClientCreated', $payload, 'PENDING', $created_at, 0)",
                        ).with_params(ydb_params!(
                            "$event_id" => event_id,
                            "$client_id" => client_id,
                            "$payload" => payload,
                            "$created_at" => created_at
                        )),
                    )
                        .await?;

                    Ok(())
                }
            })
            .await
    }

    pub async fn get_client(&self, client_id: &str) -> RepoResult<Option<ClientRecord>> {
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
                                "SELECT client_id, last_name, first_name, middle_name, birth_date, passport_series, passport_number, status, created_at \
                                 FROM clients WHERE client_id = $client_id",
                            ).with_params(ydb_params!("$client_id" => cid)),
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

        Ok(Some(ClientRecord {
            client_id: row.remove_field_by_name("client_id")?.try_into()?,
            last_name: row.remove_field_by_name("last_name")?.try_into()?,
            first_name: row.remove_field_by_name("first_name")?.try_into()?,
            middle_name: row
                .remove_field_by_name("middle_name")
                .ok()
                .and_then(|v| v.try_into().ok()),
            birth_date: row
                .remove_field_by_name("birth_date")
                .ok()
                .and_then(|v| v.try_into().ok()),
            passport_series: row
                .remove_field_by_name("passport_series")
                .ok()
                .and_then(|v| v.try_into().ok()),
            passport_number: row
                .remove_field_by_name("passport_number")
                .ok()
                .and_then(|v| v.try_into().ok()),
            status: row.remove_field_by_name("status")?.try_into()?,
            created_at: row.remove_field_by_name("created_at")?.try_into()?,
        }))
    }

    pub async fn update_status(
        &self,
        client_id: &str,
        new_status: &str,
        event_payload: &str,
    ) -> RepoResult<String> {
        let cid = client_id.to_string();
        let ns = new_status.to_string();
        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event_payload.to_string();
        let now = chrono::Utc::now().timestamp();

        self.client
            .table_client()
            .retry_transaction(|mut t| {
                let cid = cid.clone();
                let ns = ns.clone();
                let event_id = event_id.clone();
                let payload = payload.clone();

                async move {
                    let res = t
                        .query(
                            Query::from(
                                "SELECT status FROM clients WHERE client_id = $client_id",
                            ).with_params(ydb_params!("$client_id" => cid.clone())),
                        )
                        .await?;
                    let old_status: String = res
                        .into_only_row()?
                        .remove_field_by_name("status")?
                        .try_into()?;

                    t.query(
                        Query::from(
                            "UPDATE clients SET status = $status WHERE client_id = $client_id",
                        ).with_params(ydb_params!(
                            "$status" => ns.clone(),
                            "$client_id" => cid.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'client', $client_id, 'ClientStatusChanged', $payload, 'PENDING', $created_at, 0)",
                        ).with_params(ydb_params!(
                            "$event_id" => event_id.clone(),
                            "$client_id" => cid.clone(),
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