use common::config::YdbConfig;
use common::ydb_client::create_ydb_client;
use serde::{Deserialize, Serialize};
use ydb::{ydb_params, Client, Query, YdbOrCustomerError};

type RepoResult<T> = Result<T, YdbOrCustomerError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxEvent {
    pub event_id: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub payload: String,
    pub created_at: i64,
    pub retry_count: u32,
}

pub struct OutboxRepository {
    client: Client,
}

impl OutboxRepository {
    pub async fn new(config: &YdbConfig) -> RepoResult<Self> {
        let client = create_ydb_client(config)
            .await
            .map_err(YdbOrCustomerError::from)?;
        Ok(Self { client })
    }

    pub async fn fetch_pending(&self, limit: u64) -> RepoResult<Vec<OutboxEvent>> {
        let result = self
            .client
            .table_client()
            .retry_transaction(|mut t| async move {
                let res = t
                    .query(
                        Query::from(
                            "SELECT event_id, aggregate_type, aggregate_id, event_type, payload, created_at, retry_count \
                             FROM outbox WHERE status = 'PENDING' \
                             ORDER BY created_at \
                             LIMIT $limit",
                        ).with_params(ydb_params!("$limit" => limit)),
                    )
                    .await?;
                Ok(res)
            })
            .await?;

        let mut events = Vec::new();
        for mut row in result.into_only_result()?.rows() {
            events.push(OutboxEvent {
                event_id: row.remove_field_by_name("event_id")?.try_into()?,
                aggregate_type: row.remove_field_by_name("aggregate_type")?.try_into()?,
                aggregate_id: row.remove_field_by_name("aggregate_id")?.try_into()?,
                event_type: row.remove_field_by_name("event_type")?.try_into()?,
                payload: row.remove_field_by_name("payload")?.try_into()?,
                created_at: row.remove_field_by_name("created_at")?.try_into()?,
                retry_count: row
                    .remove_field_by_name("retry_count")
                    .ok()
                    .and_then(|v| v.try_into().ok())
                    .unwrap_or(0),
            });
        }
        Ok(events)
    }

    pub async fn mark_sent(&self, event_id: &str) -> RepoResult<()> {
        let eid = event_id.to_string();
        let now = chrono::Utc::now().timestamp();
        self.client
            .table_client()
            .retry_transaction(|mut t| {
                let eid = eid.clone();
                async move {
                    t.query(
                        Query::from(
                            "UPDATE outbox SET status = 'SENT', sent_at = $sent_at \
                             WHERE event_id = $event_id",
                        ).with_params(ydb_params!(
                            "$sent_at" => now,
                            "$event_id" => eid
                        )),
                    )
                        .await?;
                    Ok(())
                }
            })
            .await
    }

    pub async fn mark_retry(&self, event_id: &str, max_retries: u32) -> RepoResult<()> {
        let eid = event_id.to_string();
        self.client
            .table_client()
            .retry_transaction(|mut t| {
                let eid = eid.clone();
                async move {
                    let res = t
                        .query(
                            Query::from(
                                "SELECT retry_count FROM outbox WHERE event_id = $event_id",
                            ).with_params(ydb_params!("$event_id" => eid.clone())),
                        )
                        .await?;
                    let current: u32 = res
                        .into_only_row()?
                        .remove_field_by_name("retry_count")?
                        .try_into()?;

                    let new_status = if current + 1 >= max_retries {
                        "FAILED"
                    } else {
                        "PENDING"
                    };

                    t.query(
                        Query::from(
                            "UPDATE outbox SET retry_count = $retry_count, status = $status \
                             WHERE event_id = $event_id",
                        ).with_params(ydb_params!(
                            "$retry_count" => current + 1,
                            "$status" => new_status,
                            "$event_id" => eid
                        )),
                    )
                        .await?;
                    Ok(())
                }
            })
            .await
    }

    pub async fn count_pending(&self) -> RepoResult<i64> {
        let result = self
            .client
            .table_client()
            .retry_transaction(|mut t| async move {
                let res = t
                    .query(Query::from(
                        "SELECT COUNT(*) AS cnt FROM outbox WHERE status = 'PENDING'",
                    ))
                    .await?;
                Ok(res)
            })
            .await?;

        let cnt: i64 = result
            .into_only_row()?
            .remove_field_by_name("cnt")?
            .try_into()?;
        Ok(cnt)
    }
}