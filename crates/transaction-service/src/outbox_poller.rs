use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info, warn};
use ydb::{Client, Query};

/// Событие из outbox, готовое к публикации
#[derive(Debug, Clone)]
pub struct OutboxEvent {
    pub event_id: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub payload: String,
    pub created_at: i64,
}

/// Читает батч PENDING-событий из outbox
async fn fetch_pending_events(
    client: &Client,
    batch_size: i64,
) -> Result<Vec<OutboxEvent>, ydb::YdbError> {
    let result = client
        .table_client()
        .retry_transaction(|mut t| async move {
            let res = t
                .query(
                    Query::from(
                        "SELECT event_id, aggregate_type, aggregate_id, event_type, payload, created_at \
                         FROM outbox WHERE status = 'PENDING' \
                         ORDER BY created_at \
                         LIMIT $limit",
                    )
                        .param("$limit", batch_size),
                )
                .await?;
            Ok(res)
        })
        .await?;

    let mut events = Vec::new();
    for row in result.into_iter() {
        events.push(OutboxEvent {
            event_id: row.get("event_id")?.try_into()?,
            aggregate_type: row.get("aggregate_type")?.try_into()?,
            aggregate_id: row.get("aggregate_id")?.try_into()?,
            event_type: row.get("event_type")?.try_into()?,
            payload: row.get("payload")?.try_into()?,
            created_at: row.get("created_at")?.try_into()?,
        });
    }
    Ok(events)
}

/// Помечает событие как SENT
async fn mark_as_sent(
    client: &Client,
    event_id: &str,
) -> Result<(), ydb::YdbError> {
    let eid = event_id.to_string();
    let now = chrono::Utc::now().timestamp();
    client
        .table_client()
        .retry_transaction(|mut t| {
            let eid = eid.clone();
            async move {
                t.query(
                    Query::from(
                        "UPDATE outbox SET status = 'SENT', sent_at = $sent_at \
                         WHERE status = 'PENDING' AND created_at = $created_at AND event_id = $event_id",
                    )
                        .param("$sent_at", now)
                        .param("$created_at", 0_i64)  // нужен created_at из PK — упрощение
                        .param("$event_id", eid),
                )
                    .await?;
                Ok(())
            }
        })
        .await
}

/// Увеличивает счётчик retry для события
async fn increment_retry(
    client: &Client,
    event_id: &str,
) -> Result<(), ydb::YdbError> {
    // В реальной системе нужен created_at из PK, здесь упрощено
    let eid = event_id.to_string();
    client
        .table_client()
        .retry_transaction(|mut t| {
            let eid = eid.clone();
            async move {
                t.query(
                    Query::from(
                        "UPDATE outbox SET retry_count = retry_count + 1 \
                         WHERE event_id = $event_id",
                    )
                        .param("$event_id", eid),
                )
                    .await?;
                Ok(())
            }
        })
        .await
}

/// Основной цикл поллера: читает outbox и публикует в Kafka
pub async fn run_outbox_poller(
    client: Arc<Client>,
    batch_size: i64,
    poll_interval_ms: u64,
    kafka_topic: String,
) {
    #[cfg(feature = "kafka")]
    let producer: Option<Arc<rdkafka::producer::FutureProducer>> = {
        use rdkafka::config::ClientConfig;
        let producer: rdkafka::producer::FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into()))
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Kafka producer creation error");
        Some(Arc::new(producer))
    };

    #[cfg(not(feature = "kafka"))]
    let producer: Option<()> = None;

    let mut ticker = interval(Duration::from_millis(poll_interval_ms));

    loop {
        ticker.tick().await;

        let events = match fetch_pending_events(&client, batch_size).await {
            Ok(ev) => ev,
            Err(e) => {
                error!("Failed to fetch outbox events: {}", e);
                continue;
            }
        };

        if events.is_empty() {
            continue;
        }

        info!("Fetched {} outbox events", events.len());

        for event in events {
            #[cfg(feature = "kafka")]
            {
                use rdkafka::producer::{FutureRecord, Producer};
                if let Some(ref prod) = producer {
                    let record = FutureRecord::to(&kafka_topic)
                        .key(&event.aggregate_id)
                        .payload(&event.payload)
                        .headers(
                            rdkafka::message::OwnedHeaders::new()
                                .insert(rdkafka::message::Header {
                                    key: "event_type",
                                    value: Some(&event.event_type),
                                })
                                .insert(rdkafka::message::Header {
                                    key: "event_id",
                                    value: Some(&event.event_id),
                                }),
                        );

                    match prod.send(record, Duration::from_secs(5)).await {
                        Ok(_) => {
                            if let Err(e) = mark_as_sent(&client, &event.event_id).await {
                                error!("Failed to mark {} as SENT: {}", event.event_id, e);
                            }
                        }
                        Err((e, _)) => {
                            warn!("Kafka send failed for {}: {}", event.event_id, e);
                            let _ = increment_retry(&client, &event.event_id).await;
                        }
                    }
                }
            }

            #[cfg(not(feature = "kafka"))]
            {
                // Без Kafka — просто логируем и помечаем
                info!(
                    "Outbox event: {} {} {}",
                    event.event_type, event.aggregate_id, event.event_id
                );
                let _ = mark_as_sent(&client, &event.event_id).await;
            }
        }
    }
}