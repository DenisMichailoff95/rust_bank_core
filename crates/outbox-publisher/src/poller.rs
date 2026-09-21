use crate::metrics::{OUTBOX_BATCH_SIZE, OUTBOX_ERRORS, OUTBOX_PENDING, OUTBOX_PUBLISHED};
use crate::repository::OutboxRepository;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info, warn};

const MAX_RETRIES: u32 = 5;

pub async fn run_poller(
    repo: Arc<OutboxRepository>,
    batch_size: i64,
    poll_interval_ms: u64,
    kafka_topic: String,
) {
    // Создаём Kafka producer (или заглушку, если фича отключена)
    #[cfg(feature = "kafka")]
    let producer = {
        use rdkafka::config::ClientConfig;
        let brokers = std::env::var("KAFKA_BROKERS")
            .unwrap_or_else(|_| "localhost:9092".into());
        let producer: rdkafka::producer::FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .create()
            .expect("Kafka producer creation error");
        Arc::new(producer)
    };

    let mut ticker = interval(Duration::from_millis(poll_interval_ms));
    let mut metrics_tick = 0u64;

    loop {
        ticker.tick().await;
        metrics_tick += 1;

        // Каждые 20 итераций обновляем метрику pending
        if metrics_tick % 20 == 0 {
            if let Ok(cnt) = repo.count_pending().await {
                OUTBOX_PENDING.set(cnt);
            }
        }

        let events = match repo.fetch_pending(batch_size).await {
            Ok(ev) => ev,
            Err(e) => {
                error!("Failed to fetch outbox: {}", e);
                OUTBOX_ERRORS.inc();
                continue;
            }
        };

        if events.is_empty() {
            continue;
        }

        OUTBOX_BATCH_SIZE.observe(events.len() as f64);
        info!("Publishing {} events to Kafka", events.len());

        for event in events {
            #[cfg(feature = "kafka")]
            {
                use rdkafka::producer::FutureRecord;
                let record = FutureRecord::to(&kafka_topic)
                    .key(&event.aggregate_id)
                    .payload(&event.payload);

                match producer.send(record, Duration::from_secs(5)).await {
                    Ok(_) => {
                        if let Err(e) = repo.mark_sent(&event.event_id).await {
                            error!("Failed to mark {} SENT: {}", event.event_id, e);
                        } else {
                            OUTBOX_PUBLISHED.inc();
                        }
                    }
                    Err((e, _)) => {
                        warn!("Kafka send failed for {}: {}", event.event_id, e);
                        OUTBOX_ERRORS.inc();
                        let _ = repo.mark_retry(&event.event_id, MAX_RETRIES).await;
                    }
                }
            }

            #[cfg(not(feature = "kafka"))]
            {
                info!(
                    "Event: {} {} {}",
                    event.event_type, event.aggregate_id, event.event_id
                );
                let _ = repo.mark_sent(&event.event_id).await;
                OUTBOX_PUBLISHED.inc();
            }
        }
    }
}