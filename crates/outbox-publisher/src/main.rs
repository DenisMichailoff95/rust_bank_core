mod metrics;
mod poller;
mod repository;

use common::config::YdbConfig;
use poller::run_poller;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let ydb_config = YdbConfig::from_env();
    tracing::info!(
        "Outbox Publisher connecting to YDB: {}",
        ydb_config.connection_string
    );

    let repo = Arc::new(repository::OutboxRepository::new(&ydb_config).await?);

    let metrics_addr = std::env::var("METRICS_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:9100".into());
    let metrics_app = metrics::router();
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&metrics_addr).await.unwrap();
        tracing::info!("Metrics endpoint on http://{}", metrics_addr);
        axum::serve(listener, metrics_app).await.unwrap();
    });

    let batch_size: i64 = std::env::var("OUTBOX_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let poll_interval_ms: u64 = std::env::var("OUTBOX_POLL_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500);
    let kafka_topic = std::env::var("KAFKA_TOPIC")
        .unwrap_or_else(|_| "bank.events".into());

    tracing::info!(
        "Outbox Publisher started: batch={}, interval={}ms, topic={}",
        batch_size,
        poll_interval_ms,
        kafka_topic
    );

    let shutdown = async {
        let mut sigterm = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate(),
        )
            .unwrap();
        let mut sigint = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::interrupt(),
        )
            .unwrap();
        tokio::select! {
            _ = sigterm.recv() => tracing::info!("SIGTERM received"),
            _ = sigint.recv() => tracing::info!("SIGINT received"),
        }
    };

    tokio::select! {
        _ = run_poller(repo, batch_size, poll_interval_ms, kafka_topic) => {},
        _ = shutdown => tracing::info!("Shutting down Outbox Publisher"),
    }

    Ok(())
}