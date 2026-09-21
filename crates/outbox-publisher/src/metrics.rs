use axum::{routing::get, Router};
use prometheus::{Encoder, IntCounter, Histogram, Opts, TextEncoder, Gauge, register_gauge, register_histogram, register_int_counter};

lazy_static::lazy_static! {
    pub static ref OUTBOX_PUBLISHED: IntCounter = register_int_counter!(
        "outbox_published_total",
        "Total number of outbox events successfully published to Kafka"
    ).unwrap();

    pub static ref OUTBOX_ERRORS: IntCounter = register_int_counter!(
        "outbox_errors_total",
        "Total number of outbox publish errors"
    ).unwrap();

    pub static ref OUTBOX_PENDING: Gauge = register_gauge!(
        "outbox_pending_count",
        "Current number of PENDING events in outbox"
    ).unwrap();

    pub static ref OUTBOX_BATCH_SIZE: Histogram = register_histogram!(
        "outbox_batch_size",
        "Size of batches fetched from outbox"
    ).unwrap();
}

pub fn router() -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(|| async { "OK" }))
}

async fn metrics_handler() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}