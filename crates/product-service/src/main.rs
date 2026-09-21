mod cache;
mod handler;
mod repository;
mod rules;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/bank.product.v1.rs"));
}

use cache::new_shared_cache;
use common::config::YdbConfig;
use handler::ProductServiceImpl;
use proto::product_service_server::ProductServiceServer;
use repository::ProductRepository;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tonic_reflection::server::Builder as ReflectionBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let ydb_config = YdbConfig::from_env();
    tracing::info!("Connecting to YDB: {}", ydb_config.connection_string);

    let repo = Arc::new(ProductRepository::new(&ydb_config).await?);

    // Первичная загрузка
    let shared_cache = new_shared_cache();
    {
        let products = repo.load_products().await?;
        let terms = repo.load_terms().await?;
        let tariffs = repo.load_tariffs().await?;

        let mut guard = shared_cache.write().await;
        guard.replace(products, terms, tariffs);
        tracing::info!(
            "Cache loaded: {} products, {} terms, {} tariffs",
            guard.products_count(),
            guard.terms_count(),
            guard.tariffs_count()
        );
    }

    // Периодический refresh
    let refresh_repo = repo.clone();
    let refresh_cache = shared_cache.clone();
    let refresh_interval_sec: u64 = std::env::var("PRODUCT_REFRESH_SEC")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);

    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(refresh_interval_sec));
        ticker.tick().await;
        loop {
            ticker.tick().await;
            tracing::info!("Refreshing product cache...");
            match (
                refresh_repo.load_products().await,
                refresh_repo.load_terms().await,
                refresh_repo.load_tariffs().await,
            ) {
                (Ok(p), Ok(t), Ok(tf)) => {
                    let mut guard = refresh_cache.write().await;
                    guard.replace(p, t, tf);
                    tracing::info!(
                        "Cache refreshed: {} products, {} terms, {} tariffs",
                        guard.products_count(),
                        guard.terms_count(),
                        guard.tariffs_count()
                    );
                }
                (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
                    tracing::error!("Failed to refresh product cache: {}", e);
                }
            }
        }
    });

    let (mut health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<ProductServiceServer<ProductServiceImpl>>()
        .await;

    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(tonic_reflection::pb::v1::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    let addr: SocketAddr = std::env::var("GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50057".into())
        .parse()?;

    tracing::info!("Product Service listening on {}", addr);

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

    Server::builder()
        .add_service(health_service)
        .add_service(reflection)
        .add_service(ProductServiceServer::new(ProductServiceImpl::new(
            shared_cache,
        )))
        .serve_with_shutdown(addr, shutdown)
        .await?;

    tracing::info!("Product Service stopped");
    Ok(())
}