mod handler;
mod repository;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/bank.account.v1.rs"));
}

use common::config::YdbConfig;
use handler::AccountServiceImpl;
use proto::account_service_server::AccountServiceServer;
use repository::AccountRepository;
use std::net::SocketAddr;
use std::sync::Arc;
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

    let repo = Arc::new(AccountRepository::new(&ydb_config).await?);

    let (mut health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<AccountServiceServer<AccountServiceImpl>>()
        .await;

    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(tonic_reflection::pb::v1::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    let addr: SocketAddr = std::env::var("GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50052".into())
        .parse()?;

    tracing::info!("Account Service listening on {}", addr);

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
        .add_service(AccountServiceServer::new(AccountServiceImpl::new(repo)))
        .serve_with_shutdown(addr, shutdown)
        .await?;

    tracing::info!("Account Service stopped");
    Ok(())
}