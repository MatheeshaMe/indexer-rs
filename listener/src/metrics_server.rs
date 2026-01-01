use axum::{
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Router,
};
use indexer_metrics::gather_metrics;
use tokio::net::TcpListener;

pub async fn start_metrics_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler));

    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    println!("Metrics server listening on http://{}", addr);

    axum::serve(listener, app).await?;
    Ok(())
}

async fn metrics_handler() -> impl IntoResponse {
    match gather_metrics() {
        Ok(metrics) => (StatusCode::OK, metrics),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error gathering metrics: {}", e),
        ),
    }
}

async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

