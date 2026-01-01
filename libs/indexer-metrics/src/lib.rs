use prometheus::{
    register_counter_vec, register_gauge_vec, register_histogram_vec, CounterVec, GaugeVec,
    HistogramVec, Encoder, TextEncoder,
};

lazy_static::lazy_static! {
    // Listener metrics
    pub static ref LISTENER_BLOCKS_SYNCED: CounterVec = register_counter_vec!(
        "listener_blocks_synced_total",
        "Total number of blocks synced",
        &["chain_id", "address"]
    ).unwrap();

    pub static ref LISTENER_LOGS_FETCHED: CounterVec = register_counter_vec!(
        "listener_logs_fetched_total",
        "Total number of logs fetched from RPC",
        &["chain_id", "address"]
    ).unwrap();

    pub static ref LISTENER_LOGS_SAVED: CounterVec = register_counter_vec!(
        "listener_logs_saved_total",
        "Total number of logs saved to database",
        &["chain_id", "address"]
    ).unwrap();

    pub static ref LISTENER_LOGS_REMOVED: CounterVec = register_counter_vec!(
        "listener_logs_removed_total",
        "Total number of logs marked as removed (reorgs)",
        &["chain_id", "address"]
    ).unwrap();

    pub static ref LISTENER_RPC_ERRORS: CounterVec = register_counter_vec!(
        "listener_rpc_errors_total",
        "Total number of RPC errors",
        &["chain_id", "address", "error_type"]
    ).unwrap();

    pub static ref LISTENER_DB_ERRORS: CounterVec = register_counter_vec!(
        "listener_db_errors_total",
        "Total number of database errors",
        &["chain_id", "address"]
    ).unwrap();

    pub static ref LISTENER_REORGS_DETECTED: CounterVec = register_counter_vec!(
        "listener_reorgs_detected_total",
        "Total number of reorgs detected",
        &["chain_id", "address"]
    ).unwrap();

    pub static ref LISTENER_CURRENT_BLOCK: GaugeVec = register_gauge_vec!(
        "listener_current_block",
        "Current block number being synced",
        &["chain_id", "address"]
    ).unwrap();

    pub static ref LISTENER_BLOCKS_BEHIND: GaugeVec = register_gauge_vec!(
        "listener_blocks_behind",
        "Number of blocks behind latest",
        &["chain_id", "address"]
    ).unwrap();

    pub static ref LISTENER_RPC_REQUEST_DURATION: HistogramVec = register_histogram_vec!(
        "listener_rpc_request_duration_seconds",
        "Duration of RPC requests in seconds",
        &["chain_id", "address", "method"]
    ).unwrap();

    // Processor metrics
    pub static ref PROCESSOR_LOGS_PROCESSED: CounterVec = register_counter_vec!(
        "processor_logs_processed_total",
        "Total number of logs processed",
        &["chain_id", "contract"]
    ).unwrap();

    pub static ref PROCESSOR_LOGS_ERRORS: CounterVec = register_counter_vec!(
        "processor_logs_errors_total",
        "Total number of processing errors",
        &["chain_id", "contract", "error_type"]
    ).unwrap();

    pub static ref PROCESSOR_PROCESSING_DURATION: HistogramVec = register_histogram_vec!(
        "processor_processing_duration_seconds",
        "Duration of log processing in seconds",
        &["chain_id", "contract"]
    ).unwrap();

    pub static ref PROCESSOR_BATCH_SIZE: GaugeVec = register_gauge_vec!(
        "processor_batch_size",
        "Current batch size being processed",
        &["chain_id"]
    ).unwrap();

    pub static ref PROCESSOR_UNPROCESSED_LOGS: GaugeVec = register_gauge_vec!(
        "processor_unprocessed_logs",
        "Number of unprocessed logs in database",
        &["chain_id"]
    ).unwrap();

    pub static ref PROCESSOR_CLEANUP_REMOVED: CounterVec = register_counter_vec!(
        "processor_cleanup_removed_total",
        "Total number of removed logs cleaned up",
        &["chain_id"]
    ).unwrap();
}

/// Collect all metrics and return as Prometheus text format
pub fn gather_metrics() -> Result<String, Box<dyn std::error::Error>> {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer)?;
    Ok(String::from_utf8(buffer)?)
}

