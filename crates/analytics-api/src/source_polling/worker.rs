use crate::clock::now_ms;

pub(crate) fn load_worker_id() -> String {
    std::env::var("AUX_ANALYTICS_WORKER_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| std::env::var("ECS_CONTAINER_METADATA_URI_V4").ok())
        .filter(|value| !value.trim().is_empty())
        .or_else(|| std::env::var("ECS_CONTAINER_METADATA_URI").ok())
        .filter(|value| !value.trim().is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| format!("pid-{}-{}", std::process::id(), now_ms()))
}
