use std::{future::IntoFuture, sync::Arc, time::Duration};

use tokio::signal;

use crate::background_jobs::BackgroundJobRuntime;

pub(super) async fn await_shutdown_signal(
    background_jobs: Option<Arc<BackgroundJobRuntime>>,
    grace: Duration,
) {
    wait_for_shutdown_signal().await;
    tracing::info!(target = "shutdown", "shutdown signal received");
    if let Some(runtime) = background_jobs.as_ref() {
        runtime.shutdown().await;
    }
    tokio::time::sleep(grace).await;
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    if let Ok(mut terminate) = signal::unix::signal(signal::unix::SignalKind::terminate()) {
        tokio::select! {
            result = signal::ctrl_c() => {
                if result.is_err() { tracing::warn!(target = "shutdown", "failed to await Ctrl+C shutdown signal"); }
            }
            _ = terminate.recv() => {}
        }
        return;
    }
    if signal::ctrl_c().await.is_err() {
        tracing::warn!(target = "shutdown", "failed to await shutdown signal");
    }
}

pub(super) fn shutdown_grace_period() -> Duration {
    parse_shutdown_grace_period(std::env::var("APP_SHUTDOWN_GRACE_SECONDS").ok().as_deref())
}

pub(crate) fn parse_shutdown_grace_period(raw: Option<&str>) -> Duration {
    raw.and_then(|raw| raw.parse::<u64>().ok())
        .map_or_else(|| Duration::from_secs(5), Duration::from_secs)
}

pub(super) async fn serve_requests(
    listener: tokio::net::TcpListener,
    router: axum::Router,
    background_jobs: Option<Arc<BackgroundJobRuntime>>,
    web: Option<&lambda_web_runtime::LambdaWebEnvironment>,
) -> std::io::Result<()> {
    let router = attach_release_identity(router)?;
    let grace = web.map_or_else(shutdown_grace_period, |_| Duration::ZERO);
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let signal = async move {
        await_shutdown_signal(background_jobs, grace).await;
        let _ = sender.send(());
    };
    let server = axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = receiver.await;
        })
        .into_future();
    if let Some(web) = web {
        return web
            .run_until_shutdown(server, signal)
            .await
            .unwrap_or(Ok(()));
    }
    tokio::pin!(server);
    tokio::pin!(signal);
    tokio::select! {
        result = &mut server => result,
        () = &mut signal => server.await,
    }
}

fn attach_release_identity(mut app: axum::Router) -> Result<axum::Router, std::io::Error> {
    if let Ok(release) = std::env::var("AUX_RELEASE_ID") {
        app = app.layer(tower_http::set_header::SetResponseHeaderLayer::overriding(
            axum::http::HeaderName::from_static("x-aux-release-id"),
            axum::http::HeaderValue::from_str(&release).map_err(std::io::Error::other)?,
        ));
    }
    Ok(app)
}
