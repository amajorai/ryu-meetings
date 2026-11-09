//! Best-effort Shadow capture control: Shadow is the device-local sensor that
//! records the mic + system loopback and streams chunks back to the sidecar.

use std::time::Duration;

use super::MeetingEngine;

/// Default Shadow base URL Core uses to drive device-local capture. Overridable
/// via `RYU_SHADOW_URL` (the "nothing hardcoded" knob); Shadow is the local
/// sensor, so this stays loopback by default.
pub(crate) fn shadow_url() -> String {
    std::env::var("RYU_SHADOW_URL")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "http://127.0.0.1:3030".to_string())
}

/// The shared-secret bearer Shadow's HTTP surface requires (everything except
/// `/health` — see `apps/shadow/src/server.rs`). Read from `SHADOW_API_TOKEN`,
/// the SAME env var Shadow itself resolves: Core injects it at sidecar spawn
/// (`manifest_sidecar::inject_ext_env`), and an operator export works for a
/// standalone run. `None` = send no bearer; Shadow then 401s and the
/// best-effort capture-control calls degrade exactly like an absent Shadow.
///
/// Core also passes the token file's path (`SHADOW_API_TOKEN_FILE`). The file
/// is read on every call and wins over the env value, because Core can rotate
/// the token after this sidecar was spawned.
pub(crate) fn shadow_token() -> Option<String> {
    resolve_shadow_token(
        std::env::var_os("SHADOW_API_TOKEN_FILE"),
        std::env::var("SHADOW_API_TOKEN").ok(),
    )
}

/// The token from the file at `file` when it is readable and non-empty, else
/// `env_token`. Either value is trimmed; an empty one counts as absent.
pub(crate) fn resolve_shadow_token(
    file: Option<std::ffi::OsString>,
    env_token: Option<String>,
) -> Option<String> {
    let non_empty = |value: String| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    };
    file.and_then(|path| std::fs::read_to_string(path).ok())
        .and_then(non_empty)
        .or_else(|| env_token.and_then(non_empty))
}

/// Recent successful Shadow peer proofs (see [`send_to_shadow`]).
pub(crate) static SHADOW_PEER_PROOF: ryu_sidecar_runtime::peer_proof::ProofCache =
    ryu_sidecar_runtime::peer_proof::ProofCache::new(Duration::from_secs(2));

/// Send a capture-control request to Shadow with its bearer — but only after
/// the listener proved it holds the token (`ryu_sidecar_runtime::peer_proof`),
/// so a process squatting on Shadow's port gets neither the token nor the
/// request. Without a token nothing is sent (Shadow would 401 it anyway).
pub(crate) async fn send_to_shadow(
    request: reqwest::RequestBuilder,
) -> Result<reqwest::Response, String> {
    let token = shadow_token().ok_or_else(|| "no Shadow API token is configured".to_owned())?;
    ryu_sidecar_runtime::peer_proof::send_with_verified_bearer(request, &token, &SHADOW_PEER_PROOF)
        .await
        .map_err(|error| error.to_string())
}

impl MeetingEngine {
    pub(crate) async fn notify_shadow_start(&self, meeting_id: &str) {
        let url = format!("{}/meeting/start", shadow_url().trim_end_matches('/'));
        let body = serde_json::json!({ "meeting_id": meeting_id });
        let req = self
            .http
            .post(&url)
            .timeout(Duration::from_secs(5))
            .json(&body);
        if let Err(e) = send_to_shadow(req).await {
            tracing::debug!(
                "meetings: shadow capture start not available ({e}); GUI mic can still feed chunks"
            );
        }
    }

    pub(crate) async fn notify_shadow_stop(&self, meeting_id: &str) {
        let url = format!("{}/meeting/stop", shadow_url().trim_end_matches('/'));
        let body = serde_json::json!({ "meeting_id": meeting_id });
        let req = self
            .http
            .post(&url)
            .timeout(Duration::from_secs(5))
            .json(&body);
        let _ = send_to_shadow(req).await;
    }
}
