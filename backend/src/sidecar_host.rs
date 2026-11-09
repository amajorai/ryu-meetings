//! The sidecar's standalone [`MeetingsHost`] and [`SttHost`] shims (see the
//! `main.rs` module docs for what each host callback reaches).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_trait::async_trait;
use ryu_meetings::MeetingsHost;
use ryu_stt::SttHost;
use serde_json::json;

use crate::paths;

/// The bundled local default notes/title model when no pref/env is set — mirrors
/// Core's `registry::DEFAULT_LLM_MODEL`. Nothing is hardcoded to a remote provider;
/// a pref/env (`RYU_MEETING_NOTES_MODEL` / `RYU_DEFAULT_LLM_MODEL`) still overrides it.
const DEFAULT_NOTES_MODEL: &str = "gemma-4-E2B-it-Q4_K_M";

/// The local gateway default (mirrors `apps/core/src/sidecar/gateway.rs`).
const DEFAULT_GATEWAY_URL: &str = "http://127.0.0.1:7981";

/// The local whisper.cpp voice server default (mirrors Core's `WHISPER_ADDR`).
const DEFAULT_WHISPER_URL: &str = "http://127.0.0.1:8090";

/// This app's plugin id (mirrors Core's `plugins::builtins::MEETINGS_PLUGIN_ID`).
/// Presented on the `x-ryu-plugin-id` header of the save-notes host callback so
/// Core's `authenticate_sidecar` recomputes the matching ext token.
const MEETINGS_PLUGIN_ID: &str = "@ryu/meetings";

/// The `x-ryu-plugin-id` header Core's `authenticate_sidecar` reads — mirrors
/// `apps/core/src/sidecar/ext_proxy.rs::HDR_PLUGIN_ID`.
const HDR_PLUGIN_ID: &str = "x-ryu-plugin-id";

/// Core's loopback port default when `RYU_CORE_PORT` is unset (mirrors Core's
/// bind default). Core injects `RYU_CORE_PORT` (profile-shifted) at spawn.
const DEFAULT_CORE_PORT: u16 = 7980;

// ── env resolution (shared by both host shims, "nothing hardcoded") ────────────

/// The local Gateway base URL (env-first, else the local gateway default port).
fn gateway_url_env() -> String {
    std::env::var("RYU_GATEWAY_URL")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_GATEWAY_URL.to_string())
}

/// The Gateway bearer token, if one is configured (`None` when unset — never the
/// fabricated `"ryu-local"` literal; that fallback is the STT bearer's, below).
fn gateway_token_env() -> Option<String> {
    std::env::var("RYU_GATEWAY_TOKEN")
        .ok()
        .filter(|s| !s.is_empty())
}

/// The sidecar's [`SttHost`]: resolves the whisper base-url, Gateway url/bearer, and
/// parakeet model dir from env / the shared models layout. Mirrors Core's `CoreSttHost`, but every
/// value is env-resolved rather than read from Core config. The parakeet dir is
/// never read here (the sidecar builds `ryu_stt` at default features, so the engine
/// resolves to whisper); it is provided for completeness.
struct SidecarSttHost;

impl SttHost for SidecarSttHost {
    fn whisper_base_url(&self) -> String {
        std::env::var("RYU_WHISPER_URL")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_WHISPER_URL.to_string())
    }

    fn audio_cpp_base_url(&self) -> String {
        std::env::var("RYU_AUDIOCPP_URL")
            .ok()
            .map(|value| value.trim().trim_end_matches('/').to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| {
                let port = std::env::var("RYU_AUDIOCPP_PORT")
                    .ok()
                    .and_then(|value| value.trim().parse::<u16>().ok())
                    .unwrap_or_else(|| {
                        let offset = std::env::var("RYU_PORT_OFFSET")
                            .ok()
                            .and_then(|value| value.trim().parse::<u16>().ok())
                            .unwrap_or(0);
                        8086u16.saturating_add(offset)
                    });
                format!("http://127.0.0.1:{port}")
            })
    }

    fn gateway_url(&self) -> String {
        gateway_url_env()
    }

    fn gateway_bearer(&self) -> Result<String, String> {
        // Mirrors `gateway::gateway_bearer` local path: a configured token wins,
        // else fall back to the local gateway's `"ryu-local"` dev bearer (the local
        // gateway accepts it). The sidecar is a local data plane, so there is no
        // remote-fleet fail-closed branch here.
        Ok(gateway_token_env().unwrap_or_else(|| "ryu-local".to_string()))
    }

    fn parakeet_model_dir(&self) -> PathBuf {
        ryu_sidecar_runtime::layout::parakeet_v3_dir(&paths::ryu_dir())
    }
}

/// The sidecar's standalone [`MeetingsHost`]: everything the moved meeting code needs
/// from the host, provided by the process itself rather than by Core.
pub(crate) struct SidecarMeetingsHost {
    prefs_path: PathBuf,
    prefs: Mutex<HashMap<String, String>>,
    client: reqwest::Client,
    /// Core's loopback base URL (`http://127.0.0.1:<RYU_CORE_PORT>`), resolved once —
    /// the save-notes host callback target.
    core_base: String,
    /// The injected ext bearer (`RYU_EXT_TOKEN`) presented on the callback. `None`
    /// leaves the callback fail-closed (Core rejects it, and filing degrades to `None`).
    ext_token: Option<String>,
}

impl SidecarMeetingsHost {
    pub(crate) fn new(prefs_path: PathBuf) -> Self {
        let prefs = load_prefs(&prefs_path);
        let core_port: u16 = std::env::var("RYU_CORE_PORT")
            .ok()
            .and_then(|p| p.trim().parse().ok())
            .unwrap_or(DEFAULT_CORE_PORT);
        let ext_token = std::env::var("RYU_EXT_TOKEN")
            .ok()
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty());
        Self {
            prefs_path,
            prefs: Mutex::new(prefs),
            client: reqwest::Client::new(),
            core_base: format!("http://127.0.0.1:{core_port}"),
            ext_token,
        }
    }
}

/// Read the persisted preference map (empty on missing/corrupt file — a fresh
/// install just falls back to defaults).
fn load_prefs(path: &Path) -> HashMap<String, String> {
    let Ok(bytes) = std::fs::read(path) else {
        return HashMap::new();
    };
    serde_json::from_slice(&bytes).unwrap_or_default()
}

/// Persist the preference map atomically (write a temp file, then rename) so a
/// crash mid-write cannot corrupt the live config file.
fn save_prefs(path: &Path, map: &HashMap<String, String>) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let bytes = serde_json::to_vec_pretty(map).map_err(|e| e.to_string())?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, &bytes).map_err(|e| e.to_string())?;
    std::fs::rename(&tmp, path).map_err(|e| e.to_string())
}

#[async_trait]
impl MeetingsHost for SidecarMeetingsHost {
    async fn pref_get(&self, key: &str) -> Option<String> {
        self.prefs.lock().ok()?.get(key).cloned()
    }

    async fn pref_set(&self, key: &str, value: &str) -> Result<(), String> {
        // Hold the lock across the write: concurrent sets otherwise race on the
        // shared `.json.tmp` path (one rename fails, or an older snapshot lands
        // last and silently drops the newer preference).
        let mut guard = self
            .prefs
            .lock()
            .map_err(|_| "preferences lock poisoned".to_string())?;
        guard.insert(key.to_string(), value.to_string());
        save_prefs(&self.prefs_path, &guard)
    }

    fn gateway_url(&self) -> String {
        gateway_url_env()
    }

    fn gateway_token(&self) -> Option<String> {
        gateway_token_env()
    }

    fn default_notes_model(&self) -> String {
        std::env::var("RYU_DEFAULT_LLM_MODEL")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_NOTES_MODEL.to_string())
    }

    async fn transcribe(
        &self,
        wav: Vec<u8>,
        filename: String,
        engine: Option<String>,
    ) -> Result<String, String> {
        // Genuine STT via the extracted `ryu_stt` crate — the SAME data path Core
        // runs, injecting the env-resolved `SidecarSttHost`. See the module docs for
        // the audio-double-hop note (the flagged hot path).
        ryu_stt::transcribe_wav(
            &self.client,
            &SidecarSttHost,
            wav,
            filename,
            engine.as_deref(),
        )
        .await
    }

    async fn generate_title(&self, summary: &str) -> Option<String> {
        // A real Gateway chat completion (mirrors `notes::complete`) so auto-naming
        // works out-of-process. Best-effort: any failure / empty reply → `None`, and
        // the engine leaves the existing title alone.
        let summary = summary.trim();
        if summary.len() < 16 {
            return None;
        }
        let title = gateway_title(
            &self.client,
            &gateway_url_env(),
            gateway_token_env().as_deref(),
            &self.default_notes_model(),
            summary,
        )
        .await;
        match title {
            Some(t) if !t.trim().is_empty() => Some(t.trim().to_string()),
            _ => None,
        }
    }

    async fn save_notes_to_space(&self, title: &str, markdown: &str) -> Option<(String, String)> {
        // Filing into the "Meetings" Space reaches Core's `SpaceStore` +
        // background-owner tenancy, which the sidecar does not host. Post the notes to
        // Core's host callback, which files them under the background owner and returns
        // the `{space_id, doc_id}` the engine attaches to the meeting row. Best-effort:
        // any transport/auth/2xx failure degrades to `None` (notes still persist in
        // `meetings.db`; they are just not mirrored into a Space).
        let Some(token) = self.ext_token.as_deref() else {
            tracing::warn!(
                "ryu-meetings: no RYU_EXT_TOKEN for the save-notes callback; notes persist in meetings.db but are not mirrored into the Meetings Space"
            );
            return None;
        };
        let body = json!({ "title": title, "markdown": markdown });
        let resp = self
            .client
            .post(format!(
                "{}/api/host/capability/spaces.fileNotes",
                self.core_base
            ))
            .timeout(std::time::Duration::from_secs(30))
            .bearer_auth(token)
            .header(HDR_PLUGIN_ID, MEETINGS_PLUGIN_ID)
            .json(&body)
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            tracing::warn!(
                "ryu-meetings: save-notes callback failed (HTTP {}); notes not mirrored into the Meetings Space",
                resp.status()
            );
            return None;
        }
        let parsed: serde_json::Value = resp.json().await.ok()?;
        let space_id = parsed.get("space_id").and_then(|v| v.as_str())?.to_string();
        let doc_id = parsed.get("doc_id").and_then(|v| v.as_str())?.to_string();
        Some((space_id, doc_id))
    }
}

/// One Gateway chat completion that returns a concise meeting title from a summary.
/// Mirrors `notes::complete`'s request shape (`/v1/chat/completions`, `stream:false`,
/// optional bearer). Returns the raw assistant text, or `None` on any error.
async fn gateway_title(
    client: &reqwest::Client,
    gateway_url: &str,
    gateway_token: Option<&str>,
    model: &str,
    summary: &str,
) -> Option<String> {
    const SYSTEM: &str = "You name meetings. Given a short summary, reply with ONLY a concise, specific title of at most 8 words. No quotes, no punctuation at the end, no preamble.";
    let base = gateway_url.trim_end_matches('/');
    let payload = json!({
        "model": model,
        "stream": false,
        "messages": [
            { "role": "system", "content": SYSTEM },
            { "role": "user", "content": format!("Summary:\n\n{summary}") },
        ],
    });
    let mut req = client
        .post(format!("{base}/v1/chat/completions"))
        .timeout(std::time::Duration::from_secs(30))
        .json(&payload);
    if let Some(t) = gateway_token {
        req = req.bearer_auth(t);
    }
    let resp = req.send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body: serde_json::Value = resp.json().await.ok()?;
    let text = body
        .get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("message"))
        .and_then(|m| m.get("content"))
        .and_then(|t| t.as_str())
        .unwrap_or_default()
        .trim()
        .trim_matches('"')
        .to_string();
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

#[cfg(test)]
#[path = "sidecar_host_tests.rs"]
mod tests;
