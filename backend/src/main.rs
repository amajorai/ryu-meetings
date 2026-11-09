//! `ryu-meetings` — the standalone, out-of-process meeting-notes sidecar.
//!
//! Runs the extracted `ryu_meetings` capability crate (the SQLite [`MeetingStore`] +
//! the [`MeetingEngine`] + the audio/diarize pipeline + the `/api/meetings/*`
//! surface, defined in `lib.rs` / `api.rs`) as a SEPARATE PROCESS that Core spawns,
//! health-checks, and proxies to on loopback — exactly like `ryu-quests` /
//! `ryu-teams` / `ryu-mail`. The store, engine, pipeline, and handlers live in the
//! crate lib; this binary is only the process shell around them, so the SAME crate
//! still compiles into Core in-process as a path dependency (no code is duplicated).
//!
//! The crate's [`ryu_meetings::routes`] returns a state-baked, state-less
//! `Router<()>` whose paths are RELATIVE to `/api/meetings` (Core nests it at that
//! prefix in-process). This binary nests it under the same `/api/meetings` prefix, so
//! the external paths are byte-identical to Core's in-process mount and the generic
//! ext-proxy forwards `/api/meetings/*` to it unchanged.
//!
//! SECURITY: loopback-only bind (127.0.0.1) + a shared-secret bearer gate
//! (`RYU_EXT_TOKEN`, injected by Core at spawn and presented on the health probe +
//! every proxied hop). EVERY `/api/meetings/*` route is protected. The gate is
//! FAIL-CLOSED: with no token configured every protected route rejects with 401.
//! `/health` is the ONE un-gated route (loopback probe, returns no meeting data), so
//! Core's pre-auth health check succeeds.
//!
//! Port: `RYU_MEETINGS_PORT` env, default `7998`. Data: the app's plugin data dir via
//! `paths::data_dir` (`RYU_PLUGIN_DATA_DIR`, injected by Core at spawn;
//! `<data dir>/plugin-data/@ryu+meetings`), holding `meetings.db`, `prefs.json` and
//! the persisted diarization PCM under `audio/`.
//!
//! HOST SHIM (the sidecar's [`ryu_meetings::MeetingsHost`] impl): this crate inverts
//! every cross-cutting Core call through the host trait. In-process, Core wires these
//! to its real machinery (`apps/core/src/meetings_host.rs`). Out-of-process this shell
//! provides standalone implementations for the ones the sidecar can own by itself:
//!
//! - **preferences** → `prefs.json` in the plugin data dir (durable across restarts);
//! - **transcription (STT)** → the extracted [`ryu_stt`] crate, injecting an
//!   env-resolved [`SidecarSttHost`]. This is the SAME code path Core runs
//!   (whisper.cpp `/inference` or the Gateway `/v1/audio/transcriptions`), so the
//!   sidecar transcribes genuinely — no stub. NOTE (flagged hot path): a chunk
//!   arrives here over multipart, then this callback forwards the audio to the STT
//!   engine — the "audio double-hop". It is inherent (Core's in-process path hops to
//!   whisper.cpp too); with no `public_mount` yet the chunk also proxies through Core
//!   first, adding one hop until CoreDecouple lands the public mount.
//! - **Gateway note-gen + auto-title** → env `RYU_GATEWAY_URL` / `RYU_GATEWAY_TOKEN`
//!   (mirroring `apps/core/src/sidecar/gateway.rs`); `generate_title` runs a real
//!   Gateway chat completion here, so auto-naming works out-of-process.
//! - **notes → Space (`save_notes_to_space`)** → a Core host callback. Filing into the
//!   "Meetings" Space reaches Core's `SpaceStore` + background-owner tenancy, which the
//!   sidecar does not host, so this posts `{title, markdown}` to Core's
//!   `POST /api/host/capability/spaces.fileNotes` (ext-bearer + `x-ryu-plugin-id` authed, the
//!   monitors-callback posture). Core files the notes under the background owner and
//!   returns `{space_id, doc_id}`, which the engine attaches to the meeting row — so a
//!   decoupled node mirrors notes into the Meetings Space exactly as the in-process
//!   node did. If the callback is unreachable/unauthed it degrades to `None` (notes
//!   still persist in `meetings.db`; they are just not mirrored into a Space).

mod paths;
mod sidecar_host;

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::{
    extract::Request,
    http::{header::AUTHORIZATION, StatusCode},
    middleware::{from_fn, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde_json::json;

use ryu_meetings::{routes, MeetingEngine, MeetingStore, MeetingsCtx, MeetingsHost};
use sidecar_host::SidecarMeetingsHost;

/// Default loopback port for the meetings sidecar (overridable via
/// `RYU_MEETINGS_PORT`). 7998 is free (7990 finetune · 7991 quests · 7992 clips ·
/// 7993 browser · 7994 teams · 7995 research · 7996 mail · 7997 dashboards are
/// taken). Kept identical in `meetings.manifest.json`.
const DEFAULT_PORT: u16 = 7998;

fn main() -> anyhow::Result<()> {
    // Take the sealing key Core delivered out of the environment before any
    // thread starts (the async runtime spawns workers).
    ryu_meetings::seal_keys::capture()?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let port: u16 = std::env::var("RYU_MEETINGS_PORT")
        .ok()
        .and_then(|p| p.trim().parse().ok())
        .unwrap_or(DEFAULT_PORT);

    // Shared-secret bearer Core injects via the generic ext-proxy loader
    // (`RYU_EXT_TOKEN`) — the per-plugin minted secret it stamps on every proxied
    // hop + the health probe. The protected `/api/meetings/*` routes require it.
    let token = std::env::var("RYU_EXT_TOKEN")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty());
    if token.is_some() {
        tracing::info!(
            "ryu-meetings: protected /api/meetings/* routes require the injected shared-secret bearer"
        );
    } else {
        tracing::warn!(
            "ryu-meetings: no RYU_EXT_TOKEN set; protected /api/meetings/* routes are FAIL-CLOSED (reject all). Core injects this token when it spawns the sidecar."
        );
    }

    let dir = paths::data_dir();
    // Publish the plugin data dir so the engine's persisted diarization PCM lands in
    // `audio/` beside the database (`data_dir()` reads this OnceLock).
    ryu_meetings::init_data_dir(dir.clone());
    let keys = ryu_meetings::seal_keys::PluginKeys::resolve(ryu_meetings::store::PLUGIN_ID, &dir)?;
    let store = MeetingStore::open_with_keys(dir.join("meetings.db"), &keys)?;
    // Seal legacy plaintext PCM files in the background and record their counts.
    {
        let store = store.clone();
        tokio::spawn(async move {
            let seal = store.audio_seal().clone();
            let sealed = tokio::task::spawn_blocking(move || {
                ryu_meetings::audio::seal_legacy_pcm_files(&seal)
            })
            .await
            .unwrap_or_default();
            for (meeting_id, records) in sealed {
                let _ = store.set_pcm_records(&meeting_id, records).await;
            }
        });
    }

    // The sidecar host shim: preferences persist to `prefs.json` in the data dir (so
    // a `detection-config`/notes-model change survives a restart, matching the
    // in-process PreferencesStore-backed behaviour); STT reuses `ryu_stt`; note-gen
    // + auto-title reach the Gateway; Spaces filing degrades (see the module docs).
    let host: Arc<dyn MeetingsHost> = Arc::new(SidecarMeetingsHost::new(dir.join("prefs.json")));
    let engine = MeetingEngine::new(store.clone(), host, reqwest::Client::new());

    // Publish the process-global engine (mirrors `ryu-quests`). In the sidecar its
    // off-`ServerState` readers do not run, so it is an inert-but-harmless consumer;
    // the HTTP handlers use the state-baked `MeetingsCtx` below.
    ryu_meetings::set_global_engine(engine.clone());

    // The recap tick: a retention sweep right now (boot), then every five minutes
    // the sweep, stale-session finalize and the evening digests. The sidecar is
    // declared `lazy: false`, so this runs without a client connected.
    ryu_meetings::recap::tick::spawn(engine.clone());

    // The crate router (paths relative to `/api/meetings`) nested under the external
    // prefix, with the shared-secret gate layered over the whole nest — meetings has
    // no public route. `from_fn` closes over the resolved token so no extra state
    // field is needed.
    let gated_token = token.clone();
    //
    // `/openapi.json` rides INSIDE that same gate, at the SERVER ROOT. Core fetches
    // `http://127.0.0.1:<port>/openapi.json` on this sidecar's first Healthy edge and
    // lowers every operation it finds into searchable LLM tools, so routing this one
    // endpoint is what makes the whole `/api/meetings` surface callable by an agent
    // (`ryu_meetings::api::openapi()` was dead code until now — only tests read it).
    //
    // Root, not under `/api/meetings`: Core tries the root FIRST and only falls back
    // to the mount-prefixed form, and keeping the document off the mount keeps it out
    // of the manifest's declared `http.routes[]` — anything declared there is
    // reachable through the generic ext-proxy, and the schema is Core's to read, not
    // an app surface. Inside the gate, not next to the un-gated `/health`: Core stamps
    // the injected `RYU_EXT_TOKEN` on the fetch (the Python sidecars already require
    // the bearer for everything but `/health`), so the gate costs the fetcher nothing
    // — while un-gated it would disclose this app's entire internal API surface to any
    // other process on loopback.
    let meetings = Router::new()
        .nest("/api/meetings", routes(MeetingsCtx::new(engine)))
        .route(
            "/openapi.json",
            get(|| async { Json(ryu_meetings::api::openapi()) }),
        )
        .layer(from_fn(move |req: Request, next: Next| {
            let expected = gated_token.clone();
            async move { require_meetings_token(req, next, expected.as_deref()).await }
        }));

    // `/health` sits OUTSIDE the gated nest so the loopback health probe succeeds
    // before auth. It asserts the store is readable (a cheap `list`) and returns no
    // meeting data.
    let health_store = store;
    let app = Router::new()
        .route(
            "/health",
            get(move || {
                let store = health_store.clone();
                async move { health(store).await }
            }),
        )
        .merge(meetings);

    // LOOPBACK ONLY (belt) + shared-secret bearer (suspenders): Core is the auth
    // front and re-stamps the bearer on the proxied hop.
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("ryu-meetings sidecar listening on http://{addr}");

    axum::serve(listener, app).await?;
    Ok(())
}

/// Loopback health probe: asserts the store is readable (a cheap `list`) so health
/// also confirms DB readiness, not just process liveness. Un-gated and data-free.
async fn health(store: MeetingStore) -> Response {
    match store.list_meetings().await {
        Ok(meetings) => (
            StatusCode::OK,
            Json(json!({
                "ok": true,
                "meetingCount": meetings.len(),
                "sealing": store.seal_health(),
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// Shared-secret bearer gate for the proxied `/api/meetings/*` surface. Core stays
/// the auth front — it runs `require_auth`, then re-stamps `Authorization: Bearer
/// <RYU_EXT_TOKEN>` on the loopback hop — so a request that did NOT come through Core
/// (any other local process on a shared host) is rejected with 401.
///
/// **Fail-closed:** `expected == None`/empty (no token configured) rejects every
/// request rather than falling open.
async fn require_meetings_token(req: Request, next: Next, expected: Option<&str>) -> Response {
    let provided = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if ryu_sidecar_runtime::token_ok(provided, expected) {
        next.run(req).await
    } else {
        (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
    }
}
