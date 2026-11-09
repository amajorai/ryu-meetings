//! Per-member recap settings, the consent record, and the node rollout gate.
//!
//! Settings are sealed per member (`recap_settings`). Consent is stored inside
//! them as `{version, processors, model, transcription, at}` and must be current
//! before a session starts, for every chunk, and before any summary: a new
//! [`CONSENT_VERSION`], a different resolved summary model, or a different
//! transcription route asks again.

use axum::{extract::State, http::HeaderMap, Json};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{enter, RecapError, RecapResult};
use crate::api::MeetingsCtx;
use crate::MeetingEngine;

/// The node-wide rollout preference. Off by default: no recap can be captured on
/// a node until it is turned on.
pub const NODE_ENABLED_PREF: &str = "meeting-recaps-enabled";
/// The consent text version clients show. Bump it to ask everyone again.
pub const CONSENT_VERSION: u32 = 1;

/// Retention windows, in days.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Retention {
    pub segments_days: u32,
    pub notes_days: u32,
    pub digests_days: u32,
    pub audio_days: u32,
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            segments_days: 7,
            notes_days: 30,
            digests_days: 30,
            audio_days: 7,
        }
    }
}

/// The consent a member gave, with the processors it named.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Consent {
    pub version: u32,
    #[serde(default)]
    pub processors: Vec<String>,
    /// The summary model resolved when consent was given.
    #[serde(default)]
    pub model: String,
    /// The transcription route resolved when consent was given.
    #[serde(default)]
    pub transcription: String,
    pub at: String,
}

/// One member's recap settings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct RecapSettings {
    /// Local `HH:MM` after which the daily digest is built and announced.
    pub digest_time: String,
    /// The member's IANA zone for the digest (falls back to the recap's zone).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tz: Option<String>,
    pub digest_notify: bool,
    pub interim: bool,
    /// Keep people's names in notes (off by default; never in digests or search).
    pub people: bool,
    pub retention: Retention,
    pub keep_audio_default: bool,
    pub file_to_space: bool,
    /// `inherit` (the Meetings notes model) or a model id.
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consent: Option<Consent>,
}

impl Default for RecapSettings {
    fn default() -> Self {
        Self {
            digest_time: "21:00".into(),
            tz: None,
            digest_notify: true,
            interim: false,
            people: false,
            retention: Retention::default(),
            keep_audio_default: false,
            file_to_space: false,
            model: "inherit".into(),
            consent: None,
        }
    }
}

/// Whether the node has turned recaps on.
pub async fn node_enabled(engine: &MeetingEngine) -> bool {
    engine
        .pref_get(NODE_ENABLED_PREF)
        .await
        .is_some_and(|v| v.trim() == "true")
}

/// The member's settings (defaults when none were saved).
pub async fn load(engine: &MeetingEngine, owner: &str) -> RecapResult<RecapSettings> {
    let stored = engine.store.recap_settings(owner).await?;
    Ok(stored
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default())
}

async fn save(engine: &MeetingEngine, owner: &str, settings: &RecapSettings) -> RecapResult<()> {
    let value = serde_json::to_value(settings).map_err(|e| RecapError::Internal(e.to_string()))?;
    engine.store.put_recap_settings(owner, &value).await?;
    Ok(())
}

/// The summary model recaps use for this member.
pub async fn resolved_model(engine: &MeetingEngine, settings: &RecapSettings) -> String {
    match settings.model.trim() {
        "" | "inherit" => crate::api::resolve_notes_model(engine).await,
        model => model.to_string(),
    }
}

/// The processors a member's audio and transcript reach, by name.
pub async fn processors(engine: &MeetingEngine, settings: &RecapSettings) -> Vec<String> {
    vec![
        format!("transcription:{}", engine.host.transcription_route()),
        format!("summary:{}", resolved_model(engine, settings).await),
    ]
}

/// Whether the member's consent is current for the transcription route and the
/// summary model recaps would use now.
pub async fn consent_current(engine: &MeetingEngine, settings: &RecapSettings) -> bool {
    match &settings.consent {
        Some(consent) => {
            consent.version == CONSENT_VERSION
                && consent.transcription == engine.host.transcription_route()
                && consent.model == resolved_model(engine, settings).await
        }
        None => false,
    }
}

/// The 409 a client answers by showing the consent screen again.
pub async fn consent_required(engine: &MeetingEngine, settings: &RecapSettings) -> RecapError {
    RecapError::Conflict(
        "consent_required",
        json!({
            "consent_version": CONSENT_VERSION,
            "processors": processors(engine, settings).await,
        }),
    )
}

/// Refuse unless the owner's consent is current (capture and finalize).
pub async fn require_consent(engine: &MeetingEngine, owner: &str) -> RecapResult<()> {
    let settings = load(engine, owner).await?;
    if consent_current(engine, &settings).await {
        return Ok(());
    }
    Err(consent_required(engine, &settings).await)
}

/// Whether the owner's consent is current (summaries in background paths).
pub async fn owner_consent_current(engine: &MeetingEngine, owner: &str) -> bool {
    match load(engine, owner).await {
        Ok(settings) => consent_current(engine, &settings).await,
        Err(_) => false,
    }
}

async fn settings_view(
    engine: &MeetingEngine,
    settings: &RecapSettings,
    can_enable: bool,
) -> Value {
    json!({
        "enabled": node_enabled(engine).await,
        "can_enable": can_enable,
        "settings": settings,
        "consent_version": CONSENT_VERSION,
        "consent_current": consent_current(engine, settings).await,
        "processors": processors(engine, settings).await,
    })
}

/// `GET /recaps/settings`.
pub async fn get_settings(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
) -> RecapResult<Json<Value>> {
    let caller = enter(&ctx, &headers).await?;
    let settings = load(&ctx.engine, &caller.user_id).await?;
    Ok(Json(
        settings_view(&ctx.engine, &settings, may_switch_node(&ctx, &headers)).await,
    ))
}

/// The writable settings fields (consent is not writable here).
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettingsPatch {
    pub digest_time: Option<String>,
    pub tz: Option<String>,
    pub digest_notify: Option<bool>,
    pub interim: Option<bool>,
    pub people: Option<bool>,
    pub retention: Option<Retention>,
    pub keep_audio_default: Option<bool>,
    pub file_to_space: Option<bool>,
    pub model: Option<String>,
}

fn valid_hhmm(value: &str) -> bool {
    chrono::NaiveTime::parse_from_str(value, "%H:%M").is_ok() && value.len() == 5
}

/// Validate and apply a patch.
pub fn apply_patch(settings: &mut RecapSettings, patch: SettingsPatch) -> RecapResult<()> {
    let bad = |m: &str| Err(RecapError::BadRequest(m.to_string()));
    if let Some(time) = patch.digest_time {
        if !valid_hhmm(&time) {
            return bad("digest_time must be HH:MM");
        }
        settings.digest_time = time;
    }
    if let Some(tz) = patch.tz {
        if tz.parse::<chrono_tz::Tz>().is_err() {
            return bad("tz must be an IANA zone");
        }
        settings.tz = Some(tz);
    }
    if let Some(retention) = patch.retention {
        let in_range = |d: u32| (1..=365).contains(&d);
        if !in_range(retention.segments_days)
            || !in_range(retention.notes_days)
            || !in_range(retention.digests_days)
            || ![1, 7, 30].contains(&retention.audio_days)
        {
            return bad("retention days out of range");
        }
        settings.retention = retention;
    }
    if let Some(model) = patch.model {
        if model.trim().is_empty() || model.len() > 200 {
            return bad("model must be `inherit` or a model id");
        }
        settings.model = model.trim().to_string();
    }
    if patch.file_to_space == Some(true) {
        // Filing recaps into a shared Space needs per-member Space ACLs first.
        return bad("file_to_space is not available");
    }
    settings.digest_notify = patch.digest_notify.unwrap_or(settings.digest_notify);
    settings.interim = patch.interim.unwrap_or(settings.interim);
    settings.people = patch.people.unwrap_or(settings.people);
    settings.keep_audio_default = patch
        .keep_audio_default
        .unwrap_or(settings.keep_audio_default);
    Ok(())
}

/// `PUT /recaps/settings`.
pub async fn put_settings(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
    Json(patch): Json<SettingsPatch>,
) -> RecapResult<Json<Value>> {
    let caller = enter(&ctx, &headers).await?;
    let mut settings = load(&ctx.engine, &caller.user_id).await?;
    let had_interim = settings.interim;
    apply_patch(&mut settings, patch)?;
    save(&ctx.engine, &caller.user_id, &settings).await?;
    if had_interim && !settings.interim {
        // Switching the opt-in off discards what was produced under it.
        ctx.engine.interim.clear_owner(&caller.user_id);
    }
    Ok(Json(
        settings_view(&ctx.engine, &settings, may_switch_node(&ctx, &headers)).await,
    ))
}

#[derive(Debug, Deserialize)]
pub struct AvailabilityBody {
    pub enabled: bool,
}

/// Whether this request may flip the node rollout switch. The route's manifest
/// level (`recaps.admin`, implied by no member level) already limits it to people
/// Core grants it; on a managed node it is further limited to the operator's
/// headerless node-bearer call, because every member arrives with a verified
/// caller header there and the switch covers the whole node.
pub fn may_switch_node(ctx: &MeetingsCtx, headers: &HeaderMap) -> bool {
    !(ctx.managed_node && super::caller::header_present(headers))
}

/// `PUT /recaps/availability` — the node rollout switch (node owner only).
pub async fn put_availability(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
    Json(body): Json<AvailabilityBody>,
) -> RecapResult<Json<Value>> {
    if !may_switch_node(&ctx, &headers) {
        return Err(RecapError::Forbidden("admin_required"));
    }
    // On a managed node the operator's call carries no member, so `enter` (which
    // needs one there) does not apply.
    if !ctx.managed_node {
        enter(&ctx, &headers).await?;
    }
    let value = if body.enabled { "true" } else { "false" };
    ctx.engine
        .pref_set(NODE_ENABLED_PREF, value)
        .await
        .map_err(RecapError::Internal)?;
    Ok(Json(json!({ "enabled": body.enabled })))
}

#[derive(Debug, Deserialize)]
pub struct ConsentBody {
    pub version: u32,
}

/// `POST /recaps/consent` — record consent to the CURRENT processors. The client
/// shows the names from `GET /recaps/settings`; the server records what it
/// resolves now, not what the client claims.
pub async fn record_consent(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
    Json(body): Json<ConsentBody>,
) -> RecapResult<Json<Value>> {
    let caller = enter(&ctx, &headers).await?;
    if body.version != CONSENT_VERSION {
        return Err(RecapError::Conflict(
            "consent_version_mismatch",
            json!({ "consent_version": CONSENT_VERSION }),
        ));
    }
    let mut settings = load(&ctx.engine, &caller.user_id).await?;
    settings.consent = Some(Consent {
        version: CONSENT_VERSION,
        processors: processors(&ctx.engine, &settings).await,
        model: resolved_model(&ctx.engine, &settings).await,
        transcription: ctx.engine.host.transcription_route(),
        at: chrono::Utc::now().to_rfc3339(),
    });
    save(&ctx.engine, &caller.user_id, &settings).await?;
    Ok(Json(
        settings_view(&ctx.engine, &settings, may_switch_node(&ctx, &headers)).await,
    ))
}

/// `DELETE /recaps/consent` — withdraw consent. New sessions, further chunks and
/// summaries are refused until consent is given again; sessions still recording
/// are closed now without a summary (see [`super::finalize::close_unsummarized`]).
/// The reply offers deleting everything.
pub async fn withdraw_consent(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
) -> RecapResult<Json<Value>> {
    let caller = enter(&ctx, &headers).await?;
    let mut settings = load(&ctx.engine, &caller.user_id).await?;
    settings.consent = None;
    save(&ctx.engine, &caller.user_id, &settings).await?;
    ctx.engine.interim.clear_owner(&caller.user_id);
    let closed =
        super::finalize::close_owner_recordings_unsummarized(&ctx.engine, &caller.user_id).await?;
    let count = ctx
        .engine
        .store
        .list_owned_recaps(&caller.user_id, None, None, None, 100_000)
        .await?
        .len();
    Ok(Json(json!({
        "withdrawn": true,
        "recap_count": count,
        "closed_recordings": closed,
        "delete_all": { "method": "DELETE", "path": "/recaps" },
    })))
}
