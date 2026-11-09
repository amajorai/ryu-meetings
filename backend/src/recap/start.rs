//! Starting a recap session (idempotent on the client's session id) and
//! confirming a fast-path start.

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use chrono::{DateTime, NaiveDate, Utc};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{enter, enter_capture, events, limits, settings, Caller, RecapError, RecapResult};
use crate::api::MeetingsCtx;
use crate::{Meeting, MeetingEvent, MeetingKind, MeetingSource, MeetingStatus, RecapMeta};

/// How long a fast-path start may stay unconfirmed before it is discarded.
pub const CONFIRM_WINDOW_SECS: i64 = 30;

/// `POST /recaps` body.
#[derive(Debug, Deserialize)]
pub struct StartBody {
    pub client_session_id: String,
    #[serde(default)]
    pub source: Option<String>,
    pub tz: String,
    pub local_day: String,
    #[serde(default)]
    pub locale: Option<String>,
    #[serde(default)]
    pub keep_audio: Option<bool>,
    pub consent_version: u32,
    /// Set by a client whose start bypassed the in-app acknowledgement (e.g. a
    /// watch complication); Action Button and Control sources imply it.
    #[serde(default)]
    pub fast_path: bool,
}

/// Validate the session metadata a client sends.
pub fn validate_start(body: &StartBody, now: DateTime<Utc>) -> RecapResult<chrono_tz::Tz> {
    if uuid::Uuid::parse_str(body.client_session_id.trim()).is_err() {
        return Err(RecapError::BadRequest(
            "client_session_id must be a UUID".into(),
        ));
    }
    let tz: chrono_tz::Tz = body
        .tz
        .parse()
        .map_err(|_| RecapError::BadRequest("tz must be an IANA zone".into()))?;
    let day = NaiveDate::parse_from_str(&body.local_day, "%Y-%m-%d")
        .map_err(|_| RecapError::BadRequest("local_day must be YYYY-MM-DD".into()))?;
    let today = now.with_timezone(&tz).date_naive();
    if (day - today).num_days().abs() > 1 {
        return Err(RecapError::BadRequest(
            "local_day is not today in tz".into(),
        ));
    }
    if body.locale.as_deref().is_some_and(|l| l.len() > 35) {
        return Err(RecapError::BadRequest("locale is too long".into()));
    }
    Ok(tz)
}

fn new_recap(caller: &Caller, body: &StartBody, source: MeetingSource, now: &str) -> Meeting {
    Meeting {
        id: format!("rcp_{}", uuid::Uuid::new_v4().simple()),
        title: "Recap".into(),
        title_custom: false,
        icon: None,
        app: Some("recap".into()),
        source,
        status: MeetingStatus::Recording,
        started_at: now.to_string(),
        ended_at: None,
        participants: Vec::new(),
        notes: None,
        space_id: None,
        doc_id: None,
        created_at: now.to_string(),
        updated_at: now.to_string(),
        kind: MeetingKind::Recap,
        recap: Some(RecapMeta {
            owner_user_id: caller.user_id.clone(),
            client_session_id: body.client_session_id.trim().to_ascii_lowercase(),
            tz: body.tz.clone(),
            local_day: body.local_day.clone(),
            locale: body.locale.clone(),
            needs_confirm: source.is_fast_path() || body.fast_path,
            ..RecapMeta::default()
        }),
    }
}

/// `POST /recaps` — start a session. Never drives Shadow (the client captures).
/// Refused without current consent; replaying a `client_session_id` returns the
/// same recap.
pub async fn start(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
    Json(body): Json<StartBody>,
) -> RecapResult<(StatusCode, Json<Value>)> {
    let caller = enter_capture(&ctx, &headers).await?;
    validate_start(&body, Utc::now())?;
    let session = body.client_session_id.trim().to_ascii_lowercase();
    if let Some(existing) = ctx
        .engine
        .store
        .find_recap_by_session(&caller.user_id, &session)
        .await?
    {
        return Ok((
            StatusCode::OK,
            Json(json!({ "recap": existing, "upload": limits::upload_contract() })),
        ));
    }
    let member = settings::load(&ctx.engine, &caller.user_id).await?;
    if body.consent_version != settings::CONSENT_VERSION
        || !settings::consent_current(&ctx.engine, &member).await
    {
        return Err(settings::consent_required(&ctx.engine, &member).await);
    }
    let source = MeetingSource::parse(body.source.as_deref().unwrap_or("phone"));
    let now = Utc::now().to_rfc3339();
    let recap = new_recap(&caller, &body, source, &now);
    ctx.engine.store.upsert_meeting(&recap).await?;
    ctx.engine.store.emit_recap(
        &recap.id,
        MeetingEvent::Started {
            meeting: recap.clone(),
        },
    );
    events::emit_started(&ctx.engine, &recap).await;
    Ok((
        StatusCode::CREATED,
        Json(json!({ "recap": recap, "upload": limits::upload_contract() })),
    ))
}

/// `POST /recaps/:id/confirm` — confirm a fast-path start within the window.
pub async fn confirm(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> RecapResult<Json<Value>> {
    let caller = enter(&ctx, &headers).await?;
    let recap = ctx
        .engine
        .store
        .get_owned_recap(&caller.user_id, &id)
        .await?
        .ok_or(RecapError::NotFound)?;
    if recap.status != MeetingStatus::Recording {
        return Err(RecapError::conflict("recap_closed"));
    }
    let now = Utc::now();
    // The window is enforced here, not only by the sweep (which may run minutes
    // later): a late confirm discards the session, exactly as the sweep would.
    if unconfirmed_expired(&recap, now) {
        super::records::delete_recap(&ctx.engine, &recap).await?;
        return Err(RecapError::conflict("confirm_expired"));
    }
    let stamp = now.to_rfc3339();
    let mut expired = false;
    let updated = ctx
        .engine
        .store
        .mutate_recap(&id, |m| {
            // Re-checked on the row being written (the read above may be stale).
            if unconfirmed_expired(m, now) {
                expired = true;
                return false;
            }
            match m.recap.as_mut() {
                Some(meta) if meta.confirmed_at.is_none() => {
                    meta.confirmed_at = Some(stamp.clone());
                    m.updated_at = stamp.clone();
                    true
                }
                _ => false,
            }
        })
        .await?
        .ok_or(RecapError::NotFound)?;
    if expired {
        super::records::delete_recap(&ctx.engine, &updated).await?;
        return Err(RecapError::conflict("confirm_expired"));
    }
    Ok(Json(json!({ "recap": updated })))
}

/// Whether a fast-path recap is past its confirm window without a confirm.
pub fn unconfirmed_expired(recap: &Meeting, now: DateTime<Utc>) -> bool {
    let Some(meta) = recap.recap.as_ref() else {
        return false;
    };
    if !meta.needs_confirm || meta.confirmed_at.is_some() {
        return false;
    }
    DateTime::parse_from_rfc3339(&recap.started_at)
        .map(|start| (now - start.with_timezone(&Utc)).num_seconds() >= CONFIRM_WINDOW_SECS)
        .unwrap_or(true)
}

/// Whether a recap accepts audio now (recording, and confirmed when required).
pub fn accepts_audio(recap: &Meeting) -> RecapResult<()> {
    if recap.status != MeetingStatus::Recording {
        return Err(RecapError::conflict("recap_closed"));
    }
    match recap.recap.as_ref() {
        Some(meta) if meta.needs_confirm && meta.confirmed_at.is_none() => {
            Err(RecapError::conflict("recap_unconfirmed"))
        }
        Some(_) => Ok(()),
        None => Err(RecapError::NotFound),
    }
}
