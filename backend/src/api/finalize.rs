//! Finalize: notes, optional diarization, auto-title, Space filing, events.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use ryu_diarization::{DiarizationModel, DiarizationRequest};
use serde_json::json;

use super::{
    resolve_notes_effort, resolve_notes_model, resolve_notes_prompt, MeetingsCtx,
    DIARIZATION_ENABLED_PREF, DIARIZATION_MODEL_PREF,
};
use crate::{
    audio, diarize, notes::MeetingNotes, Meeting, MeetingEngine, EVENT_MEETING_ENDED,
    EVENT_NOTES_READY,
};

/// Run diarization on a finalized meeting's persisted audio when the toggle is on,
/// writing speaker labels onto the transcript segments. Best-effort throughout: a
/// disabled toggle, a missing sidecar, or no persisted audio all just no-op.
pub(crate) async fn resolve_diarization_model(engine: &MeetingEngine) -> DiarizationModel {
    let model_value = engine
        .pref_get(DIARIZATION_MODEL_PREF)
        .await
        .unwrap_or_default();
    match model_value.trim() {
        "" => DiarizationModel::Pyannote,
        value => match DiarizationModel::try_from(value) {
            Ok(model) => model,
            Err(error) => {
                tracing::warn!(
                    "meetings: invalid diarization model preference; using pyannote: {error}"
                );
                DiarizationModel::Pyannote
            }
        },
    }
}

pub(crate) async fn diarize_if_enabled(engine: &MeetingEngine, id: &str) {
    let enabled = engine
        .pref_get(DIARIZATION_ENABLED_PREF)
        .await
        .map(|v| v.trim() == "true")
        .unwrap_or(false);
    if !enabled {
        return;
    }
    let model = resolve_diarization_model(engine).await;
    let records = engine.store.pcm_records(id).await.unwrap_or(0);
    let seal = engine.store.audio_seal();
    // One read of the (possibly sealed) log serves both the WAV the sidecar
    // diarizes and the PCM the speaker assignment aligns against.
    let pcm = match audio::read_pcm(seal, id, records) {
        Ok(Some(pcm)) => pcm,
        Ok(None) => return,
        Err(e) => {
            tracing::warn!("meetings: diarization skipped for {id}: {e:#}");
            return;
        }
    };
    let wav = match audio::read_pcm_as_wav(seal, id, records) {
        Ok(Some(w)) => w,
        _ => return,
    };
    let turns = match diarize::diarize_wav(wav, DiarizationRequest::new(model)).await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!("meetings: diarization skipped for {id}: {e}");
            return;
        }
    };
    let segments = match engine.store.list_segments(id).await {
        Ok(s) => s,
        Err(_) => return,
    };
    for (seg_id, speaker) in diarize::assign(&segments, &turns, &pcm) {
        let _ = engine.store.set_segment_speaker(seg_id, &speaker).await;
    }
}

/// `POST /api/meetings/:id/finalize` — stop capture, generate notes, mark done,
/// and save the notes into the "Meetings" Space so they're editable + searchable
/// through the existing Spaces UI (best-effort; a Space failure doesn't fail the
/// finalize — the notes still live on the meeting record). A second request for
/// the same meeting while one is still running gets 409 instead of generating
/// (and filing) the notes twice.
#[utoipa::path(
    post,
    path = "/api/meetings/{id}/finalize",
    tag = "Meetings",
    summary = "stop capture, generate notes, mark done,",
    params(("id" = String, Path)),
    responses(
        (status = 200, description = "OK", body = serde_json::Value),
        (status = 409, description = "This meeting is already being finalized", body = serde_json::Value),
        (status = 502, description = "Note generation failed or the meeting does not exist", body = serde_json::Value)
    )
)]
pub async fn finalize_meeting(
    State(ctx): State<MeetingsCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    if let Err(refused) = super::refuse_recap(&ctx, &id).await {
        return refused;
    }
    finalize_and_save(&ctx.engine, &id).await
}

/// Shared finalize tail: generate notes (model/effort/prompt from prefs), run
/// diarization if enabled, auto-title, and save into the Meetings Space. Used by
/// both the live finalize and the import path.
pub(crate) async fn finalize_and_save(
    engine: &MeetingEngine,
    id: &str,
) -> (StatusCode, Json<serde_json::Value>) {
    // Held until this function returns, so the auto-title and Space filing below
    // are covered too: an overlapping request cannot file a second document.
    let Some(claim) = engine.begin_finalize(id) else {
        return (
            StatusCode::CONFLICT,
            Json(json!({ "error": "meeting is already being finalized" })),
        );
    };
    let model = resolve_notes_model(engine).await;
    let effort = resolve_notes_effort(engine).await;
    let prompt = resolve_notes_prompt(engine).await;
    let mut meeting = match engine.finalize(&claim, &model, &effort, &prompt).await {
        Ok(m) => m,
        Err(e) => return (StatusCode::BAD_GATEWAY, Json(json!({ "error": e }))),
    };

    // Speaker diarization (opt-in) — label the transcript's segments before the
    // notes are rendered into the Space. Best-effort: a missing sidecar or a
    // disabled toggle just leaves speakers unlabeled.
    diarize_if_enabled(engine, id).await;

    // Auto-name the meeting from its summary with the default local model, unless
    // the user already chose a title. Best-effort; on success update the local
    // copy so the Space document below uses the new title.
    if !meeting.title_custom {
        if let Some(summary) = meeting.notes.as_ref().map(|n| n.summary.clone()) {
            if let Some(new_title) = engine.auto_title(id, &summary).await {
                meeting.title = new_title;
            }
        }
    }

    let filed = save_notes_to_space(engine, &meeting).await;
    let final_meeting = match &filed {
        Some((space_id, doc_id)) => engine
            .attach_space(id, space_id, doc_id)
            .await
            .unwrap_or(meeting),
        None => meeting,
    };

    // The app events fire here rather than inside `MeetingEngine::finalize` because
    // only at this point are the two things a consumer actually needs settled: the
    // auto-generated title (the one the user will see) and whether the notes reached
    // the Space. Both emits are best-effort and cannot fail the finalize.
    //
    // The transcript is deliberately not in the payload — it is unbounded and app-event
    // payloads are size-capped; a consumer reads it from `/api/meetings/{id}/transcript`.
    let notes = final_meeting.notes.as_ref();
    engine
        .events
        .emit_with_activity(
            EVENT_MEETING_ENDED,
            json!({
                "meeting_id": final_meeting.id,
                "title": final_meeting.title,
                "app": final_meeting.app,
                "started_at": final_meeting.started_at,
                "ended_at": final_meeting.ended_at,
                "summary": notes.map(|n| n.summary.as_str()),
                "action_items": notes.map(|n| n.action_items.as_slice()).unwrap_or_default(),
            }),
            crate::notes_ready_activity(&final_meeting),
        )
        .await;
    // Only when the notes really landed in the Space: `notes.ready` promises a document
    // that exists, so emitting it on a failed filing would hand consumers a dead doc id.
    if let Some((space_id, doc_id)) = &filed {
        engine
            .events
            .emit_with_notify(
                EVENT_NOTES_READY,
                json!({
                    "meeting_id": final_meeting.id,
                    "title": final_meeting.title,
                    "space_id": space_id,
                    "doc_id": doc_id,
                }),
                // Notes filed to a Space is exactly the "your long job is done" moment.
                // Core's notification inbox is not sealed, so the notice is generic:
                // the meeting title is sealed content and stays in the event payload.
                Some(crate::notes_ready_notify_hint()),
            )
            .await;
    }

    (StatusCode::OK, Json(json!({ "meeting": final_meeting })))
}

/// Write a finalized meeting's notes (+ transcript) into the Meetings Space as a
/// markdown document. Returns `(space_id, doc_id)` on success, `None` on any
/// failure (logged) so finalize stays best-effort.
pub(crate) async fn save_notes_to_space(
    engine: &MeetingEngine,
    meeting: &Meeting,
) -> Option<(String, String)> {
    let notes = meeting.notes.as_ref()?;
    let transcript = engine.transcript(&meeting.id).await.unwrap_or_default();
    let markdown = build_notes_markdown(meeting, notes, &transcript);
    // Finding/creating the Meetings Space, the background owner/tenancy, and the
    // Spaces ingest all stay Core-side behind the host — this crate only produces
    // the document title + markdown.
    engine.save_notes_to_space(&meeting.title, &markdown).await
}

/// Render a meeting's notes + transcript as a markdown document for the Space.
pub(crate) fn build_notes_markdown(
    meeting: &Meeting,
    notes: &MeetingNotes,
    transcript: &str,
) -> String {
    fn bullets(items: &[String]) -> String {
        if items.is_empty() {
            return "_None_".to_string();
        }
        items
            .iter()
            .map(|i| format!("- {i}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    let subtitle = match &meeting.app {
        Some(app) if !app.is_empty() => format!("{app} · {}", meeting.started_at),
        _ => meeting.started_at.clone(),
    };
    let mut md = format!("# {}\n\n_{subtitle}_\n\n", meeting.title);
    md.push_str(&format!("## Summary\n\n{}\n\n", notes.summary));
    md.push_str(&format!(
        "## Key points\n\n{}\n\n",
        bullets(&notes.key_points)
    ));
    md.push_str(&format!(
        "## Action items\n\n{}\n\n",
        bullets(&notes.action_items)
    ));
    md.push_str(&format!(
        "## Decisions\n\n{}\n\n",
        bullets(&notes.decisions)
    ));
    if !transcript.trim().is_empty() {
        md.push_str(&format!("## Transcript\n\n{transcript}\n"));
    }
    md
}
