//! Audio-file import (`POST /import`).

use axum::{
    extract::{Multipart, State},
    http::StatusCode,
    Json,
};
use serde_json::json;

use super::{finalize_and_save, MeetingsCtx};
use crate::audio;

/// Multipart field parse for import; everything but `file` is optional.
//
// Same reasoning as [`ChunkUpload`]: the wire form is `multipart/form-data`, so
// saying so is the only honest schema. Nothing deserialises into this struct.
#[derive(utoipa::ToSchema)]
pub struct ImportUpload {
    /// The audio file to import (WAV v1).
    #[schema(format = Binary)]
    pub file: String,
    /// STT engine override; the configured default is used when absent.
    pub engine: Option<String>,
    /// Meeting title; auto-titled from the transcript when absent or blank.
    pub title: Option<String>,
}

/// `POST /api/meetings/import` — create a meeting from an uploaded audio file
/// (WAV v1), transcribe it window-by-window through the same pipeline as a live
/// recording, then finalize (notes + optional diarization + Space save).
#[utoipa::path(
    post,
    path = "/api/meetings/import",
    tag = "Meetings",
    summary = "create a meeting from an uploaded audio file",
    request_body(content = ImportUpload, content_type = "multipart/form-data"),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn import_meeting(
    State(ctx): State<MeetingsCtx>,
    mut multipart: Multipart,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut audio: Option<Vec<u8>> = None;
    let mut engine: Option<String> = None;
    let mut title = String::new();
    while let Ok(Some(field)) = multipart.next_field().await {
        match field.name() {
            Some("file") => {
                if let Ok(bytes) = field.bytes().await {
                    audio = Some(bytes.to_vec());
                }
            }
            Some("engine") => engine = field.text().await.ok().filter(|s| !s.is_empty()),
            Some("title") => title = field.text().await.unwrap_or_default(),
            _ => {}
        }
    }
    let Some(bytes) = audio else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "missing `file` field (the audio to import)" })),
        );
    };

    // WAV-only in v1. Real-world files (mp3/m4a/mov) need an ffmpeg decode step,
    // which is gated/optional — reject clearly rather than mis-transcribing.
    let decoded = match audio::decode_wav(&bytes) {
        Ok(d) => audio::resample_to_16k(&d),
        Err(_) => {
            return (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                Json(json!({
                    "error": "import currently accepts WAV files only; convert mp3/m4a to WAV first"
                })),
            )
        }
    };

    let meeting = match ctx.engine.start_import(title).await {
        Ok(m) => m,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e })),
            )
        }
    };
    let id = meeting.id.clone();

    // Feed the file through the live-chunk pipeline (transcribe + persist stereo),
    // one 30 s window at a time, with real offsets.
    for (offset_ms, wav) in audio::window_wavs(&decoded, 30) {
        let _ = ctx
            .engine
            .ingest_chunk(
                &id,
                wav,
                "import.wav".to_string(),
                engine.as_deref(),
                Some(offset_ms),
            )
            .await;
    }

    finalize_and_save(&ctx.engine, &id).await
}
