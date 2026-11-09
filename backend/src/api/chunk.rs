//! Live chunk ingest (`POST /:id/chunk`).

use axum::{
    extract::{Multipart, Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::json;

use super::MeetingsCtx;

/// Optional `?engine=` selector (mirroring the voice transcribe route) and
/// `?offset_ms=` — the chunk's sample-accurate position from the recorder, used
/// to time the transcript segment instead of wall-clock.
#[derive(Debug, Deserialize)]
pub struct ChunkQuery {
    #[serde(default)]
    pub engine: Option<String>,
    #[serde(default)]
    pub offset_ms: Option<i64>,
}

/// The multipart form this route reads. Nothing deserialises into it — the
/// handler walks `Multipart` field by field — it exists so the document says what
/// the form actually is.
//
// Declared `multipart/form-data` rather than typed as JSON, because that is the
// truth: the old `request_body = serde_json::Value` claimed a JSON body that
// would 400 here. Core's tool importer only reads
// `content/application~1json/schema`, so this route contributes no LLM arguments
// either way — which is the right outcome for a binary upload no model can send.
#[derive(utoipa::ToSchema)]
pub struct ChunkUpload {
    /// The captured WAV chunk. The only field read; anything else is ignored.
    #[schema(format = Binary)]
    pub file: String,
}

/// `POST /api/meetings/:id/chunk` — ingest one captured WAV chunk (multipart
/// `file` field), transcribe it, and append it to the live transcript.
#[utoipa::path(
    post,
    path = "/api/meetings/{id}/chunk",
    tag = "Meetings",
    summary = "ingest one captured WAV chunk (multipart",
    params(("id" = String, Path)),
    request_body(content = ChunkUpload, content_type = "multipart/form-data"),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn ingest_chunk(
    State(ctx): State<MeetingsCtx>,
    Path(id): Path<String>,
    Query(query): Query<ChunkQuery>,
    mut multipart: Multipart,
) -> (StatusCode, Json<serde_json::Value>) {
    if let Err(refused) = super::refuse_recap(&ctx, &id).await {
        return refused;
    }
    let mut audio: Option<(String, Vec<u8>)> = None;
    while let Ok(Some(field)) = multipart.next_field().await {
        if field.name() == Some("file") {
            let filename = field
                .file_name()
                .map(str::to_string)
                .unwrap_or_else(|| "chunk.wav".to_string());
            match field.bytes().await {
                Ok(bytes) => audio = Some((filename, bytes.to_vec())),
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({ "error": format!("could not read audio field: {e}") })),
                    );
                }
            }
        }
    }
    let Some((filename, bytes)) = audio else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "missing `file` field (the audio chunk)" })),
        );
    };

    match ctx
        .engine
        .ingest_chunk(
            &id,
            bytes,
            filename,
            query.engine.as_deref(),
            query.offset_ms,
        )
        .await
    {
        Ok(segment) => (StatusCode::OK, Json(json!({ "segment": segment }))),
        // A silent chunk is not an error worth a 5xx — report it softly.
        Err(e) if e.contains("silence") => (
            StatusCode::OK,
            Json(json!({ "segment": null, "skipped": e })),
        ),
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({ "error": e }))),
    }
}
