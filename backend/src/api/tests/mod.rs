mod crud;
mod finalize;
mod openapi;
mod routes;

use super::*;
use crate::test_support::{engine_with, FakeHost};
use crate::{Meeting, MeetingNotes, MeetingSource, MeetingStatus};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn ctx_with(host: FakeHost) -> MeetingsCtx {
    MeetingsCtx::new(engine_with(host))
}

// ── Capability routes ──────────────────────────────────────────────────

fn audio_body(id: &str, wav: &[u8]) -> Json<CapabilityAppendAudioBody> {
    use base64::Engine as _;
    Json(CapabilityAppendAudioBody {
        id: id.to_owned(),
        filename: Some("seg.wav".into()),
        audio_base64: base64::engine::general_purpose::STANDARD.encode(wav),
    })
}

#[tokio::test]
async fn capability_routes_resolve_append_count_and_purge() {
    let ctx = ctx_with(FakeHost::default());
    let m = ctx
        .engine
        .start(
            "Ambient".into(),
            Some("ryu-hardware".into()),
            MeetingSource::Auto,
        )
        .await
        .unwrap();

    let (code, Json(found)) = capability_resolve(
        State(ctx.clone()),
        Json(CapabilityResolveBody { id: m.id.clone() }),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(found["meeting"]["id"], m.id.as_str());
    let (code, _) = capability_resolve(
        State(ctx.clone()),
        Json(CapabilityResolveBody {
            id: "mtg_gone".into(),
        }),
    )
    .await;
    assert_eq!(code, StatusCode::NOT_FOUND);

    let wav = crate::test_support::stereo_wav_16k(10);
    let (code, Json(appended)) =
        capability_append_audio(State(ctx.clone()), audio_body(&m.id, &wav)).await;
    assert_eq!(code, StatusCode::OK, "{appended}");
    assert!(appended["segment"]["id"].is_number(), "{appended}");
    let (code, _) = capability_append_audio(
        State(ctx.clone()),
        Json(CapabilityAppendAudioBody {
            id: m.id.clone(),
            filename: None,
            audio_base64: "not base64!".into(),
        }),
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);

    let category = |c: &str| {
        Json(DataCategoryBody {
            category: c.to_owned(),
            user_id: None,
        })
    };
    let (_, Json(count)) = capability_data_count(
        State(ctx.clone()),
        axum::http::HeaderMap::new(),
        category("meetings"),
    )
    .await;
    assert_eq!(count["count"], 1);
    let (code, _) = capability_data_purge(
        State(ctx.clone()),
        axum::http::HeaderMap::new(),
        category("monitors"),
    )
    .await;
    assert_eq!(code, StatusCode::NOT_FOUND);
    let (code, Json(purged)) = capability_data_purge(
        State(ctx.clone()),
        axum::http::HeaderMap::new(),
        category("meetings"),
    )
    .await;
    assert_eq!(
        (code, purged["removed"].as_u64()),
        (StatusCode::OK, Some(1))
    );
    let (_, Json(count)) = capability_data_count(
        State(ctx),
        axum::http::HeaderMap::new(),
        category("meetings"),
    )
    .await;
    assert_eq!(count["count"], 0);
}

/// A silent segment keeps the soft 200 `{ segment: null, skipped }` shape the
/// hardware path maps to an ambient skip.
#[tokio::test]
async fn capability_append_reports_silence_softly() {
    let host = FakeHost {
        transcribe_text: "   ".into(),
        ..FakeHost::default()
    };
    let ctx = ctx_with(host);
    let m = ctx
        .engine
        .start(String::new(), None, MeetingSource::Auto)
        .await
        .unwrap();
    let wav = crate::test_support::stereo_wav_16k(10);
    let (code, Json(reply)) = capability_append_audio(State(ctx), audio_body(&m.id, &wav)).await;
    assert_eq!(code, StatusCode::OK);
    assert!(reply["segment"].is_null());
    assert!(reply["skipped"].as_str().unwrap().contains("silence"));
    crate::audio::remove_pcm(&m.id);
}

#[tokio::test]
async fn list_templates_returns_catalog() {
    let ctx = ctx_with(FakeHost::default());
    let Json(v) = list_templates(State(ctx)).await;
    assert!(v.get("templates").and_then(|t| t.as_array()).is_some());
}

/// The Store tab renders these as cards, so the listing must carry what a card
/// draws — a bare id/name pair would render as a blank tile.
#[tokio::test]
async fn list_templates_carries_browse_metadata() {
    let ctx = ctx_with(FakeHost::default());
    let Json(v) = list_templates(State(ctx)).await;
    let first = &v["templates"][0];
    for key in ["id", "name", "description", "category", "icon", "tags"] {
        assert!(!first[key].is_null(), "listing is missing '{key}'");
    }
}

/// Selecting a template writes the SAME preference the notes pipeline reads, so
/// the Store's install and the resolved prompt cannot disagree.
#[tokio::test]
async fn select_template_sets_the_notes_prompt() {
    let ctx = ctx_with(FakeHost::default());
    let (code, Json(v)) = select_template(
        State(ctx.clone()),
        Json(SelectTemplateBody {
            template_id: "customer_call".into(),
        }),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(v["success"], true);
    assert_eq!(
        ctx.engine.pref_get(NOTES_TEMPLATE_PREF).await.as_deref(),
        Some("customer_call")
    );
    assert_eq!(
        resolve_notes_prompt(&ctx.engine).await,
        crate::templates::prompt_for("customer_call")
    );
}

/// The active flag is what the Store card reads for "Added", so it must follow
/// the preference rather than a client-side guess.
#[tokio::test]
async fn selected_template_is_marked_active_in_the_listing() {
    let ctx = ctx_with(FakeHost::default());
    select_template(
        State(ctx.clone()),
        Json(SelectTemplateBody {
            template_id: "retro".into(),
        }),
    )
    .await;
    let Json(v) = list_templates(State(ctx)).await;
    let active: Vec<&str> = v["templates"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|t| t["active"] == true)
        .map(|t| t["id"].as_str().unwrap())
        .collect();
    assert_eq!(active, vec!["retro"]);
}

/// An unknown id would silently fall back to the default prompt, leaving the UI
/// claiming a selection that does nothing — so it is refused, not stored.
#[tokio::test]
async fn select_template_rejects_an_unknown_id() {
    let ctx = ctx_with(FakeHost::default());
    let (code, Json(v)) = select_template(
        State(ctx.clone()),
        Json(SelectTemplateBody {
            template_id: "not-a-template".into(),
        }),
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert_eq!(v["success"], false);
    assert!(ctx.engine.pref_get(NOTES_TEMPLATE_PREF).await.is_none());
}
