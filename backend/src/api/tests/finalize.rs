use super::*;
use ryu_diarization::DiarizationModel;

#[tokio::test]
async fn finalize_empty_transcript_saves_space_when_host_files_it() {
    let mut host = FakeHost::default();
    host.space = Some(("space-1".into(), "doc-1".into()));
    let ctx = ctx_with(host);
    let m = ctx
        .engine
        .start("Fixed title".into(), None, MeetingSource::Manual)
        .await
        .unwrap();
    let (code, Json(v)) = finalize_meeting(State(ctx), Path(m.id)).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(v["meeting"]["status"], "done");
    assert_eq!(v["meeting"]["space_id"], "space-1");
    assert_eq!(v["meeting"]["doc_id"], "doc-1");
}

/// A finalize arriving while another is still running for the same meeting
/// (double-click, client retry) is refused with 409: no second note generation,
/// no second Space document, and the meeting is left to the first request.
#[tokio::test]
async fn overlapping_finalize_is_conflict_and_files_space_once() {
    let host = FakeHost {
        space: Some(("space-1".into(), "doc-1".into())),
        ..FakeHost::default()
    };
    let space_calls = Arc::clone(&host.space_calls);
    let ctx = ctx_with(host);
    let m = ctx
        .engine
        .start("Fixed title".into(), None, MeetingSource::Manual)
        .await
        .unwrap();

    let in_flight = ctx.engine.begin_finalize(&m.id).expect("first claim");
    let (code, Json(v)) = finalize_meeting(State(ctx.clone()), Path(m.id.clone())).await;
    assert_eq!(code, StatusCode::CONFLICT);
    assert_eq!(v["error"], "meeting is already being finalized");
    assert_eq!(space_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        ctx.engine.get(&m.id).await.unwrap().unwrap().status,
        MeetingStatus::Recording,
        "the refused request must not touch the meeting"
    );

    drop(in_flight);
    let (code, Json(v)) = finalize_meeting(State(ctx.clone()), Path(m.id.clone())).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(v["meeting"]["doc_id"], "doc-1");
    assert_eq!(space_calls.load(Ordering::SeqCst), 1);
    // The request released its claim on the way out.
    assert!(ctx.engine.begin_finalize(&m.id).is_some());
}

/// A loopback Gateway whose chat-completions call parks until `release`
/// fires, so a test can act while a real finalize is mid note generation.
#[derive(Clone)]
struct HeldGateway {
    url: String,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    hits: Arc<AtomicUsize>,
}

async fn held_completion(State(gateway): State<HeldGateway>) -> Json<serde_json::Value> {
    gateway.hits.fetch_add(1, Ordering::SeqCst);
    gateway.entered.notify_one();
    gateway.release.notified().await;
    Json(json!({
        "choices": [{ "message": { "content": "{\"summary\":\"We shipped the release.\"}" } }]
    }))
}

async fn held_gateway() -> HeldGateway {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gateway = HeldGateway {
        url: format!("http://{}", listener.local_addr().unwrap()),
        entered: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
        hits: Arc::new(AtomicUsize::new(0)),
    };
    let app = Router::new()
        .route("/v1/chat/completions", post(held_completion))
        .with_state(gateway.clone());
    tokio::spawn(async move { axum::serve(listener, app).await });
    gateway
}

/// The race end to end, against a real (held) note generation: a second
/// finalize meanwhile is refused, and a rename made meanwhile survives the
/// finalize write and keeps the auto-namer off. Notes are generated and filed
/// exactly once.
#[tokio::test]
async fn rename_during_note_generation_survives_finalize() {
    let gateway = held_gateway().await;
    let host = FakeHost {
        gateway_url: gateway.url.clone(),
        title: Some("Auto title".into()),
        space: Some(("space-1".into(), "doc-1".into())),
        ..FakeHost::default()
    };
    let space_calls = Arc::clone(&host.space_calls);
    let ctx = ctx_with(host);
    let m = ctx
        .engine
        .start(String::new(), None, MeetingSource::Manual)
        .await
        .unwrap();
    assert!(!m.title_custom);
    ctx.engine
        .store
        .insert_segment(&m.id, 0, None, "we shipped the release")
        .await
        .unwrap();

    let first = tokio::spawn(finalize_meeting(State(ctx.clone()), Path(m.id.clone())));
    gateway.entered.notified().await;
    let (code, _) = finalize_meeting(State(ctx.clone()), Path(m.id.clone())).await;
    assert_eq!(code, StatusCode::CONFLICT);
    ctx.engine.store.set_title(&m.id, "Mine").await.unwrap();
    gateway.release.notify_one();

    let (code, Json(v)) = first.await.unwrap();
    assert_eq!(code, StatusCode::OK);
    assert_eq!(v["meeting"]["title"], "Mine");
    assert_eq!(v["meeting"]["doc_id"], "doc-1");
    let stored = ctx.engine.get(&m.id).await.unwrap().unwrap();
    assert_eq!(stored.title, "Mine");
    assert!(stored.title_custom);
    assert_eq!(stored.status, MeetingStatus::Done);
    assert_eq!(stored.notes.unwrap().summary, "We shipped the release.");
    assert_eq!(gateway.hits.load(Ordering::SeqCst), 1);
    assert_eq!(space_calls.load(Ordering::SeqCst), 1);
}

/// A meeting deleted while its notes generate stays deleted: the finalize
/// fails instead of re-inserting the row, and nothing is filed to the Space.
#[tokio::test]
async fn delete_during_note_generation_is_not_undone() {
    let gateway = held_gateway().await;
    let host = FakeHost {
        gateway_url: gateway.url.clone(),
        space: Some(("space-1".into(), "doc-1".into())),
        ..FakeHost::default()
    };
    let space_calls = Arc::clone(&host.space_calls);
    let ctx = ctx_with(host);
    let m = ctx
        .engine
        .start(String::new(), None, MeetingSource::Manual)
        .await
        .unwrap();
    ctx.engine
        .store
        .insert_segment(&m.id, 0, None, "we shipped the release")
        .await
        .unwrap();

    let first = tokio::spawn(finalize_meeting(State(ctx.clone()), Path(m.id.clone())));
    gateway.entered.notified().await;
    assert!(ctx.engine.delete(&m.id).await.unwrap());
    gateway.release.notify_one();

    let (code, Json(v)) = first.await.unwrap();
    assert_eq!(code, StatusCode::BAD_GATEWAY);
    assert!(
        v["error"].as_str().is_some_and(|e| e.contains("deleted")),
        "got: {v}"
    );
    assert!(ctx.engine.get(&m.id).await.unwrap().is_none());
    assert!(ctx.engine.list().await.unwrap().is_empty());
    assert_eq!(space_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn finalize_missing_meeting_is_bad_gateway() {
    let ctx = ctx_with(FakeHost::default());
    let (code, _) = finalize_meeting(State(ctx), Path("nope".into())).await;
    assert_eq!(code, StatusCode::BAD_GATEWAY);
}

// ---- private helpers -------------------------------------------------

#[tokio::test]
async fn resolve_model_prefers_pref() {
    let ctx = ctx_with(FakeHost::default());
    ctx.engine
        .pref_set("meeting-notes-model", "  pref-model  ")
        .await
        .unwrap();
    assert_eq!(resolve_notes_model(&ctx.engine).await, "pref-model");
}

#[tokio::test]
async fn resolve_prompt_uses_custom_then_template() {
    let ctx = ctx_with(FakeHost::default());
    // A custom prompt wins outright.
    ctx.engine
        .pref_set("meeting-notes-prompt", "custom sys prompt")
        .await
        .unwrap();
    assert_eq!(resolve_notes_prompt(&ctx.engine).await, "custom sys prompt");

    // Cleared prompt ⇒ falls to the selected template.
    ctx.engine
        .pref_set("meeting-notes-prompt", "  ")
        .await
        .unwrap();
    ctx.engine
        .pref_set("meeting-notes-template", "sales")
        .await
        .unwrap();
    let p = resolve_notes_prompt(&ctx.engine).await;
    assert!(p.contains("sales/customer call"));
}

#[tokio::test]
async fn resolve_effort_reads_pref() {
    let ctx = ctx_with(FakeHost::default());
    ctx.engine
        .pref_set("meeting-notes-effort", " high ")
        .await
        .unwrap();
    assert_eq!(resolve_notes_effort(&ctx.engine).await, "high");
}

#[tokio::test]
async fn diarize_if_enabled_noops_when_disabled() {
    let ctx = ctx_with(FakeHost::default());
    // Toggle unset ⇒ returns immediately, no sidecar call, no panic.
    diarize_if_enabled(&ctx.engine, "any-id").await;
}

#[tokio::test]
async fn diarization_model_preference_defaults_and_selects_adapters() {
    let ctx = ctx_with(FakeHost::default());
    assert_eq!(
        resolve_diarization_model(&ctx.engine).await,
        DiarizationModel::Pyannote
    );
    ctx.engine
        .pref_set(DIARIZATION_MODEL_PREF, "nemotron")
        .await
        .unwrap();
    assert_eq!(
        resolve_diarization_model(&ctx.engine).await,
        DiarizationModel::Nemotron
    );
    ctx.engine
        .pref_set(DIARIZATION_MODEL_PREF, "unknown")
        .await
        .unwrap();
    assert_eq!(
        resolve_diarization_model(&ctx.engine).await,
        DiarizationModel::Pyannote
    );
}

#[test]
fn build_notes_markdown_renders_sections_and_empty_bullets() {
    let meeting = Meeting {
        id: "m1".into(),
        title: "Weekly".into(),
        title_custom: false,
        icon: None,
        app: Some("zoom".into()),
        source: MeetingSource::Manual,
        status: crate::MeetingStatus::Done,
        started_at: "2026-01-01T00:00:00Z".into(),
        ended_at: None,
        participants: vec![],
        notes: None,
        space_id: None,
        doc_id: None,
        kind: crate::MeetingKind::Meeting,
        recap: None,
        created_at: "2026-01-01T00:00:00Z".into(),
        updated_at: "2026-01-01T00:00:00Z".into(),
    };
    let notes = MeetingNotes {
        summary: "We shipped.".into(),
        key_points: vec!["point a".into()],
        action_items: vec![],
        decisions: vec!["ship it".into()],
        generated_at: String::new(),
        model: String::new(),
        ..MeetingNotes::default()
    };
    let md = build_notes_markdown(&meeting, &notes, "line one\nline two");
    assert!(md.starts_with("# Weekly\n"));
    assert!(md.contains("_zoom · 2026-01-01T00:00:00Z_"));
    assert!(md.contains("## Summary\n\nWe shipped."));
    assert!(md.contains("- point a"));
    assert!(md.contains("_None_"), "empty action items render _None_");
    assert!(md.contains("- ship it"));
    assert!(md.contains("## Transcript\n\nline one\nline two"));
}

#[test]
fn build_notes_markdown_omits_empty_transcript_and_app() {
    let meeting = Meeting {
        id: "m2".into(),
        title: "No app".into(),
        title_custom: false,
        icon: None,
        app: None,
        source: MeetingSource::Manual,
        status: crate::MeetingStatus::Done,
        started_at: "2026-02-02T00:00:00Z".into(),
        ended_at: None,
        participants: vec![],
        notes: None,
        space_id: None,
        doc_id: None,
        kind: crate::MeetingKind::Meeting,
        recap: None,
        created_at: "2026-02-02T00:00:00Z".into(),
        updated_at: "2026-02-02T00:00:00Z".into(),
    };
    let notes = MeetingNotes::default();
    let md = build_notes_markdown(&meeting, &notes, "   ");
    assert!(md.contains("_2026-02-02T00:00:00Z_"));
    assert!(!md.contains("## Transcript"));
}
