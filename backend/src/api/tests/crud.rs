use super::*;

#[tokio::test]
async fn create_and_get_meeting_roundtrip() {
    let ctx = ctx_with(FakeHost::default());
    let body = StartBody {
        title: "Kickoff".into(),
        app: Some("zoom".into()),
        source: Some("auto".into()),
    };
    let (code, Json(v)) = create_meeting(State(ctx.clone()), Json(body)).await;
    assert_eq!(code, StatusCode::OK);
    let id = v["meeting"]["id"].as_str().unwrap().to_string();
    assert_eq!(v["meeting"]["source"], "auto");

    let (code, Json(got)) = get_meeting(State(ctx), Path(id)).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(got["meeting"]["title"], "Kickoff");
}

#[tokio::test]
async fn get_missing_meeting_is_404() {
    let ctx = ctx_with(FakeHost::default());
    let (code, Json(v)) = get_meeting(State(ctx), Path("nope".into())).await;
    assert_eq!(code, StatusCode::NOT_FOUND);
    assert_eq!(v["error"], "not found");
}

#[tokio::test]
async fn create_meeting_defaults_source_to_manual() {
    let ctx = ctx_with(FakeHost::default());
    let body = StartBody {
        title: String::new(),
        app: None,
        source: None,
    };
    let (_, Json(v)) = create_meeting(State(ctx), Json(body)).await;
    assert_eq!(v["meeting"]["source"], "manual");
}

#[tokio::test]
async fn rename_rejects_empty_title() {
    let ctx = ctx_with(FakeHost::default());
    let (code, _) = rename_meeting(
        State(ctx),
        Path("x".into()),
        Json(RenameBody {
            title: "   ".into(),
        }),
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rename_missing_is_404_and_valid_is_200() {
    let ctx = ctx_with(FakeHost::default());
    let (code, _) = rename_meeting(
        State(ctx.clone()),
        Path("missing".into()),
        Json(RenameBody {
            title: "New".into(),
        }),
    )
    .await;
    assert_eq!(code, StatusCode::NOT_FOUND);

    let m = ctx
        .engine
        .start(String::new(), None, MeetingSource::Manual)
        .await
        .unwrap();
    let (code, Json(v)) = rename_meeting(
        State(ctx),
        Path(m.id),
        Json(RenameBody {
            title: "Renamed".into(),
        }),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(v["meeting"]["title"], "Renamed");
    assert_eq!(v["meeting"]["title_custom"], true);
}

#[tokio::test]
async fn delete_meeting_ok_then_404() {
    let ctx = ctx_with(FakeHost::default());
    let m = ctx
        .engine
        .start(String::new(), None, MeetingSource::Manual)
        .await
        .unwrap();
    let (code, _) = delete_meeting(State(ctx.clone()), Path(m.id.clone())).await;
    assert_eq!(code, StatusCode::OK);
    let (code, _) = delete_meeting(State(ctx), Path(m.id)).await;
    assert_eq!(code, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn list_meetings_reports_created() {
    let ctx = ctx_with(FakeHost::default());
    ctx.engine
        .start("m".into(), None, MeetingSource::Manual)
        .await
        .unwrap();
    let Json(v) = list_meetings(State(ctx)).await;
    assert_eq!(v["meetings"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn transcript_endpoint_returns_segments_and_text() {
    let ctx = ctx_with(FakeHost::default());
    let m = ctx
        .engine
        .start(String::new(), None, MeetingSource::Manual)
        .await
        .unwrap();
    ctx.engine
        .store
        .insert_segment(&m.id, 0, None, "hi")
        .await
        .unwrap();
    ctx.engine
        .store
        .insert_segment(&m.id, 5, None, "there")
        .await
        .unwrap();
    let (code, Json(v)) = get_transcript(State(ctx), Path(m.id)).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(v["text"], "hi\nthere");
    assert_eq!(v["segments"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn detect_respects_master_toggle() {
    let ctx = ctx_with(FakeHost::default());
    ctx.engine
        .pref_set("meeting-detection-enabled", "false")
        .await
        .unwrap();
    let (code, Json(v)) = detect(
        State(ctx),
        Json(DetectBody {
            app: "zoom".into(),
            title: None,
        }),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(v["broadcast"], false);
    assert_eq!(v["reason"], "detection disabled");
}

#[tokio::test]
async fn detect_ignores_non_meeting_app() {
    let ctx = ctx_with(FakeHost::default());
    let (_, Json(v)) = detect(
        State(ctx),
        Json(DetectBody {
            app: "notepad".into(),
            title: None,
        }),
    )
    .await;
    assert_eq!(v["broadcast"], false);
    assert_eq!(v["reason"], "not a known meeting app");
}

#[tokio::test]
async fn detect_known_app_broadcasts_once_then_debounces() {
    let ctx = ctx_with(FakeHost::default());
    // Substring match against the default app list ("zoom").
    let (_, Json(first)) = detect(
        State(ctx.clone()),
        Json(DetectBody {
            app: "ZoomMtg".into(),
            title: Some("Standup".into()),
        }),
    )
    .await;
    assert_eq!(first["broadcast"], true);
    let (_, Json(second)) = detect(
        State(ctx),
        Json(DetectBody {
            app: "ZoomMtg".into(),
            title: None,
        }),
    )
    .await;
    assert_eq!(second["broadcast"], false);
}

#[tokio::test]
async fn detection_config_defaults_and_updates() {
    let ctx = ctx_with(FakeHost::default());
    // Default: enabled true, non-empty app list.
    let Json(def) = get_detection_config(State(ctx.clone())).await;
    assert_eq!(def["enabled"], true);
    assert!(!def["apps"].as_array().unwrap().is_empty());

    // Update both fields.
    let (code, Json(after)) = put_detection_config(
        State(ctx.clone()),
        Json(DetectionConfigBody {
            enabled: Some(false),
            apps: Some(vec!["custom".into()]),
        }),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(after["enabled"], false);
    assert_eq!(after["apps"], json!(["custom"]));

    // Persisted: a fresh read reflects the update.
    let Json(reread) = get_detection_config(State(ctx)).await;
    assert_eq!(reread["enabled"], false);
    assert_eq!(reread["apps"], json!(["custom"]));
}
