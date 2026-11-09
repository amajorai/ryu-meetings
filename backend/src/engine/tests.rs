use crate::test_support::*;
use crate::*;

#[tokio::test]
async fn start_defaults_title_and_stays_auto_nameable() {
    let engine = engine_with(FakeHost::default());
    let mut rx = engine.store.subscribe();
    let m = engine
        .start(String::new(), Some("zoom".into()), MeetingSource::Manual)
        .await
        .unwrap();
    assert!(m.id.starts_with("mtg_"));
    assert_eq!(m.status, MeetingStatus::Recording);
    assert!(
        !m.title_custom,
        "empty title stays eligible for auto-naming"
    );
    assert!(m.title.starts_with("Zoom meeting — "));
    // Persisted and broadcast.
    assert!(engine.get(&m.id).await.unwrap().is_some());
    match rx.try_recv() {
        Ok(MeetingEvent::Started { meeting }) => assert_eq!(meeting.id, m.id),
        other => panic!("expected Started event, got {other:?}"),
    }
}

#[tokio::test]
async fn start_with_user_title_marks_custom() {
    let engine = engine_with(FakeHost::default());
    let m = engine
        .start("Board sync".into(), None, MeetingSource::Auto)
        .await
        .unwrap();
    assert_eq!(m.title, "Board sync");
    assert!(m.title_custom);
    assert_eq!(m.source, MeetingSource::Auto);
}

#[tokio::test]
async fn start_import_tags_app_and_defaults_title() {
    let engine = engine_with(FakeHost::default());
    let m = engine.start_import(String::new()).await.unwrap();
    assert_eq!(m.app.as_deref(), Some("import"));
    assert!(!m.title_custom);
    assert!(m.title.starts_with("Meeting — "));
}

#[tokio::test]
async fn ingest_chunk_appends_transcribed_segment() {
    let mut host = FakeHost::default();
    host.transcribe_text = "  captured speech  ".into();
    let engine = engine_with(host);
    let m = engine
        .start(String::new(), None, MeetingSource::Manual)
        .await
        .unwrap();
    let wav = stereo_wav_16k(16_000);
    let seg = engine
        .ingest_chunk(&m.id, wav, "chunk.wav".into(), None, Some(4200))
        .await
        .unwrap();
    assert_eq!(seg.text, "captured speech", "text is trimmed");
    assert_eq!(
        seg.t_offset_ms, 4200,
        "explicit offset wins over wall-clock"
    );
    // Persisted stereo PCM is readable back as a WAV (diarization audio).
    let records = engine.store.pcm_records(&m.id).await.unwrap();
    let pcm = audio::read_pcm_as_wav(engine.store.audio_seal(), &m.id, records).unwrap();
    assert!(pcm.is_some());
    audio::remove_pcm(&m.id);
}

#[tokio::test]
async fn ingest_chunk_wallclock_offset_when_absent() {
    let engine = engine_with(FakeHost::default());
    let m = engine
        .start(String::new(), None, MeetingSource::Manual)
        .await
        .unwrap();
    let seg = engine
        .ingest_chunk(&m.id, stereo_wav_16k(10), "c.wav".into(), None, None)
        .await
        .unwrap();
    assert!(seg.t_offset_ms >= 0);
}

#[tokio::test]
async fn ingest_chunk_silence_is_an_error() {
    let mut host = FakeHost::default();
    host.transcribe_text = "   ".into();
    let engine = engine_with(host);
    let m = engine
        .start(String::new(), None, MeetingSource::Manual)
        .await
        .unwrap();
    let err = engine
        .ingest_chunk(&m.id, stereo_wav_16k(10), "c.wav".into(), None, Some(0))
        .await
        .unwrap_err();
    assert!(err.contains("silence"), "got: {err}");
}

#[tokio::test]
async fn ingest_chunk_unknown_meeting_errors() {
    let engine = engine_with(FakeHost::default());
    let err = engine
        .ingest_chunk(
            "mtg_missing",
            stereo_wav_16k(10),
            "c.wav".into(),
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(err.contains("not found"), "got: {err}");
}

#[tokio::test]
async fn transcript_joins_segments_in_order() {
    let engine = engine_with(FakeHost::default());
    let m = engine
        .start(String::new(), None, MeetingSource::Manual)
        .await
        .unwrap();
    engine
        .store
        .insert_segment(&m.id, 0, None, "one")
        .await
        .unwrap();
    engine
        .store
        .insert_segment(&m.id, 10, None, "two")
        .await
        .unwrap();
    assert_eq!(engine.transcript(&m.id).await.unwrap(), "one\ntwo");
}

/// Claim and finalize in one step (the guard drops on return).
async fn finalize_now(engine: &MeetingEngine, id: &str, effort: &str) -> Result<Meeting, String> {
    let claim = engine.begin_finalize(id).expect("no finalize in flight");
    engine.finalize(&claim, "model", effort, "prompt").await
}

#[tokio::test]
async fn finalize_empty_transcript_writes_placeholder() {
    let engine = engine_with(FakeHost::default());
    let m = engine
        .start(String::new(), None, MeetingSource::Manual)
        .await
        .unwrap();
    let done = finalize_now(&engine, &m.id, "").await.unwrap();
    assert_eq!(done.status, MeetingStatus::Done);
    assert!(done.ended_at.is_some());
    let notes = done.notes.expect("notes present");
    assert!(notes.summary.contains("No speech"));
}

#[tokio::test]
async fn finalize_with_transcript_but_unreachable_gateway_errors() {
    let engine = engine_with(FakeHost::default());
    let m = engine
        .start(String::new(), None, MeetingSource::Manual)
        .await
        .unwrap();
    engine
        .store
        .insert_segment(&m.id, 0, None, "we shipped the release")
        .await
        .unwrap();
    // gateway_url points at an unused loopback port ⇒ generate_notes errors.
    let err = finalize_now(&engine, &m.id, "low").await.unwrap_err();
    assert!(err.contains("gateway"), "got: {err}");
}

/// A failed note generation must not strand the meeting in Processing: the
/// GUI only offers "Stop & summarize" (the retry) on a recording meeting.
#[tokio::test]
async fn finalize_gateway_error_reverts_to_recording() {
    let engine = engine_with(FakeHost::default());
    let m = engine
        .start(String::new(), None, MeetingSource::Manual)
        .await
        .unwrap();
    engine
        .store
        .insert_segment(&m.id, 0, None, "we shipped the release")
        .await
        .unwrap();
    let mut rx = engine.store.subscribe();
    assert!(finalize_now(&engine, &m.id, "low").await.is_err());

    let stored = engine.get(&m.id).await.unwrap().unwrap();
    assert_eq!(stored.status, MeetingStatus::Recording);
    assert!(stored.notes.is_none());
    let mut statuses = Vec::new();
    while let Ok(event) = rx.try_recv() {
        if let MeetingEvent::Status { status, .. } = event {
            statuses.push(status);
        }
    }
    assert_eq!(
        statuses,
        vec![MeetingStatus::Processing, MeetingStatus::Recording]
    );
    // The claim was released, so the user can retry.
    assert!(engine.begin_finalize(&m.id).is_some());
}

#[tokio::test]
async fn finalize_unknown_meeting_errors() {
    let engine = engine_with(FakeHost::default());
    let err = finalize_now(&engine, "nope", "").await.unwrap_err();
    assert!(err.contains("not found"));
}

/// One finalize per meeting at a time: a second claim is refused until the
/// first guard drops, and claims on other meetings are independent.
#[tokio::test]
async fn begin_finalize_refuses_an_overlapping_claim() {
    let engine = engine_with(FakeHost::default());
    let first = engine.begin_finalize("mtg_a").expect("first claim");
    assert_eq!(first.meeting_id(), "mtg_a");
    assert!(engine.begin_finalize("mtg_a").is_none());
    // Clones share the in-flight set (handlers hold clones of one engine).
    assert!(engine.clone().begin_finalize("mtg_a").is_none());
    assert!(engine.begin_finalize("mtg_b").is_some());
    drop(first);
    assert!(engine.begin_finalize("mtg_a").is_some());
}

/// Finalizing a finished meeting again regenerates its notes (documented:
/// finalizing is not one-shot) without touching the user's title.
#[tokio::test]
async fn refinalize_keeps_user_title() {
    let engine = engine_with(FakeHost::default());
    let m = engine
        .start(String::new(), None, MeetingSource::Manual)
        .await
        .unwrap();
    finalize_now(&engine, &m.id, "").await.unwrap();
    engine.store.set_title(&m.id, "Mine").await.unwrap();
    let again = finalize_now(&engine, &m.id, "").await.unwrap();
    assert_eq!(again.status, MeetingStatus::Done);
    assert_eq!(again.title, "Mine");
    assert!(again.title_custom);
}

#[tokio::test]
async fn record_detection_debounces_same_app() {
    let engine = engine_with(FakeHost::default());
    assert!(engine.record_detection("zoom", Some("Standup")).await);
    // Same app within the debounce window ⇒ suppressed.
    assert!(!engine.record_detection("zoom", None).await);
    // A different app is a fresh detection.
    assert!(engine.record_detection("teams", None).await);
}

#[tokio::test]
async fn auto_title_applies_only_when_not_custom() {
    let mut host = FakeHost::default();
    host.title = Some("Q3 planning".into());
    let engine = engine_with(host);
    let m = engine
        .start(String::new(), None, MeetingSource::Manual)
        .await
        .unwrap();
    let applied = engine.auto_title(&m.id, "long enough summary text").await;
    assert_eq!(applied.as_deref(), Some("Q3 planning"));
    assert_eq!(
        engine.get(&m.id).await.unwrap().unwrap().title,
        "Q3 planning"
    );
}

#[tokio::test]
async fn auto_title_leaves_user_titled_alone() {
    let mut host = FakeHost::default();
    host.title = Some("Model pick".into());
    let engine = engine_with(host);
    let m = engine
        .start("My title".into(), None, MeetingSource::Manual)
        .await
        .unwrap();
    assert!(engine.auto_title(&m.id, "summary").await.is_none());
    assert_eq!(engine.get(&m.id).await.unwrap().unwrap().title, "My title");
}

#[tokio::test]
async fn auto_title_none_when_host_declines() {
    let engine = engine_with(FakeHost::default()); // title = None
    let m = engine
        .start(String::new(), None, MeetingSource::Manual)
        .await
        .unwrap();
    assert!(engine.auto_title(&m.id, "summary").await.is_none());
}

#[tokio::test]
async fn attach_space_persists_linkage() {
    let engine = engine_with(FakeHost::default());
    let m = engine
        .start(String::new(), None, MeetingSource::Manual)
        .await
        .unwrap();
    let updated = engine.attach_space(&m.id, "space1", "doc1").await.unwrap();
    assert_eq!(updated.space_id.as_deref(), Some("space1"));
    assert_eq!(updated.doc_id.as_deref(), Some("doc1"));
    let reloaded = engine.get(&m.id).await.unwrap().unwrap();
    assert_eq!(reloaded.doc_id.as_deref(), Some("doc1"));
}

#[tokio::test]
async fn attach_space_unknown_meeting_errors() {
    let engine = engine_with(FakeHost::default());
    assert!(engine.attach_space("nope", "s", "d").await.is_err());
}

/// Filing finishes after the meeting row was deleted ⇒ the link fails rather
/// than re-creating the deleted meeting.
#[tokio::test]
async fn attach_space_does_not_resurrect_a_deleted_meeting() {
    let engine = engine_with(FakeHost::default());
    let m = engine
        .start(String::new(), None, MeetingSource::Manual)
        .await
        .unwrap();
    assert!(engine.delete(&m.id).await.unwrap());
    assert!(engine.attach_space(&m.id, "s", "d").await.is_err());
    assert!(engine.get(&m.id).await.unwrap().is_none());
}

#[tokio::test]
async fn delete_removes_meeting() {
    let engine = engine_with(FakeHost::default());
    let m = engine
        .start(String::new(), None, MeetingSource::Manual)
        .await
        .unwrap();
    assert!(engine.delete(&m.id).await.unwrap());
    assert!(engine.get(&m.id).await.unwrap().is_none());
    // Deleting again reports "not removed".
    assert!(!engine.delete(&m.id).await.unwrap());
}

#[tokio::test]
async fn list_returns_created_meetings() {
    let engine = engine_with(FakeHost::default());
    engine
        .start("a".into(), None, MeetingSource::Manual)
        .await
        .unwrap();
    engine
        .start("b".into(), None, MeetingSource::Manual)
        .await
        .unwrap();
    assert_eq!(engine.list().await.unwrap().len(), 2);
}

#[tokio::test]
async fn pref_and_model_pass_through_host() {
    let engine = engine_with(FakeHost::default());
    engine.pref_set("k", "v").await.unwrap();
    assert_eq!(engine.pref_get("k").await.as_deref(), Some("v"));
    assert_eq!(engine.default_notes_model(), "test-model");
}

#[tokio::test]
async fn pref_set_surfaces_host_error() {
    let mut host = FakeHost::default();
    host.pref_set_fails = true;
    let engine = engine_with(host);
    assert!(engine.pref_set("k", "v").await.is_err());
}

#[tokio::test]
async fn save_notes_to_space_passes_through() {
    let mut host = FakeHost::default();
    host.space = Some(("s".into(), "d".into()));
    let engine = engine_with(host);
    assert_eq!(
        engine.save_notes_to_space("t", "# md").await,
        Some(("s".to_string(), "d".to_string()))
    );
}
