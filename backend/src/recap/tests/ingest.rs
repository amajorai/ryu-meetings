//! Chunk upload: server-side digests, idempotency, limits, decode and silence.

use axum::http::{Method, StatusCode};
use sha2::Digest;

use super::support::*;

fn sha(bytes: &[u8]) -> String {
    format!("{:x}", sha2::Sha256::digest(bytes))
}

#[tokio::test]
async fn the_server_sha_drives_dedupe_conflict_and_mismatch() {
    let h = harness().await;
    let id = started(&h.app, "alice").await;
    let wav = speech_wav(1_000);
    let (code, body) = put_chunk(
        &h.app,
        "alice",
        &id,
        0,
        0,
        1_000,
        wav.clone(),
        &[("x-recap-sha256", &sha(&wav))],
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "transcribed");
    assert_eq!(body["duplicate"], false);
    let (code, body) = put_chunk(&h.app, "alice", &id, 0, 0, 1_000, wav.clone(), &[]).await;
    assert_eq!(
        (code, body["duplicate"].as_bool()),
        (StatusCode::OK, Some(true))
    );
    let other = speech_wav(900);
    let (code, body) = put_chunk(&h.app, "alice", &id, 0, 0, 900, other.clone(), &[]).await;
    assert_eq!(
        (code, body["error"].as_str()),
        (StatusCode::CONFLICT, Some("seq_conflict"))
    );
    let (code, body) = put_chunk(
        &h.app,
        "alice",
        &id,
        1,
        1_000,
        900,
        other,
        &[("x-recap-sha256", &sha(b"lie"))],
    )
    .await;
    assert_eq!(
        (code, body["error"].as_str()),
        (StatusCode::CONFLICT, Some("sha_mismatch"))
    );
    assert_eq!(
        h.ctx.engine.store.recap_segments(&id).await.unwrap().len(),
        1,
        "one segment, no doubles"
    );
}

#[tokio::test]
async fn seq_offset_overlap_size_and_codec_limits_return_typed_errors() {
    let h = harness().await;
    let id = started(&h.app, "alice").await;
    let wav = speech_wav(1_000);
    let (code, _) = put_chunk(&h.app, "alice", &id, 1441, 0, 1_000, wav.clone(), &[]).await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    let (code, _) = put_chunk(&h.app, "alice", &id, 0, 43_200_000, 1_000, wav.clone(), &[]).await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    let (code, _) = put_chunk(&h.app, "alice", &id, 0, 0, 1_000, wav.clone(), &[]).await;
    assert_eq!(code, StatusCode::OK);
    let (code, body) = put_chunk(&h.app, "alice", &id, 1, 500, 1_000, wav.clone(), &[]).await;
    assert_eq!(
        (code, body["error"].as_str()),
        (StatusCode::CONFLICT, Some("offset_overlap"))
    );
    let (code, body) = put_chunk(
        &h.app,
        "alice",
        &id,
        2,
        2_000,
        1_000,
        vec![0u8; 2_000_001],
        &[],
    )
    .await;
    assert_eq!(
        (code, body["error"].as_str()),
        (StatusCode::PAYLOAD_TOO_LARGE, Some("chunk_too_large"))
    );
    let (code, _) = put_chunk(
        &h.app,
        "alice",
        &id,
        3,
        3_000,
        1_000,
        vec![0u8; 2_700_000],
        &[],
    )
    .await;
    assert_eq!(
        code,
        StatusCode::PAYLOAD_TOO_LARGE,
        "the route body limit refuses before buffering more"
    );
    let (code, _) = put_chunk(
        &h.app,
        "alice",
        &id,
        4,
        4_000,
        1_000,
        wav.clone(),
        &[("content-type", "audio/ogg; codecs=opus")],
    )
    .await;
    assert_eq!(code, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    let (code, _) = put_chunk(&h.app, "alice", &id, 5, 5_000, 300, wav, &[]).await;
    assert_eq!(
        code,
        StatusCode::BAD_REQUEST,
        "audio longer than declared is refused"
    );
}

#[tokio::test]
async fn the_daily_quota_returns_429() {
    let h = harness().await;
    let id = started(&h.app, "alice").await;
    let day = chrono::Utc::now().format("%Y-%m-%d").to_string();
    h.ctx
        .engine
        .store
        .add_recap_usage(
            "alice",
            &day,
            crate::recap::limits::DEFAULT_DAILY_QUOTA_BYTES,
        )
        .await
        .unwrap();
    let (code, body) = put_chunk(&h.app, "alice", &id, 0, 0, 1_000, speech_wav(1_000), &[]).await;
    assert_eq!(
        (code, body["error"].as_str()),
        (StatusCode::TOO_MANY_REQUESTS, Some("quota_exceeded"))
    );
}

#[tokio::test]
async fn silent_chunks_never_reach_stt_and_are_ledgered() {
    let h = harness().await;
    let id = started(&h.app, "alice").await;
    let (code, body) = put_chunk(
        &h.app,
        "alice",
        &id,
        0,
        0,
        1_000,
        Vec::new(),
        &[("x-recap-silent", "1")],
    )
    .await;
    assert_eq!(
        (code, body["status"].as_str()),
        (StatusCode::OK, Some("skipped_client"))
    );
    let (code, body) = put_chunk(
        &h.app,
        "alice",
        &id,
        1,
        1_000,
        1_000,
        silent_wav(1_000),
        &[],
    )
    .await;
    assert_eq!(
        (code, body["status"].as_str()),
        (StatusCode::OK, Some("skipped_silent"))
    );
    assert!(h
        .ctx
        .engine
        .store
        .recap_segments(&id)
        .await
        .unwrap()
        .is_empty());
    let (code, body) = call(
        &h.app,
        Method::GET,
        &format!("/recaps/{id}/chunks"),
        Some("alice"),
        None,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(body["received"], serde_json::json!([0, 1]));
    assert_eq!(body["missing"], serde_json::json!([]));
}

#[tokio::test]
async fn out_of_order_chunks_read_back_in_offset_order_and_resume_lists_gaps() {
    let h = harness().await;
    let id = started(&h.app, "alice").await;
    for (seq, offset) in [(2u32, 2_000i64), (0, 0)] {
        let (code, _) = put_chunk(
            &h.app,
            "alice",
            &id,
            seq,
            offset,
            1_000,
            speech_wav(1_000),
            &[],
        )
        .await;
        assert_eq!(code, StatusCode::OK);
    }
    let offsets: Vec<i64> = h
        .ctx
        .engine
        .store
        .recap_segments(&id)
        .await
        .unwrap()
        .iter()
        .map(|s| s.t_offset_ms)
        .collect();
    assert_eq!(offsets, vec![0, 2_000]);
    let (_, body) = call(
        &h.app,
        Method::GET,
        &format!("/recaps/{id}/chunks"),
        Some("alice"),
        None,
    )
    .await;
    assert_eq!(body["missing"], serde_json::json!([1]));
}

#[tokio::test]
async fn aac_chunks_decode_through_symphonia() {
    let h = harness().await;
    let id = started(&h.app, "alice").await;
    let m4a = include_bytes!("../../../tests/fixtures/tone.m4a").to_vec();
    let (code, body) = put_chunk(
        &h.app,
        "alice",
        &id,
        0,
        0,
        1_000,
        m4a,
        &[("content-type", "audio/mp4")],
    )
    .await;
    assert_eq!(
        (code, body["status"].as_str()),
        (StatusCode::OK, Some("transcribed")),
        "{body}"
    );
}
