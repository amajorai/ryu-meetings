//! Pure helpers of the recap pipeline.

use chrono::{Duration, Utc};

use crate::notes::ActionItem;
use crate::recap::{digest, finalize, ingest, records, start};
use crate::test_support::recap_row;
use crate::{MeetingNotes, MeetingStatus};

fn notes(summary: &str) -> MeetingNotes {
    MeetingNotes {
        summary: summary.into(),
        title: Some("  \"A very long title that keeps going well past the eighty character limit for titles\"  ".into()),
        topics: (0..12).map(|i| format!("topic {i}")).collect(),
        people: vec!["Dana".into()],
        action_items_detail: vec![ActionItem {
            text: "send it".into(),
            owner: Some("Dana".into()),
            due: Some("2026-11-20".into()),
        }],
        ..MeetingNotes::default()
    }
}

#[test]
fn shape_notes_drops_names_caps_topics_and_trims_the_title() {
    let shaped = finalize::shape_notes(notes("Call 415-555-0100."), false);
    assert!(shaped.people.is_empty());
    assert_eq!(shaped.action_items_detail[0].owner, None);
    assert_eq!(
        shaped.action_items_detail[0].due.as_deref(),
        Some("2026-11-20")
    );
    assert_eq!(shaped.topics.len(), 8);
    assert_eq!(shaped.summary, "Call [phone].");
    let title = shaped.title.unwrap();
    assert!(title.starts_with("A very long title") && title.chars().count() == 80);
    let kept = finalize::shape_notes(notes("x"), true);
    assert_eq!(kept.people, vec!["Dana"]);
    assert_eq!(kept.action_items_detail[0].owner.as_deref(), Some("Dana"));
}

#[test]
fn the_digest_is_assembled_without_names() {
    let mut a = recap_row("rcp_a", "u", "2026-11-08", "2026-11-08T09:00:00Z");
    a.status = MeetingStatus::Done;
    a.notes = Some(notes("First talk. More detail."));
    a.recap.as_mut().unwrap().duration_ms = 60_000;
    let mut b = recap_row("rcp_b", "u", "2026-11-08", "2026-11-08T10:00:00Z");
    b.status = MeetingStatus::Done;
    b.notes = Some(MeetingNotes {
        summary: "Second talk.".into(),
        action_items: vec!["plain item".into()],
        topics: vec!["Topic 1".into()],
        ..MeetingNotes::default()
    });
    let body = digest::assemble("2026-11-08", "UTC", &[a.clone(), b.clone()]);
    assert_eq!(body["count"], 2);
    assert_eq!(body["total_duration_ms"], 60_000);
    assert_eq!(body["summary"], "First talk. Second talk.");
    assert_eq!(body["action_items_detail"][0]["recap_id"], "rcp_a");
    assert!(body["action_items_detail"][0].get("owner").is_none());
    assert_eq!(body["action_items_detail"][1]["text"], "plain item");
    assert!(!body.to_string().contains("Dana"));
    assert_eq!(digest::source_ids(&[b, a]), "rcp_a,rcp_b");
}

#[test]
fn markdown_export_renders_notes_and_transcript() {
    let mut recap = recap_row("rcp_a", "u", "2026-11-08", "2026-11-08T09:00:00Z");
    recap.title = "Budget".into();
    recap.notes = Some(MeetingNotes {
        summary: "We met.".into(),
        key_points: vec!["one".into()],
        ..MeetingNotes::default()
    });
    let md = records::recap_markdown(&recap, &["hello".into()]);
    assert!(md.starts_with("## Budget\n\n_2026-11-08"));
    assert!(md.contains("### Key points\n\n- one"));
    assert!(md.contains("### Transcript\n\nhello"));
    assert!(!md.contains("### Decisions"), "empty sections are left out");
}

#[test]
fn start_validation_and_the_confirm_window() {
    let body: start::StartBody = serde_json::from_value(serde_json::json!({
        "client_session_id": uuid::Uuid::new_v4().to_string(),
        "tz": "Pacific/Kiritimati",
        "local_day": (Utc::now() + Duration::hours(14)).format("%Y-%m-%d").to_string(),
        "consent_version": 1,
    }))
    .unwrap();
    assert!(
        start::validate_start(&body, Utc::now()).is_ok(),
        "today in the client's zone"
    );

    let mut recap = recap_row("rcp_a", "u", "2026-11-08", &Utc::now().to_rfc3339());
    assert!(!start::unconfirmed_expired(
        &recap,
        Utc::now() + Duration::seconds(60)
    ));
    recap.recap.as_mut().unwrap().needs_confirm = true;
    assert!(!start::unconfirmed_expired(&recap, Utc::now()));
    assert!(start::unconfirmed_expired(
        &recap,
        Utc::now() + Duration::seconds(31)
    ));
    assert!(start::accepts_audio(&recap).is_err());
    recap.recap.as_mut().unwrap().confirmed_at = Some(Utc::now().to_rfc3339());
    assert!(start::accepts_audio(&recap).is_ok());
}

#[test]
fn missing_seqs_lists_the_gaps() {
    assert_eq!(ingest::missing_seqs(&[0, 2, 5], 6), vec![1, 3, 4]);
    assert!(ingest::missing_seqs(&[], 0).is_empty());
}
