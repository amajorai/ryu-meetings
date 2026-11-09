//! Shared test scaffolding (a fake [`MeetingsHost`] + a temp-backed engine) reused
//! by the engine tests and the handler tests in [`crate::api`].

use crate::*;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

/// An in-memory, deterministic [`MeetingsHost`]: prefs live in a map, the host
/// callbacks return whatever the fields say. No network, no env, no Core.
pub(crate) struct FakeHost {
    pub prefs: StdMutex<HashMap<String, String>>,
    /// What `transcribe` returns for every chunk (whitespace ⇒ silence path).
    pub transcribe_text: String,
    /// What `generate_title` returns (`None` ⇒ auto-title no-op).
    pub title: Option<String>,
    /// What `save_notes_to_space` returns (`None` ⇒ no Space linkage).
    pub space: Option<(String, String)>,
    pub default_model: String,
    /// Points at an unused loopback port so any real note-gen call fails fast.
    pub gateway_url: String,
    pub gateway_token: Option<String>,
    /// Force `pref_set` to error (to exercise the write-failure path).
    pub pref_set_fails: bool,
    /// How many times `save_notes_to_space` ran (shared, so a test can keep a
    /// handle after the host moves into the engine).
    pub space_calls: Arc<AtomicUsize>,
    /// What `transcription_route` answers (shared, so a test can move it).
    pub stt_route: Arc<StdMutex<String>>,
}

impl Default for FakeHost {
    fn default() -> Self {
        Self {
            prefs: StdMutex::new(HashMap::new()),
            transcribe_text: "hello world".to_string(),
            title: None,
            space: None,
            default_model: "test-model".to_string(),
            gateway_url: "http://127.0.0.1:59991".to_string(),
            gateway_token: None,
            pref_set_fails: false,
            space_calls: Arc::new(AtomicUsize::new(0)),
            stt_route: Arc::new(StdMutex::new("Parakeet (on this Ryu node)".to_string())),
        }
    }
}

#[async_trait]
impl MeetingsHost for FakeHost {
    async fn pref_get(&self, key: &str) -> Option<String> {
        self.prefs.lock().unwrap().get(key).cloned()
    }
    async fn pref_set(&self, key: &str, value: &str) -> Result<(), String> {
        if self.pref_set_fails {
            return Err("pref write failed".to_string());
        }
        self.prefs
            .lock()
            .unwrap()
            .insert(key.to_string(), value.to_string());
        Ok(())
    }
    fn gateway_url(&self) -> String {
        self.gateway_url.clone()
    }
    fn gateway_token(&self) -> Option<String> {
        self.gateway_token.clone()
    }
    fn default_notes_model(&self) -> String {
        self.default_model.clone()
    }
    fn transcription_route(&self) -> String {
        self.stt_route.lock().unwrap().clone()
    }
    async fn transcribe(
        &self,
        _wav: Vec<u8>,
        _filename: String,
        _engine: Option<String>,
    ) -> Result<String, String> {
        Ok(self.transcribe_text.clone())
    }
    async fn generate_title(&self, _summary: &str) -> Option<String> {
        self.title.clone()
    }
    async fn save_notes_to_space(&self, _title: &str, _markdown: &str) -> Option<(String, String)> {
        self.space_calls.fetch_add(1, Ordering::SeqCst);
        self.space.clone()
    }
}

/// A fresh SQLite store under a unique temp path (WAL files land in the OS temp
/// dir; harmless leftovers for a test run).
pub(crate) fn temp_store() -> MeetingStore {
    let dir = std::env::temp_dir().join(format!(
        "ryu-meetings-test-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    MeetingStore::open(dir.join("meetings.db")).expect("open temp meetings store")
}

/// Build an engine over a temp store and the given fake host.
pub(crate) fn engine_with(host: FakeHost) -> MeetingEngine {
    MeetingEngine::new(temp_store(), Arc::new(host), reqwest::Client::new())
}

/// A valid interleaved-stereo 16 kHz WAV of `frames` L/R pairs.
pub(crate) fn stereo_wav_16k(frames: usize) -> Vec<u8> {
    let samples: Vec<i16> = (0..frames).flat_map(|i| [i as i16, -(i as i16)]).collect();
    crate::audio::encode_wav(2, crate::audio::TARGET_RATE, &samples).unwrap()
}

/// A recap row owned by `owner` on local day `day` (status recording).
pub(crate) fn recap_row(id: &str, owner: &str, day: &str, created_at: &str) -> Meeting {
    Meeting {
        id: id.to_string(),
        title: "Recap".to_string(),
        title_custom: false,
        icon: None,
        app: Some("recap".into()),
        source: MeetingSource::Phone,
        status: MeetingStatus::Recording,
        started_at: created_at.to_string(),
        ended_at: None,
        participants: vec![],
        notes: None,
        space_id: None,
        doc_id: None,
        created_at: created_at.to_string(),
        updated_at: created_at.to_string(),
        kind: MeetingKind::Recap,
        recap: Some(RecapMeta {
            owner_user_id: owner.to_string(),
            client_session_id: uuid::Uuid::new_v4().to_string(),
            tz: "UTC".into(),
            local_day: day.to_string(),
            ..RecapMeta::default()
        }),
    }
}
