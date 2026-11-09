//! Chunk ingest: decode, persist stereo PCM for diarization, transcribe, append.

use crate::{audio, MeetingEngine, MeetingEvent, Segment};

impl MeetingEngine {
    /// Ingest one captured audio chunk: downmix it to mono for transcription
    /// (whisper default, or the requested engine), persist the stereo audio for
    /// later diarization, append the text as a transcript segment, and broadcast
    /// it. `offset_ms` is the chunk's sample-accurate position from Shadow; when
    /// absent (e.g. a GUI-mic feed) we fall back to wall-clock since start.
    pub async fn ingest_chunk(
        &self,
        meeting_id: &str,
        wav: Vec<u8>,
        filename: String,
        engine: Option<&str>,
        offset_ms: Option<i64>,
    ) -> Result<Segment, String> {
        let meeting = self
            .store
            .get_meeting(meeting_id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("meeting '{meeting_id}' not found"))?;

        // Decode once, then fan out: mono for whisper, stereo (16 kHz) persisted
        // for diarization. A decode failure falls back to sending the raw bytes so
        // an odd WAV still transcribes.
        let mono_wav = match audio::decode_wav(&wav) {
            Ok(decoded) => {
                if decoded.rate == audio::TARGET_RATE {
                    let stereo = audio::to_stereo_i16(&decoded);
                    match audio::append_pcm(self.store.audio_seal(), meeting_id, &stereo) {
                        Ok(0) => {}
                        Ok(records) => {
                            if let Err(e) = self.store.set_pcm_records(meeting_id, records).await {
                                tracing::debug!(
                                    "meetings: recording the audio length failed: {e:#}"
                                );
                            }
                        }
                        Err(e) => tracing::debug!("meetings: persisting chunk audio failed: {e:#}"),
                    }
                }
                audio::to_mono_wav(&decoded).unwrap_or(wav)
            }
            Err(_) => wav,
        };

        let text = self
            .host
            .transcribe(mono_wav, filename, engine.map(str::to_string))
            .await?;
        let text = text.trim().to_string();
        // Whisper emits blank/placeholder text for silence; skip empty segments so
        // the transcript stays clean.
        if text.is_empty() {
            return Err("empty transcription (silence)".to_string());
        }

        let t_offset_ms = offset_ms.unwrap_or_else(|| millis_since(&meeting.started_at));
        let segment = self
            .store
            .insert_segment(meeting_id, t_offset_ms, None, &text)
            .await
            .map_err(|e| e.to_string())?;
        self.store.emit(MeetingEvent::Segment {
            segment: segment.clone(),
        });
        Ok(segment)
    }

    /// The full transcript as one newline-joined string.
    pub async fn transcript(&self, meeting_id: &str) -> Result<String, String> {
        let segments = self
            .store
            .list_segments(meeting_id)
            .await
            .map_err(|e| e.to_string())?;
        Ok(segments
            .into_iter()
            .map(|s| s.text)
            .collect::<Vec<_>>()
            .join("\n"))
    }
}

/// Milliseconds between an RFC3339 start time and now (clamped at 0).
pub(crate) fn millis_since(started_at: &str) -> i64 {
    match chrono::DateTime::parse_from_rfc3339(started_at) {
        Ok(start) => {
            let elapsed =
                chrono::Utc::now().signed_duration_since(start.with_timezone(&chrono::Utc));
            elapsed.num_milliseconds().max(0)
        }
        Err(_) => 0,
    }
}
