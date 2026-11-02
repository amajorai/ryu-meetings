//! Speaker diarization for a finished meeting — "who said what".
//!
//! Diarization is a local model call, not an LLM call. The reusable
//! `ryu-diarization` primitive owns the typed model-neutral request and authenticated
//! sidecar client; Meetings owns transcript assignment and its stereo-channel rules.
//! It is opt-in / default-off because the selected model runtime is large.
//!
//! Two signals are combined for the label:
//!   1. The selected local model splits the recording into speaker turns.
//!   2. The persisted recording is **stereo** (L = mic, R = system). Whenever the
//!      mic channel dominates a segment's window, that segment is *you* — labeled
//!      **"Me"** — regardless of the selected model's guess. This free, reliable split
//!      is why Core keeps the channels separate instead of diarizing a mono blob.
//!
//! The assignment ([`assign`]) is pure math over segment windows + turns + the raw
//! stereo PCM, so it is unit-tested; only the sidecar HTTP call touches the world.

use ryu_diarization::{DiarizationClient, DiarizationRequest, SpeakerTurn};

use super::Segment;

pub use ryu_diarization::DiarizationModel;

/// The 16 kHz rate the persisted stereo PCM is stored at.
const RATE: u32 = 16_000;
/// Release-profile port declared by the Meetings manifest.
const SIDECAR_PORT: u16 = 8020;
/// The mic (L) channel must be at least this many times louder than the system
/// (R) channel for a window to count as "you speaking".
const MIC_DOMINANCE: f32 = 1.3;

/// The diarize sidecar base URL. Local engine → loopback by default; overridable.
pub fn sidecar_url() -> String {
    std::env::var("RYU_DIARIZE_URL")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            format!(
                "http://127.0.0.1:{}",
                ryu_sidecar_runtime::profiled_port(SIDECAR_PORT)
            )
        })
}

/// Send the recording through the shared diarization client. Core injects the same
/// per-app bearer into both managed sidecars; it is never put in the request body.
pub async fn diarize_wav(
    wav: Vec<u8>,
    request: DiarizationRequest,
) -> Result<Vec<SpeakerTurn>, String> {
    let token = std::env::var("RYU_EXT_TOKEN")
        .map_err(|_| "Ryu sidecar bearer token is missing".to_owned())?;
    let client = DiarizationClient::new(&sidecar_url(), &token).map_err(|e| e.to_string())?;
    client
        .diarize(wav, request)
        .await
        .map(|response| response.turns)
        .map_err(|e| e.to_string())
}

/// Compute each segment's time window `[start_ms, end_ms)`: a segment runs until
/// the next segment starts (the last one to the end of the audio).
fn segment_windows(segments: &[Segment], audio_ms: i64) -> Vec<(i64, i64)> {
    let mut out = Vec::with_capacity(segments.len());
    for (i, seg) in segments.iter().enumerate() {
        let start = seg.t_offset_ms.max(0);
        let end = segments
            .get(i + 1)
            .map(|n| n.t_offset_ms)
            .unwrap_or(audio_ms.max(start + 1));
        out.push((start, end.max(start + 1)));
    }
    out
}

/// RMS of one channel (`chan` = 0 mic/L, 1 system/R) over a frame range of the
/// interleaved stereo i16 PCM.
fn channel_rms(pcm: &[u8], start_frame: usize, end_frame: usize, chan: usize) -> f32 {
    let mut sum_sq = 0.0f64;
    let mut n = 0u64;
    for f in start_frame..end_frame {
        let byte = (f * 2 + chan) * 2;
        if byte + 1 >= pcm.len() {
            break;
        }
        let s = i16::from_le_bytes([pcm[byte], pcm[byte + 1]]) as f64 / 32768.0;
        sum_sq += s * s;
        n += 1;
    }
    if n == 0 {
        0.0
    } else {
        (sum_sq / n as f64).sqrt() as f32
    }
}

/// Assign a speaker label to each segment. Returns `(segment_id, label)` pairs.
///
/// `pcm` is the raw interleaved stereo i16 (L = mic, R = system) at 16 kHz; when a
/// segment's mic channel dominates, it's labeled "Me". Otherwise the dominant
/// overlapping model turn decides, remapped to friendly "Speaker N" labels in
/// first-appearance order.
pub fn assign(segments: &[Segment], turns: &[SpeakerTurn], pcm: &[u8]) -> Vec<(i64, String)> {
    let audio_ms = (pcm.len() as i64 / 4) * 1000 / RATE as i64; // 4 bytes/frame
    let windows = segment_windows(segments, audio_ms);

    // Stable remap of model labels (SPEAKER_00, …) to "Speaker 1", … in
    // the order they're first assigned.
    let mut label_map: Vec<(String, String)> = Vec::new();
    let mut friendly = |raw: &str| -> String {
        if let Some((_, f)) = label_map.iter().find(|(r, _)| r == raw) {
            return f.clone();
        }
        let f = format!("Speaker {}", label_map.len() + 1);
        label_map.push((raw.to_string(), f.clone()));
        f
    };

    let mut out = Vec::with_capacity(segments.len());
    for (seg, &(start_ms, end_ms)) in segments.iter().zip(windows.iter()) {
        // Mic-channel override → "Me".
        let sf = (start_ms as usize) * RATE as usize / 1000;
        let ef = (end_ms as usize) * RATE as usize / 1000;
        let mic = channel_rms(pcm, sf, ef, 0);
        let sys = channel_rms(pcm, sf, ef, 1);
        if mic > 1e-4 && mic >= sys * MIC_DOMINANCE {
            out.push((seg.id, "Me".to_string()));
            continue;
        }
        // Otherwise, the model turn with the most overlap.
        let best = turns
            .iter()
            .map(|t| {
                let ts = (t.start * 1000.0) as i64;
                let te = (t.end * 1000.0) as i64;
                let overlap = end_ms.min(te) - start_ms.max(ts);
                (overlap, t)
            })
            .filter(|(o, _)| *o > 0)
            .max_by_key(|(o, _)| *o);
        match best {
            Some((_, t)) => out.push((seg.id, friendly(&t.speaker))),
            // No overlap and mic didn't dominate — leave it to system side generic.
            None => out.push((seg.id, "Speaker 1".to_string())),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(id: i64, offset_ms: i64) -> Segment {
        Segment {
            id,
            meeting_id: "m".into(),
            t_offset_ms: offset_ms,
            speaker: None,
            text: "x".into(),
            created_at: String::new(),
        }
    }

    /// Build stereo PCM: `frames` of (L, R) i16.
    fn pcm(frames: &[(i16, i16)]) -> Vec<u8> {
        let mut v = Vec::new();
        for &(l, r) in frames {
            v.extend_from_slice(&l.to_le_bytes());
            v.extend_from_slice(&r.to_le_bytes());
        }
        v
    }

    #[test]
    fn mic_dominant_window_is_me() {
        // One 1 s segment; mic (L) loud, system (R) silent → "Me".
        let segs = vec![seg(1, 0)];
        let frames = vec![(10_000i16, 0i16); RATE as usize];
        let turns = vec![SpeakerTurn {
            start: 0.0,
            end: 1.0,
            speaker: "SPEAKER_01".into(),
        }];
        let out = assign(&segs, &turns, &pcm(&frames));
        assert_eq!(out, vec![(1, "Me".to_string())]);
    }

    #[test]
    fn system_dominant_uses_model_speaker() {
        let segs = vec![seg(1, 0)];
        // System (R) loud, mic (L) silent → model decides.
        let frames = vec![(0i16, 10_000i16); RATE as usize];
        let turns = vec![SpeakerTurn {
            start: 0.0,
            end: 1.0,
            speaker: "SPEAKER_00".into(),
        }];
        let out = assign(&segs, &turns, &pcm(&frames));
        assert_eq!(out, vec![(1, "Speaker 1".to_string())]);
    }

    #[test]
    fn distinct_model_labels_map_in_order() {
        let segs = vec![seg(1, 0), seg(2, 1000)];
        // Both windows system-dominant.
        let mut frames = vec![(0i16, 9000i16); RATE as usize]; // 0–1s
        frames.extend(vec![(0i16, 9000i16); RATE as usize]); // 1–2s
        let turns = vec![
            SpeakerTurn {
                start: 0.0,
                end: 1.0,
                speaker: "SPEAKER_05".into(),
            },
            SpeakerTurn {
                start: 1.0,
                end: 2.0,
                speaker: "SPEAKER_02".into(),
            },
        ];
        let out = assign(&segs, &turns, &pcm(&frames));
        assert_eq!(
            out,
            vec![(1, "Speaker 1".to_string()), (2, "Speaker 2".to_string())]
        );
    }

    #[test]
    fn no_turns_and_no_mic_falls_back_to_speaker_one() {
        // System-dominant window but the model returned no overlapping turn.
        let segs = vec![seg(1, 0)];
        let frames = vec![(0i16, 9000i16); RATE as usize];
        let out = assign(&segs, &[], &pcm(&frames));
        assert_eq!(out, vec![(1, "Speaker 1".to_string())]);
    }

    #[test]
    fn empty_pcm_yields_speaker_one_without_panicking() {
        // No audio at all: mic/sys RMS are 0, no turns ⇒ generic label, no index panic.
        let segs = vec![seg(1, 0)];
        let out = assign(&segs, &[], &[]);
        assert_eq!(out, vec![(1, "Speaker 1".to_string())]);
    }

    #[test]
    fn same_raw_label_reuses_the_friendly_name() {
        // Two system-dominant segments both overlapping the same model speaker.
        let segs = vec![seg(1, 0), seg(2, 1000)];
        let mut frames = vec![(0i16, 9000i16); RATE as usize];
        frames.extend(vec![(0i16, 9000i16); RATE as usize]);
        let turns = vec![SpeakerTurn {
            start: 0.0,
            end: 2.0,
            speaker: "SPEAKER_09".into(),
        }];
        let out = assign(&segs, &turns, &pcm(&frames));
        assert_eq!(
            out,
            vec![(1, "Speaker 1".to_string()), (2, "Speaker 1".to_string())]
        );
    }

    #[test]
    fn dominant_overlap_turn_wins_over_lesser_overlap() {
        // One 1 s system-dominant segment overlapped by two turns; the larger overlap
        // (SPEAKER_00, mapped first ⇒ "Speaker 1") should win.
        let segs = vec![seg(1, 0)];
        let frames = vec![(0i16, 9000i16); RATE as usize];
        let turns = vec![
            SpeakerTurn {
                start: 0.9,
                end: 1.0,
                speaker: "SPEAKER_07".into(),
            },
            SpeakerTurn {
                start: 0.0,
                end: 0.9,
                speaker: "SPEAKER_00".into(),
            },
        ];
        let out = assign(&segs, &turns, &pcm(&frames));
        assert_eq!(out, vec![(1, "Speaker 1".to_string())]);
    }

    #[test]
    fn sidecar_url_defaults_to_loopback() {
        if std::env::var_os("RYU_DIARIZE_URL").is_none() {
            assert_eq!(
                sidecar_url(),
                format!(
                    "http://127.0.0.1:{}",
                    ryu_sidecar_runtime::profiled_port(SIDECAR_PORT)
                )
            );
        }
    }
}
