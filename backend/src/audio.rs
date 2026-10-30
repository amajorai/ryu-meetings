//! Meeting audio helpers: split one uploaded chunk into the two things Core needs
//! from it.
//!
//! Shadow uploads an **interleaved stereo** 16 kHz WAV (L = mic, R = system). Two
//! consumers want different shapes of it:
//!   - **whisper** wants mono — so we downmix for transcription;
//!   - **diarization** wants the raw recording — so we append the stereo PCM to a
//!     per-meeting file, keeping the channel split (mic is always "you") that makes
//!     "Me vs. everyone-else" a free, reliable speaker label.
//!
//! The GUI-mic path still sends mono; we upmix that to stereo (same signal on both
//! channels) so the persisted file has one consistent 2-channel/16 kHz layout that
//! the finalizer can wrap in a WAV header without bookkeeping.
//!
//! At rest the per-meeting PCM is an append-only `RYU-SL01` sealed log (store
//! `meetings-audio`, AAD bound to the meeting id): every [`append_pcm`] call
//! appends one record under a random nonce, a torn tail left by a crash is
//! dropped when the log is reopened, and the caller keeps the committed record
//! count in the meeting row so a log cut at a record boundary is detected.
//! Diarization reads the whole log back ([`read_pcm`], [`read_pcm_as_wav`]). A
//! legacy plaintext `.pcm` file still reads, and is rewritten as a sealed log
//! (verified before it replaces the original) on the next append or by
//! [`seal_legacy_pcm_files`].

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use anyhow::{bail, Context, Result};
use ryu_crypto::sealed_log::{is_sealed_log, open_recovering};
use ryu_crypto::{ContentContext, SealedLogWriter};

use crate::seal_keys::StoreSeal;

/// Largest legacy chunk written as one record when a plaintext file is sealed.
const LEGACY_RECORD_BYTES: usize = 8 * 1024 * 1024;
/// Open log writers kept per process (one per recording meeting).
const MAX_OPEN_WRITERS: usize = 16;

/// The rate every persisted meeting recording is normalized to (whisper's rate).
pub const TARGET_RATE: u32 = 16_000;

/// A decoded WAV: channel count, sample rate, and interleaved i16 samples.
pub struct DecodedWav {
    pub channels: u16,
    pub rate: u32,
    pub samples: Vec<i16>,
}

/// Decode a WAV blob into interleaved i16 samples. Float WAVs are converted to i16,
/// and integer WAVs of any bit depth are rescaled to 16-bit full scale.
pub fn decode_wav(bytes: &[u8]) -> Result<DecodedWav> {
    let cursor = std::io::Cursor::new(bytes);
    let reader = hound::WavReader::new(cursor).context("opening WAV chunk")?;
    let spec = reader.spec();
    let samples: Vec<i16> = match spec.sample_format {
        hound::SampleFormat::Int => {
            // hound yields each sample at its native magnitude (±2^23 for 24-bit,
            // -128..127 for 8-bit), so it has to be shifted to 16-bit full scale —
            // clamping would square-wave 24/32-bit audio and leave 8-bit near-silent.
            let bits = u32::from(spec.bits_per_sample);
            let to_i16 = |s: i32| {
                let scaled = if bits > 16 {
                    s >> (bits - 16)
                } else {
                    s << (16 - bits)
                };
                scaled.clamp(i16::MIN as i32, i16::MAX as i32) as i16
            };
            let mut r = reader;
            r.samples::<i32>().map(|s| to_i16(s.unwrap_or(0))).collect()
        }
        hound::SampleFormat::Float => {
            let mut r = reader;
            r.samples::<f32>()
                .map(|s| (s.unwrap_or(0.0) * 32767.0).clamp(-32768.0, 32767.0) as i16)
                .collect()
        }
    };
    Ok(DecodedWav {
        channels: spec.channels,
        rate: spec.sample_rate,
        samples,
    })
}

/// Downmix a decoded chunk to a **mono** WAV (same rate) for whisper. A mono input
/// is re-encoded unchanged; a stereo (or N-channel) input is averaged per frame.
pub fn to_mono_wav(decoded: &DecodedWav) -> Result<Vec<u8>> {
    let ch = decoded.channels.max(1) as usize;
    let mono: Vec<i16> = if ch == 1 {
        decoded.samples.clone()
    } else {
        decoded
            .samples
            .chunks(ch)
            .map(|frame| {
                let sum: i32 = frame.iter().map(|&s| s as i32).sum();
                (sum / ch as i32) as i16
            })
            .collect()
    };
    let spec = hound::WavSpec {
        channels: 1,
        sample_rate: decoded.rate,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
    {
        let mut writer = hound::WavWriter::new(&mut cursor, spec)?;
        for &s in &mono {
            writer.write_sample(s)?;
        }
        writer.finalize()?;
    }
    Ok(cursor.into_inner())
}

/// Return the chunk as interleaved **stereo** i16 for persistence. Stereo passes
/// through; mono is duplicated onto both channels; >2 channels are folded to the
/// first two. Only meaningful at [`TARGET_RATE`]; the caller skips persistence for
/// other rates so the per-meeting PCM file stays single-rate.
pub fn to_stereo_i16(decoded: &DecodedWav) -> Vec<i16> {
    let ch = decoded.channels.max(1) as usize;
    match ch {
        1 => {
            let mut out = Vec::with_capacity(decoded.samples.len() * 2);
            for &s in &decoded.samples {
                out.push(s);
                out.push(s);
            }
            out
        }
        2 => decoded.samples.clone(),
        _ => {
            let mut out = Vec::with_capacity((decoded.samples.len() / ch) * 2);
            for frame in decoded.samples.chunks(ch) {
                out.push(frame.first().copied().unwrap_or(0));
                out.push(frame.get(1).copied().unwrap_or(0));
            }
            out
        }
    }
}

/// Linear-resample a decoded clip to [`TARGET_RATE`], preserving channel count.
/// Used by the import path so an arbitrary-rate uploaded file is normalized to the
/// 16 kHz everything downstream (whisper, persistence, diarization) expects. A
/// clip already at the target rate is returned unchanged.
pub fn resample_to_16k(decoded: &DecodedWav) -> DecodedWav {
    if decoded.rate == TARGET_RATE || decoded.samples.is_empty() {
        return DecodedWav {
            channels: decoded.channels,
            rate: TARGET_RATE,
            samples: decoded.samples.clone(),
        };
    }
    let ch = decoded.channels.max(1) as usize;
    let in_frames = decoded.samples.len() / ch;
    let ratio = TARGET_RATE as f64 / decoded.rate as f64;
    let out_frames = ((in_frames as f64) * ratio).round() as usize;
    let mut out = Vec::with_capacity(out_frames * ch);
    for i in 0..out_frames {
        let src = i as f64 / ratio;
        let idx = src.floor() as usize;
        let frac = src - idx as f64;
        for c in 0..ch {
            let a = decoded.samples.get(idx * ch + c).copied().unwrap_or(0) as f64;
            let b = decoded
                .samples
                .get((idx + 1) * ch + c)
                .copied()
                .unwrap_or(a as i16) as f64;
            out.push((a + (b - a) * frac).round() as i16);
        }
    }
    DecodedWav {
        channels: decoded.channels,
        rate: TARGET_RATE,
        samples: out,
    }
}

/// Encode interleaved i16 samples as a WAV with the given layout.
pub fn encode_wav(channels: u16, rate: u32, samples: &[i16]) -> Result<Vec<u8>> {
    let spec = hound::WavSpec {
        channels: channels.max(1),
        sample_rate: rate,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
    {
        let mut writer = hound::WavWriter::new(&mut cursor, spec)?;
        for &s in samples {
            writer.write_sample(s)?;
        }
        writer.finalize()?;
    }
    Ok(cursor.into_inner())
}

/// Split a 16 kHz decoded clip into `window_secs`-long WAV windows, each tagged
/// with its offset from the start in ms. Used by import to feed a whole file
/// through the same chunk-ingest pipeline as a live recording.
pub fn window_wavs(decoded: &DecodedWav, window_secs: usize) -> Vec<(i64, Vec<u8>)> {
    let ch = decoded.channels.max(1) as usize;
    let frames_per_window = window_secs * TARGET_RATE as usize;
    let samples_per_window = frames_per_window * ch;
    let mut out = Vec::new();
    let mut frame0 = 0usize;
    for window in decoded.samples.chunks(samples_per_window) {
        if let Ok(wav) = encode_wav(decoded.channels, TARGET_RATE, window) {
            let offset_ms = (frame0 as i64) * 1000 / TARGET_RATE as i64;
            out.push((offset_ms, wav));
        }
        frame0 += window.len() / ch;
    }
    out
}

/// The append-only raw stereo PCM file backing a meeting's diarization audio.
pub fn pcm_path(meeting_id: &str) -> PathBuf {
    crate::data_dir()
        .join("audio")
        .join(format!("{meeting_id}.pcm"))
}

/// The AAD of a meeting's PCM log.
fn pcm_ctx(meeting_id: &str) -> ContentContext {
    ContentContext::for_id("log", meeting_id)
}

/// Writers kept open across appends, so an append never re-reads the log.
fn writers() -> &'static Mutex<HashMap<PathBuf, SealedLogWriter>> {
    static WRITERS: OnceLock<Mutex<HashMap<PathBuf, SealedLogWriter>>> = OnceLock::new();
    WRITERS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn temp_sibling(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    path.with_file_name(format!(".{name}.{}.sealing", uuid::Uuid::new_v4().simple()))
}

/// Rewrite `records` as a fresh sealed log at `path` (temp file, fsync, read
/// back and compare, rename). Returns the record count.
fn rewrite_log(
    path: &Path,
    seal: &StoreSeal,
    meeting_id: &str,
    records: &[Vec<u8>],
) -> Result<u64> {
    let Some(cipher) = seal.cipher() else {
        bail!("no key to seal meeting audio");
    };
    let ctx = pcm_ctx(meeting_id);
    let temp = temp_sibling(path);
    let written = (|| -> Result<u64> {
        let mut writer = SealedLogWriter::create(&temp, cipher, &ctx)?;
        for record in records {
            writer.append(record)?;
        }
        writer.sync()?;
        let count = writer.record_count();
        drop(writer);
        let back = ryu_crypto::sealed_log::read_log(&temp, cipher, &ctx)?;
        if back.as_slice() != records {
            bail!("the sealed copy of meeting audio did not read back identically");
        }
        std::fs::rename(&temp, path)?;
        Ok(count)
    })();
    if written.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    written
}

/// Seal one legacy plaintext PCM file in place (verified before the original is
/// replaced). Returns the record count of the sealed log.
fn seal_legacy_pcm(path: &Path, seal: &StoreSeal, meeting_id: &str) -> Result<u64> {
    let raw = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let records: Vec<Vec<u8>> = raw
        .chunks(LEGACY_RECORD_BYTES)
        .map(<[u8]>::to_vec)
        .collect();
    rewrite_log(path, seal, meeting_id, &records)
}

/// Append interleaved stereo i16 samples to the meeting's PCM log as one sealed
/// record (in compat mode: to the plaintext file, as before). Returns the
/// number of committed records (0 in compat mode). Best-effort for the caller:
/// diarization is opt-in, so a persistence failure must not fail ingest.
pub fn append_pcm(seal: &StoreSeal, meeting_id: &str, stereo: &[i16]) -> Result<u64> {
    let path = pcm_path(meeting_id);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut bytes = Vec::with_capacity(stereo.len() * 2);
    for &s in stereo {
        bytes.extend_from_slice(&s.to_le_bytes());
    }
    let cipher = match seal {
        StoreSeal::Sealing(cipher) => cipher,
        StoreSeal::Compat => {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .with_context(|| format!("opening pcm file {}", path.display()))?;
            f.write_all(&bytes)?;
            return Ok(0);
        }
        StoreSeal::Unhealthy(reason) => bail!("{reason}"),
    };
    let mut writers = writers()
        .lock()
        .map_err(|_| anyhow::anyhow!("pcm writer lock poisoned"))?;
    if !writers.contains_key(&path) {
        if path.exists() && !is_sealed_log(&path)? {
            seal_legacy_pcm(&path, seal, meeting_id)?;
        }
        let ctx = pcm_ctx(meeting_id);
        let writer = match SealedLogWriter::open_append(&path, cipher, &ctx) {
            Ok(writer) => writer,
            Err(error) if error.to_string().contains("retired key") => {
                // Reseal a log written under a retired key, then append.
                let records = open_recovering(&path, cipher, &ctx)?.records;
                rewrite_log(&path, seal, meeting_id, &records)?;
                SealedLogWriter::open_append(&path, cipher, &ctx)?
            }
            Err(error) => return Err(error),
        };
        if writers.len() >= MAX_OPEN_WRITERS {
            writers.clear();
        }
        writers.insert(path.clone(), writer);
    }
    let writer = writers.get_mut(&path).expect("writer inserted above");
    let appended = writer.append(&bytes).and_then(|_| writer.sync());
    if let Err(error) = appended {
        // Drop the writer: the next append reopens the log and truncates any
        // torn tail this failure left.
        writers.remove(&path);
        return Err(error);
    }
    Ok(writer.record_count())
}

/// A meeting's persisted stereo PCM bytes, `None` if no audio was persisted.
/// `expected_records` is the committed count the meeting row recorded: a sealed
/// log with fewer records was truncated and is refused rather than misaligned.
pub fn read_pcm(
    seal: &StoreSeal,
    meeting_id: &str,
    expected_records: u64,
) -> Result<Option<Vec<u8>>> {
    let path = pcm_path(meeting_id);
    if !path.exists() {
        return Ok(None);
    }
    if !is_sealed_log(&path)? {
        let raw = std::fs::read(&path)?;
        return Ok((!raw.is_empty()).then_some(raw));
    }
    let Some(cipher) = seal.cipher() else {
        bail!("this meeting's audio is sealed and this process has no key for it");
    };
    let log = open_recovering(&path, cipher, &pcm_ctx(meeting_id))?;
    if (log.records.len() as u64) < expected_records {
        bail!(
            "meeting audio is truncated ({} of {expected_records} records)",
            log.records.len()
        );
    }
    let raw = log.records.concat();
    Ok((!raw.is_empty()).then_some(raw))
}

/// Wrap a meeting's accumulated stereo PCM as an in-memory WAV (16 kHz / 2ch) for
/// the diarization sidecar. `None` if no audio was persisted.
pub fn read_pcm_as_wav(
    seal: &StoreSeal,
    meeting_id: &str,
    expected_records: u64,
) -> Result<Option<Vec<u8>>> {
    let Some(raw) = read_pcm(seal, meeting_id, expected_records)? else {
        return Ok(None);
    };
    let spec = hound::WavSpec {
        channels: 2,
        sample_rate: TARGET_RATE,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
    {
        let mut writer = hound::WavWriter::new(&mut cursor, spec)?;
        for pair in raw.chunks_exact(2) {
            writer.write_sample(i16::from_le_bytes([pair[0], pair[1]]))?;
        }
        writer.finalize()?;
    }
    Ok(Some(cursor.into_inner()))
}

/// Remove a meeting's persisted audio (called on delete; best-effort).
pub fn remove_pcm(meeting_id: &str) {
    let path = pcm_path(meeting_id);
    if let Ok(mut writers) = writers().lock() {
        writers.remove(&path);
    }
    let _ = std::fs::remove_file(path);
}

/// Seal one meeting's legacy plaintext `.pcm` file, unless it is already a
/// sealed log, missing, or held by an open appender. Returns the record count
/// of the sealed log when it sealed one.
pub fn seal_legacy_pcm_file(seal: &StoreSeal, meeting_id: &str) -> Result<Option<u64>> {
    let path = pcm_path(meeting_id);
    if !seal.is_sealing() || !path.exists() || is_sealed_log(&path)? {
        return Ok(None);
    }
    // Never race an appender that holds this meeting's writer.
    let writers = writers()
        .lock()
        .map_err(|_| anyhow::anyhow!("pcm writer lock poisoned"))?;
    if writers.contains_key(&path) {
        return Ok(None);
    }
    seal_legacy_pcm(&path, seal, meeting_id).map(Some)
}

/// Seal every legacy plaintext `.pcm` file under `audio/` (one at a time, each
/// verified before it replaces the original). Returns `(meeting id, records)`
/// for each sealed file so the caller can record the counts.
pub fn seal_legacy_pcm_files(seal: &StoreSeal) -> Vec<(String, u64)> {
    if !seal.is_sealing() {
        return Vec::new();
    }
    let Ok(entries) = std::fs::read_dir(crate::data_dir().join("audio")) else {
        return Vec::new();
    };
    let mut sealed = Vec::new();
    for entry in entries.flatten() {
        let Some(meeting_id) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.strip_suffix(".pcm"))
            .filter(|id| !id.starts_with('.'))
            .map(str::to_owned)
        else {
            continue;
        };
        match seal_legacy_pcm_file(seal, &meeting_id) {
            Ok(Some(records)) => sealed.push((meeting_id, records)),
            Ok(None) => {}
            Err(error) => tracing::warn!(
                "meetings: sealing legacy audio {meeting_id} failed (retried next start): {error:#}"
            ),
        }
    }
    sealed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stereo_wav(frames: &[(i16, i16)]) -> Vec<u8> {
        let spec = hound::WavSpec {
            channels: 2,
            sample_rate: TARGET_RATE,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };
        let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
        {
            let mut w = hound::WavWriter::new(&mut cursor, spec).unwrap();
            for &(l, r) in frames {
                w.write_sample(l).unwrap();
                w.write_sample(r).unwrap();
            }
            w.finalize().unwrap();
        }
        cursor.into_inner()
    }

    #[test]
    fn decode_roundtrips_stereo() {
        let wav = stereo_wav(&[(100, -100), (200, -200)]);
        let d = decode_wav(&wav).unwrap();
        assert_eq!(d.channels, 2);
        assert_eq!(d.rate, TARGET_RATE);
        assert_eq!(d.samples, vec![100, -100, 200, -200]);
    }

    #[test]
    fn downmix_averages_channels() {
        let wav = stereo_wav(&[(100, 200), (0, 0)]);
        let d = decode_wav(&wav).unwrap();
        let mono = decode_wav(&to_mono_wav(&d).unwrap()).unwrap();
        assert_eq!(mono.channels, 1);
        assert_eq!(mono.samples, vec![150, 0]); // (100+200)/2, (0+0)/2
    }

    #[test]
    fn to_stereo_passes_through_and_upmixes() {
        let stereo = DecodedWav {
            channels: 2,
            rate: TARGET_RATE,
            samples: vec![1, 2, 3, 4],
        };
        assert_eq!(to_stereo_i16(&stereo), vec![1, 2, 3, 4]);
        let mono = DecodedWav {
            channels: 1,
            rate: TARGET_RATE,
            samples: vec![5, 6],
        };
        assert_eq!(to_stereo_i16(&mono), vec![5, 5, 6, 6]);
    }

    #[test]
    fn to_stereo_folds_multichannel_to_first_two() {
        // 3-channel interleaved: two frames of (L, R, C).
        let three = DecodedWav {
            channels: 3,
            rate: TARGET_RATE,
            samples: vec![1, 2, 3, 4, 5, 6],
        };
        assert_eq!(to_stereo_i16(&three), vec![1, 2, 4, 5]);
    }

    #[test]
    fn decode_float_wav_scales_to_i16() {
        let spec = hound::WavSpec {
            channels: 1,
            sample_rate: TARGET_RATE,
            bits_per_sample: 32,
            sample_format: hound::SampleFormat::Float,
        };
        let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
        {
            let mut w = hound::WavWriter::new(&mut cursor, spec).unwrap();
            w.write_sample(1.0f32).unwrap();
            w.write_sample(-1.0f32).unwrap();
            w.write_sample(0.0f32).unwrap();
            w.finalize().unwrap();
        }
        let d = decode_wav(&cursor.into_inner()).unwrap();
        assert_eq!(d.channels, 1);
        // -1.0 * 32767.0 = -32767 (the scale factor is 32767, not 32768).
        assert_eq!(d.samples, vec![32767, -32767, 0]);
    }

    fn int_wav(bits: u16, samples: &[i32]) -> Vec<u8> {
        let spec = hound::WavSpec {
            channels: 1,
            sample_rate: TARGET_RATE,
            bits_per_sample: bits,
            sample_format: hound::SampleFormat::Int,
        };
        let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
        {
            let mut w = hound::WavWriter::new(&mut cursor, spec).unwrap();
            for &s in samples {
                w.write_sample(s).unwrap();
            }
            w.finalize().unwrap();
        }
        cursor.into_inner()
    }

    #[test]
    fn decode_rescales_non_16_bit_integer_wavs_to_full_scale() {
        // 24-bit full scale maps to 16-bit full scale instead of clipping.
        let d = decode_wav(&int_wav(24, &[8_388_607, -8_388_608, 256, 0])).unwrap();
        assert_eq!(d.samples, vec![32767, -32768, 1, 0]);
        // 32-bit likewise.
        let d = decode_wav(&int_wav(32, &[i32::MAX, i32::MIN, 1 << 16])).unwrap();
        assert_eq!(d.samples, vec![32767, -32768, 1]);
        // 8-bit is scaled up rather than left 48 dB too quiet.
        let d = decode_wav(&int_wav(8, &[127, -128, 1])).unwrap();
        assert_eq!(d.samples, vec![32512, -32768, 256]);
        // 16-bit is unchanged.
        let d = decode_wav(&int_wav(16, &[32767, -32768, 5])).unwrap();
        assert_eq!(d.samples, vec![32767, -32768, 5]);
    }

    #[test]
    fn decode_rejects_non_wav_bytes() {
        assert!(decode_wav(b"not a wav at all").is_err());
    }

    #[test]
    fn encode_then_decode_roundtrips() {
        let wav = encode_wav(2, TARGET_RATE, &[10, 20, 30, 40]).unwrap();
        let d = decode_wav(&wav).unwrap();
        assert_eq!(d.channels, 2);
        assert_eq!(d.rate, TARGET_RATE);
        assert_eq!(d.samples, vec![10, 20, 30, 40]);
    }

    #[test]
    fn resample_passthrough_at_target_rate() {
        let d = DecodedWav {
            channels: 1,
            rate: TARGET_RATE,
            samples: vec![1, 2, 3],
        };
        let out = resample_to_16k(&d);
        assert_eq!(out.rate, TARGET_RATE);
        assert_eq!(out.samples, vec![1, 2, 3]);
    }

    #[test]
    fn resample_empty_is_target_rate_and_empty() {
        let d = DecodedWav {
            channels: 2,
            rate: 48_000,
            samples: vec![],
        };
        let out = resample_to_16k(&d);
        assert_eq!(out.rate, TARGET_RATE);
        assert!(out.samples.is_empty());
    }

    #[test]
    fn resample_downsamples_frame_count() {
        // 32 kHz mono, 32 frames → ~16 frames at 16 kHz.
        let d = DecodedWav {
            channels: 1,
            rate: 32_000,
            samples: (0..32).collect(),
        };
        let out = resample_to_16k(&d);
        assert_eq!(out.rate, TARGET_RATE);
        assert_eq!(out.channels, 1);
        assert_eq!(out.samples.len(), 16);
    }

    #[test]
    fn window_wavs_splits_by_seconds_with_offsets() {
        // 2.5 s of 16 kHz mono → 1 s windows at offsets 0, 1000, 2000 ms.
        let frames = (TARGET_RATE as usize) * 5 / 2;
        let d = DecodedWav {
            channels: 1,
            rate: TARGET_RATE,
            samples: vec![7; frames],
        };
        let windows = window_wavs(&d, 1);
        assert_eq!(windows.len(), 3);
        assert_eq!(windows[0].0, 0);
        assert_eq!(windows[1].0, 1000);
        assert_eq!(windows[2].0, 2000);
        // Each emitted window is a decodable WAV.
        assert!(decode_wav(&windows[0].1).is_ok());
    }

    #[test]
    fn pcm_append_read_remove_lifecycle() {
        let id = format!(
            "pcm-test-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        );
        let seal = StoreSeal::Compat;
        // No file yet ⇒ read returns None.
        assert!(read_pcm_as_wav(&seal, &id, 0).unwrap().is_none());

        append_pcm(&seal, &id, &[100, -100, 200, -200]).unwrap();
        append_pcm(&seal, &id, &[300, -300]).unwrap();
        let wav = read_pcm_as_wav(&seal, &id, 0)
            .unwrap()
            .expect("wav present");
        // Wrapped as a 2ch/16k WAV; decodes back to the concatenated samples.
        let d = decode_wav(&wav).unwrap();
        assert_eq!(d.channels, 2);
        assert_eq!(d.samples, vec![100, -100, 200, -200, 300, -300]);

        remove_pcm(&id);
        assert!(read_pcm_as_wav(&seal, &id, 0).unwrap().is_none());
    }

    fn sealing(n: u8) -> StoreSeal {
        use base64::Engine as _;
        let subkey = base64::engine::general_purpose::STANDARD.encode([n; 32]);
        let cipher = ryu_crypto::PluginCipher::from_subkey_b64("@ryu/meetings", &subkey).unwrap();
        crate::seal_keys::PluginKeys::sealing(cipher, crate::seal_keys::KeySource::Delivered)
            .store_state(crate::store::AUDIO_STORE, None, false)
    }

    fn pcm_id() -> String {
        format!("pcm-seal-{}", uuid::Uuid::new_v4().simple())
    }

    fn samples_on_disk(id: &str, needle: &[i16]) -> bool {
        let bytes = std::fs::read(pcm_path(id)).unwrap();
        let needle: Vec<u8> = needle.iter().flat_map(|s| s.to_le_bytes()).collect();
        bytes.windows(needle.len()).any(|w| w == needle.as_slice())
    }

    const MARK: [i16; 8] = [12345, -12345, 23456, -23456, 3456, -3456, 4567, -4567];

    #[test]
    fn sealed_pcm_appends_records_and_reads_back_whole() {
        let (seal, id) = (sealing(1), pcm_id());
        assert_eq!(append_pcm(&seal, &id, &MARK).unwrap(), 1);
        assert_eq!(append_pcm(&seal, &id, &[1, 2]).unwrap(), 2);
        assert!(!samples_on_disk(&id, &MARK), "no plaintext samples at rest");
        let raw = read_pcm(&seal, &id, 2).unwrap().unwrap();
        let samples: Vec<i16> = raw
            .chunks_exact(2)
            .map(|p| i16::from_le_bytes([p[0], p[1]]))
            .collect();
        assert_eq!(samples[..8], MARK);
        assert_eq!(samples[8..], [1, 2]);
        let wav = read_pcm_as_wav(&seal, &id, 2).unwrap().unwrap();
        assert_eq!(decode_wav(&wav).unwrap().samples.len(), 10);
        // Another meeting's id (AAD) or another key does not open it.
        std::fs::copy(pcm_path(&id), pcm_path(&format!("{id}-copy"))).unwrap();
        assert!(read_pcm(&seal, &format!("{id}-copy"), 0).is_err());
        assert!(read_pcm(&sealing(2), &id, 0).is_err());
        remove_pcm(&id);
        remove_pcm(&format!("{id}-copy"));
    }

    #[test]
    fn a_torn_tail_is_dropped_and_a_cut_log_is_detected() {
        let (seal, id) = (sealing(3), pcm_id());
        append_pcm(&seal, &id, &MARK).unwrap();
        append_pcm(&seal, &id, &MARK).unwrap();
        // Forget the cached writer (a restart), then tear the last record.
        writers().lock().unwrap().clear();
        let path = pcm_path(&id);
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..bytes.len() - 5]).unwrap();
        // Readers ignore the torn tail; the recorded count reveals the loss.
        assert_eq!(
            read_pcm(&seal, &id, 1).unwrap().unwrap().len(),
            MARK.len() * 2
        );
        assert!(read_pcm(&seal, &id, 2).is_err());
        // The appender truncates the torn tail and continues after record 1.
        assert_eq!(append_pcm(&seal, &id, &[7, 7]).unwrap(), 2);
        assert_eq!(
            read_pcm(&seal, &id, 2).unwrap().unwrap().len(),
            MARK.len() * 2 + 4
        );
        remove_pcm(&id);
    }

    #[test]
    fn legacy_plaintext_pcm_reads_and_is_sealed_after_verification() {
        let id = pcm_id();
        append_pcm(&StoreSeal::Compat, &id, &MARK).unwrap();
        assert!(samples_on_disk(&id, &MARK));
        let seal = sealing(4);
        // Still readable before migration.
        assert_eq!(
            read_pcm(&seal, &id, 0).unwrap().unwrap().len(),
            MARK.len() * 2
        );
        assert_eq!(seal_legacy_pcm_file(&seal, &id).unwrap(), Some(1));
        assert!(!samples_on_disk(&id, &MARK));
        assert_eq!(
            seal_legacy_pcm_file(&seal, &id).unwrap(),
            None,
            "idempotent"
        );
        // Appending continues the sealed log.
        assert_eq!(append_pcm(&seal, &id, &[5, 5]).unwrap(), 2);
        assert_eq!(
            read_pcm(&seal, &id, 2).unwrap().unwrap().len(),
            MARK.len() * 2 + 4
        );
        remove_pcm(&id);
    }

    #[test]
    fn mono_input_to_mono_wav_passes_through() {
        let d = DecodedWav {
            channels: 1,
            rate: TARGET_RATE,
            samples: vec![11, 22, 33],
        };
        let out = decode_wav(&to_mono_wav(&d).unwrap()).unwrap();
        assert_eq!(out.channels, 1);
        assert_eq!(out.samples, vec![11, 22, 33]);
    }
}
