//! In-memory decode of one uploaded recap chunk to 16 kHz mono PCM.
//!
//! WAV keeps the existing `hound` path ([`crate::audio`]); AAC/MP4 and Ogg Vorbis
//! go through symphonia. Nothing is written to disk. Decoding is bounded: output is
//! capped at the declared duration + 10% and never more than [`MAX_DECODED_MS`];
//! exceeding it aborts with [`DecodeError::Overflow`] rather than truncating, so a
//! chunk that lies about its length is refused. Callers run this on a blocking
//! thread under a time budget.

use std::io::Cursor;

use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::{DecoderOptions, CODEC_TYPE_NULL};
use symphonia::core::errors::Error as SymphoniaError;
use symphonia::core::formats::FormatOptions;
use symphonia::core::io::MediaSourceStream;
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;

use crate::audio::{self, DecodedWav, TARGET_RATE};

/// The hard ceiling on one chunk's decoded audio.
pub const MAX_DECODED_MS: i64 = 70_000;

/// The MIME types a recap chunk may use (Opus is not decodable here).
pub const ACCEPTED_TYPES: &[&str] = &["audio/wav", "audio/mp4", "audio/aac", "audio/ogg"];

/// Why a chunk could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// The content type, container or codec is not supported.
    Unsupported(String),
    /// The decoded audio is longer than the declared duration allows.
    Overflow,
    /// The bytes are not a valid stream of the declared kind.
    Invalid(String),
}

/// The container family a content type names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkFormat {
    Wav,
    Mp4,
    Aac,
    Ogg,
}

impl ChunkFormat {
    /// Parse a `Content-Type` header (parameters ignored, except that Opus in Ogg
    /// is refused because there is no Opus decoder here).
    pub fn from_content_type(value: &str) -> Result<Self, DecodeError> {
        let lower = value.trim().to_ascii_lowercase();
        let base = lower.split(';').next().unwrap_or_default().trim();
        if lower.contains("opus") {
            return Err(DecodeError::Unsupported("opus".into()));
        }
        match base {
            "audio/wav" | "audio/wave" | "audio/x-wav" => Ok(Self::Wav),
            "audio/mp4" | "audio/m4a" | "audio/x-m4a" => Ok(Self::Mp4),
            "audio/aac" | "audio/aacp" => Ok(Self::Aac),
            "audio/ogg" => Ok(Self::Ogg),
            other => Err(DecodeError::Unsupported(other.to_string())),
        }
    }

    fn hint_extension(self) -> &'static str {
        match self {
            Self::Wav => "wav",
            Self::Mp4 => "m4a",
            Self::Aac => "aac",
            Self::Ogg => "ogg",
        }
    }
}

/// The decoded-sample cap for a declared duration, at `rate` Hz.
pub fn max_frames(declared_ms: i64, rate: u32) -> u64 {
    let allowed_ms = (declared_ms.max(0) * 11 / 10).min(MAX_DECODED_MS);
    (allowed_ms as u64) * u64::from(rate) / 1000
}

/// Decode `bytes` to 16 kHz mono i16 samples.
pub fn decode_chunk(
    bytes: &[u8],
    format: ChunkFormat,
    declared_ms: i64,
) -> Result<Vec<i16>, DecodeError> {
    let decoded = match format {
        ChunkFormat::Wav => {
            audio::decode_wav(bytes).map_err(|e| DecodeError::Invalid(e.to_string()))?
        }
        other => decode_with_symphonia(bytes, other, declared_ms)?,
    };
    let frames = decoded.samples.len() as u64 / u64::from(decoded.channels.max(1));
    if frames > max_frames(declared_ms, decoded.rate) {
        return Err(DecodeError::Overflow);
    }
    let resampled = audio::resample_to_16k(&decoded);
    Ok(downmix_i16(&resampled))
}

fn downmix_i16(decoded: &DecodedWav) -> Vec<i16> {
    let channels = usize::from(decoded.channels.max(1));
    if channels == 1 {
        return decoded.samples.clone();
    }
    decoded
        .samples
        .chunks_exact(channels)
        .map(|frame| {
            let sum: i32 = frame.iter().map(|&s| i32::from(s)).sum();
            (sum / channels as i32) as i16
        })
        .collect()
}

fn decode_with_symphonia(
    bytes: &[u8],
    format: ChunkFormat,
    declared_ms: i64,
) -> Result<DecodedWav, DecodeError> {
    let stream = MediaSourceStream::new(Box::new(Cursor::new(bytes.to_vec())), Default::default());
    let mut hint = Hint::new();
    hint.with_extension(format.hint_extension());
    let probed = symphonia::default::get_probe()
        .format(
            &hint,
            stream,
            &FormatOptions::default(),
            &MetadataOptions::default(),
        )
        .map_err(map_error)?;
    let mut reader = probed.format;
    let track = reader
        .tracks()
        .iter()
        .find(|t| t.codec_params.codec != CODEC_TYPE_NULL)
        .ok_or_else(|| DecodeError::Invalid("no audio track".into()))?;
    let track_id = track.id;
    let mut decoder = symphonia::default::get_codecs()
        .make(&track.codec_params, &DecoderOptions::default())
        .map_err(map_error)?;

    let mut mono: Vec<i16> = Vec::new();
    let mut rate: u32 = 0;
    let mut sample_buf: Option<SampleBuffer<f32>> = None;
    loop {
        let packet = match reader.next_packet() {
            Ok(packet) => packet,
            Err(SymphoniaError::IoError(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                break
            }
            Err(SymphoniaError::ResetRequired) => break,
            Err(e) => return Err(map_error(e)),
        };
        if packet.track_id() != track_id {
            continue;
        }
        let decoded = match decoder.decode(&packet) {
            Ok(decoded) => decoded,
            Err(SymphoniaError::DecodeError(_)) => continue,
            Err(e) => return Err(map_error(e)),
        };
        let spec = *decoded.spec();
        if rate == 0 {
            rate = spec.rate;
        } else if rate != spec.rate {
            return Err(DecodeError::Invalid("sample rate changed mid-chunk".into()));
        }
        let buf = sample_buf
            .get_or_insert_with(|| SampleBuffer::<f32>::new(decoded.capacity() as u64, spec));
        buf.copy_interleaved_ref(decoded);
        let channels = spec.channels.count().max(1);
        for frame in buf.samples().chunks_exact(channels) {
            let avg = frame.iter().sum::<f32>() / channels as f32;
            mono.push((avg.clamp(-1.0, 1.0) * f32::from(i16::MAX)) as i16);
        }
        if mono.len() as u64 > max_frames(declared_ms, rate) {
            return Err(DecodeError::Overflow);
        }
    }
    if rate == 0 || mono.is_empty() {
        return Err(DecodeError::Invalid("no audio decoded".into()));
    }
    Ok(DecodedWav {
        channels: 1,
        rate,
        samples: mono,
    })
}

fn map_error(error: SymphoniaError) -> DecodeError {
    match error {
        SymphoniaError::Unsupported(what) => DecodeError::Unsupported(what.to_string()),
        other => DecodeError::Invalid(other.to_string()),
    }
}

/// Whether 16 kHz mono PCM holds any speech, per the energy VAD in `ryu-vad`.
pub fn has_speech(pcm_16k: &[i16]) -> bool {
    let mut vad = ryu_vad::Vad::new();
    let events = vad.push(pcm_16k);
    !events.is_empty() || vad.in_speech()
}

/// Encode 16 kHz mono PCM as the WAV the STT host accepts.
pub fn to_wav(pcm_16k: &[i16]) -> Result<Vec<u8>, DecodeError> {
    audio::encode_wav(1, TARGET_RATE, pcm_16k).map_err(|e| DecodeError::Invalid(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const M4A: &[u8] = include_bytes!("../../tests/fixtures/tone.m4a");
    const OGG: &[u8] = include_bytes!("../../tests/fixtures/tone.ogg");

    fn tone_wav(ms: usize) -> Vec<u8> {
        let samples: Vec<i16> = (0..ms * 16)
            .map(|i| ((i as f32 * 0.17).sin() * 8_000.0) as i16)
            .collect();
        audio::encode_wav(1, TARGET_RATE, &samples).unwrap()
    }

    #[test]
    fn content_types_map_to_formats_and_opus_is_refused() {
        assert_eq!(
            ChunkFormat::from_content_type("audio/mp4").unwrap(),
            ChunkFormat::Mp4
        );
        assert_eq!(
            ChunkFormat::from_content_type("audio/ogg; codecs=vorbis").unwrap(),
            ChunkFormat::Ogg
        );
        assert!(matches!(
            ChunkFormat::from_content_type("audio/ogg;codecs=opus"),
            Err(DecodeError::Unsupported(_))
        ));
        assert!(ChunkFormat::from_content_type("video/mp4").is_err());
    }

    #[test]
    fn aac_and_vorbis_fixtures_decode_to_about_one_second() {
        for (bytes, format) in [(M4A, ChunkFormat::Mp4), (OGG, ChunkFormat::Ogg)] {
            let pcm = decode_chunk(bytes, format, 1_000).unwrap();
            assert!(
                (14_000..=17_600).contains(&pcm.len()),
                "{format:?}: {}",
                pcm.len()
            );
            assert!(has_speech(&pcm), "a loud tone reads as voiced");
        }
    }

    #[test]
    fn audio_longer_than_declared_is_refused() {
        assert_eq!(
            decode_chunk(M4A, ChunkFormat::Mp4, 300),
            Err(DecodeError::Overflow)
        );
        assert_eq!(
            decode_chunk(&tone_wav(2_000), ChunkFormat::Wav, 1_000),
            Err(DecodeError::Overflow)
        );
        assert_eq!(max_frames(120_000, 16_000), 70 * 16_000);
    }

    #[test]
    fn truncated_and_garbage_inputs_error_without_panicking() {
        for cut in [0, 10, 100, M4A.len() / 3, M4A.len() / 2] {
            let _ = decode_chunk(&M4A[..cut], ChunkFormat::Mp4, 1_000);
            let _ = decode_chunk(&OGG[..cut.min(OGG.len())], ChunkFormat::Ogg, 1_000);
        }
        let noise: Vec<u8> = (0..4096u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        assert!(decode_chunk(&noise, ChunkFormat::Aac, 1_000).is_err());
        assert!(decode_chunk(&noise, ChunkFormat::Wav, 1_000).is_err());
    }

    #[test]
    fn silence_has_no_speech() {
        assert!(!has_speech(&vec![0i16; 16_000]));
    }
}
