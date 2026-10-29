"""Shared adapter contract and credential lookup for local diarization models."""

from __future__ import annotations

import io
import os
import wave
from typing import Any, NamedTuple, Protocol


class DiarizeUnavailable(RuntimeError):
    """Raised when the selected model runtime cannot be loaded or run."""


class InvalidAudio(ValueError):
    """Raised when the uploaded body is not a PCM WAV this sidecar can decode."""


class DiarizationBackend(Protocol):
    def diarize(self, audio: bytes, num_speakers: int | None) -> list[dict[str, object]]: ...

    def close(self) -> None: ...


def hugging_face_token() -> str | None:
    for var in ("HUGGINGFACE_TOKEN", "HF_TOKEN", "HUGGING_FACE_HUB_TOKEN"):
        val = os.environ.get(var)
        if val:
            return val
    return None



class WavFrames(NamedTuple):
    channels: int
    sample_width: int
    sample_rate: int
    frames: bytes


def decode_wav(audio: bytes) -> WavFrames:
    """Parse an in-memory WAV with the standard library.

    The meeting audio is sealed at rest, so this sidecar never writes the decrypted
    recording to a file: every adapter decodes the uploaded bytes from memory.
    """
    try:
        with wave.open(io.BytesIO(audio), "rb") as reader:
            return WavFrames(
                channels=reader.getnchannels(),
                sample_width=reader.getsampwidth(),
                sample_rate=reader.getframerate(),
                frames=reader.readframes(reader.getnframes()),
            )
    except (wave.Error, EOFError) as exc:
        raise InvalidAudio(f"audio is not a readable PCM WAV: {exc}") from exc


_INT_SCALE = {1: 128.0, 2: 32768.0, 4: 2147483648.0}


def load_waveform(audio: bytes) -> tuple[Any, int]:
    """Return a `(channels, samples)` float32 tensor and its sample rate, in memory."""
    try:
        import torch
    except Exception as exc:  # pragma: no cover - environment-dependent
        raise DiarizeUnavailable("torch is not installed in the Meetings sidecar") from exc

    clip = decode_wav(audio)
    dtype = {1: torch.uint8, 2: torch.int16, 4: torch.int32}.get(clip.sample_width)
    if dtype is None or clip.channels < 1:
        raise InvalidAudio(
            f"unsupported WAV layout: {clip.channels} channel(s), "
            f"{clip.sample_width * 8}-bit samples"
        )
    samples = torch.frombuffer(bytearray(clip.frames), dtype=dtype).to(torch.float32)
    if clip.sample_width == 1:
        samples = samples - 128.0
    samples = samples / _INT_SCALE[clip.sample_width]
    waveform = samples.reshape(-1, clip.channels).transpose(0, 1).contiguous()
    return waveform, clip.sample_rate
