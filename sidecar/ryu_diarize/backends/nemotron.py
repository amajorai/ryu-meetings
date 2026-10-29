"""NVIDIA Nemotron 3 adapter using its Transformers audio-frame classifier."""

from __future__ import annotations

import os
from typing import Any

from .base import DiarizeUnavailable, hugging_face_token, load_waveform

MODEL_ID = "nvidia/Nemotron-3-Diarization"


def normalize_segments(rows: list[dict[str, Any]]) -> list[dict[str, object]]:
    turns: list[dict[str, object]] = []
    for row in rows:
        try:
            start = float(row["Start"])
            end = float(row["End"])
            speaker_id = int(row["Speaker"])
        except (KeyError, TypeError, ValueError) as exc:
            raise DiarizeUnavailable("Nemotron returned an unrecognized speaker segment") from exc
        if speaker_id < 0:
            raise DiarizeUnavailable("Nemotron returned a negative speaker index")
        turns.append(
            {"start": start, "end": end, "speaker": f"SPEAKER_{speaker_id:02d}"}
        )
    return turns


class NemotronBackend:
    def __init__(self) -> None:
        self._processor: Any = None
        self._model: Any = None

    def _load(self) -> tuple[Any, Any, Any, Any]:
        if self._processor is not None and self._model is not None:
            import torch
            import torchaudio

            return self._processor, self._model, torch, torchaudio

        try:
            import torch
            import torchaudio
            from transformers import AutoModelForAudioFrameClassification, AutoProcessor
        except Exception as exc:  # pragma: no cover - environment-dependent
            raise DiarizeUnavailable(
                "Nemotron dependencies are not installed; use Python 3.12+ and run "
                "`pip install -e '.[nemotron]'` in the Meetings sidecar"
            ) from exc

        token = hugging_face_token()
        if not token:
            raise DiarizeUnavailable(
                "no Hugging Face token found; Nemotron model download requires a token"
            )
        os.environ.setdefault("HF_TOKEN", token)
        try:
            processor = AutoProcessor.from_pretrained(MODEL_ID, token=token)
            model = AutoModelForAudioFrameClassification.from_pretrained(
                MODEL_ID,
                token=token,
                device_map="auto",
            )
        except Exception as exc:  # pragma: no cover - network/model access
            raise DiarizeUnavailable(
                f"failed to load Nemotron model '{MODEL_ID}': {exc}. Check the Hugging "
                "Face token, model access, and installed Transformers version."
            ) from exc

        self._processor = processor
        self._model = model
        return processor, model, torch, torchaudio

    def diarize(self, audio: bytes, num_speakers: int | None) -> list[dict[str, object]]:
        if num_speakers is not None:
            raise DiarizeUnavailable(
                "Nemotron determines speaker count automatically; num_speakers is not supported"
            )

        processor, model, torch, torchaudio = self._load()
        # Decoded from memory; the recording never touches disk.
        waveform, sample_rate = load_waveform(audio)
        if waveform.ndim == 2:
            waveform = waveform.mean(dim=0)
        target_rate = int(processor.feature_extractor.sampling_rate)
        if sample_rate != target_rate:
            waveform = torchaudio.functional.resample(
                waveform,
                orig_freq=sample_rate,
                new_freq=target_rate,
            )

        inputs = processor(
            waveform.cpu().numpy(),
            sampling_rate=target_rate,
            return_tensors="pt",
        ).to(model.device, dtype=model.dtype)
        with torch.inference_mode():
            logits = model(**inputs).logits
        segments = processor.extract_speaker_dict(
            logits,
            inputs.get("attention_mask"),
        )[0]
        return normalize_segments(segments)

    def close(self) -> None:
        self._processor = None
        self._model = None
        try:
            import torch

            if torch.cuda.is_available():
                torch.cuda.empty_cache()
        except Exception:  # pragma: no cover - torch not installed
            pass

