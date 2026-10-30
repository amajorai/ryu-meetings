"""pyannote.audio adapter for the normalized Ryu speaker-turn contract."""

from __future__ import annotations

from typing import Any

from .base import DiarizeUnavailable, hugging_face_token, load_waveform

# The pyannote pipeline this adapter loads. Fixed in code, not read from the
# environment: the Meetings settings pick the adapter, not the checkpoint.
DEFAULT_MODEL = "pyannote/speaker-diarization-3.1"


class PyannoteBackend:
    def __init__(self) -> None:
        self._pipeline: Any = None

    def _load(self) -> Any:
        if self._pipeline is not None:
            return self._pipeline
        try:
            from pyannote.audio import Pipeline
        except Exception as exc:  # pragma: no cover - environment-dependent
            raise DiarizeUnavailable(
                "pyannote.audio is not installed; run `pip install -e '.[diarize]'` "
                "in the Meetings sidecar"
            ) from exc

        token = hugging_face_token()
        if not token:
            raise DiarizeUnavailable(
                "no Hugging Face token found; the selected pyannote model requires "
                "accepting its terms and providing a token"
            )

        model = DEFAULT_MODEL
        try:
            pipeline = Pipeline.from_pretrained(model, use_auth_token=token)
        except Exception as exc:  # pragma: no cover - network/model access
            raise DiarizeUnavailable(
                f"failed to load pyannote model '{model}': {exc}. Accept the model "
                "terms on Hugging Face and check the token."
            ) from exc

        try:
            import torch

            if torch.cuda.is_available():
                pipeline.to(torch.device("cuda"))
        except Exception:  # pragma: no cover - optional GPU runtime
            pass

        self._pipeline = pipeline
        return pipeline

    def diarize(self, audio: bytes, num_speakers: int | None) -> list[dict[str, object]]:
        pipeline = self._load()
        # pyannote takes an in-memory waveform; the recording never touches disk.
        waveform, sample_rate = load_waveform(audio)
        clip = {"waveform": waveform, "sample_rate": sample_rate}
        result = (
            pipeline(clip, num_speakers=num_speakers) if num_speakers else pipeline(clip)
        )
        return [
            {"start": float(turn.start), "end": float(turn.end), "speaker": str(speaker)}
            for turn, _, speaker in result.itertracks(yield_label=True)
        ]

    def close(self) -> None:
        self._pipeline = None

