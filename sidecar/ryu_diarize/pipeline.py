"""Model-neutral dispatch to lazily loaded local diarization adapters."""

from __future__ import annotations

import gc
import importlib.util
import threading
from typing import Any

from .backends.base import DiarizeUnavailable, InvalidAudio, hugging_face_token

DEFAULT_MODEL = "pyannote/speaker-diarization-3.1"
NEMOTRON_MODEL = "nvidia/Nemotron-3-Diarization"
SUPPORTED_MODELS = ("pyannote", "nemotron")

_active_model: str | None = None
_active_backend: Any = None
_lock = threading.Lock()


def _dependencies_installed(model: str) -> bool:
    modules = {
        "pyannote": ("pyannote.audio", "torch"),
        "nemotron": ("transformers", "torch", "torchaudio"),
    }.get(model)
    if modules is None:
        return False
    try:
        return all(importlib.util.find_spec(module) is not None for module in modules)
    except (ImportError, ModuleNotFoundError, ValueError):
        return False


def deps_available(model: str | None = None) -> bool:
    """Whether model dependencies can be imported without loading model weights."""
    if model is not None:
        return _dependencies_installed(model)
    return any(_dependencies_installed(name) for name in SUPPORTED_MODELS)


def available_models() -> list[str]:
    """Return model adapters whose dependencies and Hugging Face token are ready."""
    if not hugging_face_token():
        return []
    return [name for name in SUPPORTED_MODELS if _dependencies_installed(name)]


def model_id(model: str) -> str:
    if model == "pyannote":
        from .backends.pyannote import DEFAULT_MODEL as pyannote_default

        return pyannote_default
    if model == "nemotron":
        from .backends.nemotron import MODEL_ID

        return MODEL_ID
    return ""


def _create_backend(model: str) -> Any:
    if model == "pyannote":
        from .backends.pyannote import PyannoteBackend

        return PyannoteBackend()
    if model == "nemotron":
        from .backends.nemotron import NemotronBackend

        return NemotronBackend()
    raise DiarizeUnavailable(f"unsupported diarization model '{model}'")


def _backend_for(model: str) -> Any:
    global _active_backend, _active_model
    with _lock:
        if _active_model == model and _active_backend is not None:
            return _active_backend

        if _active_backend is not None:
            _active_backend.close()
            _active_backend = None
            _active_model = None
            gc.collect()

        _active_backend = _create_backend(model)
        _active_model = model
        return _active_backend


def diarize(
    audio: bytes,
    model: str = "pyannote",
    num_speakers: int | None = None,
) -> list[dict[str, object]]:
    """Return normalized turns for one supported model adapter.

    `audio` is the WAV body Core uploaded; adapters decode it in memory.
    """
    backend = _backend_for(model)
    return backend.diarize(audio, num_speakers)
