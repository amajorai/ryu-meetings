"""Ryu Diarize sidecar — model-neutral HTTP front over local adapters.

One small contract Core drives:

    GET   /health   -> { ok, can_diarize, has_token, gpu, model }
    POST  /diarize  -> { turns: [{ start, end, speaker }] }   (multipart `file`)

Core owns persistence and the mic-channel "Me" override; this process only turns
a WAV into normalized speaker turns.

The meeting audio is sealed at rest in Core's Meetings store, so this process keeps
the decrypted recording in memory only: the multipart upload is never spooled to a
temporary file, and the adapters decode the bytes in memory.
"""

from __future__ import annotations

import asyncio
import hmac
import os
import threading
from typing import Optional

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse
from starlette import formparsers
from starlette.concurrency import run_in_threadpool

from . import __version__
from .pipeline import (
    DEFAULT_MODEL,
    SUPPORTED_MODELS,
    DiarizeUnavailable,
    InvalidAudio,
    available_models,
    deps_available,
    diarize,
    model_id,
)

app = FastAPI(title="Ryu Diarize Sidecar", version=__version__)

# Starlette spools multipart file parts above 1 MiB to a temporary file. A meeting
# upload is the decrypted recording, so keep every part in memory instead (the
# attribute was `max_file_size` before Starlette 0.40 and `spool_max_size` after).
_IN_MEMORY_UPLOAD_LIMIT = 1 << 40
for _spool_attr in ("spool_max_size", "max_file_size"):
    if hasattr(formparsers.MultiPartParser, _spool_attr):
        setattr(formparsers.MultiPartParser, _spool_attr, _IN_MEMORY_UPLOAD_LIMIT)

# Keep one inference at a time without blocking health/auth on the event loop.
_diarize_slot = asyncio.Semaphore(1)
_inference_lock = threading.Lock()


def _run_diarize(data: bytes, model: str, num_speakers: Optional[int]) -> list[dict]:
    # A disconnected request can cancel its await before a worker exits. Keep
    # inference serialization inside the worker. The audio stays in memory.
    with _inference_lock:
        return diarize(data, model=model, num_speakers=num_speakers)


# Shared-secret bearer Core stamps on every proxied hop and injects at spawn
# (`RYU_EXT_TOKEN`). FAIL-CLOSED for every route except /health: no token
# configured => reject all. Closes unauthenticated loopback/CSRF access to /diarize.
_EXPECTED_TOKEN = (os.environ.get("RYU_EXT_TOKEN") or "").strip()


@app.middleware("http")
async def _require_ext_token(request: Request, call_next):
    if request.url.path == "/health":
        return await call_next(request)
    header = request.headers.get("authorization", "")
    presented = header[len("Bearer ") :].strip() if header.startswith("Bearer ") else ""
    if not (_EXPECTED_TOKEN and hmac.compare_digest(presented, _EXPECTED_TOKEN)):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return await call_next(request)


def _gpu() -> bool:
    try:
        import torch

        return bool(torch.cuda.is_available())
    except Exception:
        return False


def _has_token() -> bool:
    return any(
        os.environ.get(v)
        for v in ("HUGGINGFACE_TOKEN", "HF_TOKEN", "HUGGING_FACE_HUB_TOKEN")
    )


@app.get("/health")
def health() -> dict:
    models = available_models()
    return {
        "ok": True,
        "can_diarize": bool(models),
        "deps_installed": deps_available(),
        "has_token": _has_token(),
        "gpu": _gpu(),
        "model": DEFAULT_MODEL,
        "supported_models": list(SUPPORTED_MODELS),
        "available_models": models,
        "model_ids": {name: model_id(name) for name in SUPPORTED_MODELS},
        "version": __version__,
    }


@app.post("/diarize")
async def diarize_endpoint(
    file: UploadFile = File(...),
    model: str = Form("pyannote"),
    num_speakers: Optional[int] = Form(None),
) -> JSONResponse:
    if model not in SUPPORTED_MODELS:
        return JSONResponse(
            {
                "error": f"unsupported diarization model '{model}'",
                "supported_models": list(SUPPORTED_MODELS),
            },
            status_code=400,
        )
    data = await file.read()
    if not data:
        return JSONResponse({"error": "empty audio"}, status_code=400)

    try:
        async with _diarize_slot:
            turns = await run_in_threadpool(_run_diarize, data, model, num_speakers)
        return JSONResponse({"turns": turns})
    except DiarizeUnavailable as exc:
        # A configuration problem (missing deps/token/gating) — 503 so Core logs a
        # clear reason and simply skips speaker labels.
        return JSONResponse({"error": str(exc)}, status_code=503)
    except InvalidAudio as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    except Exception as exc:  # pragma: no cover - runtime dependent
        return JSONResponse({"error": f"diarization failed: {exc}"}, status_code=500)
