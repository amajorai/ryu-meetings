# Ryu Diarize Sidecar

> The out-of-process **speaker-diarization sidecar for the Meetings app**
> (`apps-store/meetings`). Co-located under the app it serves; the meetings
> backend consumes it at `apps-store/meetings/backend/src/diarize.rs`.

A Core-managed speaker-diarization runtime. It exposes one normalized turn contract and dispatches to local pyannote.audio or NVIDIA Nemotron adapters. The Meetings backend uses the shared `ryu-diarization` client and applies its transcript-specific assignment rules.

Diarization is **opt-in** in Ryu (default off) and the default model is HF-gated, so this stays a thin, self-contained runtime that Core starts only when the user turns it on.

## Contract

```
GET   /health   -> { ok, supported_models, available_models, model_ids, gpu }
POST  /diarize  -> { turns: [{ start, end, speaker }] }
                      # multipart `file` (WAV), `model` (pyannote | nemotron)
```

Core sends the meeting's persisted stereo recording, gets back seconds-based speaker turns, and does segment assignment itself — including the "Me" label whenever the mic (left) channel dominates a window. Both adapters return the same contract.

## Install & run

```bash
cd apps-store/meetings/sidecar
pip install -e .            # base server (boots, serves /health)
pip install -e ".[diarize]" # pyannote.audio + torch + torchaudio
pip install -e ".[nemotron]" # Transformers source + torch + torchaudio + accelerate
```

The default adapter is pyannote 3.1. Accept its model terms on Hugging Face and set a token before starting the service. Nemotron uses the Hugging Face Transformers implementation. To install both model runtimes, use `pip install -e ".[diarize,nemotron]"`.

```bash
export HF_TOKEN=hf_xxx
python -m ryu_diarize
```

The Meetings manifest registers `ryu-diarize` as a managed local sidecar. If it is installed in a virtual environment, point `RYU_DIARIZE_BIN` at that environment's console script. Core injects the profile-aware port and the same per-app bearer token into both Meetings processes. Pyannote remains the default; the Meetings settings select the adapter for each finalized recording.
