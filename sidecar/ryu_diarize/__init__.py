"""Ryu's model-neutral, Core-managed local speaker-diarization runtime.

The server normalizes pyannote and Nemotron outputs to one speaker-turn contract.
Model libraries stay lazy and optional: the base process serves health checks even
when neither model runtime is installed.
"""

from __future__ import annotations

__version__ = "0.1.0"

# Default HTTP port for a standalone run. Core overrides it with a profile-shifted
# RYU_DIARIZE_PORT; 8020 stays separate from the shared model runtime ports.
DEFAULT_PORT = 8020
