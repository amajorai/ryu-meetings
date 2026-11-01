"""Entry point: `python -m ryu_diarize` starts the uvicorn server.

The server always binds loopback (127.0.0.1). The port is overridable via env
so Core can pin it at spawn time: RYU_DIARIZE_PORT (default 8020)
"""

from __future__ import annotations

import os

import uvicorn

from . import DEFAULT_PORT


def main() -> None:
    # Loopback only: Core proxies this process from the same machine.
    host = "127.0.0.1"
    port = int(os.environ.get("RYU_DIARIZE_PORT", str(DEFAULT_PORT)))
    # Single worker on purpose: each adapter holds model state in-process and is
    # loaded lazily on the first request.
    uvicorn.run("ryu_diarize.server:app", host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
