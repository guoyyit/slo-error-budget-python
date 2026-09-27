"""Local entry point: runs the SLO tracker HTTP API on loopback."""

from __future__ import annotations

from slo_tracker.api import create_app

app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
