"""HTTP responsiveness proof without downloading a model or using personal audio.
Run: uv run --no-project --with fastapi --with python-multipart --with httpx python -m unittest test_responsiveness.py
"""
import asyncio
import builtins
import os
import tempfile
import threading
import unittest
from unittest.mock import patch
import httpx
from ryu_diarize import server

class ResponsivenessTests(unittest.IsolatedAsyncioTestCase):
    async def test_health_responds_during_serial_inference(self):
        started = threading.Event()
        release = threading.Event()
        paths = []
        active = 0
        maximum = 0
        def inference(audio, model="pyannote", num_speakers=None):
            nonlocal active, maximum
            self.assertEqual(model, "nemotron")
            self.assertEqual(audio, b"synthetic audio")
            paths.append(audio)
            active += 1
            maximum = max(maximum, active)
            started.set()
            release.wait(3)
            active -= 1
            return [{"start": 0, "end": 1, "speaker": "A"}]
        server._diarize_slot = asyncio.Semaphore(1)
        with patch.object(server, "_EXPECTED_TOKEN", "test-token"), patch.object(server, "diarize", inference), patch.object(server, "deps_available", return_value=False), patch.object(server, "available_models", return_value=[]), patch.object(server, "_gpu", return_value=False):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app), base_url="http://test") as client:
                async def submit():
                    return await client.post("/diarize", headers={"authorization": "Bearer test-token"}, data={"model": "nemotron"}, files={"file": ("clip.wav", b"synthetic audio")})
                first = asyncio.create_task(submit())
                second = None
                try:
                    self.assertTrue(await asyncio.to_thread(started.wait, 1))
                    self.assertFalse(first.done())
                    health = await asyncio.wait_for(client.get("/health"), 1)
                    self.assertEqual(health.status_code, 200)
                    self.assertFalse(release.is_set())
                    self.assertFalse(first.done(), "inference must still be running during health proof")
                    second = asyncio.create_task(submit())
                    await asyncio.sleep(0.05)
                    self.assertEqual(len(paths), 1, "second inference must queue")
                finally:
                    release.set()
                    responses = await asyncio.gather(first, *([second] if second else []))
                self.assertTrue(all(r.status_code == 200 for r in responses))
                self.assertEqual(maximum, 1)
                denied = await client.post("/diarize", files={"file": ("clip.wav", b"audio")})
                self.assertEqual(denied.status_code, 401)

    async def test_unavailable_and_empty_upload_keep_status_contract(self):
        server._diarize_slot = asyncio.Semaphore(1)
        def unavailable(_audio, **kwargs):
            raise server.DiarizeUnavailable("model unavailable")
        with patch.object(server, "_EXPECTED_TOKEN", "test-token"), patch.object(server, "diarize", unavailable):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app), base_url="http://test", headers={"authorization": "Bearer test-token"}) as client:
                response = await client.post("/diarize", files={"file": ("clip.wav", b"audio")})
                self.assertEqual(response.status_code, 503)
                self.assertEqual(response.json(), {"error": "model unavailable"})
                response = await client.post("/diarize", files={"file": ("clip.wav", b"")})
                self.assertEqual(response.status_code, 400)

    async def test_diarize_keeps_a_large_recording_in_memory(self):
        # The upload is the decrypted meeting recording; it is sealed at rest, so
        # neither the multipart parser nor inference may write it to a file.
        recording = bytes(range(256)) * (3 * 4096)  # 3 MiB, above Starlette's 1 MiB spool
        received = []

        def inference(audio, model="pyannote", num_speakers=None):
            received.append(audio)
            return [{"start": 0, "end": 1, "speaker": "SPEAKER_00"}]

        def refuse_file(*_args, **_kwargs):
            raise AssertionError("diarize must not create a file")

        real_open = builtins.open
        real_os_open = os.open

        def guarded_open(file, mode="r", *args, **kwargs):
            if any(flag in mode for flag in "wax+"):
                raise AssertionError(f"diarize must not write {file!r}")
            return real_open(file, mode, *args, **kwargs)

        def guarded_os_open(path, flags, *args, **kwargs):
            if flags & (os.O_CREAT | os.O_WRONLY | os.O_RDWR):
                raise AssertionError(f"diarize must not write {path!r}")
            return real_os_open(path, flags, *args, **kwargs)

        server._diarize_slot = asyncio.Semaphore(1)
        with patch.object(server, "_EXPECTED_TOKEN", "test-token"), patch.object(
            server, "diarize", inference
        ):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=server.app),
                base_url="http://test",
                headers={"authorization": "Bearer test-token"},
            ) as client:
                with patch.object(tempfile, "TemporaryFile", refuse_file), patch.object(
                    tempfile, "NamedTemporaryFile", refuse_file
                ), patch.object(tempfile, "mkstemp", refuse_file), patch.object(
                    builtins, "open", guarded_open
                ), patch.object(os, "open", guarded_os_open):
                    response = await client.post(
                        "/diarize", files={"file": ("meeting.wav", recording)}
                    )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(received, [recording])

    async def test_undecodable_audio_is_a_client_error(self):
        server._diarize_slot = asyncio.Semaphore(1)

        def invalid(_audio, **_kwargs):
            raise server.InvalidAudio("audio is not a readable PCM WAV")

        with patch.object(server, "_EXPECTED_TOKEN", "test-token"), patch.object(
            server, "diarize", invalid
        ):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=server.app),
                base_url="http://test",
                headers={"authorization": "Bearer test-token"},
            ) as client:
                response = await client.post("/diarize", files={"file": ("clip.wav", b"x")})
        self.assertEqual(response.status_code, 400)

    async def test_unknown_model_is_rejected_before_inference(self):
        with patch.object(server, "_EXPECTED_TOKEN", "test-token"):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=server.app),
                base_url="http://test",
                headers={"authorization": "Bearer test-token"},
            ) as client:
                response = await client.post(
                    "/diarize",
                    data={"model": "made-up"},
                    files={"file": ("clip.wav", b"audio")},
                )
            self.assertEqual(response.status_code, 400)
            self.assertEqual(response.json()["supported_models"], ["pyannote", "nemotron"])
