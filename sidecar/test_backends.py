"""Model adapter contract tests that run without downloading model weights."""

import io
import os
import sys
import wave
import types
import unittest
from unittest.mock import patch

from ryu_diarize.backends.nemotron import NemotronBackend, normalize_segments
from ryu_diarize.backends import pyannote as pyannote_backend
from ryu_diarize.backends.base import InvalidAudio, decode_wav
from ryu_diarize.backends.pyannote import PyannoteBackend
from ryu_diarize import pipeline


class ModelAdapterTests(unittest.TestCase):
    def test_nemotron_segments_normalize_to_shared_turn_shape(self):
        turns = normalize_segments(
            [
                {"Start": 0.25, "End": 1.5, "Speaker": 0},
                {"Start": 2, "End": 3, "Speaker": 7},
            ]
        )
        self.assertEqual(
            turns,
            [
                {"start": 0.25, "end": 1.5, "speaker": "SPEAKER_00"},
                {"start": 2.0, "end": 3.0, "speaker": "SPEAKER_07"},
            ],
        )

    def test_nemotron_rejects_unsupported_speaker_count_without_loading_weights(self):
        with self.assertRaisesRegex(RuntimeError, "determines speaker count automatically"):
            NemotronBackend().diarize(b"", num_speakers=2)

    def test_pyannote_output_uses_the_same_turn_shape(self):
        test_case = self
        fake_pyannote = types.ModuleType("pyannote")
        fake_pyannote.__path__ = []
        fake_audio = types.ModuleType("pyannote.audio")

        class Interval:
            start = 0.5
            end = 1.75

        class FakePipeline:
            @classmethod
            def from_pretrained(cls, model, use_auth_token):
                test_case.assertEqual(model, "pyannote/speaker-diarization-3.1")
                test_case.assertEqual(use_auth_token, "test-hf-token")
                return cls()

            def __call__(self, clip):
                test_case.assertEqual(clip, {"waveform": "in-memory-waveform", "sample_rate": 16000})
                return self

            def itertracks(self, yield_label):
                test_case.assertTrue(yield_label)
                yield Interval(), None, "SPEAKER_03"

        fake_audio.Pipeline = FakePipeline
        with patch.dict(sys.modules, {"pyannote": fake_pyannote, "pyannote.audio": fake_audio}), patch.dict(
            os.environ, {"HF_TOKEN": "test-hf-token"}
        ), patch.object(
            pyannote_backend, "load_waveform", return_value=("in-memory-waveform", 16000)
        ) as load:
            turns = PyannoteBackend().diarize(b"wav-bytes", num_speakers=None)
        load.assert_called_once_with(b"wav-bytes")

        self.assertEqual(
            turns,
            [{"start": 0.5, "end": 1.75, "speaker": "SPEAKER_03"}],
        )

    def test_dispatch_switches_adapters_and_releases_the_previous_model(self):
        created = []

        class FakeBackend:
            def __init__(self, name):
                self.name = name
                self.closed = False

            def diarize(self, _path, _num_speakers):
                return [{"start": 0, "end": 1, "speaker": self.name}]

            def close(self):
                self.closed = True

        def create_backend(name):
            backend = FakeBackend(name)
            created.append(backend)
            return backend

        original_backend = pipeline._active_backend
        original_model = pipeline._active_model
        pipeline._active_backend = None
        pipeline._active_model = None
        try:
            with patch.object(pipeline, "_create_backend", side_effect=create_backend):
                self.assertEqual(pipeline.diarize(b"audio", "pyannote")[0]["speaker"], "pyannote")
                self.assertEqual(pipeline.diarize(b"audio", "nemotron")[0]["speaker"], "nemotron")
            self.assertEqual(len(created), 2)
            self.assertTrue(created[0].closed)
            self.assertFalse(created[1].closed)
        finally:
            if pipeline._active_backend is not None:
                pipeline._active_backend.close()
            pipeline._active_backend = original_backend
            pipeline._active_model = original_model


class InMemoryWavTests(unittest.TestCase):
    def test_decode_wav_reads_the_core_upload_layout_from_memory(self):
        buffer = io.BytesIO()
        with wave.open(buffer, "wb") as writer:
            writer.setnchannels(2)
            writer.setsampwidth(2)
            writer.setframerate(16000)
            writer.writeframes(b"\x01\x00\x02\x00\x03\x00\x04\x00")
        clip = decode_wav(buffer.getvalue())
        self.assertEqual((clip.channels, clip.sample_width, clip.sample_rate), (2, 2, 16000))
        self.assertEqual(clip.frames, b"\x01\x00\x02\x00\x03\x00\x04\x00")

    def test_decode_wav_rejects_non_wav_bytes(self):
        with self.assertRaises(InvalidAudio):
            decode_wav(b"not a wav")


if __name__ == "__main__":
    unittest.main()
