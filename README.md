# ryu-meetings

Meeting notes for Ryu (Granola/Notion-AI style) — record a call, transcribe it live, and generate structured AI notes. (Python diarization sidecar held back: not yet public.)

> **Read-only mirror.** Developed in https://github.com/amajorai/ryu —
> please open issues and pull requests there, not on this repository.

## Install

- Binary: `ryu-meetings` from the [Ryu releases](https://github.com/amajorai/ryu/releases).
- Crate: `cargo install ryu-meetings`.

## License

Apache-2.0 — see [LICENSE](./LICENSE).

---

# Meetings

Meeting notes, Granola / Notion-AI style: record a call, transcribe it live, and generate
structured AI notes (summary, action items, decisions) when it ends — with automatic
detection of an in-progress meeting from mic-in-use, so notes can start without opening any
app. Notes auto-save into the Meetings Space, staying editable and RAG-searchable.

## Parts

- **`backend/` (`ryu-meetings`)** — an extracted Core capability crate and the **brain**:
  the meeting session lifecycle, the chunked-transcription pipeline (reusing the existing
  whisper/parakeet voice path), transcript accumulation + diarization, AI note generation,
  persistence (SQLite `MeetingStore`), templates, and the live SSE stream. **Now served
  OUT-OF-PROCESS** by the standalone `[[bin]] ryu-meetings` (`kind:local`, `public_mount
  /api/meetings`, port 7998) via the generic ext-proxy loader; Core links **zero meeting code** (no
  path-dep, no in-process mount). Its host needs — preferences, the STT transcribe path, the Gateway
  note-gen + auto-title calls, and the Spaces note store — are inverted through the `MeetingsHost`
  trait, so the crate has **zero dependency on `apps/core`**. **Sidecar-ization status (2026-07-18):
  OUT-OF-PROCESS.** The kernel weld — `ryu_hardware::HardwareSession` holding a concrete
  `MeetingEngine` on the ambient-audio path — was **inverted** behind a minimal **`MeetingIngest`
  trait** (`crates/ryu-hardware/src/ingest.rs`: resume-check / open ambient meeting / append one WAV
  segment); `MeetingsClient` **is** the out-of-process impl. The append hop is **segment-rate, not
  frame-rate** — `on_audio` accumulates each ~20 ms Opus frame and flushes once per ~1 s of buffered
  PCM (`AMBIENT_FLUSH_SAMPLES`), so it is ~**1 POST/s/device** carrying a ~1 s WAV, never the
  audio-hot-path kernel case. The `save_notes_to_space` weld became the Core host callback `POST
  /api/host/meetings/save-notes`.
- **Audio capture is a device-bound sensor, not in this crate.** Because Core can run on a
  remote node, mic + system-loopback capture and mic-in-use detection live in **Shadow**,
  which streams raw WAV chunks up to `POST /api/meetings/:id/chunk` and posts detections to
  `POST /api/meetings/detect`. Core only ingests, transcribes, and debounces.
- **`ui/` (`@ryu/meetings-app`)** — the companion surface: a React app built to one
  self-contained HTML via `vite-plugin-singlefile`, consuming `@ryu/ui`. Full-page
  Companion (Path B, `ui_format: "html"`).

## Manifest

- **id** `com.ryu.meetings` · companion `Meetings` (icon `mic-01`).
- **requires** `com.ryu.spaces` (>=1.0.0) with grant `spaces:docs` — the dependency graph
  refuses to disable Spaces out from under it.
- **grants** `spaces:docs` (write notes into the Meetings Space) + `meetings:crud`.

## Surface

`/api/meetings` (list/create) · per-meeting `:id`, `title`, `chunk`, `finalize`,
`transcript` · `detect` · `templates` · `import` (audio-file) · `stream` (SSE).

## Swap seam

The notes model is never hardcoded (`default_notes_model` → prefs → Gateway-routed); STT
reuses the swappable voice path. All model calls stay Gateway-governed.
