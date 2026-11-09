<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./icon-dark.png" />
    <img src="./icon-light.png" alt="Meetings" width="144" />
  </picture>
</p>

<div align="center">

# Meetings

</div>

Record a call, transcribe it live, and generate structured AI notes with optional local speaker labeling.

> **The public home of `ryu-meetings`.** Source, builds, and releases live here —
> binaries for every platform are attached to each release.
>
> This tree is generated from the Ryu monorepo, so commits pushed here
> directly are replaced on the next sync. **Pull requests are welcome** —
> open them here and they are ported into the monorepo, then flow back out.
> Ryu as a whole: https://github.com/amajorai/ryu

## Install

**App:** [Install](ryu://apps/@ryu/meetings) (opens the Ryu desktop app and asks you to confirm)

**CLI:**

```bash
ryu apps add @ryu/meetings
```

**From source:**

```bash
cargo build --release --manifest-path backend/Cargo.toml
```

Prebuilt binaries for every platform are attached to [each release](https://github.com/amajorai/ryu/releases).

## License

Apache-2.0 — see [LICENSE](./LICENSE).

## Parts

- **`backend/` (`ryu-meetings`)** — an extracted Core capability crate and the **brain**:
  the meeting session lifecycle, the chunked-transcription pipeline (reusing the existing
  whisper/parakeet voice path), transcript accumulation + the shared `ryu-diarization`
  primitive, AI note generation,
  persistence (SQLite `MeetingStore`), templates, and the live SSE stream. **Now served
  OUT-OF-PROCESS** by the standalone `[[bin]] ryu-meetings` (`kind:local`, `public_mount
  /api/meetings`, port 7998) via the generic ext-proxy loader; Core links **zero meeting code** (no
  path-dep, no in-process mount). Its host needs (preferences, the STT transcribe path, the Gateway
  note-gen + auto-title calls, and the Spaces note store) are inverted through the `MeetingsHost`
  trait, so the crate has **zero dependency on `apps/core`**. **Sidecar-ization status (2026-07-18):
  OUT-OF-PROCESS.** The kernel weld (`ryu_hardware::HardwareSession` holding a concrete
  `MeetingEngine` on the ambient-audio path) was **inverted** behind a minimal **`MeetingIngest`
  trait** (`crates/ryu-hardware/src/ingest.rs`: resume-check / open ambient meeting / append one WAV
  segment); `MeetingsClient` **is** the out-of-process impl. The append hop is **segment-rate, not
  frame-rate** — `on_audio` accumulates each ~20 ms Opus frame and flushes once per ~1 s of buffered
  PCM (`AMBIENT_FLUSH_SAMPLES`), so it is ~**1 POST/s/device** carrying a ~1 s WAV, never the
  audio-hot-path kernel case. The `save_notes_to_space` weld became the Core kernel capability `POST
  /api/host/capability/spaces.fileNotes` (gated on the declared `spaces:docs` grant).
- **Audio capture is a device-bound sensor, not in this crate.** Because Core can run on a
  remote node, mic + system-loopback capture and mic-in-use detection live in **Shadow**,
  which streams raw WAV chunks up to `POST /api/meetings/:id/chunk` and posts detections to
  `POST /api/meetings/detect`. Core only ingests, transcribes, and debounces.
- **`ui/` (`@ryu/meetings-app`)** — the companion surface: a React app built to one
  self-contained HTML via `vite-plugin-singlefile`, consuming `@ryu/ui`. Full-page
  Companion (Path B, `ui_format: "html"`).

## Manifest

- **id** `@ryu/meetings` · companion `Meetings` (icon `mic-01`).
- **requires** `@ryu/spaces` (>=1.0.0) with grant `spaces:docs` — the dependency graph
  refuses to disable Spaces out from under it.
- **grants** `spaces:docs` (write notes into the Meetings Space) + `meetings:crud`.

## Surface

`/api/meetings` (list/create) · per-meeting `:id`, `title`, `chunk`, `finalize`,
`transcript` · `detect` · `templates` · `import` (audio-file) · `stream` (SSE).

## Swap seam

The notes model is never hardcoded (`default_notes_model` → prefs → Gateway-routed); STT
reuses the swappable voice path. Speaker-turn inference uses the model-neutral
`ryu-diarization` request/response contract with local pyannote and Nemotron adapters;
Meetings owns transcript mapping and the stereo microphone override. Notes calls stay
Gateway-governed, while diarization remains local.

## At rest

`meetings.db` seals each meeting's JSON (title, participants, app, generated notes) and
`segments.text` / `segments.speaker` per value (`encb:v1:`, store `meetings.db`; a meeting
binds its id, a segment its meeting id and row id) under the `@ryu/meetings` plugin subkey
Core delivers to the `ryu-meetings` sidecar (`crypto:seal` in that sidecar's
`host_api.grants`; the Python `ryu-diarize` sidecar gets no key, receives audio only over
loopback and keeps it in memory: the upload is never spooled to a temp file and the adapters
decode it in memory, so the decrypted recording is never written to disk). Ids, offsets and timestamps stay plaintext. The diarization audio
(`audio/<meeting>.pcm`) is one `RYU-SL01` sealed log per meeting (store `meetings-audio`,
AAD bound to the meeting id): each ingested chunk appends one record under a random nonce
through a writer kept open per meeting (an append never re-reads the log), a torn tail from
a crash is truncated when the log is reopened, and the committed record count is stored in
`meetings.pcm_records` so a log cut at a record boundary is refused instead of misaligning
speakers. Diarization reads the log back whole (`audio::read_pcm`).

Migration: legacy rows are sealed on open (`ryu_crypto::content_sqlite`, resumable and
idempotent; on a background connection above 20,000 segments), then freed pages are
scrubbed (`secure_delete = ON` on every connection). Legacy plaintext PCM files still read;
the sidecar rewrites each as a sealed log in the background (read back and compared before
it replaces the original) and records its count; an append to a legacy file converts it
first. Key states (`backend/src/seal_keys.rs`):

- **Compat mode**: under an older Core that delivers no key, a never-sealed store keeps
  working in plaintext (PCM included) and `/health` reports `sealing.state = "compat"`
  ("waiting for a Ryu update"); a store that holds sealed data refuses writes instead.
- **Key mismatch**: a store sealed under another key reports `sealing.state = "unhealthy"`
  and refuses writes; it is never resealed or reset.
- **Standalone** (no `RYU_PLUGIN_DATA_DIR`): `RYU_MASTER_KEY` if set, else a generated root
  in `<plugin data dir>/secrets/standalone-root.key` (0600) whose key check value is pinned.

The standalone satellite mirror builds `ryu-crypto` from crates.io at the declared version,
so the release train that ships this must publish `ryu-crypto` first (already ordered before
app backends in `tools/publish-crates.sh`).

## Recaps

Recaps are user-started conversation recaps captured by a phone or watch client and
uploaded here. Code: `backend/src/recap/`; client SDK: `packages/core-client/src/recaps.ts`.

- **Kind and scoping.** A recap is a `meetings` row with `kind = "recap"` and a `recap`
  block (`owner_user_id`, `client_session_id`, `tz`, `local_day`, `pinned`, …); the kind,
  owner, day and session id are also plaintext columns so queries never open another
  member's sealed row. The caller comes from Core's verified `x-ryu-caller-user-id`
  header; with none, a managed node (`RYU_MANAGED_NODE`) answers 401 and a local node
  means its owner (`local`). Another member's recap is a 404 on every route. Legacy
  routes, `/stream`, `/resolve`, `/ambient/chunk` and the OpenAPI tool document never
  see a recap (recap routes are deliberately unannotated), and no recap capability is
  declared until Core passes the caller's principal on capability and tool calls.
- **Gates.** Capture routes need the node preference `meeting-recaps-enabled` (default
  off; `PUT /recaps/availability`, permission level `recaps.admin`, which no member level
  implies; on a managed node only the headerless operator call may flip it, members get 403
  `admin_required`, and `GET /recaps/settings` reports `can_enable`) and the caller's
  current consent (`POST /recaps/consent`, version 1, bound to the resolved transcription
  route and summary model). Consent is re-checked on every chunk, at finalize and before
  any model call (finalize tail, digest), so a withdrawal or a changed route answers 409
  `consent_required` and nothing reaches a model; withdrawing closes the caller's
  recordings without a summary (`unsummarized: true`). Read, export and delete always work.
- **Data calls.** `data.count` / `data.purge` cover only the caller: a member's request
  (verified caller header) that names another `user_id` is 403 `other_member`; a
  headerless call (Core's fan-out, the node bearer) may name any member. `DELETE /recaps`
  keeps consent, settings and the day's upload usage; only `data.purge` clears them.
- **Routes** (under the mount, reached as `/api/ext/@ryu/meetings/recaps/...`):
  `GET|POST|DELETE /recaps`, `GET|PUT /recaps/settings`, `PUT /recaps/availability`,
  `POST|DELETE /recaps/consent`, `GET /recaps/daily`, `POST /recaps/daily/regenerate`,
  `POST /recaps/search`, `GET /recaps/export`, `GET|PATCH|DELETE /recaps/:id/detail`,
  `POST /recaps/:id/confirm`, `GET /recaps/:id/chunks`, `PUT /recaps/:id/chunks/:seq`,
  `POST /recaps/:id/finalize`, `GET /recaps/:id/events`. Single-recap routes use
  `/recaps/:id/<verb>` because a two-segment `/recaps/:id` would tie with the legacy
  method-less `/:id/title` routes in Core's route validation.
- **Chunk contract.** Raw audio body (`audio/wav`, `audio/mp4`, `audio/aac`, `audio/ogg`
  Vorbis; Opus is refused with 415), headers `x-recap-offset-ms`, `x-recap-duration-ms`,
  optional `x-recap-sha256`, `x-recap-silent: 1` with an empty body. Limits: seq ≤ 1440,
  duration ≤ 60 s, offsets within 12 h and not overlapping (250 ms jitter allowed), body ≤
  2,000,000 bytes (route body limit 2.5 MB), decoded audio ≤ declared + 10% and ≤ 70 s,
  120 chunks/min and 1 GB/day per member (`RYU_RECAP_DAILY_QUOTA_BYTES`). The server
  computes the sha256: same bytes → `duplicate: true`, different bytes → 409 (also when two
  uploads of one seq race). Audio is
  decoded in memory (symphonia; WAV via hound), silence is skipped with `ryu-vad`, and no
  audio or diarization data is stored. The ledger row and the segment commit together,
  after re-checking inside the transaction that the recap still exists and records (a
  recap deleted or closed during STT is 404 / 409 `recap_closed`, with no orphan segment).
- **Finalize.** 409 `{missing}` unless `force`; otherwise 202 and a background tail:
  redact the transcript (`recap/redact.rs`) → recap prompt → redact the notes → drop names
  unless `people` is on → title → `recap.ready` → rebuild that day's digest if one exists.
- **Retention and tick.** The sidecar is `lazy: false`. `recap::tick` runs a sweep on boot
  and every 5 minutes (also throttled on request entry): unconfirmed fast-path starts are
  discarded after 30 s (a late `confirm` also discards it: 409 `confirm_expired`), transcripts expire 7 days and unpinned recaps 30 days after they
  finish, digests after 30 days; stale sessions (no chunk for 30 min, or 12 h) are
  finalized as partial; each member's digest is built and announced once per local day
  after `digest_time` (DST-safe via chrono-tz). Trade-off: one resident process per node.
- **Events.** `recap.started`, `recap.ready`, `recap.digest.ready` carry ids, the local day
  and counts only. Notices are generic and set `target_user_id` to the owner; the headerless
  local owner has no concrete id, so no notice is sent for it (never the node's active
  account). Core delivers to another member only with the reviewed
  `notifications:send-to-user` grant, and drops the notice otherwise.

## Star History

<a href="https://github.com/amajorai/ryu-meetings/stargazers">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./.github/shieldcn/star-chart-dark.svg" />
    <img alt="Star history" src="./.github/shieldcn/star-chart-light.svg" />
  </picture>
</a>
