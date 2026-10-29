# Session layout

This is the on-disk layout the agent consumes. Any producer that writes it (the
native gpufl-client, the Python gpufl-serving session writer, or a new tool)
has its channels uploaded with no agent change. Every statement here comes
from the agent code (`SessionWatcher`, `TailerManager`, `LogTailer`,
`SessionOwnership`, `AcknowledgedWindowCleaner`, `HttpPublisher`) or from the
native writer (`include/gpufl/core/logger/` in gpufl-client). Items marked
**Tolerated** work today but a producer should not rely on them.

## Session directories

- The agent watches the folders given by `--folder`, `--folders` or the
  config file. A session is a directory either directly under a watched folder
  (`<folder>/<session_id>/`) or one grouping level below it
  (`<folder>/<group>/<session_id>/`, as multi-pass runs use
  `run-<app>-<analysis_id>`). Deeper directories are not found, and names
  starting with `.` are skipped.
- The directory name is the session id. The agent sends it as
  `X-GpuFlight-Session-Id` and requires every sidecar's `session_id` to equal
  it. The native writer uses `<base_path>/<session_id>`.
- A directory is a session once it holds at least one channel (next section).
  The agent scans every 2 seconds.
- With `--ignore-preexisting`, session directories last modified before the
  agent started are ignored.

## Channels

- A channel name matches `[A-Za-z0-9_-]{1,64}`, the rule the backend's stream
  endpoint accepts. It cannot contain a dot.
- A name becomes a channel when the session directory holds a sidecar
  `.gpufl-window.<channel>.<N>.json` for it: the sidecar of a published window,
  or the same file kept as an ACK tombstone after the payload is deleted. The
  sidecar counts once it parses as a JSON object with integer `payload_bytes`
  and `payload_crc32`; the rest is checked when the window is uploaded.
- A bare `*.log` file never makes a channel. An application log next to the
  windows (`server.log`, `server.1.log`) is never read or uploaded, and a
  directory holding only such files is not a session.
- **Tolerated (legacy):** the four native names `device`, `scope`, `system`
  and `sass` are also recognized by `<name>.log`, `<name>.<N>.log` or
  `<name>.<N>.log.gz` without a sidecar, for clients that predate sidecars.
  Do not use these four names for anything else, and do not put an unrelated
  file with such a name in a session directory.
- At most 16 channels per session. A session with more has the extra channels
  left untailed; the agent never sends session-complete for it, marks it
  `.failed` and never prunes it.
- `--log-types` (`GPUFL_LOG_TYPES`, `logTypes` in the config file) is an
  optional filter. Without it every channel found is uploaded. With it only the
  listed channels are uploaded, and a directory counts as a session only if a
  listed channel is found in it. Such an agent still settles a directory once
  its listed channels finish, so channels outside the filter in the same
  directory are not uploaded by it or by a later agent.
- A channel whose first window appears while the session is live is tailed
  from the next scan. Before sending session-complete the agent scans the
  directory once more, so a channel published just before the producer
  finished is uploaded too.

## Lock file

- `.gpufl-session.lock` in the session directory. The producer creates it
  before it writes anything else, holds an exclusive OS lock on it for its
  whole lifetime, and releases it last: after every window is published and
  `.tmp/` is removed (or deliberately left behind).
- The native writer opens it with `O_CREAT | O_RDWR`, mode `0660` masked by the
  umask, and takes `fcntl(F_SETLK)` with `F_WRLCK` over the whole file; on
  Windows it uses `CreateFileW(OPEN_ALWAYS)` with read, write and delete
  sharing and `LockFileEx` exclusive on the first byte.
- The agent probes it by opening it read-only and trying a shared lock over
  the whole file (PR #30): not acquired means the session is active; acquired
  means the writer is gone (the agent releases it at once). A shared lock
  conflicts with the writer's exclusive one (`fcntl` on POSIX, `LockFileEx` on
  Windows), so the agent's user only needs read access to the file. If the
  agent cannot open it or the probe fails, it treats the session as active;
  it warns once when it cannot open the file.
- On POSIX, lock with an `fcntl` record lock (for example Python
  `fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)`). The agent's probe is an
  `fcntl` lock, and Linux keeps `flock(2)` locks separate from those, so an
  `flock` holder would look gone to the agent.
- Finished windows upload while the lock is held; only completion and the
  orphan decision depend on the probe.
- **Tolerated (legacy):** a session without a lock file. Completion then
  depends on `.tmp/` alone, and a payload without a sidecar is sent without
  its window identity. With a lock file present, a payload whose sidecar is
  missing is not uploaded; the agent retries every 5 seconds.

## `.tmp/` staging

- `<session>/.tmp/` holds the files still being written. The native writer
  keeps the active `.tmp/<channel>.log`, retired `.tmp/<channel>.<N>.log` and
  gzip staging files there. The agent never reads files in `.tmp/`; it checks
  only whether the directory exists and, without a lock file, how recently it
  or its direct entries changed.
- Create `.tmp/` before the first window. Remove it only after the last window
  of every channel is published: its removal tells the agent that no more
  windows will appear. Then release the lock.
- Per channel, the agent finishes as follows:
  - `.tmp/` present and lock held (or probe failed): keep waiting, with no
    time limit.
  - `.tmp/` present and lock released: the producer died without finishing.
    The channel finishes and the session is marked `.failed`.
  - `.tmp/` present, no lock file, and nothing in it changed for 30 seconds:
    the same.
  - `.tmp/` gone and lock held: keep waiting.
  - `.tmp/` gone and lock released (or no lock file): wait 4.5 seconds, look
    for the next window once more, then finish.
- An `--exit-when-drained` agent does not exit while a session's `.tmp/`
  changed within the last 30 seconds.

## Window files

- A window is `<channel>.<N>.log.gz` in the session directory: a gzip file of
  NDJSON, one JSON record per line.
- `N` starts at 1 and grows by 1 per channel, with no gaps. The agent sends a
  channel's windows strictly in order and waits at the first `N` it cannot
  find. A window after a gap is never sent: the channel finishes at the gap
  when the session ends. The only exception is an `N` whose ACK tombstone
  proves it was already sent.
- Publish the sidecar first, then the payload. Make each appear atomically:
  write it elsewhere (the native writer stages in `.tmp/`) and rename it into
  place without replacing an existing file. A visible window is complete and
  never changes; the agent may read it at once.
- The agent deletes acknowledged payloads, so derive the next `N` from the
  sidecars as well as the payloads. The native writer takes the highest index
  found in `.tmp/`, in the session directory and in the sidecars, plus one.
- The agent reads a whole window into memory and sends it in one request, so
  keep windows bounded. The native writer rotates them by size and time and
  never publishes an empty one.
- **Tolerated:** a plain `<channel>.<N>.log` (uncompressed NDJSON, from native
  builds without a compressor). The agent prefers it over a `.log.gz` with the
  same `N`, reads it line by line and sends batches without the window
  identity, the CRC check, the ACK or payload cleanup.

## Sidecars

`.gpufl-window.<channel>.<N>.json`, one per window, a single JSON object that
never changes once published. The native writer writes it to
`<name>.part.<id>` and moves it into place without replacing; that staging
name is not matched by the agent. Unknown fields are ignored.

| Field | Value | Agent check |
|---|---|---|
| `schema_version` | `1` | must be 1 |
| `type` | `"transport_window"` | must match |
| `window_id` | UUID, new for every window | must not be blank; sent as `X-GpuFlight-Window-Id` |
| `session_id` | the session directory name | must match |
| `channel` | the channel in the file names | must match; sent as `X-GpuFlight-Window-Channel` |
| `window_sequence` | `N` | must match; sent as `X-GpuFlight-Window-Sequence` |
| `opened_mono_ms`, `closed_mono_ms` | producer monotonic clock when the window opened and closed, valid within that run only (native writes -1 when unknown) | not checked |
| `created_wall_ms` | wall clock, ms since the epoch, when the sidecar was made | not checked |
| `payload_file` | the published payload's file name (`serving.3.log.gz`), never a staging name | must match |
| `payload_bytes` | size of the payload file in bytes (the gzip bytes) | must be >= 0 and equal the bytes read |
| `payload_crc32` | CRC-32 (zlib, as `java.util.zip.CRC32`) of the payload file bytes, unsigned decimal | must equal; sent as `X-GpuFlight-Window-CRC32` |

A sidecar that is malformed or does not match its payload is a permanent
failure: the agent writes `.gpufl-transport-loss.agent-<channel>.<N>.json`,
keeps the payload and sidecar, stops that channel and never sends
session-complete. The backend rejects a reused `window_id` with different bytes
(409 `window_identity_conflict`); the agent keeps the window and retries.

## ACK tombstones

- When the backend answers a window with `"window_acknowledged": true`, the
  agent writes `.gpufl-window-ack.<channel>.<N>` holding the `window_id` and a
  newline, synced to disk, before it moves on.
- It then deletes the payload, unless `--retain-acked-payloads` is set, but
  only if the sidecar matches the payload and the ACK file holds the sidecar's
  `window_id`. The sidecar stays as the tombstone: it keeps `N` used and still
  marks the channel for discovery.
- A producer must never delete or rewrite sidecars or `.gpufl-window-ack.*`
  files, never reuse an `N`, and must expect payloads to disappear.

## Markers

- `.uploaded`: the agent finished every channel cleanly. `.failed`: it
  finished off an orphaned `.tmp/`, found a transport-loss marker, or the
  session was over the channel cap. Every later scan, by any agent, skips a
  directory with either marker. With `--prune-failed` the agent deletes such a
  session instead of marking it `.failed`, except over the channel cap.
  Producers never create these files and never reuse a settled directory.
- `.gpufl-transport-loss.*`: any file with this prefix means data was lost.
  The agent then never sends session-complete and marks the session `.failed`.
  The native writer writes `.gpufl-transport-loss.<channel>.<index>.json`
  (`schema_version`, `type: "transport_window_loss"`, `channel`,
  `window_index`, `reason`) when it drops data; a producer that drops records
  should write one too, so the session is not reported complete.
- The agent ignores every other file in the session directory, including the
  native writer's `.gpufl-control.<N>.json` and `.gpufl-control-ack.<N>.json`.

## Completion

In stream mode the agent sends session-complete once: after every channel it
tails has finished and a last scan found no new channel. It does not send it
when a transport-loss marker exists or the session is over the channel cap. It
does send it for an orphaned session (lock released with `.tmp/` left), which
it then marks `.failed`. It tries three times with backoff; after that the
backend's grace finalize applies.

## Record contents

In stream mode (the HTTP default) the agent sends each gzip window
byte-for-byte; it never decompresses it or reads its records. The backend
routes each record by its `type` field.

**Tolerated:** plain `.log` windows, `--upload-mode=legacy` and the Kafka
publisher take the line path instead: the agent decompresses the window, parses
every line as JSON, drops malformed lines, reads `type` (which picks the
endpoint in legacy mode) and filters `device_metric_batch` rows on the `system`
channel. A producer whose records must stay unread by the agent publishes gzip
windows and relies on stream mode.

## Lifecycle rows

The native writer writes a `job_start` row when the session starts and a
`shutdown` row as its last row before it closes, both to every channel. The
agent never reads or checks them; they are for the backend. A producer should
write `job_start` first and `shutdown` last in each of its channels.
