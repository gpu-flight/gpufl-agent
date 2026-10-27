# gpufl-agent

Uploads the session logs that gpufl-client, the gpufl-serving session writer
or any other producer writes to the GPUFlight backend.

## Channels

The agent uploads every channel it finds in a session directory: a channel is
a name with a published window sidecar (`.gpufl-window.<channel>.<N>.json`), so
a new producer or channel needs no agent change. A bare `*.log` file, such as
an application's `server.log`, is never a channel and is never uploaded. The
native channels `device`, `scope`, `system` and `sass` are also recognized by
their bare window files, for clients that predate sidecars. A session may have
at most 16 channels.

`--log-types=<a,b,...>` (`GPUFL_LOG_TYPES`, or `logTypes` in a config file) is
an optional filter: only those channels are uploaded, and a directory without
any of them is left alone.

[docs/session-layout.md](docs/session-layout.md) is the layout contract a
producer writes: session directories, the lock file, `.tmp/`, window files and
sidecars, ACK tombstones and markers.

## Running the agent as a separate user

gpufl-client writes each session under `<log dir>/<session_id>/` and holds an
exclusive lock on `.gpufl-session.lock` there while it runs. The agent probes
that lock with a shared lock to decide that the session has finished, so its
user needs group read on the lock file. The agent also writes markers and
tombstones in the session directory and deletes uploaded payloads there, so
its user needs group write on the session directory.

When the agent runs as its own user (for example the systemd user
`gpufl-agent`):

- Make the session directory setgid and owned by the agent's group, mode 2770
  (`chgrp gpufl-agent <dir> && chmod 2770 <dir>`), so the files the producer
  creates in it belong to that group. If the producer creates the session
  directories itself, do this for the log directory instead: each new session
  directory inherits the group, but its group write bit comes from the
  producer's umask, so the producer needs umask 002.
- The lock file itself works with umask 002 or 022 on new clients: it is
  created `0660` masked by the umask (0660 or 0640), and either gives the
  agent's group the read access the probe needs. The directory rule above
  still decides the umask when the producer creates the session directories.
- Clients up to 1.3.0rc6 create the lock owner-only (0600). The agent cannot
  open it, logs a warning, and keeps treating the session as active. With
  those clients, run `chmod g+rw <session dir>/.gpufl-session.lock` after each
  run.
