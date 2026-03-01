# Durable Dinner Demo

A live demonstration of durable execution built on Effection. Four terminal
panes show a cooking workflow that survives a hard kill and replays from its
journal on restart — with zero duplicate work.

## Prerequisites

- [Deno](https://deno.land) (v2+)
- [tmux](https://github.com/tmux/tmux) (3.x+) — `brew install tmux`

## Quick Start

```sh
deno task demo                # uses default stream ID "dinner-demo"
deno task demo my-stream      # uses custom stream ID
```

This launches a tmux session called `durable-dinner` with four panes:

```
┌──────────────────┬──────────────────┐
│  Server           │  Journal Tailer  │  30%
│  Durable Streams  │  Live event feed │
│  on :4437         │                  │
├──────────────────┬──────────────────┤
│  Cook (focused)   │  Control         │  70%
│  > deno task      │  > pkill -9 -f   │
│    demo:cook      │    "demo/cook.ts"│
└──────────────────┴──────────────────┘
```

The cook pane (bottom-left) is focused with the command pre-typed. Press
**Enter** to start cooking.

The control pane (bottom-right) has the kill command pre-typed. Switch to it
(`Ctrl+B` then arrow key) and press **Enter** to hard-kill the workflow
mid-cook.

## Demo Script

### 1. Start the demo

```sh
deno task demo
```

The server and tailer start automatically. The bottom-left pane has
`deno task demo:cook` ready — just press Enter.

### 2. Watch it cook

Three dishes run in parallel:

| Dish | Coroutine | Highlights |
|------|-----------|------------|
| Tomato Sauce | `root.0` | Sequential steps + sleep timers |
| Focaccia | `root.1` | `durableRace`: oven timer vs periodic peek |
| Roast Veg | `root.2` | Short parallel branch |

The journal tailer (top-right) shows every event as it's persisted:

```
#1   yield root.0       call(chop-onion)     ok  "onion chopped"
#2   yield root.1       call(mix-dough)      ok  "dough mixed"
...
#20  close root.1.0                          cancelled
#22  close root                              ok  "Dinner is served!"
```

### 3. Kill mid-cook

Switch to the control pane (bottom-right) with `Ctrl+B` then right arrow.
Press **Enter** to hard-kill the workflow:

```sh
pkill -9 -f "demo/cook.ts"
```

The server and tailer keep running. The journal shows the last persisted
event — everything up to that point is durable.

### 4. Restart

Switch back to the cook pane (bottom-left) with `Ctrl+B` then left arrow.
Type the command again:

```sh
deno task demo:cook
```

Watch for:

- **"Found N events in journal — replaying..."** — the workflow reads its
  journal and replays all completed checkpoints
- **The tailer (top-right) stays quiet during replay** — no new events are
  appended because replay resolves effects from the journal
- **New events appear only after replay catches up** — live execution resumes
  from the exact point where the process died

### 5. What to point out

- The journal is the source of truth. Kill the process at any point; restart
  picks up exactly where it left off.
- `Close(cancelled)` events (e.g. `#20`) show that the race loser (oven timer)
  was properly cancelled and that cancellation is itself journaled.
- Replay is deterministic: the same generator code re-executes, but every
  `durableCall` and `durableSleep` resolves from the journal instead of
  doing real work.
- The workflow uses a fresh `producerId` on each restart — no epoch/sequence
  bookkeeping needed for the demo.

## Starting Fresh

The easiest way to start fresh is to tear down and relaunch with a new stream
ID. This ensures the tailer also switches to the new stream:

```sh
tmux kill-session -t durable-dinner
deno task demo my-new-stream
```

You can also switch just the cook pane mid-session, but note that the tailer
will keep showing the old stream unless you restart it too:

```sh
# In the cook pane:
DURABLE_STREAM_ID=take-2 deno task demo:cook

# In the tailer pane (restart to pick up the new stream):
# Ctrl+C, then:
DURABLE_STREAM_ID=take-2 deno task demo:tail
```

## Cleanup

```sh
tmux kill-session -t durable-dinner
```

Or press `Ctrl+B` then type `:kill-session` inside tmux.

## Running Without tmux

Open four terminals yourself:

```sh
# Terminal 1: Server
deno task demo:server

# Terminal 2: Tailer
DURABLE_STREAM_ID=my-stream deno task demo:tail

# Terminal 3: Cook (run, kill, restart)
DURABLE_STREAM_ID=my-stream deno task demo:cook

# Terminal 4: Kill
pkill -9 -f "demo/cook.ts"
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DURABLE_SERVER_URL` | `http://localhost:4437` | Durable Streams server URL |
| `DURABLE_STREAM_ID` | `dinner-demo` | Stream name (also settable via CLI arg) |
