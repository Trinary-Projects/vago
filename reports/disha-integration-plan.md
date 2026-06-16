# talk-go — Disha integration + core library split

**Audience**: A coding agent (or human) who has filesystem access to:
- `/Users/jaideepsingh/Projects/talk-go/` (this repo)
- `/Users/jaideepsingh/Projects/disha-backend/` (the Python reference)

You have no prior conversation context. This document is self-contained. Read it top to bottom before writing any code.

Do not commit phase work unless Jaideep explicitly asks. Current exception: Phase 0 + Phase 1/1.5 were committed on branch `codex/disha-integration-core` after an explicit request. Continue working in the working tree for later phases, run tests after each phase, and stop after the phase boundaries listed in §13 unless explicitly told to continue.

---

## Implementation status

- **Phase 0 is implemented.** The reusable core is now `voicepipelinecore/`, root `main.go` only owns the demo HTTP wrapper/session map, UI events use LiveKit data packets, and exported API entry points are `voicepipelinecore.NewTask`, `PipelineTask.Start`, `PipelineTask.End`, `JoinRoom`, and `GenerateToken`.
- **Phase 1 + 1.5 are implemented.** Core now has `TaskOptions.InitialMessages`, `TaskOptions.MaxTalkTime`, typed `EndReason`, `CallStats`, FIFO `CallEvents` for lifecycle and committed turns, `callStatsTracker`, and `perTurnMetrics` snapshots. `TaskContext.EndTask` is typed as `func(EndReason)`; there is no legacy string reason mapper.
- **Phase 2 is implemented.** The `disha/` package now has Redis/API payload structs, a narrow Redis client for `conversation_data:{conversation_id}` reads and `conversation_chunks:{user_id}:{conversation_id}` appends, Disha-style Redis timeout retry, explicit `null` `end_reason` JSON support, and tests with miniredis/JSON assertions.
- **Phase 3 is implemented.** The `disha/` package now has an HTTP API client for `PATCH /bot/update_conversation`, `POST /bot/run_post_call_operations`, and `POST /common/enqueue_job`. It mirrors `VoiceBotAPIService` auth behavior by sending no Authorization header.
- **Phase 4 is implemented.** The `disha/` package now has a common `Bot` interface, common startup helpers, common call event callbacks, and a single sales-call implementation in `sales_call.go`. `SalesCallBot.BuildOptions` is the testable boundary: it loads Redis state, filters prior chunks, seeds `hello?`, applies remaining sales talk time, wires common call events, persists committed turns through the same callback path, and maps unsupported Disha end reasons to explicit JSON `null`.
- **Phase 5 was implemented and later superseded.** The direct root `/connect` path has been removed; bot sessions now start through worker-compatible routes such as `POST /bot/create_worker_room`, which always require `conversation_id` and route through the common `disha.Bot` boundary.
- **Current boundary choice:** Disha business logic still does not live in `voicepipelinecore`.

> This document is a historical implementation plan. Sections below that describe `/connect` are kept for context and do not describe the current runtime entry point.

---

## 1. Why this work exists

`talk-go` is a real-time voice-call pipeline in Go (LiveKit transport + Soniox STT + OpenAI LLM + Cartesia TTS) that mirrors Pipecat's architecture in Python. It currently runs end-to-end as a standalone demo: a browser hits `/connect`, the bot joins the same LiveKit room, the user talks to the bot.

The next step is to integrate `talk-go` into the existing **Disha** product backend (Python, lives at `/Users/jaideepsingh/Projects/disha-backend/`), specifically replacing the **`sales_call`** Pipecat bot. Disha already has all the surrounding infrastructure for sales calls: scheduling, Redis-backed conversation state, Postgres persistence, dashboard analytics, post-call STM generation, talktime accounting. We do **not** rebuild any of that in Go.

What we add to `talk-go`:
1. The ability to load a conversation's prior state from Disha's Redis at call start.
2. The ability to persist conversation chunks to Disha's Redis during the call.
3. The ability to fire the same HTTP call events Disha's Python bot fires (`update_conversation` at three moments, `run_post_call_operations` at the end, `enqueue_job` to trigger the Redis→Postgres chunk sync).

**Crucially**, the user wants all of this expressed as a clean library/integration split:

> The pipeline must remain free of business logic. All Disha-specific code (Redis, HTTP, conversation IDs, prompts, end-reason policies) lives in a separate package and hooks into the core via callbacks. Mirror Pipecat's pattern where `sales_call.py` wires event handlers onto core processors without modifying the core.

If you find yourself writing `import "talk-go/disha"` inside the core package, stop and add a callback instead.

---

## 2. Original repo state (historical pre-work snapshot)

### Module
- `go.mod` module name was `awesomeProject` before phase 0. It is now `github.com/jaideep329/talk-go`.
- Go 1.24.4.
- `github.com/redis/go-redis/v9` is already in go.sum as a transitive dep; promote to a direct dep.

### Layout (today, before this work)
All `.go` files at the repo root in `package main`. The relevant files:

```
audio_source_processor.go    Decodes LiveKit/Opus → PCM, pushes AudioFrame downstream
context_aggregator.go        Owns conversation messages, transcripts, barge-in detection
frame.go                     Frame interface + FrameMeta + all concrete frame types + constructors
frame_test.go                Tests for FrameMeta
helpers_test.go              Test fixture (testFixture, runProcessorTest, QueueProcessor)
livekit_room.go              joinRoom() + generateToken()
llm_processor.go             OpenAI streaming LLM
main.go                      HTTP server: /connect and /. UI/control messages use LiveKit DataPackets; there is no custom /ws route
metrics.go                   ProcessorMetrics + MetricsFrame
pipeline.go                  Pipeline, TaskContext, PipelineTask, sessions map, createSession
pipeline_edges.go            PipelineSourceProcessor, PipelineSinkProcessor
playback_sink_processor.go   Bot audio publishing to LiveKit + silence tail
processor.go                 BaseProcessor + Processor interface
stt_processor.go             Soniox STT websocket
talktime_monitoring_processor.go  Max-talk-time timer
tts_processor.go             Cartesia TTS websocket
ui_events.go                 Frontend LiveKit DataPacket event sender
user_idle_processor.go       Idle prompt injection
*_test.go                    One test file per processor + frame_test.go + processor_test.go
```

### Key types in the original code (so you can understand the migration)

- `Frame` interface (`frame.go:62-69`): `ID() int64; Name() string; FrameType() FrameType; IsSystem() bool; IsInterruptible() bool`. All concrete frames embed `FrameBase{Meta FrameMeta}` and have `NewXxxFrame(...)` constructors. Tests still allow struct literals (zero meta).
- `Processor` interface (`processor.go:33`): `Name; Prev; Next; Link; SetPrev; QueueFrame; ProcessFrame; PushFrame; Broadcast; Start; Stop`.
- `BaseProcessor` (`processor.go:92`): inputSysCh + inputDataCh + procCh + per-processor ctx + interrupt cancel-and-recreate. Every processor embeds `*BaseProcessor`.
- `TaskContext` was originally `Ctx, Logger, Room, UIEvents, Metrics func(MetricsFrame), EndTask func(reason string), wg *sync.WaitGroup`. It is now in `voicepipelinecore/pipeline.go` with typed `EndTask func(EndReason)` and unexported call-events/call-stats/turn-metrics helpers.
- `PipelineTask` (`pipeline.go:64`): owns `TaskCtx, Cancel, Source *PipelineSourceProcessor, Pipeline *Pipeline, RoomName, wg sync.WaitGroup, endRequested, cleanupStarted, cleanupOnce`. `End(reason)` queues an EndFrame at the source. `completeEnd(frame)` runs the cleanup once when the sink sees EndFrame.
- The global `sessions` map now lives in root `main.go`; `createSession` was replaced by `voicepipelinecore.NewTask`.

### Current `/connect` flow (`main.go:35-51`)
```go
func handleConnect(w http.ResponseWriter, r *http.Request) {
    roomName, _ := createSession()           // mints "room-<rand>", builds pipeline, starts it
    token, _ := generateToken(roomName, "web-user")
    json.NewEncoder(w).Encode(map[string]string{
        "server_url": os.Getenv("LIVEKIT_URL"),
        "token":      token,
        "room_name":  roomName,
    })
}
```

### Pipeline composition (current; from `pipeline.go:141-152`)
```
PipelineSource → AudioSource → STT → UserIdle → ContextAggregator → TalkTimeMonitor → LLM → TTS → PlaybackSink → PipelineSink
```

### Key existing design decisions (carry over unchanged)
- **EndFrame is a data frame, not a system frame.** It travels through the pipeline in order. Processors with async work (TTS) defer it until safe.
- **InterruptFrame is a system frame.** BaseProcessor cancels procCtx, drains procCh, recreates.
- **MetricsFrame is intercepted by `BaseProcessor.PushFrame`** and delivered to `taskCtx.Metrics` — never reaches downstream processors. (We'll add a second consumer in §6.)
- **`taskCtx.EndTask(reason)`** is the canonical way for any processor to request graceful task shutdown. Don't push EndFrame downstream directly.
- **`PipelineTask.completeEnd`** runs exactly once via `sync.Once`, calls `Pipeline.Stop()`, disconnects LiveKit room, waits 10s on the shared WG, then cancels and removes the session.
- **STT/TTS have `Stop()` overrides** that close their websockets in addition to cancelling ctx — required so blocking `ReadMessage` exits during cleanup.

### Existing AGENTS.md
The repo has an AGENTS.md at the root with a full architecture description. **Update it as part of phase 0 with the new core/integration rule** — exact text in §14.

---

## 3. Disha backend contracts (read this once, then keep open as reference)

All file:line citations are in `/Users/jaideepsingh/Projects/disha-backend/`.

### 3.1 HTTP endpoints we will call as a CLIENT

#### `PATCH /bot/update_conversation`
**Source**: `bots/views.py:62-67` (request model), `bots/views.py:575-578` (handler).

Request body:
```json
{
  "conversation_id": "uuid string, REQUIRED",
  "bot_joined_at":   "2026-05-22T13:45:00+05:30 or null",
  "user_joined_at":  "iso8601 or null",
  "user_first_speech_at": "iso8601 or null",
  "bot_first_speech_at":  "iso8601 or null"
}
```
- All timestamp fields are optional. Send only the one(s) you have at each call event moment.
- 10 second timeout in the Python client; replicate that.
- 200 OK on success; non-200 should be logged but must not crash the pipeline.

#### `POST /bot/run_post_call_operations`
**Source**: `bots/views.py:70-81` (request model), `bots/views.py:581-584` (handler), `bots/sales_call/sales_call.py:452-461` (call site).

Request body:
```json
{
  "conversation_id": "uuid REQUIRED",
  "end_reason": "talktime_exhausted | user_idle | null",
  "total_user_duration": 123,
  "first_user_audio_frames_received_at": "iso8601 or null",
  "ended_at": "iso8601 REQUIRED",
  "log_data_s3_key": "" ,
  "onboarding_call_done": false,
  "diet_plan_intensity_level": null,
  "fitness_plan_intensity_level": null,
  "latest_onboarding_call_stage": null,
  "conversation_variables": null
}
```
- `log_data_s3_key` is REQUIRED by the schema but accepts empty string. We send `""` for now (no RTVI port).
- `onboarding_call_done` always `false` for sales call.
- All `*_intensity_level`, `latest_onboarding_call_stage`, `conversation_variables` always `null` for sales call.
- This endpoint is server-side **idempotent** (Disha uses `post_call_operations_started_at`). Safe to retry once on transient failure.

#### `POST /common/enqueue_job`
**Source**: `common/urls.py:8-10`, `common/views.py:27-49`.

Use the existing Disha endpoint. Do **not** create a `/bot/enqueue_job` endpoint unless the user explicitly changes direction.

Request body:
```json
{
  "module_name": "services.conversation_chunk_manager",
  "func_name": "sync_conversation_chunks_to_db",
  "kwargs": {
    "user_id": "uuid",
    "conversation_id": "uuid",
    "bot_type": "sales_call"
  },
  "sqs_queue": "p1-fast-l1"
}
```

Notes:
- Existing Disha `EnqueueJob` does not accept `tracking_type`; `SQSWorkerJobManager.queue_job` defaults to FULL tracking.
- In non-prod Disha, `queue_job` may execute the function inline instead of enqueueing. Keep the Go call non-fatal on timeout/failure, but do not fire it before committed-turn Redis writes have drained.

#### Disha API auth
Replicate `bots/services/voice_bot_api_service.py`: it does not currently send an Authorization header. Therefore the Go Disha API client should not send one by default. If an auth token is later added, make it optional and send no header when unset.

### 3.2 Redis keys we read/write

#### `conversation_data:{conversation_id}` (READ at session start, written by Disha orchestrator)
**Source**: `bots/bot_session_manager.py:406-419`.

Value (JSON-encoded):
```json
{
  "conversation": {
    "id": "uuid",
    "user_id": "uuid",
    "bot_type": "sales_call",
    "resumed_from_chunk_id": "uuid or null",
    "start_chunk_id": "uuid or null",
    "end_chunk_id":   "uuid or null",
    "...": "other CallConversation columns"
  },
  "chunks": [
    ["chunk_id_uuid", "user", "text content", false, null],
    ["chunk_id_uuid", "assistant", "text content", false, null]
  ],
  "user_profile": {
    "user_id": "uuid",
    "phone":   "+91...",
    "...":     "other UserProfile columns"
  },
  "unprocessed_chat_context": "optional, present for non-onboarding bots",
  "resumed_chunk": "optional ConversationChunk dict if resumed_from_chunk_id set"
}
```

Notes:
- `chunks` is an array of 5-tuples: `[id, role, text, is_debug_log, additional_data]`.
- For sales call we only need: `conversation.id`, `conversation.user_id`, `conversation.bot_type`, `conversation.resumed_from_chunk_id`, `chunks[]` (filtered by `is_debug_log==false` and role in {`user`, `assistant`}), `user_profile.user_id`, optionally `resumed_chunk`.
- TTL: 7200 seconds. If the key is missing, **return 502** from `/connect` — do not fall back to Postgres (the user explicitly said no Postgres in Go).

#### `conversation_chunks:{user_id}:{conversation_id}` (WRITE per turn; READ never)
**Source**: `services/conversation_chunks_cache.py:23, 60-71`.

Redis LIST. We `RPUSH` one JSON-encoded chunk per committed turn. Schema (21 keys, exact match to `bots/onboarding_call/conversation_persistence_processor.py:113-132`):

```json
{
  "id": "uuid v4 minted by us",
  "text": "the message text",
  "role": "user | assistant",
  "bot_type": "sales_call",
  "conversation_id": "uuid",
  "user_id": "uuid",
  "current_agenda": null,
  "stt_ttfb_ms": null,
  "llm_ttfb_ms": 123.4,
  "tts_ttfb_ms": 234.5,
  "v2v_latency_ms": 800.1,
  "text_aggregation_ms": 15.2,
  "created": "2026-05-22T13:45:01.123456+05:30",
  "is_debug_log": false,
  "additional_data": null,
  "main_agent_system_prompt_langfuse_key": null,
  "system_message_params_s3_key": null,
  "conversation_state_s3_key": null
}
```

Field rules for our Go writer:
- `id`: fresh `uuid.NewString()` per chunk.
- `text`, `role`, `created`: from the commit event.
- `bot_type`: always `"sales_call"`.
- `conversation_id`, `user_id`: from the session.
- `current_agenda`: always `null` (sales call doesn't have agenda state).
- `stt_ttfb_ms`: always `null` (we don't measure STT TTFB today).
- `llm_ttfb_ms`, `tts_ttfb_ms`, `text_aggregation_ms`, `v2v_latency_ms`: populated only on the assistant chunk from the per-turn metrics buffer. `null` on user chunks.
- `is_debug_log`: always `false` (sales call doesn't persist debug logs).
- `additional_data`: always `null`.
- `main_agent_system_prompt_langfuse_key`: always `null` (no Langfuse port).
- `system_message_params_s3_key`, `conversation_state_s3_key`: always `null` (no S3 port).

Retry policy in the Python client: 3 attempts with exponential backoff on `redis.exceptions.TimeoutError`. Replicate this in Go with go-redis's built-in retry plus a wrapper.

### 3.3 End-of-call ordering (mirror exactly)

From `bots/sales_call/sales_call.py:413-487`:

1. (Skipped in our port — RTVI logs to S3.)
2. `POST /bot/run_post_call_operations` (HTTP, 10s timeout).
3. (Skipped — we don't use Daily transport, so no Daily metrics queue.)
4. Pipeline shutdown (`task.cancel()` in Python; `Pipeline.Stop()` + `wg.Wait` in Go — already implemented).
5. After pipeline returns and committed-turn Redis writes have drained, enqueue the chunk-sync job via `POST /common/enqueue_job`.

If you flip the order, you can race Redis-LIST deletion against in-flight chunk writes.

### 3.4 EndReason mapping

Disha's current canonical end reasons (`calling/models.py:59-61`):
- `talktime_exhausted` — talk time hit max
- `user_idle` — idle timeout fired N times (we don't end on idle today, but reserve)

Core now exposes typed reasons:
- `voicepipelinecore.EndReasonTalkTimeExhausted`
- `voicepipelinecore.EndReasonClientDisconnect`
- `voicepipelinecore.EndReasonUserIdle`
- `voicepipelinecore.EndReasonError`
- `voicepipelinecore.EndReasonUnspecified`

Translate in the Disha package before sending the post-call payload. Map:
```
EndReasonTalkTimeExhausted → "talktime_exhausted"
EndReasonUserIdle          → "user_idle"
EndReasonClientDisconnect  → null
EndReasonError             → null
EndReasonUnspecified       → null
(any unknown)              → null  (omit from payload)
```

Reason: Disha's DB enum does not currently include `client_disconnected`. Keep client disconnect as an internal Go reason and omit `end_reason` from `run_post_call_operations` until Disha adds a DB enum value.

---

## 4. Phase 0 — Module rename and package split (mechanical, biggest mechanical change)

### 4.1 Module rename

In `go.mod`:
```
- module awesomeProject
+ module github.com/jaideep329/talk-go
```

Run `go mod tidy` after.

Also delete the leftover binary `awesomeProject` at the repo root (it's a build artifact) and add it to `.gitignore` if not already excluded.

### 4.2 Package split — target layout

```
talk-go/
├── go.mod                           # module github.com/jaideep329/talk-go
├── go.sum
├── main.go                          # HTTP server, sessions map, wiring
├── clients/
│   ├── daily-client.html
│   └── livekit-client.html
├── background-office-sound.mp3      # unchanged
├── AGENTS.md                        # updated (see §14)
├── reports/
│   └── disha-integration-plan.md    # this file
├── voicepipelinecore/
│   ├── frame.go
│   ├── processor.go
│   ├── pipeline.go                  # just the Pipeline struct (chain + Start/Stop)
│   ├── task.go                      # NEW: TaskContext, PipelineTask, TaskOptions, NewTask
│   ├── call_events.go               # CallEvents FIFO dispatcher + lifecycle/turn callbacks
│   ├── pipeline_edges.go
│   ├── livekit_room.go              # joinRoom + GenerateToken (capitalized)
│   ├── ui_events.go
│   ├── metrics.go
│   ├── audio_source_processor.go
│   ├── stt_processor.go
│   ├── user_idle_processor.go
│   ├── context_aggregator.go        # accepts initialMessages + fires committed-turn call events
│   ├── talktime_monitoring_processor.go
│   ├── llm_processor.go
│   ├── tts_processor.go
│   ├── playback_sink_processor.go
│   └── *_test.go                    # all tests move here
└── disha/                           # added in phase 2+
    └── (created later)
```

### 4.3 Mechanical migration steps

1. `mkdir voicepipelinecore`
2. `git mv` every `.go` file (production + test) at the root into `voicepipelinecore/`, EXCEPT:
   - Keep `main.go` at the root (you'll rewrite it).
   - Do not move `clients/`, `reports/`, `background-office-sound.mp3`, `app.log`, the binary, `.env`, etc.
3. In every moved file, change `package main` → `package voicepipelinecore`.
4. The current `main.go` at the root needs full rewrite — you'll do this in §4.5.
5. Re-export anything `main.go` (or future `disha/`) will need. Quick checklist of probable renames (verify against the actual code):
   - `sessions`, `sessionsMu`, `getSession`, `removeSession` — these move OUT of voicepipelinecore entirely (into `main.go`). See §4.4.
   - `createSession` — gets replaced by `voicepipelinecore.NewTask(ctx, opts)` (see §6).
   - `loadEnv` — stays in `main.go` (it's a binary-level concern).
   - `joinRoom` → `JoinRoom`, `generateToken` → `GenerateToken`. These are now public API.
   - Do not add a gorilla websocket upgrader or `/ws` handler. The current app uses LiveKit DataPackets for both outbound UI events and inbound `control` messages.
   - Anything in `ui_events.go` that's only consumed inside the package can stay unexported. `UIEventSender` is already exported.
6. `taskCtx` (lowercase field on `BaseProcessor`) stays unexported — only used inside the core package.
7. `cancel` field on `BaseProcessor` (lowercase) used by `PipelineSourceProcessor.drainExternalFrames` (`pipeline_edges.go:57`) — stays unexported (same package, fine).

### 4.4 Sessions map moves to `main.go`

Today (`pipeline.go:76-91`):
```go
var (
    sessions   = map[string]*PipelineTask{}
    sessionsMu sync.Mutex
)

func getSession(roomName string) *PipelineTask { ... }
func removeSession(roomName string) { ... }
```

After the split, `voicepipelinecore` doesn't know about a sessions registry. That's a `main.go` concern (currently used for cleanup-on-removal; LiveKit DataPacket control messages route through the room callback, not a `/ws` lookup).

In `main.go`:
```go
var (
    sessions   = map[string]*voicepipelinecore.PipelineTask{}
    sessionsMu sync.Mutex
)
```

`PipelineTask.completeEnd` currently calls `removeSession(t.RoomName)` (`pipeline.go:212`). After the split it should call a callback passed via `TaskOptions.OnCleanup` instead, so the core doesn't know about the registry. Or, simpler: expose a `Task.OnCleanup func()` field that `main.go` populates with a closure that does `delete(sessions, task.RoomName)`. I recommend the latter — it's one field.

### 4.5 New `main.go` skeleton (post-phase-0; will be filled in further in phase 5)

```go
package main

import (
    "context"
    "encoding/json"
    "io"
    "log"
    "net/http"
    "os"
    "sync"

    "github.com/go-logr/stdr"
    protoLogger "github.com/livekit/protocol/logger"
    lksdk "github.com/livekit/server-sdk-go/v2"

    "github.com/jaideep329/talk-go/voicepipelinecore"
)

var (
    sessions   = map[string]*voicepipelinecore.PipelineTask{}
    sessionsMu sync.Mutex
)

func main() {
    loadEnv(".env")
    appLog, _ := os.Create("app.log")
    log.SetOutput(io.MultiWriter(os.Stderr, appLog))
    log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
    stdr.SetVerbosity(0)
    lksdk.SetLogger(protoLogger.LogRLogger(stdr.New(log.New(io.Discard, "", 0))))

    http.HandleFunc("/connect", handleConnect)
    http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        http.ServeFile(w, r, "clients/livekit-client.html")
    })
    log.Println("HTTP server listening on :3000")
    if err := http.ListenAndServe(":3000", nil); err != nil {
        log.Fatal(err)
    }
}

// handleConnect — phase 0 version: keeps today's behavior. Phase 5 will
// upgrade it to read conversation_id and dispatch through the disha.Bot boundary.
func handleConnect(w http.ResponseWriter, r *http.Request) {
    logger := log.New(log.Writer(), "[room] ", log.Flags())

    opts := voicepipelinecore.TaskOptions{
        Logger:          logger,
        InitialMessages: []voicepipelinecore.Message{
            {Role: "system", Content: defaultDevSystemPrompt},
        },
    }
    task, err := voicepipelinecore.NewTask(context.Background(), opts)
    if err != nil {
        http.Error(w, err.Error(), 500)
        return
    }

    task.OnCleanup = func() {
        sessionsMu.Lock(); delete(sessions, task.RoomName); sessionsMu.Unlock()
    }

    sessionsMu.Lock(); sessions[task.RoomName] = task; sessionsMu.Unlock()

    if err := task.Start(); err != nil { http.Error(w, err.Error(), 500); return }

    token, err := voicepipelinecore.GenerateToken(task.RoomName, "web-user")
    if err != nil { http.Error(w, err.Error(), 500); return }

    json.NewEncoder(w).Encode(map[string]string{
        "server_url": os.Getenv("LIVEKIT_URL"),
        "token":      token,
        "room_name":  task.RoomName,
    })
}

const defaultDevSystemPrompt = `You are an expert health coach...` // copy from context_aggregator.go:135-138
```

Note: `task.End` now takes an `EndReason` (enum), not a free-form string. See §6.

### 4.6 Phase 0 verification

```bash
go mod tidy
go build ./...
go test -race -count=2 ./...
./talk-go            # binary name follows module name after rename; or `go run .`
# In another terminal:
curl -XPOST http://localhost:3000/connect
# Then open http://localhost:3000 in a browser, click Connect, talk to the bot.
```

All 41 tests should still pass. The browser flow should behave identically to before.

---

## 5. Phase 1 — Public API surface (the contract)

No new behavior. Just rearrange what's already there so the public surface of `voicepipelinecore` is intentional.

### 5.1 `voicepipelinecore.TaskOptions`

In a new file `voicepipelinecore/task.go`:

```go
package voicepipelinecore

import (
    "context"
    "log"
    "time"
)

// TaskOptions configures one voice session. Pass to NewTask.
type TaskOptions struct {
    // Logger is required.
    Logger *log.Logger

    // RoomName is the LiveKit room name. If empty, NewTask mints one
    // like "room-1234567".
    RoomName string

    // InitialMessages seed the conversation history. Nil preserves the
    // local demo default prompt; an explicit empty slice means the
    // caller is intentionally providing no initial context.
    InitialMessages []Message

    // MaxTalkTime overrides the talk-time monitor's default when non-nil.
    // Pointer is intentional: Disha can send 0 remaining seconds, which
    // must mean "end immediately", not "use the default".
    MaxTalkTime *time.Duration

    // CallEvents contains call timeline and committed-turn hooks.
    CallEvents CallEvents

    // OnCleanup runs after OnCallEnded.
    // Use this to remove the task from binary-level registries.
    OnCleanup func()
}

// CallEvents are nil-safe call timeline hooks. The first five are one-shot.
// Committed-turn events can fire many times. OnCallEnded runs synchronously
// during cleanup after the call-event dispatcher drain and wg.Wait.
type CallEvents struct {
    OnBotJoined       func(at time.Time)                    // after LiveKit Connect succeeds
    OnUserJoined      func(at time.Time)                    // first non-bot LiveKit participant connects
    OnUserFirstSpeech func(at time.Time)                    // first finalized user transcript text
    OnBotFirstSpeech  func(at time.Time)                    // first bot audio frame written to wire
    OnFirstUserAudio  func(at time.Time)                    // first non-silence audio frame received
    OnUserTurnCommitted func(text string, at time.Time)      // committed user turn
    OnAssistantTurnCommitted func(text string, at time.Time, metrics TurnMetrics)
    OnCallEnded       func(reason EndReason, stats CallStats) // exactly once, after pipeline drain
}

// Message is one entry in the LLM conversation history.
type Message struct {
    Role    string // "system" | "user" | "assistant"
    Content string
}

// EndReason describes why a session ended.
type EndReason string

const (
    EndReasonUnspecified       EndReason = "unspecified"
    EndReasonTalkTimeExhausted EndReason = "talk_time_exhausted"
    EndReasonClientDisconnect  EndReason = "client_disconnected"
    EndReasonUserIdle          EndReason = "user_idle"
    EndReasonError             EndReason = "error"
)

// CallStats are filled in by the core and passed to OnCallEnded.
type CallStats struct {
    TotalUserDurationSec  float64
    FirstUserAudioFrameAt time.Time
    EndedAt               time.Time
}
```

### 5.2 `voicepipelinecore.CallEvents` dispatcher

Committed turns use the same callback system as lifecycle events. There is no separate turn-listener primitive.

`voicepipelinecore/call_events.go` owns one FIFO dispatcher per task:
- Lifecycle callbacks (`OnBotJoined`, `OnUserJoined`, `OnFirstUserAudio`, `OnUserFirstSpeech`, `OnBotFirstSpeech`) are once-only.
- Committed-turn callbacks (`OnUserTurnCommitted`, `OnAssistantTurnCommitted`) can fire many times.
- The dispatcher has a bounded queue (`cap=512`), recovers callback panics, and preserves callback ordering.
- `PipelineTask.runCleanup` calls `taskCtx.callEvents.stopAndDrain()` before `wg.Wait()` and before `OnCallEnded`. This guarantees Redis chunk writes queued before EndFrame complete before the Disha chunk-sync job is enqueued.
- Callback implementations must use bounded I/O timeouts; Disha committed-turn Redis writes use a 5s timeout.

This is intentionally closer to the mental model of Pipecat event handlers than to a generic frame tap. Core emits semantic events; business packages decide what those events mean.

### 5.3 `voicepipelinecore.PipelineTask` (revised constructor)

Replace the current `createSession()` flow with `NewTask(ctx, opts)`.

Implemented shape:
- `NewTask(parentCtx, opts)` validates `opts.Logger`, mints `RoomName` if needed, creates `callStatsTracker`, call events, per-turn metrics, and typed `TaskContext.EndTask`.
- `JoinRoom(roomName, taskCtx, audioSource)` returns an error instead of fatal-exiting. `NewTask` joins LiveKit before assembling the final pipeline so `PlaybackSink` can publish to `taskCtx.Room`.
- `ContextAggregator` receives `opts.InitialMessages` only when the slice is non-nil. Nil preserves the local demo default prompt; explicit empty disables that default.
- `TalkTimeMonitoringProcessor` uses `opts.MaxTalkTime` when non-nil.
- `PipelineTask.End(reason EndReason)` stores the typed reason and queues `NewEndFrame(string(reason))`.
- There is no `mapReason` shim in core; callers and processors use `EndReason` directly.

### 5.4 What stays the same

- `Pipeline`, `Processor`, `BaseProcessor`, `Direction`, `Envelope`, `Frame`, every concrete frame type, every processor's internal logic.
- `EndFrame` plumbing through the pipeline.
- `InterruptFrame` cancel-and-recreate.
- `MetricsFrame` interception in `BaseProcessor.PushFrame`.
- All the websocket reconnection logic in STT/TTS.

### 5.5 Phase 1 verification

- Existing tests were updated for typed `EndReason`.
- Added coverage:
  - `pipeline_task_test.go`: minimal cleanup path calls `OnCallEnded` with typed reason + `CallStats`.
  - `call_events_test.go`: stop-and-drain handles queued events and recovers callback panics.
  - `context_aggregator_test.go`: initial-message seeding, explicit empty initial context, user-first-speech call events, and committed-turn call events.
  - `metrics_turn_test.go`, `call_events_test.go`, `call_stats_tracker_test.go`, `audio_source_processor_test.go`: focused coverage for the new helpers.

---

## 6. Phase 1.5 — Call events + per-turn metrics aggregation (still in voicepipelinecore)

These are part of the public callback contract but distinct mechanical chunks.

### 6.1 Fire-once call event helper

Add `callEvents` to `TaskContext`:

```go
// In task.go
type callEventDispatcher struct {
    botJoined        sync.Once
    userJoined       sync.Once
    userFirstSpeech  sync.Once
    botFirstSpeech   sync.Once
    firstUserAudio   sync.Once
    onBotJoined        func(time.Time)
    onUserJoined       func(time.Time)
    onUserFirstSpeech  func(time.Time)
    onBotFirstSpeech   func(time.Time)
    onFirstUserAudio   func(time.Time)
    wg                 *sync.WaitGroup
}

func (l *callEventDispatcher) fireBotJoined(at time.Time) {
    l.botJoined.Do(func() {
        if l.onBotJoined == nil { return }
        l.wg.Add(1)
        go func() { defer l.wg.Done(); l.onBotJoined(at) }()
    })
}
// ... same for the other four ...
```

The processors fire these:
- `LiveKit room joined` → in `livekit_room.go` after `Connect` returns successfully, call `taskCtx.callEvents.fireBotJoined(time.Now())`. But `JoinRoom` is called from `NewTask`, BEFORE TaskCtx is fully set up. **Refactor**: pass `taskCtx` into `JoinRoom`, fire after Connect.
- `Participant connected (non-bot)` → in `livekit_room.go`'s `RoomCallback`, add an `OnParticipantConnected` handler that checks identity != bot identity, calls `taskCtx.callEvents.fireUserJoined(time.Now())`.
- `First non-silence audio frame received` → `audio_source_processor.go`'s `readAudioTrack`: detect non-silence (any frame where `bytes.Contains(data, nonZero)` or any sample magnitude > some threshold; simplest: any frame whose PCM has a sample > 1000) and call `taskCtx.callEvents.fireFirstUserAudio(time.Now())`. Also stash that timestamp on the task for `CallStats.FirstUserAudioFrameAt`.
- `First finalized user transcript text` → `context_aggregator.go`'s `submitUserMessage`: call `taskCtx.callEvents.fireUserFirstSpeech(time.Now())` before pushing the LLMMessagesFrame.
- `First bot audio frame written to wire` → `playback_sink_processor.go`'s `tick()`: where it currently calls `p.Broadcast(NewBotStartedSpeakingFrame())` (only fires on first audio of a turn), also fire `taskCtx.callEvents.fireBotFirstSpeech(time.Now())` — `sync.Once` ensures it only fires once across the whole call.

### 6.2 Per-turn metrics aggregator

Today `MetricsFrame` is intercepted by `BaseProcessor.PushFrame` and consumed by the UI handler. The core also collects them per turn so the assistant commit can carry them to `CallEvents.OnAssistantTurnCommitted`.

Add to `PipelineTask`:

```go
type perTurnMetrics struct {
    mu    sync.Mutex
    current TurnMetrics
}

func (m *perTurnMetrics) absorb(mf MetricsFrame) {
    m.mu.Lock(); defer m.mu.Unlock()
    for _, d := range mf.Data {
        v := d.ValueMs
        switch {
        case d.Processor == "llm" && d.Label == MetricTTFB:
            m.current.LLMTTFBMs = v
        case d.Processor == "llm" && d.Label == MetricProcessing:
            m.current.LLMProcessingMs = v
        case d.Processor == "tts" && d.Label == MetricTTFB:
            m.current.TTSTTFBMs = v
        case d.Processor == "tts" && d.Label == MetricTextAggregation:
            m.current.TTSTextAggregationMs = v
        case d.Processor == "playback" && d.Label == MetricE2ELatency:
            m.current.E2ELatencyMs = v
        }
    }
}

func (m *perTurnMetrics) snapshotAndReset() TurnMetrics {
    m.mu.Lock(); defer m.mu.Unlock()
    snap := m.current
    m.current = TurnMetrics{}
    return snap
}
```

In `ContextAggregator.commitSpokenText(interrupted bool)`:
```go
spoken := a.spokenSoFar()
if spoken != "" {
    // ... existing logging + a.messages append + UIEvents.Send ...
    metricsSnapshot := a.taskCtx.metrics.snapshotAndReset()
    if a.taskCtx.callEvents != nil {
        a.taskCtx.callEvents.fireAssistantTurnCommitted(spoken, time.Now(), metricsSnapshot)
    }
}
```

In `ContextAggregator.addUserMessage(text string)`:
```go
// ... existing concat or append logic ...
if a.taskCtx.callEvents != nil {
    a.taskCtx.callEvents.fireUserTurnCommitted(text, time.Now())
}
```

**Wiring nuance**: TaskContext does not currently have a back-pointer to the task (and probably shouldn't). Simpler approach: have `task.metrics` and `task.callEvents` accessible via `TaskContext` indirection. Add `taskCtx.metrics *perTurnMetrics` and `taskCtx.callEvents *callEventDispatcher` fields populated by `NewTask`. The context aggregator reads them directly.

### 6.3 EndReason translation table

Implemented cleaner option: `taskCtx.EndTask` is `func(EndReason)`, so core does not carry a legacy string mapping table. Disha-specific mapping from `voicepipelinecore.EndReason` to Disha's optional end-reason string belongs in the Disha package.

### 6.4 `OnCallEnded` dispatch

In `PipelineTask.completeEnd`, after `wg.Wait` returns (or times out):

```go
if t.onCallEnded != nil {
    endedAt := time.Now()
    stats := CallStats{
        TotalUserDurationSec:  t.callStats.TotalDurationSec(endedAt),
        FirstUserAudioFrameAt: t.callStats.FirstUserAudioFrameAt(),
        EndedAt:               endedAt,
    }
    // Run synchronously here after call-event drain and wg.Wait.
    t.onCallEnded(t.endReason, stats)
}
if t.OnCleanup != nil {
    t.OnCleanup()
}
```

### 6.5 `callStatsTracker` (per-call duration tracking)

New `voicepipelinecore/call_stats_tracker.go`:

```go
package voicepipelinecore

import (
    "sync"
    "time"
)

type callStatsTracker struct {
    mu           sync.Mutex
    joinedAt     time.Time
    leftAt       time.Time
    firstAudioAt time.Time
    total        time.Duration
    present      bool
}

func (p *callStatsTracker) MarkUserJoined(at time.Time) {
    p.mu.Lock(); defer p.mu.Unlock()
    if !p.present {
        p.present = true
        p.joinedAt = at
    }
}

func (p *callStatsTracker) MarkUserLeft(at time.Time) {
    p.mu.Lock(); defer p.mu.Unlock()
    if p.present && !p.joinedAt.IsZero() && at.After(p.joinedAt) {
        p.total += at.Sub(p.joinedAt)
    }
    p.leftAt = at
    p.present = false
}

func (p *callStatsTracker) MarkFirstUserAudio(at time.Time) bool {
    p.mu.Lock(); defer p.mu.Unlock()
    if !p.firstAudioAt.IsZero() {
        return false
    }
    p.firstAudioAt = at
    return true
}

func (p *callStatsTracker) TotalDurationSec(at time.Time) float64 {
    p.mu.Lock(); defer p.mu.Unlock()
    total := p.total
    if p.present && !p.joinedAt.IsZero() && at.After(p.joinedAt) {
        total += at.Sub(p.joinedAt)
    }
    return total.Seconds()
}

func (p *callStatsTracker) FirstUserAudioFrameAt() time.Time {
    p.mu.Lock(); defer p.mu.Unlock()
    return p.firstAudioAt
}
```

`callStatsTracker` lives on `PipelineTask` (and indirectly via `TaskContext.callStats`). Update accordingly. LiveKit `RoomCallback.OnParticipantConnected`/`OnParticipantDisconnected` call `MarkUserJoined`/`MarkUserLeft`. AudioSource's first-audio detection calls `MarkFirstUserAudio` AND fires the call events callback.

### 6.6 Phase 1.5 verification

Unit tests:
- Test call event helper once-only dispatch directly; test ContextAggregator fires `OnUserFirstSpeech`; test AudioSource marks first audible user audio.
- Send fake `MetricsFrame`s through, verify the next assistant commit's `TurnMetrics` contains the values.
- Verify call-event queue handles burst (200 commits sent rapidly) without dropping.
- Verify call-event callback panic is caught and logged, doesn't crash the task.

---

## 7. Phase 2 — Disha package: types + Redis client

Create `disha/` directory.

### 7.1 `disha/types.go`

```go
package disha

import "time"

// ConversationData mirrors what Disha writes to
// "conversation_data:{conversation_id}" in Redis.
// Source: bots/bot_session_manager.py:406-419
type ConversationData struct {
    Conversation             ConversationRow        `json:"conversation"`
    Chunks                   [][]any                `json:"chunks"` // 5-tuples
    UserProfile              UserProfileData        `json:"user_profile"`
    UnprocessedChatContext   *string                `json:"unprocessed_chat_context,omitempty"`
    ResumedChunk             map[string]any         `json:"resumed_chunk,omitempty"`
}

type ConversationRow struct {
    ID                  string  `json:"id"`
    UserID              string  `json:"user_id"`
    BotType             string  `json:"bot_type"`
    ResumedFromChunkID  *string `json:"resumed_from_chunk_id"`
    StartChunkID        *string `json:"start_chunk_id"`
    EndChunkID          *string `json:"end_chunk_id"`
    // Many other fields on CallConversation; add only what we read.
}

type UserProfileData struct {
    UserID                            string   `json:"user_id"`
    Phone                             string   `json:"phone"`
    RemainingSalesCallTalktimeSeconds *float64 `json:"remaining_sales_call_talktime_seconds"`
    // Many other fields on UserProfile; add only what we read.
}

// ConversationChunk mirrors the 21-key dict at
// bots/onboarding_call/conversation_persistence_processor.py:113-132
type ConversationChunk struct {
    ID                               string   `json:"id"`
    Text                             string   `json:"text"`
    Role                             string   `json:"role"`
    BotType                          string   `json:"bot_type"`
    ConversationID                   string   `json:"conversation_id"`
    UserID                           string   `json:"user_id"`
    CurrentAgenda                    *string  `json:"current_agenda"`
    STTTtfbMs                        *float64 `json:"stt_ttfb_ms"`
    LLMTtfbMs                        *float64 `json:"llm_ttfb_ms"`
    TTSTtfbMs                        *float64 `json:"tts_ttfb_ms"`
    V2VLatencyMs                     *float64 `json:"v2v_latency_ms"`
    TextAggregationMs                *float64 `json:"text_aggregation_ms"`
    Created                          string   `json:"created"`
    IsDebugLog                       bool     `json:"is_debug_log"`
    AdditionalData                   any      `json:"additional_data"`
    MainAgentSystemPromptLangfuseKey *string  `json:"main_agent_system_prompt_langfuse_key"`
    SystemMessageParamsS3Key         *string  `json:"system_message_params_s3_key"`
    ConversationStateS3Key           *string  `json:"conversation_state_s3_key"`
}

// UpdateConversationRequest mirrors bots/views.py:62-67
type UpdateConversationRequest struct {
    ConversationID    string     `json:"conversation_id"`
    BotJoinedAt       *time.Time `json:"bot_joined_at,omitempty"`
    UserJoinedAt      *time.Time `json:"user_joined_at,omitempty"`
    UserFirstSpeechAt *time.Time `json:"user_first_speech_at,omitempty"`
    BotFirstSpeechAt  *time.Time `json:"bot_first_speech_at,omitempty"`
}

// PostCallOperationsRequest mirrors bots/views.py:70-81
type PostCallOperationsRequest struct {
    ConversationID                 string         `json:"conversation_id"`
    EndReason                      *string        `json:"end_reason,omitempty"`
    TotalUserDuration              int            `json:"total_user_duration"`
    FirstUserAudioFramesReceivedAt *time.Time     `json:"first_user_audio_frames_received_at,omitempty"`
    EndedAt                        time.Time      `json:"ended_at"`
    LogDataS3Key                   string         `json:"log_data_s3_key"`     // we send ""
    OnboardingCallDone             bool           `json:"onboarding_call_done"` // false for sales
    DietPlanIntensityLevel         *string        `json:"diet_plan_intensity_level,omitempty"`
    FitnessPlanIntensityLevel      *string        `json:"fitness_plan_intensity_level,omitempty"`
    LatestOnboardingCallStage      *string        `json:"latest_onboarding_call_stage,omitempty"`
    ConversationVariables          map[string]any `json:"conversation_variables,omitempty"`
}

// EnqueueJobRequest is what we send to Disha's job-enqueue endpoint
// to trigger the Redis→Postgres chunk sync.
// Mirrors the existing /common/enqueue_job contract in common/views.py.
type EnqueueJobRequest struct {
    ModuleName string         `json:"module_name"`
    FuncName   string         `json:"func_name"`
    Kwargs     map[string]any `json:"kwargs"`
    SQSQueue   string         `json:"sqs_queue"`
}
```

### 7.2 `disha/redis_client.go`

```go
package disha

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "time"

    "github.com/redis/go-redis/v9"
)

// RedisClient is the narrow surface the disha package needs from Redis.
type RedisClient interface {
    GetConversationData(ctx context.Context, conversationID string) (*ConversationData, error)
    AppendChunk(ctx context.Context, userID, conversationID string, chunk ConversationChunk) error
    Close() error
}

type redisClient struct {
    rdb    *redis.Client
    logger *log.Logger
}

func NewRedisClient(addr, password string, db int, logger *log.Logger) RedisClient {
    return &redisClient{
        rdb: redis.NewClient(&redis.Options{
            Addr:            addr,
            Password:        password,
            DB:              db,
            MaxRetries:      3,
            MinRetryBackoff: 100 * time.Millisecond,
            MaxRetryBackoff: 1 * time.Second,
            DialTimeout:     5 * time.Second,
            ReadTimeout:     3 * time.Second,
            WriteTimeout:    3 * time.Second,
        }),
        logger: logger,
    }
}

func (c *redisClient) Close() error { return c.rdb.Close() }

func (c *redisClient) GetConversationData(ctx context.Context, conversationID string) (*ConversationData, error) {
    key := fmt.Sprintf("conversation_data:%s", conversationID)
    raw, err := c.rdb.Get(ctx, key).Bytes()
    if err == redis.Nil {
        return nil, fmt.Errorf("disha: conversation_data not found in Redis: %s", conversationID)
    }
    if err != nil {
        return nil, fmt.Errorf("disha: redis GET failed: %w", err)
    }
    var data ConversationData
    if err := json.Unmarshal(raw, &data); err != nil {
        return nil, fmt.Errorf("disha: conversation_data malformed: %w", err)
    }
    return &data, nil
}

func (c *redisClient) AppendChunk(ctx context.Context, userID, conversationID string, chunk ConversationChunk) error {
    key := fmt.Sprintf("conversation_chunks:%s:%s", userID, conversationID)
    payload, err := json.Marshal(chunk)
    if err != nil {
        return fmt.Errorf("disha: chunk marshal failed: %w", err)
    }
    if err := c.rdb.RPush(ctx, key, payload).Err(); err != nil {
        return fmt.Errorf("disha: redis RPUSH failed: %w", err)
    }
    return nil
}
```

Env vars for `main.go`:
- `DISHA_REDIS_URL` (default `localhost`; append `:6379` in Go when no port is present)
- `DISHA_REDIS_PASSWORD` (default `""`)
- `REDIS_DB` (default `0`)

### 7.3 Tests

Use `github.com/alicebob/miniredis/v2` (add as dev dep) to spin up an in-memory Redis. Test:
- `GetConversationData` returns parsed struct when key exists with valid JSON.
- `GetConversationData` returns "not found" error when key missing.
- `GetConversationData` returns "malformed" error when value is invalid JSON.
- `AppendChunk` writes a JSON entry to the LIST. Verify with `miniredis.LRange`.

---

## 8. Phase 3 — Disha HTTP API client

### 8.1 `disha/api_client.go`

```go
package disha

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/http"
    "time"
)

type APIClient struct {
    baseURL    string
    httpClient *http.Client
    logger     *log.Logger
}

func NewAPIClient(baseURL string, timeout time.Duration, logger *log.Logger) *APIClient {
    return &APIClient{
        baseURL:    baseURL,
        httpClient: &http.Client{Timeout: timeout},
        logger:     logger,
    }
}

func (c *APIClient) UpdateConversation(ctx context.Context, req UpdateConversationRequest) error {
    return c.send(ctx, "PATCH", "/bot/update_conversation", req)
}

func (c *APIClient) RunPostCallOperations(ctx context.Context, req PostCallOperationsRequest) error {
    return c.send(ctx, "POST", "/bot/run_post_call_operations", req)
}

func (c *APIClient) EnqueueJob(ctx context.Context, req EnqueueJobRequest) error {
    return c.send(ctx, "POST", "/common/enqueue_job", req)
}

func (c *APIClient) send(ctx context.Context, method, path string, body any) error {
    payload, err := json.Marshal(body)
    if err != nil { return fmt.Errorf("marshal: %w", err) }

    httpReq, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(payload))
    if err != nil { return fmt.Errorf("new request: %w", err) }
    httpReq.Header.Set("Content-Type", "application/json")
    // Match Disha's Python VoiceBotAPIService today: no auth header.

    resp, err := c.httpClient.Do(httpReq)
    if err != nil { return fmt.Errorf("disha API %s %s: %w", method, path, err) }
    defer resp.Body.Close()

    if resp.StatusCode >= 400 {
        b, _ := io.ReadAll(resp.Body)
        return fmt.Errorf("disha API %s %s returned %d: %s", method, path, resp.StatusCode, string(b))
    }
    return nil
}
```

Env vars for `main.go`:
- `DISHA_API_URL` (e.g. `http://localhost:8000`)

### 8.2 Tests

Use `httptest.NewServer` to assert on the captured request bodies for each endpoint. Verify:
- Method, path, and Content-Type.
- Authorization header should be absent.
- Body is the expected JSON shape.
- Non-2xx response returns error.
- Context cancellation respected.

---

## 9. Phase 4 — Disha bot interface + call event callbacks

### 9.1 Common Disha helpers

The Disha package keeps `sales_call.go` as the sales-call entry point, but the reusable behavior lives in common helpers:

- `disha/bot.go`: shared `Deps`, `Bot` interface, `NewBot(botType)`, and `NewBotTask`. The connect boundary can use this for any Disha bot type without knowing sales-call details.
- `disha/call_startup.go`: `collectCallStartup` loads `conversation_data`, validates the expected bot type, resolves user ID, creates the call logger/room name, and returns `CallStartup`. `initialMessagesFromChunks` is common chunk-to-LLM-context conversion.
- `disha/call_event_callbacks.go`: `CallEventCallbacks` is the shared concrete callback set. It exposes `Events()` to adapt itself to `voicepipelinecore.CallEvents`, updates conversation lifecycle fields, appends committed turns to Redis, runs post-call operations, maps end reasons, and enqueues `/common/enqueue_job` with the current bot type.

These methods are intentionally not sales-call-specific because other Disha bot types need the same Redis/API lifecycle behavior.

### 9.2 `disha.CallEventCallbacks`

`disha/call_event_callbacks.go` implements the shared callback surface:
- `OnBotJoined`, `OnUserJoined`, `OnUserFirstSpeech`, and `OnBotFirstSpeech` call `PATCH /bot/update_conversation`.
- `OnFirstUserAudio` is currently a no-op because first-audio timing is sent in the post-call stats payload.
- `OnUserTurnCommitted` and `OnAssistantTurnCommitted` append Disha `ConversationChunk` JSON to `conversation_chunks:{user_id}:{conversation_id}`. Assistant chunks include per-turn metrics when present.
- `OnCallEnded` runs post-call operations and then enqueues `/common/enqueue_job` for Redis-to-Postgres chunk sync.

The Redis append is synchronous inside the call-event dispatcher worker. That is intentional: the dispatcher preserves FIFO order for committed-turn writes while keeping the pipeline itself non-blocking. Each Redis write uses a bounded 5s timeout.

### 9.3 Sales-call bot implementation

The implemented split mirrors the shape of Disha's Python `sales_call.py`: the main sales-call entry point is one file that implements the common `Bot` interface and orchestrates common helpers.

- `disha/sales_call.go`: defines `SalesCallBot`, implements `BotType()` and `BuildOptions(...)`, calls common startup collection, applies sales-call-specific prompt and talktime rules, creates common `CallEventCallbacks`, and returns `voicepipelinecore.TaskOptions`.
- `disha/call_startup.go`: common startup data and prior-chunk-to-message conversion.
- `disha/call_event_callbacks.go`: common callback implementation for lifecycle updates, turn persistence, post-call operations, and chunk sync.

Keep this split: `sales_call.go` should read as the main bot setup, while reusable startup and callback behavior stays in common files.

### 9.4 Tests

Implemented integration test in `disha/sales_call_test.go`:
- Stand up `miniredis` and `httptest` servers.
- Seed `conversation_data:test-id` in miniredis with a valid blob.
- Mock the `talk-go/voicepipelinecore` boundary: you don't need to start a real pipeline. The test calls `SalesCallBot{}.BuildOptions`, then synthesizes calling the callbacks (`OnBotJoined`, `OnUserTurnCommitted`, `OnCallEnded`) and asserts:
  - `httptest` server received `PATCH /bot/update_conversation` with the right body.
  - Redis LIST `conversation_chunks:<uid>:test-id` has the right RPUSHed entries (use miniredis `LRange`).
  - `httptest` server received `POST /bot/run_post_call_operations` and `POST /common/enqueue_job` with the right bodies, in that order.

`SalesCallBot{}.BuildOptions` is testable without spinning up LiveKit. `NewBotTask` is the common helper that calls a bot's `BuildOptions` and then `voicepipelinecore.NewTask`.

---

## 10. Phase 5 — Wire `/connect` to the Disha bot boundary

### 10.1 New `/connect` handler

```go
type connectRequest struct {
    ConversationID string `json:"conversation_id"`
}

func handleConnect(w http.ResponseWriter, r *http.Request) {
    var req connectRequest
    json.NewDecoder(r.Body).Decode(&req) // tolerate empty body for dev flow

    var task *voicepipelinecore.PipelineTask
    var err error

    if req.ConversationID != "" {
        bot := disha.SalesCallBot{}
        task, err = disha.NewBotTask(r.Context(), bot, req.ConversationID, dishaDeps)
    } else {
        // Dev/test flow: use the hardcoded prompt, no Disha integration.
        task, err = voicepipelinecore.NewTask(context.Background(), voicepipelinecore.TaskOptions{
            Logger:          newRoomLogger(""),
            InitialMessages: []voicepipelinecore.Message{{Role: "system", Content: disha.SalesCallSystemPrompt}},
        })
    }
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadGateway)
        return
    }

    task.OnCleanup = func() {
        sessionsMu.Lock(); delete(sessions, task.RoomName); sessionsMu.Unlock()
    }
    sessionsMu.Lock(); sessions[task.RoomName] = task; sessionsMu.Unlock()
    task.Start()

    token, err := voicepipelinecore.GenerateToken(task.RoomName, "web-user")
    if err != nil { http.Error(w, err.Error(), 500); return }
    json.NewEncoder(w).Encode(map[string]string{
        "server_url": os.Getenv("LIVEKIT_URL"),
        "token":      token,
        "room_name":  task.RoomName,
    })
}
```

### 10.2 Wiring `dishaDeps` in `main.go`

```go
var dishaDeps disha.Deps

func main() {
    loadEnv(".env")
    // ... existing logger setup ...

    dishaDeps = disha.Deps{
        Logger: log.Default(),
        Redis:  disha.NewRedisClient(os.Getenv("DISHA_REDIS_URL"), os.Getenv("DISHA_REDIS_PASSWORD"), 0, log.Default()),
        API:    disha.NewAPIClient(os.Getenv("DISHA_API_URL"), 10*time.Second, log.Default()),
    }
    defer dishaDeps.Redis.Close()

    // ... HTTP handlers ...
}
```

### 10.3 Phase 5 verification

- `go build && go run .`
- Manual smoke: with no Redis running, `curl -XPOST http://localhost:3000/connect -d '{}'` should still work (dev path).
- With Redis running and a seeded `conversation_data:test-id` key, `curl -XPOST http://localhost:3000/connect -d '{"conversation_id":"test-id"}'` should return a token and start the bot.
- Check Disha-side logs to see `PATCH /bot/update_conversation` arriving with `bot_joined_at` and `user_first_speech_at` (if a real LiveKit call is made).

---

## 11. Phase 6 — End-to-end live test

Once phases 0–5 are done:

1. Spin up Disha backend locally pointing to its dev Redis + Postgres.
2. Seed a `conversation_data:<id>` key via Disha (call its existing orchestrator flow, OR write a small `cmd/seed/main.go` that mirrors the Python seeding logic).
3. Run `talk-go` with `DISHA_API_URL=http://localhost:8000`, `DISHA_REDIS_URL=localhost`, and `DISHA_REDIS_PASSWORD` if needed.
4. From the browser, hit `/connect` with the conversation_id.
5. Have a 30-second conversation.
6. End the call with the frontend disconnect button or by leaving the LiveKit room; control uses LiveKit DataPackets, not a custom WebSocket.
7. Verify in Disha's database:
   - `CallConversation.bot_joined_at`, `user_joined_at`, `user_first_speech_at`, `bot_first_speech_at` populated.
   - `CallConversation.post_call_operations_started_at` populated.
   - `CallConversation.total_user_duration` populated.
   - `conversation_chunks:<uid>:<id>` in Redis populated, then drained to Postgres by Disha's SQS job within a few seconds.
   - Final `ConversationChunk` rows in Postgres match the call's turns.

---

## 12. Phase 7 — Cleanup

- Remove any stale debug logging added during development.
- Run `go vet ./...` and `golangci-lint run` (if configured).
- Run `go test -race -count=3 ./...` until all green.
- Update AGENTS.md (see §14) if not already updated in phase 0.
- **Tell the user the work is ready for commit-by-phase review.** Do not commit anything yourself.

---

## 13. Phase boundaries — stop and report after each

After each of these phases, stop work, run `go test -race -count=2 ./...`, summarize what changed, and wait for the user before continuing:

- **End of phase 0**: module renamed, package split done, all existing tests pass, browser dev flow still works. *Stop.*
- **End of phase 1 + 1.5**: TaskOptions + FIFO call events + committed-turn callbacks + per-turn metrics aggregation merged. Tests pass. No behavior change visible. *Stop.*
- **End of phase 2**: Disha types + Redis client merged, with tests. *Stop.*
- **End of phase 3**: Disha HTTP API client merged, with tests. *Stop.*
- **End of phase 4**: Disha bot interface + call event callbacks merged, with tests. *Stop.*
- **End of phase 5**: `/connect` accepts conversation_id, end-to-end ready for live test. *Stop.*
- **End of phase 6**: live test passed (or any failures + fixes documented). *Stop.*

Do not chain phases. Phase boundaries are review points.

---

## 14. AGENTS.md update (add this section near the top)

Add this block to AGENTS.md right after the "How to Collaborate" section:

```markdown
## Architectural rule: core library vs. business integration

The `voicepipelinecore/` package is the call/pipeline framework. It must
remain free of business logic: no Redis, no HTTP, no conversation IDs,
no prompts, no end-reason policies, no persistence. Treat it as an
internal library.

All business integrations (Disha persistence, post-call API calls,
prompt injection, etc.) live in their own package (`disha/`, future:
`onboarding/`, `followup/`) and hook into the core via:

  1. `voicepipelinecore.TaskOptions.CallEvents` (`OnBotJoined`,
     `OnUserFirstSpeech`, `OnUserTurnCommitted`,
     `OnAssistantTurnCommitted`, `OnCallEnded`, ...) — for lifecycle
     moments and committed-turn data hand-off. The dispatcher is
     FIFO-ordered, bounded, panic-safe, and explicitly drained during
     cleanup.
  2. `TaskOptions.InitialMessages` — for prompt + prior-context
     injection.

If you find yourself adding `if conversationID != ""` or
`import ".../disha"` inside `voicepipelinecore/`, stop and add a
callback instead.

Mirrors Pipecat's pattern: `sales_call.py` wires `@event_handler`
decorators onto core processors; the core never imports the call type.
```

Also update the file map and pipeline diagram in AGENTS.md to reflect the new package boundary.

---

## 15. Things the user explicitly said NO to (do not implement)

- No Postgres reads or writes from Go anywhere.
- No fallback to Postgres if Redis is cold — return 502.
- No RTVI logs port. Send `log_data_s3_key=""` with a TODO for future RTVI/S3 work only if needed.
- No Langfuse-managed prompt fetch. Hardcoded sales prompt in `disha/sales_call.go` only.
- No bot types other than `sales_call`.
- No pod lifecycle / GKE / scheduling. We assume the caller (Disha orchestrator) handles that and just hands us a conversation_id.
- No Daily metrics. We use LiveKit.
- No "resume from arbitrary chunk" UI. Only `resumed_chunk` from `conversation_data` if Disha set it.

---

## 16. Open items that need user clarification mid-flight

Stop and ask the user when you hit any of these:

1. **Disha API base URL** — confirm with user what they want us to default to in `.env.example` (local dev: `http://localhost:8000`; staging: `https://...`; prod: `https://...`).
2. **If Disha adds API auth later**, update `disha.APIClient` to send the agreed header only when the token is configured. Current `VoiceBotAPIService` sends no auth header, so Go sends none by default.
3. **Sales call system prompt** — user approved skipping Langfuse/Grok for now and hardcoding the prompt/model config. Still confirm exact prompt text before production rollout if copy changes.
4. **Resumed chunk handling** — if `ConversationData.ResumedChunk` is set, no extra handling is planned. Interpret: the resumed chunk's text is already in `Chunks[]`, so `buildInitialMessages` naturally includes it.

---

## 17. Quick code-navigation cheatsheet for the executing agent

When you need to find something:

| Need | Where |
|------|-------|
| Current /connect handler | `main.go:35-51` |
| Current sessions map | `pipeline.go:76-91` |
| Current `createSession` | `pipeline.go:93-155` |
| `PipelineTask.End` | `pipeline.go:157-175` |
| `PipelineTask.completeEnd` (cleanup) | `pipeline.go:177-215` |
| `TaskContext` struct | `pipeline.go:52-67` |
| `BaseProcessor` + Processor interface | `processor.go` |
| `Frame` interface + `FrameBase` + constructors | `frame.go` |
| Hardcoded sales prompt to move | `context_aggregator.go:135-138` |
| Min-word barge-in constant | `context_aggregator.go:8` |
| `addUserMessage` (where to emit OnUserTurnCommitted) | `context_aggregator.go:120-130` |
| `commitSpokenText` (where to emit OnAssistantTurnCommitted) | `context_aggregator.go:102-111` |
| `submitUserMessage` (where to fire OnUserFirstSpeech) | `context_aggregator.go:132-146` |
| `PlaybackSink` Broadcast for BotStartedSpeaking (where to fire OnBotFirstSpeech) | `playback_sink_processor.go:222` |
| AudioSource read loop (where to fire OnFirstUserAudio) | `audio_source_processor.go` |
| LiveKit Connect callsite (where to fire OnBotJoined) | `livekit_room.go` |
| Disha sales_call cleanup (reference) | `/Users/jaideepsingh/Projects/disha-backend/bots/sales_call/sales_call.py:413-487` |
| Disha bot_session_manager (conversation_data writer) | `/Users/jaideepsingh/Projects/disha-backend/bots/bot_session_manager.py:380-419` |
| Disha conversation_persistence (chunk schema) | `/Users/jaideepsingh/Projects/disha-backend/bots/onboarding_call/conversation_persistence_processor.py:113-132` |
| Disha Redis chunks cache | `/Users/jaideepsingh/Projects/disha-backend/services/conversation_chunks_cache.py` |
| Disha API request models | `/Users/jaideepsingh/Projects/disha-backend/bots/views.py:51-92` |
| Disha API handlers | `/Users/jaideepsingh/Projects/disha-backend/bots/views.py:575-585` |
| Disha URL routes | `/Users/jaideepsingh/Projects/disha-backend/bots/urls.py` |

---

## 18. Test corpus

After the work is done, ensure these test files exist and pass:

- `voicepipelinecore/*_test.go` — all existing tests should continue passing.
- `voicepipelinecore/pipeline_task_test.go` — exercises call-ended dispatch and typed `EndReason` cleanup.
- `voicepipelinecore/call_events_test.go` — exercises FIFO call-event dispatch, stop-and-drain, and panic recovery.
- `voicepipelinecore/call_stats_tracker_test.go` — new, exercises duration tracking.
- `disha/redis_client_test.go` — new, miniredis-based.
- `disha/api_client_test.go` — new, httptest-based.
- `disha/sales_call_test.go` — integration test of `SalesCallBot.BuildOptions` + call event callbacks against miniredis + httptest.

Target: `go test -race -count=3 ./...` clean.

---

## 19. Done criteria

You are done when:
1. `voicepipelinecore` package exists, contains all the core pipeline code, has zero imports of `disha/`, `redis/`, or any HTTP client.
2. `disha` package exists, contains Redis client, HTTP client, the bot interface, sales-call bot implementation, and call event callbacks.
3. `main.go` wires both packages, owns the sessions map, handles the dev fallback path.
4. AGENTS.md updated with the core/integration rule.
5. All tests pass under `-race -count=3`.
6. A live call with a real Disha conversation_id (phase 6) results in Postgres rows being created server-side via Disha's existing SQS pipeline.
7. **Nothing is committed.** Working tree changes ready for user review.

Good luck.
