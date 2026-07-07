# talk-go — Project Context

## About the User

Jaideep is an experienced Python backend engineer learning Go by building real projects. He understands concurrency concepts (threading, asyncio) but is new to Go syntax, channels, goroutines, and Go idioms. He maps Go concepts to Python equivalents to build mental models.

## How to Collaborate

- **Guide step by step, don't dump full code.** Explain the "why" alongside small code snippets. Let him write the code himself unless he explicitly asks you to make changes directly.
- **When he says "make the changes directly" or similar, do it.** Otherwise default to guiding.
- **Explain Go concepts by mapping to Python equivalents** (e.g., channels = queue.Queue, goroutines = lightweight threads, defer = try/finally, interfaces = duck typing but explicit).
- **Keep responses concise.** He can read code — don't summarize what you just did.
- **Don't over-engineer.** He will push back if something is unnecessarily complex. Prefer the simplest solution first.
- **Do not commit without review.** Do not create commits or push branches unless Jaideep has explicitly reviewed the current changes or explicitly asks to commit/push in that moment.
- **When debugging, check `app.log` first** — all `log.*` output goes there. `fmt.Print*` goes to stdout/terminal. Logs go to both `app.log` and terminal via `io.MultiWriter`.
- **When a recent staging/prod attempt is not in local `app.log`, check GCP Cloud Logging with `gcloud` next.** Use the Go worker deployment labels before concluding there are no logs: staging is project `curelinkai`, cluster `disha-voice-worker-staging`, location `us-east1`, namespace `staging`, app/deployment `disha-go-voice-worker-staging`; prod is cluster `disha-voice-worker-prod`, location `us-east4`, namespace `prod`, app/deployment `disha-go-voice-worker-prod`. Start with focused filters for the conversation id plus `STT lazy connect latency`, `STT websocket activation requested`, `STT websocket connected`, `Daily participant`, `LiveKit participant`, `EndFrame`, and `ErrorFrame`.
- **For Sentry debugging, load `SENTRY_AUTH_TOKEN` from `/Users/jaideepsingh/Projects/disha-backend/.env` when it is not already set.** Never print or persist the token value; only use it locally for read-only Sentry API calls.
- **After major design decisions or discussions, always update AGENTS.md.** Don't wait to be asked — if a design pattern, architecture choice, or strategy was decided in conversation, persist it immediately.
- **Use Pipecat as the reference for core calling-system details before implementing.** For call lifecycle, turn-taking, interruption, transport events, idle handling, frame semantics, or shutdown behavior, inspect the local Pipecat source first, mention the relevant Pipecat file/pattern in the reasoning, then implement the simplest equivalent in this Go codebase. Prefer `/Users/jaideepsingh/Projects/disha-backend/.venv/lib/python3.11/site-packages/pipecat` (`pipecat-ai==0.0.108`) and Disha's existing bot code in `/Users/jaideepsingh/Projects/disha-backend/bots` before falling back to public docs.
- **Discuss design before non-trivial architecture changes.** For pipeline, frame semantics, context aggregation, callback surfaces, transport protocol handling, persistence boundaries, or cross-package dependencies, stop and present the proposed design before implementing. The design note should state the existing pattern, the minimal change, alternatives considered, why the chosen approach fits, and the exact tests to add.
- **Do not blindly add frame types or broad abstractions.** New frames, `TaskContext` fields, callback fields, long-lived struct dependencies, or shared helpers are architectural changes, not tactical fixes. First prove the behavior cannot fit an existing frame/callback/config/method-level parameter. If a new frame or shared surface is still proposed, discuss it explicitly and wait for approval.

## Disha Integration Decisions

- **Onboarding-call port (in progress):** the migration plan lives in `reports/onboarding-call-migration-plan.html`. **Status (2026-07-07): phases 0–2 are implemented and committed on branch `onboarding-call-port`; next is phase 3 (OnboardingCallBot skeleton).** Phase 0 artifacts: the real staging `student_test` config (model group `grok-4.1-fast`) and all its stage prompts are checked in as fixtures under `disha/testdata/onboarding/`; enqueue callables for the stage-transition timing log (`llm_logging.stage_transition_log_service` / `StageTransitionLogService._save_to_db`, queue `llm-logs-parallel`) and agenda analytics (`users.managers.user_analytics_manager` / `UserAnalyticsManager.track_event_job`, queue `p1-fast-l1`) are verified unbound-safe staticmethods; the fuzzy matcher will be a hand-port of rapidfuzz `token_set_ratio`/`partial_ratio` (Go fuzzywuzzy ports are difflib-based and score-diverge; Python's `test_stage_transition_fuzzy_matcher.py` is the parity corpus). Phase 1 shipped `disha/onboarding_config.go` (config structs + Python-exact stage resolution), `disha/onboarding_state.go` (mutexed `ConversationState`), `SetUserCareplanWithFallback`/`AddTagToUserWithFallback`, `NewUSBucketJSONUploaderFromEnv`, `UserProfileData.OnboardingCallVariant`, and the `grok-4.1-fast` llmrouter group. Phase 2 shipped the approved core surfaces (`ReplaceSystemMessage`, `OnLLMCallCompleted`) and the hedged one-shot client (see the hedged-execution paragraph below). Decided up front: the Go onboarding bot uses the **stage-transition tracker architecture for all variants** (background fuzzy match on the assistant's latest utterance + LLM classifier on "maybe"), and the Python `update_stage` tool-call path is **not ported** — no `update_stage` tool, no enum refresh, no "hmm..." filler / `run_llm` handler logic, and no `StageThresholdProcessor` (that processor only polices the tool-call architecture). The LLM's only tool is the existing `end_call`. **Approved core surfaces:** `ContextAggregatorPair.ReplaceSystemMessage` (mutex-guarded on the shared-state lock) and `CallEvents.OnLLMCallCompleted(text, interrupted)` fired from `LLMProcessor` (tracker input = generated response, not played text). **Hedged background-LLM execution is in scope with Python-exact semantics** (verified against disha-backend `services/llm_failover_service.py` `generate_with_hedged_request`): a reusable `llmrouter` primitive over a fixed primary/hedge endpoint pair (Cerebras then OpenRouter-throughput, `config_list[0]`/`[1]` — not health-ranked) for "background call must respond fast" cases — deep thinking first. Semantics: 1000ms whole-call hedge threshold; primary error before threshold → immediate sequential hedge; threshold hit → hedge fired in parallel with the primary NOT cancelled; first successful finisher wins and the slower attempt is then ctx-cancelled; first finisher failing → await the other. Slow losers are never blacklisted; attempts completing with transient errors blacklist per Python's classification. Logging is per-attempt through the standard LogSink (both calls logged when both complete; cancelled-mid-flight attempts never log). Parent-ctx cancellation cancels both and is never an endpoint error. Exposed behind the same one-shot interface `get_guidance` uses; **decided: `get_guidance` adopts it later, after it soaks on deep thinking** — until that explicit switch the "no in-call failover for get_guidance" decision stays in force. **Go interface shape (decided 2026-07-07, `reports/d4-hedged-llm-design-note.md`, implemented in phase 2): reuse the existing `Router` per attempt instead of a second streaming client.** `llmrouter.Config` gained `FixedEndpoint` (pin one endpoint config key; bypasses health-ranked selection and disables ALL poll triggers, blacklist write-back kept; `Group` ignored) and per-client `Temperature`/`MaxTokens` overrides (deep thinking: 0.7/4000). `NewHedged(HedgedConfig{Pair, Redis, Logger, HTTPClient, LogSink, PromptMetadata, HedgeThreshold, Temperature, MaxTokens})` returns a thin racing wrapper over two fixed-endpoint Routers that satisfies `voicepipelinecore.LLMClient`; each attempt buffers tokens privately and the winner's full text is replayed to `onToken` (background consumers only concatenate). The pair registry key is `llmrouter.GroupGPTOSS120FastHedged` (`"gpt-oss120-fast-hedged"`): primary `cerebras_gpt_oss_120b`, hedge `openrouter_gpt_oss_120b_throughput` — a dedicated endpoint config carrying OpenRouter `provider.sort=throughput`, keyed separately from `openrouter_gpt_oss_120b` so hedge blacklists land on the config that misbehaved (deliberate tiny delta from Python's shared target key). Cancelled-attempt log suppression is a `LogSink` wrapper that drops `Interrupted` entries — the router's own logging (including live-call interrupted logging) is unchanged. Other closed decisions: trigger statements can be assumed present in every stage prompt's config_json (missing → Sentry + skip, never crash); idle prompt-cap delta accepted (Go 7 vs Python end-on-6th, no config); Daily cloud recording is started backend-side at room creation (no bot work); model-group keys get a final check against disha-backend `services/openai_config_manager.py` just before release; `post_plan_call` is out of scope; dev/QA targets the `student_test` variant config only for now (no phase-0 live-config audit — remaining variant configs are validated in the pre-release sweep). Tracker-source transitions persist only the agenda-change debug chunk (`additional_data.tool_call_id = stage_transition_tracker_{uuid}`), not the assistant/tool chunk pair. Go must write per-chunk `conversation_state_s3_key` (US bucket `conversation_state/{conversation_id}/{chunk_id}.json`) or Disha's `bot_session_manager` silently disables resume for Go-origin onboarding calls.
- **Core vs. business integration:** the `voicepipelinecore/` package is the call/pipeline framework and must stay free of business logic: no Redis, HTTP clients, conversation IDs, prompts, Disha end-reason policies, or persistence. Business packages such as `disha/` assemble the pipeline themselves (`SalesCallBot.BuildTask`) and hook in via `TaskConfig.CallEvents` plus direct processor-constructor params (initial messages, max talk-time, phonetic dict, the `LLMClient`). The `voicepipelinecore/llmrouter` sub-package is the one allowed exception: it may import `net/http`/`crypto` and read Redis/env, but the core package never imports it — the bot injects it. If core code starts needing `import ".../disha"` or `if conversationID != ""`, add a callback or constructor param instead.
- Use the existing Disha `POST /common/enqueue_job` endpoint for Disha background jobs. Payload shape is `module_name`, `func_name`, `kwargs`, `sqs_queue`; do not use or create `/bot/enqueue_job`. This covers Redis-to-Postgres chunk sync, GKE worker registration DB ops, and worker cleanup.
- Mirror Disha's current `VoiceBotAPIService` auth behavior: it sends no auth header. The Go Disha client should not send Authorization unless a token/header scheme is later added explicitly.
- Keep using the active room transport's signalling channel for frontend UI events and inbound control messages: Daily app messages for Daily calls, reliable LiveKit data packets for LiveKit calls. Frontend events must be RTVI-format `rtvi-ai` entries; the same entries are buffered and uploaded as the S3 debug log at call end. Do not reintroduce a custom `/ws` route or a separate debug-log event stream.
- For local end-to-end testing, the browser page should exercise Disha's Create Room flow through the worker-compatible `/bot/create_worker_room` path. Direct talk-go `/connect` is removed; callers must go through the bot/worker task boundary with a `conversation_id` so the injected `llmrouter` path is used.
- For staging LiveKit smoke testing, `clients/livekit-client.html` is served by talk-go at `/livekit-client.html` plus `/LiveKitClient.html` and `/LifeKitClient.html`. It defaults to Disha staging `POST https://disha-staging.curelinktech.in/bot/create_fly_room`, accepts `create_url`, `user_id`, and `token` query-param overrides, sends `environment="staging"` and `bot_type="sales_call"`, sends optional dummy bearer auth, joins the returned LiveKit room, publishes mic audio, sends `end_call` over reliable data packets, and displays raw inbound RTVI/data events.
- For GKE/staging compatibility, expose Disha worker-compatible routes under `/bot`: `POST /bot/create_worker_room`, `GET /bot/has_active_session`, `GET /bot/health_check`, `GET /bot/pre_stop_check`, `GET /bot/readiness_check`, `POST /bot/mark_machine_reserved`, and `POST /bot/trigger_exit`. `create_worker_room` must return quickly and start the bot in the background because Disha forwards to the pod with a short timeout.
- LiveKit is enabled only for Disha `/bot/create_fly_room` staging sales calls. Disha keeps the external API contract as `room_url` + `token`; for LiveKit `room_url` is the LiveKit server URL, and the token carries the room grant. The internal worker payload includes common `room_name` for both Daily and LiveKit, and does not send `transport_type`; talk-go chooses Daily for Daily room URLs and LiveKit for non-Daily room URLs. `/bot/create_room` local testing stays Daily.
- On worker startup, if `HOSTNAME`, `POD_UID`, and `GKE_DEPLOYMENT_NAME` are set, register the pod like Disha's Python worker: check Redis key `registered_pod:{pod_name}:{pod_uid}`, enqueue `bots.gke_pod_manager.register_worker_pod_db_ops` on `fifo-p0-fast-l1` via `/common/enqueue_job`, then set the Redis key for 24h. Do not run a local SQS worker in Go.
- Disha backend worker registration should use the normal scaling-buffer schedule, but when the registering `app_name` matches the env-backed TalkGo `GKE_DEPLOYMENT_NAME`, halve the computed available-machine buffer before deciding whether a matching provisioning machine should be marked reserved.
- On worker call cleanup, clear the in-memory worker active/reserved flags and enqueue `bots.signal_handler.cleanup_state` on `p0-fast-l1` via `/common/enqueue_job` when `HOSTNAME` is available.
- On `SIGTERM`, mirror Disha's Python `bots/signal_handler.py`: handle the first signal only, write Redis key `pod_sigterm:{pod_name}:{pod_uid}` for 2h when `POD_UID` is available (legacy `pod_sigterm:{pod_name}` is only a no-UID fallback), and enqueue `bots.signal_handler.on_graceful_shutdown_initiated` on `fifo-p0-fast-l1` via `/common/enqueue_job` with both `pod_name` and `pod_uid`. Disha backend registration must trust only the UID-specific SIGTERM key when `pod_uid` is present; do not keep a legacy name-only registration skip branch. If the UID-specific key is present before the DB row exists, enqueue direct pod deletion because there may be no DB machine row yet. Active/reserved pods stay alive and Disha's background job/state manager decides whether cleanup should proceed.
- Deployment scripts must be runnable as plain commands with no caller-exported env vars and no inline `VAR=value ./script.sh` assumptions. Read deploy/runtime values from repo env files such as `.staging.env` or `.prod.env` inside the script, then pass them internally to `envsubst`, `kubectl`, Docker, etc. Do not ask Jaideep to export shell vars for deploys.
- The GKE deployment name is independent from the image repository. For staging/prod, run TalkGo as a new deployment in the existing voice-worker clusters using Disha-style deployment variables and store the image in the matching existing worker Artifact Registry repository. Staging uses `deploy-staging.sh`, `.staging.env`, `k8s/worker-staging.yaml`, `GKE_DEPLOYMENT_NAME=disha-go-voice-worker-staging`, `ARTIFACT_REPOSITORY_NAME=disha-voice-worker-staging`, cluster `disha-voice-worker-staging` in `us-east1`, and namespace `staging`. Prod uses `deploy-prod.sh`, `.prod.env`, `k8s/worker.yaml`, `GKE_DEPLOYMENT_NAME=disha-go-voice-worker-prod`, `ARTIFACT_REPOSITORY_NAME=disha-voice-worker-prod`, cluster `disha-voice-worker-prod` in `us-east4`, and namespace `prod`; `deploy-prod.sh` must refuse obvious staging-looking API/Redis/bucket/repository/cluster/namespace values before touching the prod cluster. The manifests preserve Disha backend worker scheduling, affinity, KEDA, and PDB shape; prod `worker.yaml` uses the Disha prod worker resource posture, while staging keeps its current lower sizing. The prod KEDA Postgres scaler query mirrors Disha backend `k8s/worker.yaml`'s full scaling-buffer-schedule query before adding the deployment's active/reserved/provisioning worker count. Keep KEDA/PDB object names deployment-scoped so this worker does not overwrite Disha's worker scaler/PDB in the same namespace. The Disha API base URL can come from either `DISHA_API_URL` or Disha's existing `API_BASE_URL`.
- Disha's current DB enum only supports `talktime_exhausted` and `user_idle`. Internal client-disconnect reasons should map to `null`/omitted `end_reason` in `run_post_call_operations`.
- Normal Python sales-call cleanup does not set `end_type`; keep TalkGo `end_type` untouched/null. Only set `end_reason` for the same Python cases: `user_idle` during idle shutdown and `talktime_exhausted` during talk-time exhaustion. Client disconnect stays null.
- Match Python RTVI debug-log shape for sales calls. `voicepipelinecore.UIEventSender` is the single RTVI event stream: it publishes each `rtvi-ai` entry over Daily app messages and buffers the same entries. Core only returns those entries through `CallStats.DebugLogs`; it must not know whether they are uploaded or where. Bot-specific Disha code decides whether to upload them by passing a `DebugLogUploader` into the common `CallEventCallbacks`. For sales calls, `sales_call.go` wires an uploader for `debug_log_data/{conversation_id}/log_data.json` through `disha/s3_uploader.go` using `ACCESS_KEY_ID`, `SECRET_KEY_ID`, `AWS_MAIN_REGION`, and `AWS_BUCKET_NAME`; the default `OnCallEnded` uploads logs and passes the resulting object key to post-call operations. Do not pass debug-log stores through `disha.Deps`. Cover the Python-observed event types: `bot-transcription`, `user-transcription`, `bot-started-speaking`, `bot-stopped-speaking`, and `server-message` entries such as `llm_call_result`, metrics, interruptions, talk-time exhaustion, call-ended, and participant-left messages.
- Interim/live user transcription RTVI events are intentionally disabled to avoid high-frequency signalling/debug-log CPU overhead. The user-side `UserContextAggregator` still uses interim STT internally for barge-in and idle behavior, but only final committed user transcripts should emit `user-transcription` RTVI entries (`final=true`).
- After post-call operations, queue Daily metrics collection through `/common/enqueue_job` with `module_name="bots.webhooks"`, `func_name="fetch_and_store_daily_metrics"`, `sqs_queue="p1-fast-l1"`, and kwargs `conversation_id`, `meeting_id`, `bot_session_id`, and `user_session_id` when Daily session IDs are available.
- Sales prompts are fetched from the Redis-backed `disha.DocumentStore` (`document:{name}:{env}` keys Disha pre-renders from Langfuse), with the prompt name chosen by `campaign_pricing_experiment_flag`. `sales_call.go` reads prompt `config_json.tools` and registers the standard `end_call` tool when configured; the handler calls `PipelineTask.End(EndReasonUnspecified)` and returns `{"status":"call_ending"}` with no follow-up LLM run. talk-go does not call Langfuse directly. Cartesia stays a single hardcoded key/config by design for now: Python exposes a per-conversation Cartesia key field, but it is effectively a facade over one constant, so do not treat it as a parity gap unless Disha actually starts varying that key.
- `disha.DocumentStore` renders prompts through the persistent Python `jinja_renderer.py` subprocess using real Jinja2, not a Go Jinja-compatible library. Keep this renderer in `disha/` and keep rendered prompt text as the boundary passed into `voicepipelinecore`; do not move prompt rendering into core. Tests can inject a lightweight renderer, but production prompt rendering should preserve Python `documents.document_manager` semantics.
- Follow-up calls are integrated through `disha.FollowUpBot` with `bot_type="follow_up"` and share the existing Disha pipeline/callback boundary. By decision, Go follow-up uses Soniox STT only; do not port Disha's Smallest STT variant wiring unless explicitly requested later, and do not read `call_stt_variant_flag`. Regular follow-up prompts come from `DocumentStore` using Disha's agenda mapping (`d1_inactive_checkin`, `d1_inactive_checkin_weight_loss_pcos`, default `followup_call/system_prompt`), with the investor-demo phone override using the `gpt-4.1` model group and normal follow-up using `gemini-flash-3.1-lite`; all follow-up variants read prompt `config_json.tools` and register known tools such as `end_call` when configured. Dynamic check-in follow-up loads `compiled_call_flow_s3_key` from `AWS_BUCKET_NAME`, renders `disha_init_calls/dynamic_checkin_call/main_sys`, uses `get_guidance`/`end_call` tools from the prompt config, uses one-endpoint/specialized `llmrouter` model groups instead of a separate static client, and handles `end_call` by gracefully calling `PipelineTask.End`, matching Pipecat's `EndTaskFrame -> EndFrame` shutdown pattern. Follow-up wires `voicepipelinecore.LLMOutputFilterProcessor` between LLM and TTS to mirror Disha backend `bots.llm_output_filter.LLMOutputFilter`; keep it as a narrow stream filter for known leaked LLM artifacts, not a frame-semantics change. Follow-up uses the same default `UserIdleProcessor` behavior as sales calls. Parity decisions: the `gemini-flash-3.1-lite` endpoint configs carry `Temperature: 0.5` (Python's follow-up `InputParams(temperature=0.5)`) and the group declares `FallbackGroup: gpt-4.1` (Python's `FALLBACK_MODEL_GROUP` applies to every group); `patient_schedule` extracts `checkin_slots` from `ideal_call_time_slots` before falling back to the whole dict, like `fetch_conversation.py`. The `gpt-oss120-fast` guidance group is health-selected, not failover-ordered: Disha's poller polls `cerebras_gpt_oss_120b`/`openrouter_gpt_oss_120b` (added to `MODEL_GROUPS["gpt-oss120-fast"]` in disha-backend `services/openai_config_manager.py` — enum/config keys must match talk-go's), so Cerebras wins when healthy. By decision there is no in-call failover for `get_guidance`; instead a failed/empty guidance LLM call is captured to Sentry (`disha_followup`/`get_guidance_llm`), except on context cancellation. `buildResumeSystemMessage` (shared by sales and follow-up) emits a resume nudge only when `resume_gracefully` is explicitly true, matching Python's `if conversation.resume_gracefully:` — False/missing mean an explicit-chunkId rebuild (`bot_session_manager` sets `resume_gracefully = not resumed_from_specific_chunk`), where the bot must continue silently with no resume instruction.
- Follow-up prompt context follows Disha backend PR 1449: use cached `user_profile.patient_executive_profile` as the patient memory/profile value, pass `user_profile.active_chat_context` separately as `active_chat_context`, format `current_datetime` as IST with AM/PM and no seconds, and do not append `unprocessed_chat_context` into follow-up patient memory.
- Prompt profile values come from cached `user_profile.patient_executive_profile` only. Keep existing prompt variable names stable until prompts migrate: sales still fills `short_term_memory` from `patient_executive_profile`, while follow-up fills `patient_executive_profile` and the Python-compatible `patient_memory` variable from the same cached profile. Do not merge `unprocessed_chat_context` into profile prompt variables.
- Follow-up calls are integrated through `disha.FollowUpBot` with `bot_type="follow_up"` and share the existing Disha pipeline/callback boundary. By decision, Go follow-up uses Soniox STT only; do not port Disha's Smallest STT variant wiring unless explicitly requested later, and do not read `call_stt_variant_flag`. Regular follow-up prompts come from `DocumentStore` using Disha's agenda mapping (`d1_inactive_checkin`, `d1_inactive_checkin_weight_loss_pcos`, default `followup_call/system_prompt`), with the investor-demo phone override using the `gpt-4.1` model group and normal follow-up using `gemini-flash-3.1-lite`; all follow-up variants read prompt `config_json.tools` and register known tools such as `end_call` when configured. Dynamic check-in follow-up loads `compiled_call_flow_s3_key` from `AWS_BUCKET_NAME`, renders `disha_init_calls/dynamic_checkin_call/main_sys`, uses `get_guidance`/`end_call` tools from the prompt config, uses one-endpoint/specialized `llmrouter` model groups instead of a separate static client, and handles `end_call` by gracefully calling `PipelineTask.End`, matching Pipecat's `EndTaskFrame -> EndFrame` shutdown pattern. Follow-up wires `voicepipelinecore.LLMResponseTimeoutProcessor` after LLM and before `voicepipelinecore.LLMOutputFilterProcessor`; the output filter remains between timeout and TTS to mirror Disha backend `bots.llm_output_filter.LLMOutputFilter`; keep these as narrow stream processors for known timeout/filter behavior, not frame-semantics changes. Follow-up uses the same default `UserIdleProcessor` behavior as sales calls. Parity decisions: the `gemini-flash-3.1-lite` endpoint configs carry `Temperature: 0.5` (Python's follow-up `InputParams(temperature=0.5)`) and the group declares `FallbackGroup: gpt-4.1` (Python's `FALLBACK_MODEL_GROUP` applies to every group); `patient_schedule` extracts `checkin_slots` from `ideal_call_time_slots` before falling back to the whole dict, like `fetch_conversation.py`. The `gpt-oss120-fast` guidance group is health-selected, not failover-ordered: Disha's poller polls `cerebras_gpt_oss_120b`/`openrouter_gpt_oss_120b` (added to `MODEL_GROUPS["gpt-oss120-fast"]` in disha-backend `services/openai_config_manager.py` — enum/config keys must match talk-go's), so Cerebras wins when healthy. By decision there is no in-call failover for `get_guidance`; instead a failed/empty guidance LLM call is captured to Sentry (`disha_followup`/`get_guidance_llm`), except on context cancellation. `buildResumeSystemMessage` (shared by sales and follow-up) emits a resume nudge only when `resume_gracefully` is explicitly true, matching Python's `if conversation.resume_gracefully:` — False/missing mean an explicit-chunkId rebuild (`bot_session_manager` sets `resume_gracefully = not resumed_from_specific_chunk`), where the bot must continue silently with no resume instruction.
- For sales-call LLMs ending with incomplete sentences while reporting `finish_reason=stop`, fix the prompt/context instructions first. Do not add core/TTS sentence-completion heuristics, frame types, or replay filters unless logs prove the transport, TTS, or turn lifecycle actually corrupted a complete model response.
- Sales and follow-up calls wire `voicepipelinecore.LLMResponseTimeoutProcessor` after `LLMProcessor` and before TTS-side processors to mirror Disha backend `bots/onboarding_call/llm_response_timeout_processor.py`: start/reset a 7s timer on `LLMResponseStartFrame`, cancel on `LLMResponseEndFrame` or `InterruptFrame`, and on timeout broadcast `InterruptFrame` plus push `LLMMessagesAppendFrame(nil, true)` upstream as the existing Go equivalent of Pipecat `LLMRunFrame`. Do not add a separate retry frame type for this.
- **Live LLM resilience (model switching) is ported to Go in `voicepipelinecore/llmrouter`** — this supersedes the earlier "skip Grok/Azure model wiring; use hardcoded model config" note. The router selects the fastest healthy endpoint in a model group (normally `grok-4.1-fast-sales`, falling back to the `gpt-4.1` group) by reading the same `live_call_modal_health:{config}` Redis keys Disha's Python poller writes, blacklists endpoints that error during a live call (read-modify-write of the health key), and triggers a re-poll on error/slow(>3s)/fallback-recovery (guarded by the `poll_openai_models_lock:{group}` SETNX lock). Current local sales wiring intentionally passes `gpt-4.1` as the model group for temporary evaluation; Jaideep plans to revert it later, so do not flag that as an accidental parity regression. All providers (OpenAI, Azure, Grok, Vertex, OpenRouter, Google AI Studio) are spoken in OpenAI Chat-Completions format over plain `net/http` — **no provider SDKs**. Auth: Bearer for OpenAI/Grok/OpenRouter/Vertex; Azure uses its deployment-path API (`{endpoint}/openai/deployments/{model}/chat/completions?api-version=2024-02-01`) with the `api-key` header (mirrors Python's `AsyncAzureOpenAI`). **Per-provider request extras mirror Python's `get_extra_params`** (`extraBodyFor`): only OpenRouter Grok and the Gemini providers carry tuning fields — the Azure-hosted `grok` provider and Vertex grok send **no** `reasoning` field (a top-level `reasoning` 400s those endpoints, which was a real bug). **Temperature is part of the model config (the `endpointConfig` registry), not passed in by the caller** — it defaults to 0 (sales) and a config can override it. The router also captures `finish_reason`, stream usage token counts, and logs a warning on an abnormal/truncated stream close. **The model group is the only thing the caller passes**; everything else the router reads itself: `Redis` (the shared connection) and an optional `LogSink` are injected, but the poll-trigger URL and Vertex creds come from the environment (`disha.Deps` carries no LLM-router config). The group/endpoint registry lives in `llmrouter`.
- **Polling stays in Python.** `common.tasks.poll_openai_models` keeps writing endpoint health to Redis. Because it runs in an SQS-driven US Lambda (`DishaUSBackendWorkerFunction`) and `/common/enqueue_job` only reaches the `ap-south-1` queues, talk-go triggers an on-demand poll via a **public Lambda Function URL** on that US worker (`AuthType: NONE`, no secret — the body is plain JSON `{model_group, region, reason}`). The HTTP handler runs `poll_openai_models` inline in US and returns `200` when it completes. Configure talk-go with `LLM_POLL_TRIGGER_URL`. talk-go gates the trigger behind the `poll_openai_models_lock:{group}` SETNX lock and calls it fire-and-forget (in a goroutine), so the live call never waits on it.
- **Adding a new endpoint or model group to `llmrouter/groups.go` requires a matching disha-backend change.** Python is the only thing that polls endpoints and writes `live_call_modal_health:{config}` to Redis — talk-go only reads those keys. A health-selected endpoint that exists only on the Go side never gets health data, so selection silently skips it forever (and a group unknown to Python makes the poll trigger a no-op). Whenever an endpoint config or group is added here, add the matching `OpenAIModels` enum value, `OPEN_AI_MODEL_CONFIG` entry, and `MODEL_GROUPS` membership in disha-backend `services/openai_config_manager.py` (polled by `common.tasks.poll_openai_models`), with identical key strings. Exception: fixed-endpoint configs used only by the hedged one-shot path (e.g. `openrouter_gpt_oss_120b_throughput`) bypass health selection entirely, so Python-side polling for them is optional — but any config that appears in a health-selected group's `Configs` list must be polled.
- **Vertex grok over pure HTTP:** the Vertex endpoint (the most stable, a first-class member of the sales group selected by health like any other) is reached via its OpenAI-compatible URL with a Bearer token minted from a Google service-account key using only stdlib crypto (RS256-signed JWT → `oauth2.googleapis.com/token`, cached ~55m). The SA key is fetched from S3 exactly like Python's `_load_s3_json_from_env`: `VERTEX_DISHAAI_CREDS_FILE` is the S3 object key, downloaded from `AWS_BUCKET_NAME` using `ACCESS_KEY_ID`/`SECRET_KEY_ID`/`AWS_MAIN_REGION` (llmrouter has its own minimal SigV4 S3 GET so it stays independent of `disha`). The token source is process-wide and loads the key lazily on first use. A missing/bad key is non-fatal: Vertex calls fail and get blacklisted, and selection falls back to other endpoints.
- **Live-error behavior is strict Python parity:** on a live LLM error the current turn fails (no in-turn retry); the endpoint is blacklisted and the next turn's fresh selection avoids it. `ctx` cancellation (barge-in/EndFrame) counts as an interruption, not an endpoint error — it is never blacklisted.
- **LLM call logging parity** reuses Disha's existing `llm_logging_service` behavior through a module-level job wrapper: the router exposes an optional `LogSink` callback; `disha`'s `newLLMLogSink` fills it by enqueuing `services.llm_logging_service` / `log_llm_call_job` through the existing `/common/enqueue_job`. Do not enqueue bound singleton methods such as `llm_logging_service.log_llm_call_async`; Disha's SQS serialization stores `__qualname__`, loses `self`, and workers retry the unbound class method. The Python wrapper should call `llm_logging_service.log_llm_call(...)` so S3 upload + DB-save enqueue complete inside the job. The sink is **not bot-specific** — the bot passes a `usecaseType` (e.g. `sales_call_conversation`) to differentiate call types. It is best-effort and size-guarded (skipped if kwargs exceed ~200KB to respect the SQS limit); prompt/completion token counts come from OpenAI-format stream usage chunks requested with `stream_options.include_usage`.
- For LLM log `prompt_metadata`, Vago should pass only document-store prompt identity fields: `system_prompt_name`, `system_prompt_version`, `system_prompt_variables`, `user_prompt_name`, `user_prompt_version`, and `user_prompt_variables`. This is a strict requirement for every LLM call added or modified in this codebase: if a system or user prompt came from `DocumentStore`, the corresponding name, resolved version, and exact variables used to render it must be set before the call is logged. Do not pass a prompt name without its matching variables; do not rely on Disha's central logger to backfill versions for prompts Vago already resolved; and do not drop variables just because they are large or already present in the rendered prompt. Do not put business/tool metadata such as `tool_name`, `call_flow_key`, or `compiled_call_flow_s3_key` in `prompt_metadata`. Keep pseudo prompts such as `raw_situation` as name+variables only so Disha's logger skips version lookup.
- **`voicepipelinecore` stays Redis-free.** The core defines an `LLMClient` interface on `LLMProcessor`; callers must inject an implementation through `NewLLMProcessorWithClient` (Disha uses `llmrouter`). Tool calling is native to this existing LLM path: do not add a separate tool processor or parallel tool-specific LLM client. `LLMProcessor` owns streaming/tool execution, `ContextAggregatorPair` owns shared conversation history: the user-side aggregator owns user/tool updates before LLM, and the assistant-side aggregator owns played assistant text after PlaybackSink. Bot packages such as `disha` register concrete tool handlers. Match Pipecat's tool semantics: start one goroutine per tool call, emit one assistant `tool_calls` message plus one `tool` result message per in-progress tool call, and let only the final outstanding tool result trigger the next LLM run. Completed tool result pairs are exposed through the call-event boundary and Disha persists them as Redis conversation chunks using the existing Python onboarding shape: assistant chunks carry `additional_data.tool_calls`, and tool chunks carry `additional_data.tool_call_id`. Resume reconstruction must also mirror onboarding: after decoding chunks, place each matching `tool` message immediately after its assistant `tool_calls` message, because storage order can return same-timestamp tool chunks before assistant chunks. `llmrouter` is a pipeline sub-package that MAY import `net/http`/`crypto` and depends on a narrow `RedisStore` interface (satisfied structurally by `disha.RedisClient`); the core package never imports `llmrouter`. The `llm_call_result` RTVI event is emitted by `LLMProcessor` (with the real selected model), not by the pipeline metrics handler.
- Keep Disha sales-call setup shaped like Python `bots/sales_call/sales_call.py`: `sales_call.go` implements the common `disha.Bot` interface as `SalesCallBot` and is the single sales-call entry point for startup collection, initial context, talk time, sales-call debug-log upload, common call event callbacks, and assembling the `voicepipelinecore` pipeline (`BuildTask` constructs the processors and `SetPipeline`s them onto a `NewPipelineTask`). Shared behavior lives in common helpers: `call_startup.go` loads conversation data and builds messages from chunks, and `call_event_callbacks.go` handles lifecycle callbacks, turn persistence, post-call operations, and chunk-sync enqueue.
- Read remaining sales talk time from `conversation_data.user_profile.remaining_sales_call_talktime_seconds`; default to Disha's lifetime limit of 600 seconds when missing/null. A zero value must mean immediate exhaustion, so the core talk-time override should support explicit zero (e.g. pointer option).
- When there are no prior chunks, seed initial messages with the hardcoded system prompt plus `user: "hello?"`, matching Disha's Python sales call.
- **Greet-first (bot speaks first) is always on, not configurable.** On user-join, `daily_room.go` pushes a Pipecat-style `LLMMessagesAppendFrame{Messages: nil, RunLLM: true}` once (guarded by `greetOnce`) via the audio source; the user-side `UserContextAggregator` consumes it, appends any messages, and (when `RunLLM`) emits an `LLMMessagesFrame` for the current initial context so the bot takes the first turn. Mirrors Python's `on_client_connected → task.queue_frames([context_frame])`. There is no `KickoffFrame`, no `TaskConfig`/`TaskContext` greet hook — `LLMMessagesAppendFrame` is the general "append messages and/or run the LLM" frame (also usable to inject a mid-call system nudge).
- **120s no-activity watchdog lives inside `UserIdleProcessor`, not the pipeline/task.** `runCancelWatchdog` (a `Go`-tracked goroutine started in the processor's `Start`) ends the call via `EndTask(EndReasonUserIdle)` after `cancelOnIdleTimeout` (120s) with no activity; it's reset by the same frames the processor already handles — `TranscriptFrame` (user speech / interim), `BotStartedSpeakingFrame`, `BotStoppedSpeakingFrame` — through an internal non-blocking channel. Mirrors Pipecat's `PipelineTask(cancel_on_idle_timeout=True, idle_timeout_secs=120)`. It's a backstop: the 7s idle-prompt loop normally ends a quiet call first (its prompts count as bot activity and reset this timer).
- Use Disha Redis env names: `DISHA_REDIS_URL` and `DISHA_REDIS_PASSWORD`.
- `CallEvents` is the one callback surface for lifecycle events and committed turns. Its dispatcher owns a FIFO queue and must be stop-and-drained before `PipelineTask` waits on the shared `WaitGroup`; do not rely only on task context cancellation or cleanup can time out before chunk sync is enqueued.
- Sentry is initialized directly with `sentry-go` from `SENTRY_DSN`; report errors through the single generic `internal/sentryutil.Capture` helper. It uses tags plus `SetContext("details", ...)` rather than deprecated `SetExtra`, and stays generic (no Disha imports in core) while covering API, Redis, S3, Daily bridge, STT, signal-handler, worker-lifecycle, and abrupt-shutdown paths.
- Disha `update_conversation` and `run_post_call_operations` must use API-first, queue-on-failure behavior: call the HTTP endpoint, and if it fails enqueue `bots.operations.voice_bot_operations.update_conversation` or `bots.operations.voice_bot_operations.run_post_call_operations` through `/common/enqueue_job` on `p0-fast-l1`.
- Soniox connect attempts are capped at three with short backoff; after exhaustion the STT processor pushes a fatal `ErrorFrame` so the pipeline ends instead of holding a worker slot forever.
- Soniox user-join lazy connect is the default behavior: `STTProcessor` waits for activation before dialing Soniox. Daily/LiveKit user-join handlers queue an `STTConnectFrame{Reason:"user_joined", At: at}` into the existing audio-source pipeline path before the greet-first `LLMMessagesAppendFrame`; `STTProcessor` consumes that frame and opens the websocket. `AudioFrame` processing keeps a `first_audio_fallback` activation to avoid a stuck call if a transport misses user-join. Logs include `activation_at`, `activation_to_connected_ms`, `first_audio_to_connected_ms`, `preconnect_audio_observed`, and `queued_audio_frames_before_connect`.
- Daily bridge and LiveKit joins retry three times before failing, matching Disha's Python transport retry pattern. Inbound RTVI `client-message` pings must receive RTVI `server-response` pong messages over the active room transport's signalling channel.
- Sales-call chunks must carry `main_agent_system_prompt_langfuse_key` using `disha.PromptKey(promptName, promptVersion)` for both user and assistant committed turns.
- Conversation chunk latency values are persisted in seconds. The Redis JSON keys still use Disha's legacy `_ms` names (`llm_ttfb_ms`, `tts_ttfb_ms`, `v2v_latency_ms`, `text_aggregation_ms`), so convert the core `TurnMetrics` millisecond values at the Disha chunk boundary instead of changing core metric units or renaming fields.
- Do not promote one-flow values into broad task context, shared deps, long-lived callback struct fields, or config structs just because a later method needs them. For narrow values like the sales-call prompt key, pass it directly into the processor-pair constructor (`NewContextAggregatorPair(taskCtx, initialMessages, promptKey)`); the aggregators supply it at the committed-turn boundary and `CallEventCallbacks.Events()` stays a plain default callback mapping.
- Sales-call initial context kickoff must mirror Python's shape without special startup frame types: Daily user-join pushes the existing Pipecat-style `LLMMessagesAppendFrame(nil, true)`, and the user-side `UserContextAggregator` consumes it by running the LLM on its current seeded context. Do not mutate core frame semantics or add a one-flow kickoff frame for sales-call startup.
- Daily RTVI protocol handling stays generic in `DailyRoom`: inbound RTVI `client-ready` app messages should receive a `bot-ready` response with the same id and protocol version `1.2.0`, mirroring Pipecat's `RTVIProcessor.set_bot_ready()` behavior.
- Known sales-call parity notes from the 2026-05-30 report: the temporary sales `gpt-4.1` model group and single Cartesia API key are intentional local decisions, not open parity gaps. LLM prompt/completion token counts are captured from stream usage when the provider emits them.

## Project Overview

A real-time voice assistant pipeline built in Go, using Daily for browser-based audio I/O:

```
Browser Mic → Daily Room → Daily Python Bridge → Go Bot PCM → Soniox STT → LLM (sales: temporarily GPT-4.1 via llmrouter; Grok sales group remains available) → Cartesia TTS → Go PCM → Daily Python Bridge → Daily Room → Browser Speaker
```

### Architecture

The Go server joins a Daily room as a "Chatbot" participant through `daily_bridge.py`, because Daily's supported headless media SDK is Python. A browser client (`clients/daily-client.html`) joins the same room. The bridge sends 16kHz mono PCM from Daily into Go, and Go sends 24kHz mono PCM back to the bridge for Daily playback. Soniox, OpenAI, Cartesia, turn-taking, lifecycle, persistence, and metrics stay in Go.

### Current State

The pipeline is **complete and working end-to-end** with the Pipecat-style `BaseProcessor` design (single shared `WaitGroup`, two-channel system-frame priority, cancel-and-recreate on interrupt, explicit per-frame direction, frame-level auto-ID + name), barge-in (min-word detection with both in-progress and at-bot-turn-end back-channel discard), word timestamp tracking, multi-session support, Soniox/Cartesia websocket reconnection, live UI over Daily app messages (no custom WebSocket), typed source-driven `EndTask` + `EndFrame` shutdown, `ErrorFrame` propagation, FIFO call events for lifecycle and committed turns, call stats tracking, user idle detection (with a 120s no-activity watchdog), talk-time enforcement, greet-first (bot speaks first on user-join), health-based LLM model switching (`llmrouter`), metrics framework, and a Pipecat-style context aggregator pair that separates conversation orchestration from LLM execution and commits assistant text after playback. Each worker room request creates an independent `PipelineTask`.

A full test suite covering every processor lives in `_test.go` files; `go test -race ./...` passes cleanly.

#### Pipeline files:

Core pipeline files live under `voicepipelinecore/` unless marked as root-level. Disha bot/business integration files live under `disha/` and must not be imported by `voicepipelinecore/`. Worker hosting/runtime code lives under `internal/worker/`; performance diagnostics and profiling setup live under `internal/perf/`.

| File | Purpose |
|------|---------|
| root `main.go` | Entry point. Loads `.env`, configures logging (both terminal + `app.log`), initializes Disha deps, constructs `internal/worker.Runtime`, starts perf diagnostics, and serves HTTP. Port comes from `PORT`, then `FAST_API_PORT`, default `3000`; GKE should run on `7860`. Bot sessions are launched through worker-compatible routes such as `POST /bot/create_worker_room`; `/` serves `clients/daily-client.html`. There is **no** custom WebSocket route — UI events ride Daily app messages (see `ui_events.go`) |
| `internal/worker/runtime.go` | Worker-host runtime boundary. Holds Disha deps, worker active/reserved state, shutdown state, and injected `TaskStarter`; root `main.go` injects `prepareTask` so worker hosting code never learns bot assembly internals |
| `internal/worker/api.go` | Disha GKE worker-compatible HTTP API. `POST /bot/create_worker_room` accepts `room_url`, `token`, optional `bot_token`, `conversation_id`, and `bot_worker_type`, marks the worker active, starts the task asynchronously, and returns `{"status":"success","room_url":...}`. Health/readiness/active-session/reservation/exit routes mirror the Python worker API shape |
| `internal/worker/lifecycle.go` | Worker lifecycle helpers. Worker registration checks/writes `registered_pod:{pod_name}:{pod_uid}` and enqueues `bots.gke_pod_manager.register_worker_pod_db_ops`; SIGTERM writes `pod_sigterm:{pod_name}:{pod_uid}` when available and enqueues `bots.signal_handler.on_graceful_shutdown_initiated`; call cleanup enqueues `bots.signal_handler.cleanup_state`; pod env/IP detection lives here. It accepts `disha.Deps` but is outside `disha/` because this is worker-host lifecycle, not bot business logic |
| `internal/worker/gke_pod.go` | In-cluster Kubernetes client (service-account token + ca.crt) that patches `cluster-autoscaler.kubernetes.io/safe-to-evict` to `false` on reserve/call-start and `true` on cleanup. No-ops outside Kubernetes and is injected through the narrow `disha.GKEPatcher` interface |
| `internal/worker/signal_handler.go`, `internal/worker/abrupt_shutdown.go` | Process signal and abrupt-shutdown handling for worker compatibility. `SIGTERM` writes a UID-aware `pod_sigterm` key and enqueues `bots.signal_handler.on_graceful_shutdown_initiated`; duplicate signals are ignored. `SIGINT` exits for local/dev parity, while `SIGTERM` leaves cleanup to Disha's state-manager job |
| `internal/perf/` | Performance diagnostics flag and Go Pyroscope setup. `perf.Enabled()` gates process-usage/audio-timing hot-path diagnostics; `perf.StartPyroscopeIfEnabled()` starts the Go profiler when required env is present |
| `disha/types.go` | Disha Redis/API payload structs: `conversation_data:{conversation_id}`, `conversation_chunks:{user_id}:{conversation_id}`, update-conversation, post-call, and `/common/enqueue_job` request bodies |
| `disha/redis_client.go` | Narrow Redis client for Disha integration. Reads `conversation_data:{conversation_id}`, appends JSON chunks to `conversation_chunks:{user_id}:{conversation_id}`, reads/writes generic cache keys for worker registration, normalizes `DISHA_REDIS_URL` host values, and retries Redis timeouts with Disha-style exponential backoff |
| `disha/api_client.go` | Disha HTTP client for `PATCH /bot/update_conversation`, `POST /bot/run_post_call_operations`, and `POST /common/enqueue_job`. It sends JSON with a 10s default timeout and no Authorization header, matching Disha's `VoiceBotAPIService` |
| `disha/bot.go` | Common Disha bot boundary: shared `Deps`, `Bot` interface, `NewBot(botType)`, and `NewBotTask` helper that turns any bot implementation into a `voicepipelinecore.PipelineTask` |
| `disha/call_startup.go` | Common startup helpers: load `conversation_data`, validate expected bot type, resolve user ID, build the call logger/room name, and convert prior chunk tuples into initial LLM messages |
| `disha/call_event_callbacks.go` | Common Disha callback implementation wired into `voicepipelinecore.CallEvents`: update-conversation lifecycle callbacks, committed-turn Redis chunk writes, optional bot-provided debug-log upload, post-call request with the resulting S3 key, Daily metrics enqueue, end-reason mapping, and `/common/enqueue_job` chunk-sync trigger |
| `disha/s3_uploader.go` | Minimal SigV4 S3 JSON uploader and shared debug-log upload helper used by bot implementations for Python-compatible RTVI debug logs. Uses the same Disha AWS env names and stores only the object key on the conversation via post-call operations |
| `disha/sales_call.go` | `SalesCallBot` implementation of the common `Bot` interface, shaped like Disha's Python `sales_call.py`: `plan()` collects startup data + sales prompt/talktime/phonetics + callbacks; `BuildTask()` joins the Daily room, constructs the processors (incl. the `llmrouter` LLM client and `salesUsecaseType` log sink) and assembles them via `NewPipeline`/`SetPipeline` |
| `disha/document_store.go` | Redis-only prompt fetcher: reads `document:{name}:{env}` (env from `ENVIRONMENT`), renders `{{var}}` substitutions, TTL-caches. Backs `loadSalesPrompt` |
| `disha/phonetic_dict.go` | Loads the phonetic-replacement JSON from S3 (`PHONETICS_DICT_KEY` in `AWS_US_BUCKET_NAME`) via SigV4 GET, exposes `Dictionary(ctx)`. The token→replacement map is passed to `NewTTSProcessor`; the filtering logic itself lives in core `phonetic_filter.go` |
| `disha/llm_logging.go` | `newLLMLogSink(api, logger, usecaseType, userID, conversationID)` — the router's `LogSink`. Enqueues `services.llm_logging_service`/`log_llm_call_job` via `/common/enqueue_job` (Python wrapper performs S3 upload + DB-save enqueue). Size-guarded, best-effort, usecase-tagged (not bot-specific) |
| `processor.go` | The Pipecat-style core: `Direction`, `Envelope{Frame, Direction}`, `Processor` interface, `BaseProcessor` struct, `BroadcastableFrame` interface. `BaseProcessor` owns the per-processor goroutine layout (inputLoop + processLoop), the two input channels (inputSysCh + inputDataCh) for structural system-frame priority, the procCh and per-interrupt procCtx, cancel-and-recreate on `InterruptFrame`, `EndFrame` pass-through without base auto-cancel, `PushFrame` with `MetricsFrame` intercept, `Broadcast`, `Go` (shared-WG tracking), `Link`/`Prev`/`Next` |
| `task_types.go` | Public integration types: `Message`, `EndReason` constants, `CallStats`, `CallEvents`, and `TurnMetrics` |
| `debug_logs.go` | RTVI debug-log entry type and timestamp helper. The actual event stream/buffer lives in `ui_events.go` so UI and S3 debug logs cannot diverge |
| `pipeline.go` | `Pipeline` (thin: just `Link` + `Start`/`Stop`), `TaskConfig` (Logger, SessionID, CallEvents, OnCleanup), `TaskContext` (Ctx, Logger, Room, UIEvents, Metrics, typed EndTask closure, callStats, shared `wg`, call-events/turn-metrics internals), `PipelineTask` (lifecycle owner: holds source, pipeline, cleanup hook, idempotency flags). `NewPipelineTask(ctx, TaskConfig)` builds the shared infra (no processors, no room); the bot constructs processors, assembles them with `NewPipeline`, and attaches via `SetPipeline`. Root `main.go`/`disha` own the sessions map and call `PipelineTask.Start()`. `completeEnd` dispatches its body to an untracked goroutine (`runCleanup`) so the sink's processLoop — which calls `completeEnd` via `onEnd` — isn't waiting for its own `Done()`. `runCleanup` disconnects the Daily bridge before the bounded 10s `wg.Wait`; it also stop-and-drains call events before `OnCallEnded`. On WG timeout, `captureGoroutineStacks` dumps every live goroutine for diagnosis |
| `pipeline_edges.go` | `PipelineSourceProcessor` (external `Queue(EndFrame)` injection point; `drainExternalFrames` goroutine cancels b.ctx after pushing EndFrame), `PipelineSinkProcessor` (invokes `onEnd` callback on EndFrame) |
| `frame.go` | `Frame` interface with `ID()`/`Name()` (from embedded `FrameBase`, populated by `NewXxxFrame` constructors for production sites; literals still allowed in tests with zero meta), `IsSystem()` (system priority routing) and `IsInterruptible()` (false for frames that must survive interrupt purge). `BroadcastableFrame` adds `Clone() Frame`. Concrete frames: `AudioFrame`, `TextFrame`, `TranscriptFrame`, `EndFrame`, `LLMResponseStartFrame` (carries `StartedAt`), `LLMResponseEndFrame`, `WordTimestampFrame`, `TTSDoneFrame`, `TTSSpeakFrame`, `BotStartedSpeakingFrame`/`BotStoppedSpeakingFrame` (both implement Clone), `LLMMessagesFrame`, `LLMMessagesAppendFrame` (Pipecat-style: append messages + optional `RunLLM`; drives greet-first), `ErrorFrame` (system, fatal flag, propagated via `PushError`). System frames include `InterruptFrame`, `ErrorFrame`, post-playback `WordTimestampFrame`/`TTSDoneFrame`, and bot speaking frames; `MetricsFrame` is system but intercepted by `BaseProcessor.PushFrame` |
| `metrics.go` | `MetricLabel` constants, `MetricsData` struct, `MetricsFrame` (intercepted by `BaseProcessor.PushFrame` → `taskCtx.Metrics`), `ProcessorMetrics` helper with `Start`/`StartAt`/`Stop`/`Reset` (thread-safe timer map), and `perTurnMetrics` snapshot/reset for assistant committed-turn callbacks |
| `call_events.go` | FIFO call-event dispatcher for once-only lifecycle callbacks (`OnBotJoined`, `OnUserJoined`, `OnFirstUserAudio`, `OnUserFirstSpeech`, `OnBotFirstSpeech`) and committed-turn callbacks (`OnUserTurnCommitted`, `OnAssistantTurnCommitted`). `runCleanup` stop-and-drains it before final `wg.Wait` so queued persistence finishes before call-ended work |
| `call_stats_tracker.go` | Tracks non-bot participant join/leave duration and the first audible user audio frame timestamp for `CallStats` |
| `ui_events.go` | `UIEventSender` — the single RTVI event stream for the call. It emits `RTVIDebugLogEntry{label:"rtvi-ai", type, data, timestamp}`, buffers entries for `CallStats.DebugLogs`, tracks Daily meeting/bot/user session IDs, and publishes the same entry through `DailyRoom.SendAppMessage` once the room is available |
| `daily_room.go` | `JoinDailyRoom(roomURL, token, taskCtx, audioSource)` — starts `daily_bridge.py`, waits for the bot to join Daily, relays bridge events, marks participant join/leave stats, queues `STTConnectFrame` plus the greet-first `LLMMessagesAppendFrame` on user join, handles inbound `{type:"end_call"}` app messages, sends UI app messages, writes outbound PCM, and disconnects the bridge during cleanup |
| root `daily_bridge.py` | Small Python media bridge around Daily's headless SDK. Joins an existing Daily room with the provided token, captures remote microphone PCM at 16kHz mono, accepts outbound 24kHz mono PCM over stdin, publishes it through a custom audio track, and forwards Daily app messages/events as newline-delimited JSON |
| `clients/daily-client.html` | Browser client: Pico CSS + Alpine.js. Dark theme. Posts to Disha backend `/bot/create_room` with a hardcoded local test user for sales-call testing, then joins the returned Daily room with `@daily-co/daily-js`. Consumes RTVI-format `rtvi-ai` Daily app messages for live transcript, committed turns, metrics badges, and call-ended teardown |
| `clients/livekit-client.html` | Browser LiveKit smoke-test client served at `/livekit-client.html`, `/LiveKitClient.html`, and `/LifeKitClient.html` |
| `reports/` | Report artifacts and markdown project notes. Keep root `AGENTS.md` in place for Codex instructions |
| `audio_source_processor.go` | Source processor. `PushPCM` is called by `DailyRoom` with 16kHz mono PCM, marks first audible user audio when any decoded sample has magnitude > 1000, then calls `PushFrame(AudioFrame, Downstream)` directly. No internal channel |
| `stt_processor.go` | Sends PCM to Soniox websocket, emits raw `TranscriptFrame`s with `ResponseID` and response-level `Finished` metadata. `Start` spawns `runReader`, which waits for an `STTConnectFrame` before dialing, with a first-audio fallback and latency logs. Writer goroutine waits on the `connected` signal channel before sending. Auto-reconnects. `sttDialURL` is a package var so tests can redirect it |
| `user_idle_processor.go` | Sits between STT and UserContextAggregator. Tracks user activity (`TranscriptFrame`) and bot completion (`BotStoppedSpeakingFrame`). Injects `TTSSpeakFrame` after `idleTimeout` (7s). Max `maxIdlePrompts` (7) prompts; the final fire speaks once more then `EndTask(EndReasonUserIdle)`. `idlePromptCount` is `atomic.Int32`. Also hosts the **120s no-activity watchdog** (`cancelOnIdleTimeout`, `runCancelWatchdog` started in an overridden `Start` via `Go`): reset by `TranscriptFrame`/`BotStarted`/`BotStopped` through an internal channel, ends the call on timeout — Pipecat's `cancel_on_idle_timeout` |
| `aggregator_pair.go` | `ContextAggregatorPair`, mirroring Pipecat's `LLMContextAggregatorPair`: constructs the user-side `UserContextAggregator` and assistant-side `AssistantContextAggregator` over one shared state object |
| `aggregator_shared_state.go` | Shared conversation history, prompt key, and initial-message validation used by both aggregators. Constructors require non-empty filtered initial messages; nil/empty/invalid initial context is captured to Sentry and panics rather than falling back to a default prompt |
| `message_helpers.go` | Shared `Message` normalization/cloning helpers used by the aggregators and LLM processor |
| `user_context_aggregator.go` | User-side `UserContextAggregator` before LLM. Owns final STT accumulation, interim snapshot, barge-in, user commits, tool-result context updates, `LLMMessagesAppendFrame`, and `LLMMessagesFrame` emission. Discards below-threshold speech during bot talking by **both** the in-progress branch (when `<end>` arrives while `botSpeaking` is still true) and the at-bot-turn-end branch (when `BotStoppedSpeakingFrame` arrives before a lagging `<end>`, resets interim+final transcripts if `!interruptSent` — mirrors Pipecat's `reset_aggregation`). Emits committed user-turn callbacks |
| `assistant_context_aggregator.go` | Assistant-side `AssistantContextAggregator` after PlaybackSink. Accumulates actually played `WordTimestampFrame` words and commits assistant messages on `BotStoppedSpeakingFrame`, `InterruptFrame`, or `EndFrame`, before sink-triggered cleanup can tear down the task. Emits committed assistant-turn callbacks |
| `talktime_monitoring_processor.go` | Sits between UserContextAggregator and LLM. `runTimer` is a Go-tracked goroutine started in `Start`; on timeout pushes `InterruptFrame` and `TTSSpeakFrame` downstream, then calls `taskCtx.EndTask(EndReasonTalkTimeExhausted)` to enqueue `EndFrame` at `PipelineSourceProcessor` (so upstream processors — STT, AudioSource, UserIdle, UserContextAggregator — also see it). `NewTalkTimeMonitoringProcessorWithMaxTalkTime` overrides the default (incl. explicit zero). `ending` is `atomic.Bool` (timer goroutine writes, `ProcessFrame` reads) |
| `llm_processor.go` | Lean: receives `LLMMessagesFrame` → spawns a Go-tracked `runLLM` goroutine that delegates the streaming call to an injected `LLMClient` (sales = `llmrouter`; tests = stubs/fakes). The processor owns `LLMResponseStart/End` frames, TTFB/processing metrics, cancel-on-Interrupt/End, tool execution, and `llm_call_result` RTVI events with the client's real model. There is no built-in direct OpenAI streamer and no default client; callers must inject an `LLMClient` through `NewLLMProcessorWithClient` |
| `llmrouter/` (sub-package) | Live-LLM resilience: model-group + endpoint registry (`groups.go`), Redis endpoint-health parse/selection + blacklist write-back (`health.go`, `selection.go`), per-provider OpenAI-format request building + SSE streaming (`providers.go`, `client.go`), Vertex OAuth token minting over stdlib crypto with the SA key lazily fetched from S3 (`vertex_token.go`, `s3.go`), lock-guarded re-poll trigger to the Python US endpoint (`poll_trigger.go`), and the best-effort `CallLog`/`LogSink` (`logging.go`). Implements `voicepipelinecore.LLMClient`; depends on a narrow `RedisStore` interface (satisfied by `disha.RedisClient`). The poll-trigger URL and Vertex creds path are read from env by the router itself; the core package never imports it — the bot wires only the group, Redis, and LogSink |
| `phonetic_filter.go` | Core phonetic-replacement filter applied to text before each Cartesia send (token→replacement map injected by the bot via `NewTTSProcessor`). Returning `""` drops a non-speakable fragment, matching Python's `PhoneticTextFilter` |
| `tts_processor.go` | Cartesia client + sentence aggregator + EndFrame drain controller. **Three goroutines per session:** `runReader` (lazy connect + websocket read, emits typed events on `ttsEvents`), `orchestrator` (owns ALL TTS state — aggregation, synthesis, shutdown — and drives Cartesia), and base input/process loops. `ProcessFrame` is a thin relay over a `commands` channel. EndFrame blocks `ProcessFrame` until orchestrator forwards it; this lets PlaybackSink/etc. shut down in pipeline order. After forwarding EndFrame, TTS closes the Cartesia connection and exits the orchestrator but does not stop the base processor queues until PipelineTask cleanup. Synthesis state is a single bool `cartesiaTextSent` ("does Cartesia owe us a `done` event?") — replaces the older 3-state enum and the older boolean pair. No explicit pending-end timer; the global `wg.Wait` 10s timeout in `completeEnd` is the ultimate escape hatch if Cartesia hangs on `done`. Context-id validation via `atomic.Value`. `ttsDialURL` is a package var; orchestrator waits on `connected` before processing commands |
| `playback_sink_processor.go` | Writes outbound PCM to the Daily bridge. **Three goroutines:** base input/process loops + a `runPlayback` goroutine with the 20ms ticker. `ProcessFrame` relays through `queueCh`; `runPlayback` owns `playbackQueue`, PCM conversion for TTS frames, the background mixer, and pacing. Fires `OnBotFirstSpeech` on the first played audio frame and `Broadcast`s `BotStartedSpeakingFrame`; sends played `WordTimestampFrame`/`TTSDoneFrame` downstream to `AssistantContextAggregator`, then broadcasts `BotStoppedSpeakingFrame` both directions after `TTSDoneFrame` (per Pipecat's MediaSender pattern). On `InterruptFrame` drops queued playback/reconciliation frames that have not left PlaybackSink yet, preserving only queued `EndFrame`, clears room audio, and forwards the interrupt downstream so `AssistantContextAggregator` owns the partial assistant commit for already-emitted played words. On `EndFrame` writes a short silence tail before forwarding (matches Pipecat's `audio_out_end_silence_secs`). Metrics (E2E latency) |

#### Testing infrastructure (Pipecat parity)

Ported from `pipecat/tests/utils.py`. Every processor has a `_test.go` file; helpers live in `helpers_test.go`:

| File | Purpose |
|------|---------|
| `helpers_test.go` | `testFixture` (TaskContext + Logger + RootCtx/RootCancel + shared WG + metrics capture), `QueueProcessor` (captures upstream OR downstream frames), `runProcessorTest(t, fix, cfg)` (wires source → processor under test → sink, sends frames, optionally queues EndFrame, force-Stops everyone after a settle window, waits on WG with bounded timeout), `SleepFrame` (synthetic; consumed inline by the send loop), `assertFrameTypes`, `findFrame[T]`, `countFrames[T]` |
| `test_setup_test.go` | `init()` that redirects `sttDialURL`/`ttsDialURL` to an unreachable loopback so background connect goroutines fail fast in tests |
| `processor_test.go` | BaseProcessor: basic forward, upstream forward, `EndFrame` does not auto-cancel b.ctx (verified via `trackedProcessor`), MetricsFrame intercept, system-frame priority (verified via `blockingProcessor`), InterruptFrame purges procCh keeping `!IsInterruptible()` frames (uses `waitForSeen` to deterministically order the interrupt before releasing the blocked frame), Link sets neighbors, Broadcast sends both directions |
| `pipeline_edges_test.go` | PipelineSource Queue() forwards downstream; PipelineSink onEnd callback fires on EndFrame and ignores other frames |
| `pipeline_task_test.go` | Minimal `PipelineTask.runCleanup` test for typed `OnCallEnded` reason + `CallStats` without joining Daily |
| `audio_source_processor_test.go` | EndFrame forwarding, default-forward for unknown frames, first-audible-user-audio call-event/call-stats marking |
| `stt_processor_test.go` | EndFrame forwarding, AudioFrame consumed (not forwarded), pass-through for unknown frames |
| `user_idle_processor_test.go` | Timer cancellation on TranscriptFrame, BotStarted/Stopped consumption, EndFrame cancels timer |
| `user_context_aggregator_test.go` | Final transcript → `LLMMessagesFrame`, initial-message seeding, required initial context panics, once-only user-first-speech call event, committed-turn call events with metric snapshot, barge-in emits InterruptFrame at min-words threshold, no barge-in below threshold, back-channel speech discarded both in-progress and at bot-turn-end (race-case fix), barge-in preserves accumulated transcript, `LLMMessagesAppendFrame` greet-first (run on initial context) + append-messages cases |
| `assistant_context_aggregator_test.go` | Played `WordTimestampFrame` commit on `BotStoppedSpeakingFrame`; interrupted played text commits on `InterruptFrame`; final played assistant text commits before `EndFrame` reaches the sink |
| `talktime_monitoring_processor_test.go` | Timer emits Interrupt+TTSSpeak+EndFrame on timeout; frames pass through before timeout; downstream frames dropped during shutdown |
| `llm_processor_test.go` | Streams TextFrames downstream (httptest stub); InterruptFrame cancels in-flight HTTP; EndFrame cancels in-flight; upstream frames pass through; server error handled gracefully |
| `llm_processor_stub_test.go` | Verifies `NewLLMProcessorWithClient` delegates streaming to an injected `LLMClient` and emits `llm_call_result` with the client's real model |
| `llmrouter/llmrouter_test.go` | Router tests with an in-memory `RedisStore`: fastest-endpoint selection, blacklist skip, gpt-4.1 group fallback, last-resort fallback key, region filtering, last-5 latency averaging, blacklist write-back preserving poll_runs, per-provider request shaping (Grok Bearer with **no** reasoning field, Azure api-key + deployment-path URL), `extraBodyFor` (OpenRouter grok gets reasoning, Azure/Vertex grok get nothing), error classification, full SSE stream + LogSink, error→blacklist+poll-trigger, and Vertex token mint+cache against a stub token server |
| `tts_processor_test.go` | InterruptFrame forwarded downstream immediately by ProcessFrame; upstream frames (Word/TTSDone/BotSpeaking) pass through |
| `disha/redis_client_test.go`, `disha/types_test.go`, `disha/api_client_test.go`, `disha/sales_call_test.go` | Disha package tests covering Redis parsing/writes, explicit `null` end-reason JSON, API method/path/body/header behavior, sales-call option assembly, turn persistence, post-call/enqueue ordering, non-2xx errors, and context cancellation |
| `internal/worker/*_test.go` | Worker API/lifecycle/signal tests for pod-registration env detection, active-session response shape, readiness behavior, create-worker-room validation, active-worker conflict handling, worker registration/cleanup enqueue jobs, and SIGTERM graceful-shutdown enqueue/idempotency |
| `internal/perf/perf_test.go` | Diagnostics flag tests for canonical `PERF_DIAGNOSTICS_ENABLED` behavior and legacy `PYROSCOPE_ENABLED` fallback |
| `metrics_turn_test.go` | Per-turn metric absorb/snapshot/reset |
| `call_events_test.go` | Call events are once-only where needed, FIFO-drained, and panic-safe |
| `call_stats_tracker_test.go` | Joined duration accumulation and first-audio timestamp idempotency |

Run: `go test -race ./...` (≈5s).

The harness pattern matches Pipecat exactly: wrap the processor between two QueueProcessors, send frames at the source, capture upstream-pushes at the source and downstream-pushes at the sink, then assert frame counts or types. The one Go-specific addition is `settleDelay` for asynchronous upstream pushes; isolated tests explicitly stop the test chain after expected frames have had time to propagate.

#### Pipeline wiring:
```go
PipelineSourceProcessor → AudioSourceProcessor → STTProcessor → UserIdleProcessor → UserContextAggregator(user) → TalkTimeMonitoringProcessor → LLMProcessor → LLMResponseTimeoutProcessor → TTSProcessor → PlaybackSinkProcessor → AssistantContextAggregator → PipelineSinkProcessor
```

### Processor Initialization Pattern

All processors embed `*BaseProcessor` and take `taskCtx *TaskContext` (plus, for some, a config param the bot supplies — initial messages, max talk-time, phonetic dict, `LLMClient`). No shared per-turn state — all cross-processor communication happens via frames.

```go
NewAudioSourceProcessor(taskCtx)
NewSTTProcessor(taskCtx)
NewUserIdleProcessor(taskCtx)
NewContextAggregatorPair(taskCtx, initialMessages, promptKey)
NewTalkTimeMonitoringProcessor(taskCtx)
NewLLMProcessorWithClient(taskCtx, llmClient)
NewTTSProcessor(taskCtx)
NewPlaybackSinkProcessor(taskCtx)
```

Inside each constructor:

```go
func NewXxx(taskCtx *TaskContext) *XxxProcessor {
    p := &XxxProcessor{...}
    p.BaseProcessor = NewBaseProcessor("Xxx", p, taskCtx)
    return p
}
```

`TaskContext` is the single task-level dependency containing `Ctx` (root context for the task), `Logger`, `Room`, `UIEvents`, `Metrics` (handler), typed `EndTask`, and `wg` (shared WaitGroup for goroutine tracking), plus unexported call-events/call-stats/turn-metrics helpers.

### Key Design Decisions

**Pipecat-style BaseProcessor (the core):** Every processor embeds `*BaseProcessor`. The base owns:
- Two input channels — `inputSysCh` (priority) and `inputDataCh` — and a `procCh` for data frames pending processing
- Per-processor cancellation tree: `taskCtx.Ctx → b.ctx → procCtx`. `b.ctx` is set in `NewBaseProcessor` (not `Start`) so it's immutable after construction — no race between `QueueFrame` readers and `Start` writers.
- Two goroutines per processor: `inputLoop` (drains input channels with system priority) and `processLoop` (drains procCh, invokes `ProcessFrame` one frame at a time). `processLoop` prioritises `ctx.Done` over `procCh` so an in-flight `ProcessFrame` returning due to cancellation can't accidentally pull another frame from procCh before exiting.
- `PushFrame(frame, dir)` routes to `prev`/`next` via interface dispatch. `MetricsFrame` is intercepted here and delivered to `taskCtx.Metrics` (never propagates).
- `Broadcast(BroadcastableFrame)` clones the frame and pushes both directions.
- `Go(fn)` adds the goroutine to `taskCtx.wg` (the shared task-wide WaitGroup) so `PipelineTask.completeEnd` can wait for everyone.
- On `InterruptFrame` (system frame, dispatched inline by `inputLoop`), the base cancels `procCtx` (stopping in-flight ProcessFrame work that respects ctx), waits up to `procLoopExitTimeout` (3s) for the processLoop goroutine to exit, drains `procCh` keeping frames where `IsInterruptible() == false`, then starts a fresh processLoop with a new `procCtx` and requeues the kept frames. This mirrors Pipecat's `_start_interruption → cancel-and-recreate-process-task` flow.
- `EndFrame` is not a per-processor cancellation signal in the base. It travels downstream in normal frame order and the task cleanup stops the whole pipeline after `EndFrame` reaches `PipelineSink`. This keeps post-playback processors, especially `AssistantContextAggregator`, alive long enough to flush played assistant text before cleanup.

**Single shared WaitGroup per PipelineTask:** `PipelineTask` owns `wg sync.WaitGroup`. `TaskContext.wg` is `&task.wg`. Every processor goroutine spawned via `BaseProcessor.Go` is tracked there. `PipelineTask.runCleanup` does `pipeline.Stop()`, disconnects the Daily bridge, stop-and-drains the call-event dispatcher, then `wg.Wait()` with a bounded 10s timeout. No per-processor WaitGroup, no central TaskManager.

**Direction is data, not type-inferred:** Channels carry `Envelope{Frame, Direction}`. `ProcessFrame(ctx, frame, dir)` receives direction as a parameter. Pass-through processors use a `default: PushFrame(frame, dir)` branch, eliminating the ~12 hard-coded upstream-forwarding cases that existed before the BaseProcessor migration.

**Pipecat-style pipeline edges:** External lifecycle frames such as `EndFrame` enter through `PipelineSourceProcessor.Queue(...)`, never by writing directly to another processor's channel. `PipelineSinkProcessor.onEnd` triggers `PipelineTask.completeEnd` when `EndFrame` reaches it. The source's external-frame goroutine cancels `b.ctx` after pushing an EndFrame so the base's input/process loops also unwind (they have no other shutdown signal because the EndFrame leaves via a side goroutine, not procCh).

**Pipeline.Run → Pipeline.Start/Stop:** No central `Send` closure, no positional routing. `Start(ctx)` links neighbours via `Link()` (which sets `prev`/`next` pointers bidirectionally) and starts each processor. `Stop()` calls each processor's `Stop()`, which sets `cancelling = true` and cancels `b.ctx`; the actual goroutine wait happens at the `PipelineTask` level.

**EndFrame-driven call shutdown:** Call end follows Pipecat's graceful `EndFrame` pattern. External handlers (browser `"end_call"` Daily app message, Daily participant leave, future transport lifecycle events) should call `PipelineTask.End(EndReason...)` only; they must not directly cancel the task, disconnect Daily, or remove the task from the sessions map. `EndFrame` is **not** a system frame and `BaseProcessor` does **not** auto-cancel processors when it passes through: it travels through the normal data path so processors see shutdown in pipeline order, and cleanup stops processors only after the frame reaches the sink. `PipelineTask.End()` stores the typed reason and queues its string form on `PipelineSourceProcessor`; TTS defers `EndFrame` while it is still generating the current utterance; `PlaybackSinkProcessor` drains queued playback, sends played word/completion frames downstream to `AssistantContextAggregator`, writes a short silence tail, then forwards `EndFrame` downstream (matches Pipecat's `BaseOutputTransport.MediaSender` pattern); `AssistantContextAggregator` commits any unflushed played assistant text before forwarding `EndFrame` to `PipelineSinkProcessor`; `PipelineSinkProcessor` invokes `PipelineTask.completeEnd`, which dispatches `runCleanup` to an untracked goroutine — `runCleanup` sends `call_ended`, calls `pipeline.Stop()`, disconnects the Daily bridge, stop-and-drains call events, waits on the shared WG (10s bounded), calls `OnCallEnded(reason, CallStats)`, cancels session ctx, and removes the task. Resource closures inside processors are allowed only as part of handling `EndFrame` or final cleanup.

**Client disconnect stops playback immediately:** `PlaybackSinkProcessor` should drop queued playback and skip the silence tail for `client_disconnected` and `error` end reasons. Reasons that intentionally speak a final message, such as `talktime_exhausted`, should still drain normally.

**Why `completeEnd` dispatches `runCleanup` to an untracked goroutine:** `completeEnd` is invoked from inside `PipelineSinkProcessor.ProcessFrame`, which runs on the sink's `processLoop` goroutine — a goroutine tracked by `t.wg`. The sink's `wg.Done()` is deferred and only fires when `ProcessFrame` returns. If `completeEnd` ran synchronously, its `wg.Wait` would block waiting for the very goroutine it was running inside, deadlocking until the 10s timeout. Dispatching the body to `go t.runCleanup(frame)` lets `ProcessFrame` return immediately so the sink's `Done()` fires and `wg.Wait` makes progress. `cleanupOnce` gates re-entry so multiple `completeEnd` calls run `runCleanup` exactly once.

**`IsInterruptible()` on every frame:** The Go-shaped equivalent of Pipecat's `UninterruptibleFrame` mixin. `EndFrame` is uninterruptible, and post-playback reconciliation frames (`WordTimestampFrame`, `TTSDoneFrame`, `BotStartedSpeakingFrame`, `BotStoppedSpeakingFrame`) are also system-priority/uninterruptible so an `InterruptFrame` cannot overtake or purge already-played assistant words after those frames have left PlaybackSink and are travelling toward `AssistantContextAggregator`. The base's `interruptProcessLoop` reads `IsInterruptible()` when draining `procCh`. `PlaybackSink.handleQueueFrame` deliberately does **not** use this policy for its internal `playbackQueue`: queued word/done frames there may still be future, unheard audio, so interruption drops them and preserves only queued `EndFrame`.

**ContextAggregatorPair owns conversation state:** Separated from LLM processor and split like Pipecat's `LLMContextAggregatorPair`. `UserContextAggregator` is the user side before LLM: it owns live transcript aggregation, final transcript accumulation, barge-in detection, user commits, tool-result message updates, and `LLMMessagesFrame` emission. `AssistantContextAggregator` is the assistant side after PlaybackSink: it accumulates played `WordTimestampFrame` words and commits assistant messages on `BotStoppedSpeakingFrame` or `EndFrame` before the sink can trigger cleanup. The pair shares messages and spoken words behind a mutex. Initial context is a required constructor input: nil, empty, or invalid initial messages are a configuration error captured to Sentry and panic instead of falling back to a default prompt. LLM processor is lean — just receives `LLMMessagesFrame`, calls the API, streams responses, handles cancellation.

**Call events stay outside frames:** `CallEvents` (passed via `TaskConfig`) exposes one callback surface for integration code: bot joined, user joined, first audible user audio, first final user speech, first bot speech, committed user/assistant turns, and call ended. Lifecycle events are once-only; committed-turn events can fire many times. The dispatcher is a single FIFO queue (cap 512), recovers panics, and is explicitly stop-and-drained during cleanup before `OnCallEnded`. This keeps Redis turn persistence ordered without introducing a second observer primitive. `OnCallEnded` runs during `runCleanup` after the dispatcher drain and `wg.Wait`, with `EndReason` plus `CallStats`.

**Call stats tracking:** `callStatsTracker` lives on `PipelineTask` and `TaskContext` as an unexported helper. Daily participant join/leave bridge events update user duration; `runCleanup` marks the user left at end if still present. `AudioSourceProcessor` marks `FirstUserAudioFrameAt` when it receives the first audible PCM frame (sample magnitude > 1000). `CallStats` currently includes total user duration seconds, first user audio timestamp, ended timestamp, Daily meeting/session IDs, and buffered RTVI debug-log entries.

**Min-word barge-in (Pipecat-style):** STT stays dumb and forwards raw Soniox token frames with response-level metadata. Soniox interim responses are transcript snapshots, not deltas: each non-final response repeats the current hypothesis, so UserContextAggregator replaces the previous non-final transcript with the latest response's non-final tokens. Final tokens are deltas and may arrive across multiple final response chunks before `<end>`, so UserContextAggregator accumulates final tokens until the end token instead of resetting by response ID. One shared `interimTranscript` drives both frontend live transcript updates and barge-in checks. Barge-in can fire on non-final interim frames once `len(strings.Fields(interimTranscript)) >= 3`, but turn-taking starts only on final tokens ending with `<end>`. Do not use Soniox `finished` for turn-taking; it is stream/connection lifecycle metadata, not an utterance boundary.

**Back-channel discard (two branches):** Below-threshold speech during bot talking is never submitted as a user turn — matches Pipecat's `MinWordsUserTurnStartStrategy._handle_transcription` calling `trigger_reset_aggregation` for every sub-threshold transcription. We implement this at two boundaries:

1. **In-progress branch (TranscriptFrame handler):** If `<end>` arrives while `botSpeaking == true && !interruptSent`, the accumulated final transcript is dropped before `submitUserMessage`.
2. **At-bot-turn-end branch (BotStoppedSpeakingFrame handler):** If the bot finishes a turn *before* the lagging `<end>` for the user's sub-threshold speech arrives — which is the common race because Soniox finalizes ~200-500ms after the last word — `interimTranscript` and `currentTranscript` are reset on upstream `BotStoppedSpeakingFrame` when `!interruptSent`. The late `<end>` then finds empty buffers and the submit path skips.

The `interruptSent` gate is what protects legitimate barge-in turns: when the user crossed the threshold and an `InterruptFrame` was sent, the bot-stopped reset is skipped so their accumulated transcript survives to the next `<end>`.

**BotStartedSpeakingFrame / BotStoppedSpeakingFrame are broadcast:** Emitted by `PlaybackSinkProcessor.runPlayback` via `b.Broadcast(...)` (both directions). Upstream copies reach UserIdle via the user-side `UserContextAggregator` (cancels idle timer, updates `botSpeaking` state); downstream copies reach `AssistantContextAggregator`, where `BotStoppedSpeakingFrame` flushes played assistant words. Mirrors Pipecat's `BaseOutputTransport.MediaSender._bot_started_speaking` / `_bot_stopped_speaking`. Broadcast sibling IDs are intentionally not implemented — they exist in Pipecat only to let frame observers deduplicate, while our integration hooks attach at semantic call-event boundaries instead.

**User idle detection:** UserIdleProcessor sits between STT and UserContextAggregator. Starts 7s timer on `BotStoppedSpeakingFrame`, cancels on `TranscriptFrame` or `BotStartedSpeakingFrame`. On timeout, injects `TTSSpeakFrame{Text: "Hello?"}` downstream from the timer goroutine via `PushFrame`. `idlePromptCount` is `atomic.Int32` (race fix from the BaseProcessor migration). Max 7 idle prompts; the final fire speaks once more then `EndTask(EndReasonUserIdle)`. The same processor also runs the **120s no-activity watchdog** (`runCancelWatchdog`), reset by user speech / interim transcript / bot speaking; on timeout it `EndTask(EndReasonUserIdle)` — Pipecat's `cancel_on_idle_timeout`. This is deliberately inside the processor (not the pipeline/task) since the processor already observes the activity frames.

**Greet-first (bot speaks first):** Always on. On user-join, `daily_room.go` pushes `LLMMessagesAppendFrame{RunLLM:true}` once via the audio source; `UserContextAggregator` runs the LLM on the initial context so the bot opens the call. Pipecat parity with `on_client_connected → queue_frames([context_frame])`. The frame is the general "append messages and/or run LLM" primitive (Pipecat's `LLMMessagesAppendFrame`); greet-first is the no-messages + `RunLLM` case.

**Talk-time limit:** `TalkTimeMonitoringProcessor` spawns a `runTimer` goroutine in `Start` (tracked via `Go`). On timeout pushes `InterruptFrame` → `TTSSpeakFrame{Text: "Your talk time is exhausted now. Ending the call."}` downstream, then calls `taskCtx.EndTask(EndReasonTalkTimeExhausted)` to enqueue an `EndFrame` at the pipeline source. `NewTalkTimeMonitoringProcessorWithMaxTalkTime` overrides the default when non-nil (including explicit zero). Routing the EndFrame through the source (rather than pushing it downstream directly) means upstream processors — STT, AudioSource, UserIdle, UserContextAggregator — also observe the lifecycle frame in pipeline order, matching Pipecat's source-driven shutdown. `ending` is `atomic.Bool` so `ProcessFrame` (on processLoop) can safely check while `runTimer` (separate goroutine) writes. The monitor does not wait for bot-speaking events; TTS and PlaybackSink preserve frame ordering by treating `EndFrame` as a graceful drain marker.

**Frame metadata (auto-ID + name):** Every concrete frame embeds `FrameBase` which carries `FrameMeta{ID int64; Name string}`. `NewXxxFrame` constructors auto-populate these (monotonic process-wide ID, type name like `"EndFrame"`). Production code uses constructors so logs can correlate the same frame as it traverses processors (`String()` returns `"EndFrame#42"`). Tests use struct literals freely; their frames carry zero meta which is harmless. Mirrors Pipecat's `name + id` pattern. The `Frame` interface includes `ID() int64` and `Name() string`.

**Error propagation:** `BaseProcessor.PushError(msg, fatal)` emits an `ErrorFrame{Processor, Err, Fatal}` upstream (system priority, survives interrupt purges). `PipelineSourceProcessor.ProcessFrame` logs every `ErrorFrame` that bubbles up; if `Fatal` is true, it calls `taskCtx.EndTask(EndReasonError)` to terminate the task gracefully. Non-fatal errors are diagnostic-only. Mirrors Pipecat's `FrameProcessor.push_error`: detection is local, policy lives at the source.

**TaskContext.EndTask:** Any processor can request graceful task shutdown by calling `taskCtx.EndTask(EndReason...)`. Wired in `NewPipelineTask` to `PipelineTask.End`, which queues an `EndFrame` on `PipelineSourceProcessor`. This is the only way processors should request shutdown — direct pushes of `EndFrame` downstream skip upstream processors and are bug-prone. The test fixture (`helpers_test.go`) wires `EndTask` to inject `EndFrame` at the test source so processors under test can call `EndTask` as they would in production.

**TTSSpeakFrame for canned utterances:** Bypasses LLM — goes directly to TTS for standalone synthesis. TTS creates a new Cartesia context, synthesizes the text, and forwards `TTSSpeakFrame` to PlaybackSink (which resets its `interrupted` state). Audio flows normally through word tracking and commit.

**InterruptFrame for barge-in:** UserContextAggregator detects barge-in (min-word threshold met while `botSpeaking`) and only sends `InterruptFrame` downstream; it does not commit assistant text. Each downstream processor receives the interrupt on `inputSysCh` (system priority), the base cancels its procCtx (killing any in-flight `ProcessFrame`), purges procCh, then calls the user's `ProcessFrame(InterruptFrame)` to do processor-specific cleanup. TTS cancels Cartesia context, LLM resets metrics, PlaybackSink clears playbackQueue and forwards the interrupt downstream, and AssistantContextAggregator commits the partial assistant text it has already accumulated from played `WordTimestampFrame`s. This mirrors Pipecat's `LLMAssistantAggregator._handle_interruptions`.

**Cartesia context-id validation (stale audio prevention):** TTS uses `atomic.Value` for `activeContextId`. The reader goroutine (`readTTSConnectionData`) validates every Cartesia message's `context_id` against the active value before pushing typed events to the orchestrator. The orchestrator re-checks the event context against `currentContextId` before mutating PCM/word state. On interrupt, `activeContextId` is set to `""` before queued TTS events are drained — all messages from the cancelled context are dropped or ignored as stale.

**TTS synthesis state (`cartesiaTextSent` bool):** A single bool answering "does Cartesia owe us a `done` event?". Set on `sendTextToTTS` success, cleared on the `done` event or interrupt. Replaces a 3-state `ttsSynth` enum (Idle/Streaming/Closing) and before that an `awaitingTTSDone`/`ttsFlushSent` boolean pair. `handleEnd` checks `cartesiaTextSent` to decide whether to defer EndFrame. The rare race where EndFrame arrives between `LLMResponseEndFrame`'s Reset and Cartesia's `done` fires a duplicate Reset; Cartesia replies with "Invalid context ID" which the reader already filters. There is no per-EndFrame timer — the global 10s `wg.Wait` in `completeEnd` is the upper bound if Cartesia hangs. When pending EndFrame is finally forwarded, close only the Cartesia side (`closeTTSConnection`), not the base processor; the base is stopped later by task cleanup after `AssistantContextAggregator` and `PipelineSink` have seen EndFrame.

**LLMResponseStartFrame carries timestamp:** `StartedAt time.Time` is set by LLM when the turn begins. PlaybackSink uses it for accurate E2E latency measurement (measures from LLM turn start to first audio frame played).

**Word timestamp interleaving in TTS:** TTS tracks `audioTimePushed` (seconds) and buffers `pendingWords` from Cartesia timestamp messages. After each 20ms PCM frame is pushed, words whose `start <= audioTimePushed` are emitted as `WordTimestampFrame`. This means words arrive at PlaybackSink in the correct playback order.

**Commit timing:** Assistant text is committed to conversation history after playback, not on generated LLM text. PlaybackSink emits played `WordTimestampFrame`s downstream to `AssistantContextAggregator`; `BotStoppedSpeakingFrame` commits a normal completed turn, `InterruptFrame` commits a partial interrupted turn, and `EndFrame` commits any remaining played words before it can reach `PipelineSink` and start cleanup. This matches Disha/Pipecat's pipeline shape where `main_context_aggregator.assistant()` sits after `transport.output()`, and avoids relying on post-playback completion frames climbing upstream after shutdown has started.

**User message concatenation:** If the user pauses mid-thought (Soniox fires `<end>`) and the LLM gets barged in before responding, the next user utterance is concatenated with the previous one in the messages array rather than creating separate entries.

**Metrics framework (Pipecat-style):** `ProcessorMetrics` helper with `Start`/`Stop` timer pairs. Processors emit `MetricsFrame` via `PushFrame` which intercepts and routes to `taskCtx.Metrics` (handler logs + emits RTVI `server-message` entries to the shared UI/S3 stream). Current metrics: LLM TTFB, LLM processing, TTS text aggregation, TTS TTFB, E2E turn latency. `perTurnMetrics` absorbs those frames and snapshots/resets them when an assistant turn is committed for `CallEvents.OnAssistantTurnCommitted`. LLM TTFB + processing also emits Python-compatible RTVI `server-message` with `type="llm_call_result"`.

**RTVI event stream over Daily app messages:** `UIEventSender` emits RTVI entries (`label:"rtvi-ai"`, `type`, `data`, `timestamp`) and appends them to the same in-memory slice returned through `CallStats.DebugLogs`. `Emit` publishes through `DailyRoom.SendAppMessage` via the Python bridge after `SetRoom`; events emitted before the room is wired are still retained in the snapshot. Core never uploads these logs; bot-specific integrations decide whether to upload them during `OnCallEnded`. Browser → bot control messages (today `{"type":"end_call"}`) flow through Daily app messages and are handled in `daily_room.go`.

**Lazy websocket connect (STT and TTS):** Connect calls were moved from constructors into Go-tracked goroutines spawned by `Start`. `runReader` dials in a retry loop, then signals `connected` (a chan struct{}) and proceeds with the read loop. STT's writer and TTS's orchestrator both wait on `connected` before sending. The retry sleep respects `b.ctx` / `taskCtx.Ctx` / `done` so `Stop` unblocks them promptly.

**Session cleanup triggers:** Several triggers route through `PipelineTask.End(reason)` (which queues an `EndFrame` at the pipeline source):

1. **Browser disconnect button** → sends `{type:"end_call"}` as a Daily app message → `daily_room.go` → `taskCtx.EndTask(EndReasonClientDisconnect)`.
2. **Browser tab close / network drop** → Daily participant-left bridge event fires for the non-bot participant → mark user left → `taskCtx.EndTask(EndReasonClientDisconnect)`.
3. **Talk-time exhaustion** → `TalkTimeMonitoringProcessor` calls `taskCtx.EndTask(EndReasonTalkTimeExhausted)`.
4. **User idle** → `UserIdleProcessor` after the prompt cap, or its 120s no-activity watchdog, calls `taskCtx.EndTask(EndReasonUserIdle)`.

Cleanup happens only when the queued `EndFrame` propagates through the pipeline and reaches PlaybackSink (which has its own drain + silence tail) and ultimately PipelineSink (which fires `onEnd → completeEnd → runCleanup`). `End()` is idempotent and ignores late requests once cleanup has started or the task context has been cancelled. The frontend waits for `call_ended` over Daily app messages before locally leaving the Daily room. The sessions registry map name is `sessions`, even though the type is `*PipelineTask` (the user-facing concept is still "a session").

**Cartesia TTS context strategy:** Single `context_id` per LLM turn. All sentences sent with `"continue": true` and `"add_timestamps": true`. Final flush with `"continue": false`. On interrupt, sends `{"cancel": true}`.

### Key Technical Details

- **Audio formats**: Soniox expects s16le/16kHz/mono. Daily inbound audio is 16kHz mono PCM from the bridge; LiveKit inbound audio is Opus decoded/resampled by `PCMRemoteTrack` to 16kHz mono PCM for Soniox. Daily outbound bot audio stays 24kHz mono PCM because the Python bridge `CustomAudioSource` is 24k. LiveKit outbound also requests Cartesia s16le/24kHz/mono and emits 20ms 24k PCM `AudioFrame`s, but publishes a lower-level Opus local track instead of `PCMLocalTrack`; `LiveKitRoom.WriteAudioPCM` owns the 20ms PCM->Opus encode via `gopkg.in/hraban/opus.v2` and writes encoded samples to LiveKit with a 48k RTP clock. The local track must advertise only `audio/opus` and let Pion bind the negotiated clock/channels/fmtp; do not hardcode `Channels: 1`, because WebRTC commonly negotiates Opus as `/48000/2` even for mono payloads. `ClearAudioBuffer` resets the Opus encoder state because there is no longer a LiveKit PCM queue. WebRTC audio tracks are still Opus; do not try to send raw PCM as a normal LiveKit audio track. Do not reintroduce an internal Opus encode/decode round trip unless the output boundary itself changes to require encoded media.
- **Pacing**: PCM frames must be sent to Daily at real-time pace (one 20ms frame per tick) or the browser jitter buffer overflows and drops audio.
- **Websocket idle timeouts**: Soniox and Cartesia close connections after ~15-20s of no data. Pipeline is initialized lazily on client connect.
- **Concurrent websocket writes**: gorilla/websocket panics on concurrent writes. Each ws has exactly one writer goroutine (STT's `writeAudioWebsocket`; TTS writes only from the orchestrator goroutine).
- **`completeEnd` ordering**: `completeEnd` returns immediately after `go t.runCleanup(...)`. Inside `runCleanup`: `pipeline.Stop()` → `Room.Disconnect()` → `callEvents.stopAndDrain()` → `wg.Wait(timeout=10s)` → `OnCallEnded(reason, stats)` → `task.Cancel()` → `OnCleanup`. `Room.Disconnect` comes BEFORE the WG wait because it stops the Daily bridge process and its media callbacks. On timeout, `captureGoroutineStacks()` dumps every live goroutine to the log for diagnosis.

### Frontend (`clients/daily-client.html`)

Single HTML file using Pico CSS (dark theme) + Alpine.js (reactivity). No build step.

- **Connect path:** The Connect button posts `{user_id, resume_enabled:true}` to `http://localhost:8000/bot/create_room` using the hardcoded local test user ID in `clients/daily-client.html`, then joins the returned Daily `room_url`/`token`. talk-go no longer exposes direct `/connect`; bot startup goes through the worker-compatible task boundary.
- **Transport:** Audio + UI events both ride Daily. There is **no** WebSocket to the talk-go service.
  - Outbound UI events: `_call.on("app-message", …)` expects RTVI `rtvi-ai` entries and dispatches by `event.type`.
  - Inbound control messages: `_call.sendAppMessage({type:"end_call"}, "*")` sends the disconnect request.
  - `left-meeting` triggers a full local teardown so the UI stays consistent if the bot ends the call.
- **Live transcript bar:** Shows what user is saying from `user-transcription` events with `final:false` and what assistant is speaking from `bot-transcription` events. Clears on finalization.
- **Committed turns:** User turns commit on `user-transcription` with `final:true`. Assistant turns commit on `bot-stopped-speaking` with metrics badges; interrupted assistant turns are flushed by the interruption `server-message`/`bot-stopped-speaking` sequence.
- **Metrics:** Buffered from RTVI `server-message` entries with `data.type=="metric"`, attached to assistant turn when committed. Multiple badges per turn (LLM TTFB, TTS TTFB, E2E latency, etc.).
- **Daily call object stored outside Alpine proxy** (`_call`) to avoid proxying SDK internals.
- **Daily SDK lazy-loaded** via dynamic `import()` on first connect.

### Environment Variables (.env)

```
SONIOX_API_KEY=...
OPENAI_API_KEY=...
CARTESIA_API_KEY=...
DISHA_API_URL=...
# API_BASE_URL can be used instead of DISHA_API_URL in GKE.
DISHA_REDIS_URL=...
DISHA_REDIS_PASSWORD=...
REDIS_DB=0
DAILY_BRIDGE_PYTHON=/Users/jaideepsingh/Projects/disha-backend/.venv/bin/python

# LiveKit outbound Opus tuning for low-CPU staging calls.
# Unset values use libopus defaults; staging currently tests these values.
LIVEKIT_OUT_CODEC=pcmu
LIVEKIT_OPUS_COMPLEXITY=1
LIVEKIT_OPUS_BITRATE=20000
LIVEKIT_OPUS_MAX_BANDWIDTH=wideband

# Live LLM router (sales call) — model switching across endpoints.
# Per-region Grok (OpenAI-compatible base URLs ending in /openai/v1):
GROK_4_1_FNR_EASTUS_API_KEY=...
GROK_4_1_FNR_EASTUS_ENDPOINT=...
# ...and EASTUS2 / WESTUS / WESTUS2 / WESTCENTRALUS variants.
# gpt-4.1 fallback group reuses the existing Azure/OpenAI keys, e.g.:
AZURE_OPENAI_US_EAST_API_KEY=...
AZURE_OPENAI_US_EAST_ENDPOINT=...   # base ending /openai/v1 (auto-appended if absent)
OPENAI_PRIORITY_API_KEY=...
# Vertex grok (OpenAI-compatible) — service-account key fetched from S3:
VERTEX_DISHAAI_CREDS_FILE=...        # S3 object key in AWS_BUCKET_NAME (uses ACCESS_KEY_ID/SECRET_KEY_ID/AWS_MAIN_REGION)
# US poll-trigger (public Lambda Function URL on the Disha US worker):
LLM_POLL_TRIGGER_URL=...
```

For the local Disha bridge, use `DISHA_API_URL=http://localhost:8000`, `DISHA_REDIS_URL=localhost:6379`, an empty `DISHA_REDIS_PASSWORD`, and `REDIS_DB=0`. Do not print `.env` contents in chat or terminal summaries.

### Dependencies

- `github.com/gorilla/websocket` — websocket client for Soniox and Cartesia only (UI events no longer use a custom WebSocket)
- `github.com/hajimehoshi/go-mp3` — MP3 decoding for background office sound
- `github.com/redis/go-redis/v9` — Disha Redis client
- `github.com/alicebob/miniredis/v2` — Redis unit-test server
- `github.com/google/uuid` — Disha conversation chunk IDs

### Build & Run

```bash
go build ./...
./talk-go  # or: go run .
# Open http://localhost:3000, click Connect
```

### Testing

```bash
go test ./...               # all tests
go test -race ./...         # with race detector (~5s)
go test -race -count=5 ./... # stress (5x)
go test -v -run TestUserIdle ./...  # one processor's tests
```

`test_setup_test.go` redirects `sttDialURL` / `ttsDialURL` to unreachable loopbacks; LLM processor tests use injected stub `LLMClient` implementations, while provider streaming/parsing behavior is covered in `llmrouter` tests. No external service is hit during tests. Integration testing (real Soniox/Cartesia/Daily) is out of scope for the unit suite — exercise via the actual app at `http://localhost:3000`.

### Debugging / Profiling

- `net/http/pprof` is imported in `main.go` — heap profiles available at `http://localhost:3000/debug/pprof/heap`
- Typical memory depends on both the Go process and the Python Daily bridge process; check both when profiling local calls.
- Performance diagnostics are gated by the single canonical flag `PERF_DIAGNOSTICS_ENABLED=1`. Keep that flag in the active deploy env file (`.staging.env` or `.prod.env`); the deploy scripts refresh the `talk-go-worker-env` secret from that file and do not accept a separate deploy-command override. When the flag is unset/`0`, Go and Python do not start Pyroscope, `process_usage`, or `audio_timing`, and the 20ms audio hot paths skip timing measurements entirely. For backward compatibility only, `PYROSCOPE_ENABLED=1` enables diagnostics when `PERF_DIAGNOSTICS_ENABLED` is absent.
- LiveKit outbound codec selection is controlled from the active deploy env file with `LIVEKIT_OUT_CODEC`. Unset/`opus` publishes the existing Opus track at 24kHz PCM input and applies `LIVEKIT_OPUS_COMPLEXITY`, `LIVEKIT_OPUS_BITRATE` (bits/s), and `LIVEKIT_OPUS_MAX_BANDWIDTH` (`narrowband`, `mediumband`, `wideband`, `superwideband`, `fullband`). The current low-CPU experiment sets `LIVEKIT_OUT_CODEC=pcmu`, which publishes an 8kHz G.711 μ-law track and bypasses libopus entirely after mixing; if LiveKit Cloud rejects or fails to forward the PCMU track, roll back by setting `LIVEKIT_OUT_CODEC=opus` or unsetting it. The last Opus-only experiment used complexity `1`, bitrate `20000`, and `wideband`.
- In GKE diagnostics mode, use `process_usage` log lines for high-level process attribution before deeper profilers. `DailyRoom` starts a lightweight sampler for Go plus the Python bridge process; `LiveKitRoom` samples Go plus container cgroup CPU because there is no Python bridge. Logs are one JSON payload every 10s with `transport_type`, Go/Python PIDs, process CPU millicores over the sample window, current RSS, RSS high-water mark, and container cgroup CPU throttling (`container_cpu_mcores`, `container_cpu_throttled_periods`, `container_cpu_throttled_ms`, `container_cpu_throttled_fraction`). Query `textPayload:"process_usage"` for a conversation/pod to see whether `/app/talk-go`, `daily_bridge.py`, or container CPU throttling is the main jitter suspect. For LiveKit, `python_*` fields are zero/false by design.
- Pyroscope profiling also requires `PYROSCOPE_SERVER_ADDRESS`, `PYROSCOPE_BASIC_AUTH_USER`, and `PYROSCOPE_BASIC_AUTH_PASSWORD`. Go reports as `talk-go.worker.go`; the Python Daily bridge reports as `talk-go.daily_bridge.python`. Keep labels low-cardinality (`environment`, `deployment`, `pod`, `process`, `language`) and do not add conversation IDs as Pyroscope tags.
- In diagnostics mode, use `audio_timing` log lines alongside Pyroscope when chasing jitter. They summarize 10s windows for 20ms-audio-path work such as Go/Python base64+JSON bridge I/O, LiveKit data publish/unmarshal, LiveKit PCM sample copy/conversion, LiveKit outbound Opus encode (`go_livekit_out_opus_encode`), playback tick lag/work, PCM frame copy/conversion, TTS JSON/base64 decode, and user PCM scanning. Profiles show CPU stacks; `audio_timing` shows deadline misses and where the real-time path is spending wall time.

#### Live Call Investigation Runbook

When Jaideep gives a staging `conversation_id`, check Cloud Logging first and Pyroscope when diagnostics were enabled for that run. Base the answer on the retrieved logs/profiles only; if `process_usage`, `audio_timing`, or Pyroscope has no data, say that explicitly and treat it as likely diagnostics-disabled unless logs show a profiler startup error.

1. Load local env without printing secrets:

```bash
set -a
source .staging.env
set +a
```

2. Query Cloud Logging for the call and save the raw result:

```bash
CONV="<conversation_id>"
PROJECT="${GCP_PROJECT_ID:-curelinkai}"
NAMESPACE="${GKE_NAMESPACE:-staging}"
CONTAINER="talk-go-worker"
LOG_JSON="/tmp/talk-go-${CONV}.logs.json"

CONV_FILTER='resource.type="k8s_container"
resource.labels.project_id="'$PROJECT'"
resource.labels.namespace_name="'$NAMESPACE'"
resource.labels.container_name="'$CONTAINER'"
textPayload:"[conv='$CONV']"'

gcloud logging read "$CONV_FILTER" \
  --project "$PROJECT" \
  --freshness=24h \
  --order=asc \
  --limit=10000 \
  --format=json > "$LOG_JSON"

jq -r '.[] | [.timestamp, .resource.labels.pod_name, .textPayload] | @tsv' "$LOG_JSON" | less -S
```

3. Extract the pod and call window from those logs. Use a slightly padded window for follow-up log/profile queries:

```bash
eval "$(python3 - "$LOG_JSON" <<'PY'
import json, sys
from datetime import datetime, timedelta, timezone

with open(sys.argv[1]) as f:
    rows = json.load(f)
if not rows:
    raise SystemExit("no logs found for conversation")

def parse(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

start = parse(rows[0]["timestamp"]) - timedelta(minutes=2)
end = parse(rows[-1]["timestamp"]) + timedelta(minutes=2)
pod = rows[0]["resource"]["labels"]["pod_name"]

def emit(name, value):
    print(f'export {name}="{value}"')

emit("POD", pod)
emit("START", start.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"))
emit("END", end.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"))
PY
)"

echo "pod=$POD start=$START end=$END"
```

4. Query the same pod/time window for process attribution, throttling, and audio-path timing:

```bash
POD_FILTER='resource.type="k8s_container"
resource.labels.project_id="'$PROJECT'"
resource.labels.namespace_name="'$NAMESPACE'"
resource.labels.container_name="'$CONTAINER'"
resource.labels.pod_name="'$POD'"
timestamp>="'$START'"
timestamp<="'$END'"'

gcloud logging read "$POD_FILTER AND (textPayload:\"process_usage\" OR textPayload:\"audio_timing\" OR textPayload:\"pyroscope\")" \
  --project "$PROJECT" \
  --order=asc \
  --limit=10000 \
  --format='table(timestamp,textPayload)'
```

Useful quick summaries:

```bash
jq -r '.[] | select(.textPayload | contains("process_usage {")) | .textPayload | split("process_usage ")[1] | fromjson |
  [.sample_period_ms, .go_cpu_mcores, .python_cpu_mcores, .container_cpu_mcores, .container_cpu_throttled_ms, .container_cpu_throttled_fraction, .go_rss_bytes, .python_rss_bytes] | @tsv' "$LOG_JSON"

jq -r '.[] | select(.textPayload | contains("audio_timing {")) | [.timestamp, .textPayload] | @tsv' "$LOG_JSON"
```

If `process_usage` / `audio_timing` entries are missing from `LOG_JSON`, rerun the pod-window query above with `--format=json > "/tmp/talk-go-${CONV}.pod.logs.json"` and summarize from that file instead.

5. Query Pyroscope for the same pod/time window. Prefer `profilecli` because it can export pprof files for local inspection:

```bash
export PROFILECLI_URL="$PYROSCOPE_SERVER_ADDRESS"
export PROFILECLI_USERNAME="$PYROSCOPE_BASIC_AUTH_USER"
export PROFILECLI_PASSWORD="$PYROSCOPE_BASIC_AUTH_PASSWORD"

GO_QUERY='{service_name="talk-go.worker.go",pod="'$POD'"}'
PY_QUERY='{service_name="talk-go.daily_bridge.python",pod="'$POD'"}'

profilecli query series --query="$GO_QUERY" --from="$START" --to="$END" --output=json
profilecli query series --query="$PY_QUERY" --from="$START" --to="$END" --output=json
```

Use the CPU `__profile_type__` shown by `query series` for each process, normally `process_cpu:cpu:nanoseconds:cpu:nanoseconds`. Then export and inspect both profiles:

```bash
CPU_TYPE="process_cpu:cpu:nanoseconds:cpu:nanoseconds"

profilecli query profile \
  --profile-type="$CPU_TYPE" \
  --query="$GO_QUERY" \
  --from="$START" --to="$END" \
  --output="pprof=/tmp/talk-go-${CONV}-go.pprof"

profilecli query profile \
  --profile-type="$CPU_TYPE" \
  --query="$PY_QUERY" \
  --from="$START" --to="$END" \
  --output="pprof=/tmp/talk-go-${CONV}-python.pprof"

go tool pprof -top -nodecount=30 /tmp/talk-go-${CONV}-go.pprof
go tool pprof -top -cum -nodecount=30 /tmp/talk-go-${CONV}-go.pprof
go tool pprof -top -nodecount=30 /tmp/talk-go-${CONV}-python.pprof
go tool pprof -top -cum -nodecount=30 /tmp/talk-go-${CONV}-python.pprof
```

If `profilecli` is missing, install it with `go install github.com/grafana/pyroscope/cmd/profilecli@latest` or use the HTTP API fallback:

```bash
curl -sG "$PYROSCOPE_SERVER_ADDRESS/pyroscope/render" \
  -u "$PYROSCOPE_BASIC_AUTH_USER:$PYROSCOPE_BASIC_AUTH_PASSWORD" \
  --data-urlencode "query=${CPU_TYPE}${GO_QUERY}" \
  --data-urlencode "from=$START" \
  --data-urlencode "until=$END" \
  --data-urlencode "format=json" \
  | jq '.flamebearer.names[0:30], .timeline'
```

6. Report in this order: call window and pod; `process_usage` averages/peaks and throttling; top `audio_timing` max/avg offenders; Go Pyroscope top stacks; Python Pyroscope top stacks; gaps or missing data. Remember that the Python profile can under-attribute native Daily SDK CPU, so if `process_usage` says Python is hot but Pyroscope does not show matching Python stacks, call that out as native/unattributed Python-process CPU rather than guessing.

### Known Issues / Gotchas

- Audio can sound choppy if PCM frames aren't paced at 20ms intervals — use `time.Ticker`, not `time.Sleep`
- Direct `/connect` is removed. Bot sessions should be launched through Disha/worker-compatible routes such as `/bot/create_worker_room`, which build tasks from a detached/background context and route through the common `disha.Bot` boundary.
- Playback-originated `BotStartedSpeakingFrame`/`BotStoppedSpeakingFrame` are broadcast both directions: upstream copies reach UserIdle and the user-side `UserContextAggregator`; downstream copies reach `AssistantContextAggregator`. Played `WordTimestampFrame`/`TTSDoneFrame` flow downstream after PlaybackSink so assistant commits happen before PipelineSink cleanup.
- Soniox sends transcript snapshots: individual responses duplicate prior non-final tokens and add/refine the current hypothesis. STT should forward token text plus response metadata (`ResponseID`, `Finished`) without owning transcript semantics. UserContextAggregator replaces non-final text per response, updates the UI from that latest snapshot, and starts turns only from final tokens ending in `<end>`; `Finished` is logged for debugging but is not the turn boundary
- Cartesia may send late audio/done messages after context cancellation — `activeContextId` atomic check in TTS reader goroutine drops them
- The 3s `procLoopExitTimeout` in `BaseProcessor.interruptProcessLoop` is a safety net: if a user's `ProcessFrame` ignores the per-frame ctx and runs longer than 3s, the base proceeds with the purge anyway and the in-flight ProcessFrame may complete concurrently with the new processLoop. All our processors' `ProcessFrame` impls respect ctx or finish in well under a second
- TTS orchestrator waits on `connected` before processing commands. If Cartesia is permanently unreachable, commands queue in `t.commands` (capacity 100). `PipelineTask.completeEnd`'s 10s WG-wait timeout is the ultimate escape hatch
- Tests use `settleDelay` between `framesToSend` and explicit test-chain stop when the processor under test pushes upstream frames (e.g. broadcast tests, barge-in tests). `EndFrame` no longer auto-cancels processors, so isolated tests must call `Stop()`/`stopProcessorsAndWait` after the expected frames have had a chance to propagate; production `PipelineTask.runCleanup` performs that stop after `EndFrame` reaches the sink.
