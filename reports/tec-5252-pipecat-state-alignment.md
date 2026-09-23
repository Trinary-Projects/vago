# TEC-5252: Pipecat state and frame alignment

Status: implemented for PR #43, including the September 22 continuation-context correction. The shared-context baseline, processor metrics, frame/queue alignment, and routing continuation through speech output were approved by Jaideep. Both uninterrupted continuation and interruption while continuation was queued passed live staging verification.

Reference: installed **Pipecat 0.0.108**, under `/Users/jaideepsingh/Projects/disha-backend/.venv/lib/python3.11/site-packages/pipecat`. This comparison uses its standard OpenAI service, universal aggregators, TTS service, and base output transport. Provider-specific Pipecat services can differ.

## What was removed

The published PR added response admission and generated-history coordination that Pipecat does not use. This revision removes it instead of moving or renaming it:

- Cross-pipeline `ResponseID` fields and `LLMMessagesAppendFrame.AfterResponseID`.
- User aggregator `pendingRuns`, `generatingResponseID`, `speakingResponseID`, pending-user text, and interruption acknowledgement state.
- The LLM processor's second request queue/worker and its separate generation cancellation gate.
- Generated assistant placeholders, `Message.pendingPlayback`, generated text on response-end, rollback, and completed-response markers.
- Delayed upstream bot-stop forwarding through the assistant aggregator.
- Per-response metric maps and attribution IDs, including per-context TTS metric collectors.
- The `played` flag that changed word/done frame priority, BaseProcessor input epochs, and the output suppression-until-next-start flag.

The injected instruction is unchanged and remains an ordinary user message:

```xml
<system_message>The system prompt has been updated for the next stage. Continue the conversation naturally using the new instructions, without repeating what you just said.</system_message>
```

## State that remains, and its actual counterpart

| Vago | Pipecat 0.0.108 counterpart | Purpose |
| --- | --- | --- |
| Shared `LLMContext`, carried by `LLMContextFrame` | `LLMContextFrame.context`; universal aggregator `push_context_frame` | Read the current shared context when the LLM queue consumes the request. Go snapshots under a mutex for memory safety, then performs any request-only enrichment. |
| `BaseProcessor.procCh`, processing context | `FrameProcessor.__process_queue`, processing task | Await inference on the normal processor loop. No separate LLM scheduler. |
| `BaseProcessor.procCurrent` | `FrameProcessor.__process_current_frame` and `_start_interruption` | Preserve an in-flight uninterruptible frame; otherwise cancel/recreate processing and retain only uninterruptible queued frames. |
| TTS `turn`, `contextsByID`, `outputQueue` | TTS `_turn_context_id`, `_audio_contexts`, `_serialization_queue` | Receive LLM text, collect independent provider contexts, and release context/frame output in FIFO order. |
| TTS `ContextID` on start, word, and stop frames | TTS `context_id` | Identify synthesis contexts. This is not an LLM response ID. |
| Playback queue and `playbackStarted` | Output sender audio queue and `_bot_speaking` | Pace output and originate both speaking notifications. |
| Assistant `functionCallsInProgress` | `_function_calls_in_progress` keyed by `tool_call_id` | Own tool context updates and native started/progress/result/cancel handling. |
| Processor timing collector | `FrameProcessorMetrics` processor/model timings | Best-effort timings without response correlation. |
| Per-frame `FrameMeta.ID` | `Frame.id` | Diagnostics for one frame instance, not response lifecycle coordination. |

The old Soniox `TranscriptFrame.ResponseID` and provider-native Responses API IDs are unrelated and remain. The Disha output filter's local response counter is only its existing log sequence, not the removed frame/context scheduling protocol.

## Flow

```mermaid
flowchart LR
    S[Successful stage transition] --> A[Push append frame past user aggregator]
    A --> T[TTS serialization queue]
    L[LLM speech and response end] --> T
    T --> O[Output FIFO and paced audio]
    O --> W[Played words and response end]
    W --> H[Assistant aggregator commits speech]
    O --> D[Following append frame]
    D --> I[Assistant appends ordinary user instruction]
    H --> C[Shared LLMContext]
    I --> C
    I --> F[LLMContextFrame upstream]
    F --> Q[Normal LLM processor queue]
    Q --> L
    O --> B[Bot started/stopped broadcasts both ways]
```

For standalone `TTSSpeakFrame`, TTS creates an independent context. Outside an active LLM response, it queues `LLMAssistantPushAggregationFrame` to flush that speech into context. Inside an active LLM response, the ordinary response-end remains the aggregation boundary. A new generation or cue never clears earlier output.

Interruption cancels processing and clears synthesis/output queues. Word frames are ordinary interruptible data before and after playback. There is no assistant-history acknowledgement, originating-response rejection, or special transcript-retention path.

## Accepted behavior changes

- `run_llm` does not wait for playback. The next inference may not see speech that has not reached assistant context.
- A late application classifier may still append and request inference after newer patient input. Disha's existing stage-name stale-result check remains; there is no response-origin guard.
- Explicit requests are not coalesced. Each tool result with `RunLLM=true` can request another inference, matching Pipecat's explicit result property.
- Replacement patient inference does not wait for assistant history. Text still queued downstream can be discarded on interruption.
- Pipecat resets its **processing** queue; it does not invalidate its separate input queue using epochs. Input frames not yet transferred can run after interruption.
- Metrics attached to a conversation chunk are the available processor timings at commit. Overlapping generation/synthesis no longer has guaranteed response-to-chunk attribution. Jaideep explicitly selected this behavior.

## Source map for review

All paths below are relative to the pinned Pipecat package:

| Concern | Pipecat source |
| --- | --- |
| Shared context and immediate append/run | `frames/frames.py::LLMContextFrame`, `LLMMessagesAppendFrame`; `processors/aggregators/llm_response_universal.py::push_context_frame`, `_handle_llm_messages_append` |
| Normal processor-loop inference | `services/openai/base_llm.py::process_frame` |
| Queue reset and current uninterruptible frame | `processors/frame_processor.py::_start_interruption`, `__reset_process_queue` |
| Tool context ownership | `processors/aggregators/llm_response_universal.py::_handle_function_calls_started`, `_handle_function_call_in_progress`, `_handle_function_call_result`, `_handle_function_call_cancel` |
| Tool broadcasts and cancellation | `services/llm_service.py::_run_function_call`, `_cancel_function_call` |
| Independent speech and aggregation flush | `services/tts_service.py::process_frame` (`TTSSpeakFrame` branch), `_serialization_task_handler` |
| Output-originated notifications | `transports/base_output.py::_bot_started_speaking`, `_bot_stopped_speaking` |
| Metrics state | `processors/metrics/frame_processor_metrics.py` |

## Remaining implementation differences

This is a Go adaptation of those contracts, not a complete port of every Pipecat service. `WordTimestampFrame` carries provider word text (Pipecat uses `TTSTextFrame`); `TTSDoneFrame` is the local name for TTS stop. Audio and words are interleaved at 20ms PCM boundaries rather than routed through Pipecat's general presentation-timestamp clock queue. Go uses bounded channels, mutexes, cooperative cancellation contexts, and its existing process-loop shutdown timeout. Cancellable tools use the processor cancellation context; non-cancellable tools use task lifetime, rather than asyncio task cancellation.

Existing Disha policies remain: stage selection/question/tool eligibility, Soniox turn detection, consecutive-user merging, persistence callbacks, a blocking business context enricher, empty-tool-result error reporting, and TTS reconnect/shutdown policy. No new compensating guard has been added for edge cases outside the pinned Pipecat behavior.

### Request-only enrichment correction after review

The first staged revision rewrote shared history with an enriched snapshot. Fable identified that a history append or tool-result update during retrieval could be overwritten. That write-back is removed: `LLMProcessor.SetMessagesEnricher` configures the existing application callback to enrich only a private outgoing copy, after the normal LLM queue consumes the shared context. The separate `ContextEnricherProcessor` is removed. User and assistant context ownership, frame contracts, and queue behavior stay the same; no context-update API, version counter, or retry is introduced.

Retrieval runs before LLM timing and response-start, retaining the separate `context_enricher/context_enrich` measurement and the existing fail-open behavior. Interruption uses the normal processor context and prevents an abandoned enrichment from reaching the provider. Tool-result runs pass through the same request-preparation step. Updates during retrieval remain in shared history for subsequent runs; the in-flight request keeps its snapshot. Pipecat's universal OpenAI service builds invocation parameters from the context through its adapter (`services/openai/base_llm.py::_stream_chat_completions_universal_context`); the protocol-retrieval callback is a Disha-specific extension at that boundary, not a native Pipecat hook.

## Validation

Tests cover shared context read at invocation; sequential generation; independently completed speech contexts with FIFO output; cues during LLM speech; ordinary word/end ordering; tool cancellation and non-cancellable results; late provider audio after cancellation; interruption process-queue reset; in-flight uninterruptible frames; graceful shutdown; and stage continuation eligibility, failed/stale transitions, and late classifier behavior.

Enrichment regression tests cover concurrent assistant/user appends and tool-result updates while retrieval is blocked, mutation confined to the outgoing copy, nil/empty-result fallback, cancellation before a provider request, queued context read at consumption, and enrichment on tool-result runs.

`go test ./...` and `go test -race ./voicepipelinecore ./disha -timeout 120s` passed after the runtime changes. The added standalone-speech integration test also passed under the race detector. `git diff --check` passed. That validation preceded live staging QA.

After the request-only enrichment correction, the targeted enrichment/queued-request/tool-loop tests passed under `-race`, followed by fresh successful runs of `go test ./...` and `go test -race ./voicepipelinecore ./disha -timeout 120s`.

## September 22: continuation sees the previous statement

DJ's staging call `05343812-bbea-46a8-b9ce-91ae6a5ed58c` exposed a semantic gap: continuation started at 23:25:29.613 IST, while the preceding statement was committed only at 23:25:34.864. The next request lacked that statement and repeated reassurance. Both messages were eventually persisted; this was an early request, not permanent history loss.

The tracker now calls `pair.User().PushFrame` instead of `QueueFrame`. This bypasses immediate consumption by the user aggregator. The unchanged append frame passes through the LLM, TTS serialization and playback FIFO, behind the response-end boundary; the assistant aggregator commits speech before handling the append and requesting inference upstream. No core handler, frame contract, or coordination state changes.

Pipecat references: `services/tts_service.py::process_frame` serializes response-end and ordinary downstream frames; `processors/aggregators/llm_response_universal.py::LLMAssistantAggregator._handle_llm_messages_append` consumes the append and pushes context upstream. The user-side append handler still runs immediately for other use cases.

Tradeoff: continuation generation now starts after preceding playback, exposing its LLM/TTS startup latency. Ordinary queued continuations are discarded by native interruption clearing. Late classifier completion after interruption retains the existing behavior; no new stale-result guard is introduced. Generated text is never inserted as already-spoken history.

Regression coverage checks both transition/playback completion orders, exact assistant-before-instruction context, interruption while synthesis or playback is pending, and a replacement patient turn without the discarded instruction. Stage eligibility, failed/stale transitions, consecutive stages, and late-classifier behavior remain covered. The new stage-routing test failed on the old user-queue route before the correction.

Validation: `go test ./...`, `go test -race ./voicepipelinecore ./disha -timeout 120s`, and `git diff --check` passed. The correction was deployed to staging from the uncommitted checkout on September 22 and verified in call `b1e64fb3-f05c-49d6-9176-5d9c29dea1e1`: the continuation request contained the complete preceding assistant statement exactly once before the instruction, then asked the next question without repeating reassurance. Playback resumed 860 ms after the statement finished. Both chunks' stored timings matched their respective raw metric events. The question-ending transition correctly waited for patient input; client disconnect cleared 46 pending playback frames and cleanup completed normally. This call did not exercise barge-in while a continuation append was queued.

Call `97d75cd2-281c-4bdf-a935-2d5cc568226c` then verified interruption while continuation was queued. The stage update completed at 08:23:09.590 IST; barge-in at 08:23:15.389 cleared 89 queued playback frames. The next request contained the interrupted assistant prefix exactly once, followed by fresh patient input, with no unspoken suffix or continuation instruction. It used the updated RCA prompt and asked the next question without repeated reassurance. Both relevant chunks' timings matched raw metric events; playback latency was 872 ms for the interrupted statement and 875 ms for the new patient turn. Disconnect cleared pending playback and cleanup completed normally. These checks used runtime events and saved requests; neither call had a recording.

Deployment receipt: `deploy-staging.sh` built the working tree without a commit, pushed image digest `sha256:cba1ad4bc8fdd454cff516032f15e539e69f0268167fea892ea21575a08023c8`, and completed deployment generation 166 in `disha-voice-worker-staging` (`us-east1`, namespace `staging`). ReplicaSet `5b55566c56` had 2/2 updated, ready, and available pods. Both health and readiness endpoints returned HTTP 200 on both pods. Their `/app/talk-go` SHA-256 matched the locally built image: `c9fe3492cc19a634663b4345cafbadf9c44dc7c130a62d7726b4f684c35be202`. Git HEAD at deployment was `e685aee`; the correction was deployed before committing, as requested.
