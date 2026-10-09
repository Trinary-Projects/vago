# Vago / talk-go — Agent Guidance

Keep this file focused on shared working agreements, architectural intent, and non-obvious constraints that cannot be learned easily from the code. After an agreed design change, update the relevant guidance in place and remove superseded instructions. Do not include personal profiles, contributor-specific preferences, absolute workstation paths, or machine-specific setup assumptions. Do not duplicate model/group mappings, tuning values, API/field inventories, pipeline wiring, or implementation walkthroughs. Incident histories and rollout evidence belong in focused reports or Git history. Read source for implementation details and verify live state for deployment claims.

## Collaboration

- Keep responses concise, explain consequential choices, and prefer the simplest solution.
- Do not commit or push unless the requester has reviewed the current changes or explicitly asks to commit/push in that moment.
- Before changing call lifecycle, turn-taking, interruption, transport events, idle handling, frame semantics, or shutdown, inspect the Pipecat version pinned by disha-backend and its relevant `bots/` implementation. Use an available checkout or the corresponding upstream source without assuming a checkout location or virtual environment layout. Name the relevant file/pattern in the reasoning.
- Match Pipecat's frame contracts, state ownership, and processing structure, not just happy-path behavior. Identify the actual Pipecat counterpart of new coordination state; frame IDs, TTS context IDs, and tool-call IDs are not interchangeable. Record deliberate differences explicitly.
- Discuss non-trivial architecture changes before implementing: state the existing pattern, minimal change, alternatives, rationale, and exact tests. New frames, task/context fields, callback surfaces, long-lived dependencies, and shared helpers need explicit discussion and approval. First show why an existing frame, callback, config, or method parameter cannot handle the behavior.
- Do not add custom acknowledgements, provisional history, queues, guards, or guarantees beyond Pipecat without agreement. Pass narrow values directly instead of promoting them into shared state.

## Architectural Boundaries

- Keep `voicepipelinecore` free of Disha business logic, Redis, business HTTP clients, conversation IDs, prompts, and persistence. Business packages assemble the pipeline and inject dependencies. The `llmrouter` sub-package may use HTTP/crypto, environment variables, and a narrow Redis interface, but core must not import it.
- Shared call callbacks must stay bot-agnostic. Put bot-specific payload enrichment behind the existing generic decorators rather than adding bot-named members to shared structures.
- Disha backend retains the control plane and product responsibilities. Go owning voice execution does not authorize removing backend orchestration, webhooks, persistence, or shared business behavior. For backend cleanup, prove code is unused and remove it in reviewable phases; do not relocate retained functions merely to delete a file.
- Preserve one integration event surface and one frontend/debug-log event stream over the active room transport. Do not introduce a parallel observer, custom websocket, or separate debug-log stream for a single feature.
- Experiment assignment belongs to disha-backend; call preparation reads existing assignments. Do not roll experiments at call time or promote a per-call constructor choice into broad task state.

## Deliberate Semantics and Integration Constraints

- Follow Pipecat's ordering and interruption boundaries. Do not add a global playback/history barrier, speculative assistant messages, rollback, or an acknowledgement mechanism to make overlapping turns appear atomic. Processor metrics intentionally lack exact response attribution under overlap.
- Stage continuation relies on ordinary TTS/playback ordering so the preceding speech enters history before continuation inference. Preserve that ordering without introducing a stage-specific coordination queue. Immediate user-side appends and tool-result runs retain their own semantics.
- Keep request enrichment on a private invocation snapshot. Writing it back to shared history can erase concurrent user, assistant, or tool updates.
- Route shutdown through the task's existing source-driven End path. Mid-pipeline End injection or direct cancellation/disconnection bypasses ordering and persistence. Cleanup must not synchronously wait on the goroutine invoking it.
- Treat cancellation/barge-in separately from provider failure. Do not penalize endpoint health for user interruption or retry after user-visible output without discussing that behavior.
- For incomplete model sentences, inspect prompts/context and actual provider output before adding sentence-completion heuristics to core or TTS.
- Every new health-selected endpoint/group needs matching disha-backend polling registration and identical config keys. Go-only registration silently lacks health data. Fixed endpoints used solely for one-shot hedged calls do not require polling registration.
- Prompt logs must carry resolved prompt identity and the exact variables used to render it. Do not omit large variables or rely on backend version backfilling; business/tool metadata does not belong in prompt metadata. Preserve explicitly designed exceptions such as retrieved-protocol context.
- Preserve stock Gonja behavior rather than adding normalization or custom truthiness to imitate Python. The Python renderer is a parity oracle, not a reason to grow a compatibility layer.
- Persisted timing fields use seconds despite legacy `_ms` names; core uses milliseconds. Keep conversion at the integration boundary. Preserve backend/resume payload compatibility and check both repositories before changing contracts.
- Enqueued Python jobs must be callable without a bound instance; serialization does not preserve `self`. New top-level job kwargs require backend compatibility before the Go sender deploys.
- Azure websocket authentication puts credentials in the URL. Never log connection URLs, redirects, or handshake bodies containing them.

## Investigation Practices

- Check `app.log` first; `log.*` writes there, while `fmt.Print*` is stdout. For recent staging/prod calls absent locally, query GCP Cloud Logging next with the conversation ID and Go worker labels. Resolve cluster/deployment names from deployment configuration; do not mistake legacy Python worker logs for Go logs.
- Base findings on the actual call's logs, persisted data, and profiles. Derive the pod and UTC window from logs, then use the same window for diagnostics. Missing diagnostics do not prove that CPU/audio paths were healthy. Daily's Python process can spend CPU in native SDK code that Python profiles under-attribute.
- Use task-scoped Sentry hubs for call identity; a process-global mutable scope is unsafe in a multi-session worker. Use authorized credentials for read-only Sentry API access. Never print or persist credential values or assume a contributor's credential-file location.
- For persisted-call investigations, use the target environment's authorized database connection. Enforce session-level read-only mode and a statement timeout (for example, `PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=120000"`) and run SELECTs only. Preserve these restrictions if a different connection method is needed.
- LLM request/response bodies and prompt metadata may live in S3 rather than the DB row. Follow the row's bucket/key using authorized access for that environment; do not assume credentials from another environment are appropriate.
- Keep profiler labels low-cardinality; do not add conversation IDs. Verify diagnostics were enabled for the investigated call before drawing conclusions from absent profiles.

## Deployment and Verification

- Use `deployment/github-actions.md` and the deployment scripts for operational details. Deployment commands must load their configuration without requiring caller-exported values. Parameter Store is the deployment source; contributor env files must not be overwritten by deploy tooling.
- Preserve manual deployment, production branch/actor restrictions, cloud-federation checks, and environment validation. Legacy Python workers are intentionally retired; reactivation or removal of shared infrastructure requires an explicit migration/rollback decision.
- Rollout timeout can reflect old workers draining rather than unhealthy new workers. Inspect actual readiness and draining state. Historical successful runs are not evidence of current deployment health.
- Use the existing processor test harness and real worker ingress for ordering/interruption regressions. Explicitly stop isolated test chains after expected asynchronous propagation; EndFrame alone does not cancel them.
- Run relevant tests and `go test -race ./...` for concurrency/lifecycle changes. Keep provider calls out of unit tests; use opt-in probes or browser clients for live integration checks. Do not present unit-test success as live-provider verification.

## Open Limitations

These are unresolved behavior gaps, not permission to introduce new coordination architecture without discussion. Remove an entry when its underlying issue is resolved.

- STT connection failure while audio flows can wedge processing. Cartesia STT can reconnect indefinitely after a policy close, and whole-final timing can let short back-channels evade discard.
- If TTS never connects initially, queued commands including End can wedge before task cleanup begins; the cleanup wait timeout cannot rescue that path.
- A Responses websocket event timeout can fail a no-output turn before the pipeline retry timer fires; stream-end handling then cancels that timer. Health routing does not replay the lost turn.
- Onboarding continuation is not suspended merely because the user barges in during playback after generation has finished. Generated-text interruption status describes generation cancellation, and late stage classification has only the stage-name stale-result check.
