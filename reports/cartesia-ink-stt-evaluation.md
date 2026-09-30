# Cartesia Ink STT as a Soniox replacement — evaluation (2026-09-14)

Scope: can Cartesia Ink (`ink-preview`) replace Soniox `stt-rt-v5` in Vago's `STTProcessor` with the same behavior? Docs reviewed at docs.cartesia.ai (API ref for `/stt/websocket` and `/stt/turns/websocket`, turns, keyterms, audio input, concurrency, changelog 2026). Live probe: `cmd/ink-stt-probe` (untracked). Raw captures: `/tmp/ink-probe/out*`, reports `/tmp/ink-probe/results.md` and `/tmp/ink-probe/soniox-vs-ink.md`.

## What Vago needs from STT (from `stt_processor.go` / `user_context_aggregator.go`)
1. Interim vs final token stream; interims are full-hypothesis snapshots grouped by a response id.
2. Finals as deltas plus an explicit end-of-utterance marker (`<end>` final token) distinct from stream close.
3. Lazy connect on `STTConnectFrame`, 3-attempt fatal connect, mid-stream reconnect, keepalive on idle, single writer.
4. Provider error → non-fatal `ErrorFrame` + Sentry, rate-limited.
5. Hindi/Hinglish (`language_hints: ["hi"]`), s16le 16 kHz mono, 20 ms inbound frames from Daily.

## Cartesia endpoints
- **Auto** `wss://api.cartesia.ai/stt/turns/websocket` — models `ink-2` (English only) and `ink-preview` (en/fr/hi/ja/es, auto language). Events `connected`, `turn.start`, `turn.update` (cumulative transcript within a turn; 211/211 observed pairs prefix-extend), `turn.eager_end`, `turn.resume`, `turn.end` (definitive), `error`. Client text: `{"type":"close"}`, `{"type":"config","turn":{...}}`. Query knobs `turn_start_threshold`, `turn_eager_end_threshold`, `turn_end_threshold`, `turn_end_timeout_ms`, repeatable `keyterm`. Auth `X-API-Key`, `cartesia_version=2026-08-14`.
- **Manual** `wss://api.cartesia.ai/stt/websocket` — all models; finals are deltas; client sends bare `finalize`/`close`. **Observed: zero non-final transcripts on ink-preview and ink-whisper (4 runs)**, so Manual mode gives no interims → no barge-in word count → unusable for Vago without a client VAD.

## Mapping to Vago's contract (Auto mode, ink-preview)
| Need | Ink Auto | Verdict |
|---|---|---|
| Interim snapshots | `turn.update.transcript` cumulative | Fits: one interim frame per update, new response id each time |
| Final + `<end>` | `turn.end.transcript` (whole turn) | Fits: emit one final token + synthetic `<end>` |
| Stream-lifecycle flag | none needed | `Finished` stays false |
| Keepalive | **No keepalive message** (`keepalive` → 400 "Expected done, close, config"); idle close 1001 after exactly 180 s | Replace text keepalive with zero PCM, or drop it since transports stream silence |
| Endpoint tuning | 4 thresholds, also changeable mid-stream via `config` | Richer than Soniox |
| Small chunks | 10 ms / 20 ms chunks worked, no error, latency unchanged | OK with Daily's 20 ms frames |
| Errors | JSON `error` (status_code, title, message, error_code) then close 1008 on permanent errors | Reconnect loop hits same error 3× → fatal, acceptable |
| Language | ink-preview auto among 5 languages; Devanagari output, Latin script kept for English words | Hinglish OK; **no Tamil/Telugu/Marathi/Bengali/Gujarati/Punjabi** (Soniox covers them) |
| Word timestamps / diarization / confidence | words only in Manual; no diarization/confidence | Unused by Vago today |
| Concurrency | per-plan cap (Free 8 … Scale 60, Enterprise custom); idle sockets count; 429 on excess | Must confirm plan before prod |
| Pricing of ink-preview | not published | Unknown |

## Live measurements (3 runs each, latency of definitive final after last speech sample, same clips, same pacing)
| Clip | Soniox `<end>` | Ink-preview `turn.end` | Delta |
|---|---|---|---|
| English 9 s | 715.5 ms | 1063.7 ms | +348 ms slower |
| Hinglish | 708.9 ms | 564.8 ms | −144 ms faster |
| Hindi (2 sentences, last) | 629.1 ms | 1275.8 ms | +647 ms slower |
| Two utterances (last) | 730.1 ms | 1308.4 ms | +578 ms slower (see split) |
| ink-2 English, two utterances (last) | 730.1 ms | 472.5 ms | −258 ms faster |

- **Phrase split**: ink-preview cut "Please tell me more about the diet" / "plan." into two turns in 3/3 runs with no silence at that boundary (ffmpeg silencedetect −40 dB/150 ms). ink-2 did not (0/3). Soniox did not. This would trigger an early bot response in a call.
- `turn.eager_end` followed by `turn.resume` was observed on Hinglish, so eager_end cannot be treated as final without a cancel path.
- Interim cadence: Soniox ~130–200 ms median between non-final messages; ink-preview 30–52 updates over a 9–11 s clip (roughly 200–300 ms). Adequate for the 3-word barge-in check.
- Transcript quality on these TTS clips: both correct and code-switch-aware. Soniox writes digits and Devanagari for "Doctor"/"tablet"; Ink writes Hindi number words and keeps English words in Latin script.
- ink-whisper with `language=hi` badly garbled the Hinglish clip (repeated "वेट" ×13, lost the English sentence). Not a candidate.
- Idle socket closes at 180.1 s with close code 1001 "Idle timeout".

## Conclusion
Feature-complete for Vago's pipeline only in **Auto mode with `ink-preview`**: the turn-event stream maps cleanly onto `TranscriptFrame` semantics with a small adapter in `STTProcessor` (update → interim snapshot, end → final + `<end>`), and keepalive/chunking/error handling need only value-level changes. Blockers before a switch: (1) ink-preview is preview-status and its end-of-turn is 350–650 ms slower than Soniox on 3 of 4 clips, (2) the reproducible mid-phrase turn split, (3) language coverage shrinks to Hindi + English, (4) unknown pricing and per-plan concurrency cap. Recommended path if pursued: build it behind a per-call switch (Python's `call_stt_variant_flag` has no Go equivalent yet) and A/B on staging with real speech rather than replace outright.
