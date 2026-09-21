package voicepipelinecore

import "context"

// MessagesEnricher rewrites a private outgoing conversation snapshot before
// LLM generation. Its result is never written back to the shared LLMContext.
//
// It runs on the LLM processor's normal frame loop, before LLM timing starts.
// Implementations own their timeout, must honour ctx on interruption/shutdown,
// and should return usable messages on failure. A nil or empty result keeps
// the original request unchanged.
//
// Business packages supply the implementation (Disha's follow-up calls use it
// for protocol retrieval); the core knows nothing about the enrichment policy.
type MessagesEnricher func(ctx context.Context, messages []Message) []Message
