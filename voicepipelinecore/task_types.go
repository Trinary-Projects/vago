package voicepipelinecore

import "time"

// Message is the public representation of an LLM context message.
// It maps directly to the OpenAI-format chat message objects the LLM
// processor sends, including assistant tool calls and tool results.
type Message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

// ToolDefinition is an OpenAI-format function tool definition.
type ToolDefinition struct {
	Type     string       `json:"type"`
	Function ToolFunction `json:"function"`
}

type ToolFunction struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters,omitempty"`
	Strict      *bool          `json:"strict,omitempty"`
}

// ToolCall is one function call requested by the LLM. Function.Arguments is
// the raw JSON argument string streamed by the provider.
type ToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function ToolCallFunction `json:"function"`
}

type ToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// LLMRequest is the full native request shape sent by LLMProcessor to an
// LLMClient. Tools are optional; text-only bots leave them empty.
type LLMRequest struct {
	Messages   []Message        `json:"messages"`
	Tools      []ToolDefinition `json:"tools,omitempty"`
	ToolChoice any              `json:"tool_choice,omitempty"`
}

// EndReason is the typed public reason a task ended. EndFrame still
// carries the string form so existing frame logging remains simple.
type EndReason string

const (
	EndReasonUnspecified       EndReason = "unspecified"
	EndReasonTalkTimeExhausted EndReason = "talk_time_exhausted"
	EndReasonClientDisconnect  EndReason = "client_disconnected"
	EndReasonUserIdle          EndReason = "user_idle"
	EndReasonError             EndReason = "error"
)

func normalizeEndReason(reason EndReason) EndReason {
	if reason == "" {
		return EndReasonUnspecified
	}
	return reason
}

// CallStats is delivered with CallEvents.OnCallEnded.
type CallStats struct {
	TransportType         string
	TotalUserDurationSec  float64
	FirstUserAudioFrameAt time.Time
	EndedAt               time.Time
	MeetingID             string
	BotSessionID          string
	UserSessionID         string
	DebugLogs             []RTVIDebugLogEntry
}

// CallEvents are integration callbacks for call lifecycle and committed LLM
// context updates. The first five are one-shot timeline events; committed-turn
// and tool-result events can fire many times.
type CallEvents struct {
	OnBotJoined              func(time.Time)
	OnUserJoined             func(time.Time)
	OnUserFirstSpeech        func(time.Time)
	OnBotFirstSpeech         func(time.Time)
	OnFirstUserAudio         func(time.Time)
	OnUserTurnCommitted      func(text string, at time.Time, promptKey string)
	OnAssistantTurnCommitted func(text string, at time.Time, metrics TurnMetrics, promptKey string)
	OnToolResultCommitted    func(assistantToolCall Message, toolResult Message, at time.Time)
	// OnLLMCallCompleted fires when an LLM call finishes, with the
	// generated response text (not the played text) and whether the call
	// was cut short (barge-in/EndFrame cancellation or a stream error).
	// Mirrors Python CustomOpenAILLMService's on_llm_call_complete event
	// (is_interrupted = not completed); the onboarding stage-transition
	// tracker consumes it.
	OnLLMCallCompleted func(text string, interrupted bool)
	OnCallEnded        func(reason EndReason, stats CallStats)
}

// TurnMetrics is a per-assistant-turn snapshot assembled from the
// MetricsFrame stream.
type TurnMetrics struct {
	LLMTTFBMs            float64
	LLMProcessingMs      float64
	TTSTextAggregationMs float64
	TTSTTFBMs            float64
	E2ELatencyMs         float64
}
