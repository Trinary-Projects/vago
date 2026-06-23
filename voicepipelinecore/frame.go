package voicepipelinecore

import (
	"fmt"
	"sync/atomic"
	"time"
)

type FrameType int

// FrameMeta holds the identifying metadata for a frame. ID is assigned
// monotonically by newFrameMeta so a frame can be traced as it travels
// through the pipeline. Name is the Go type name, included for logs.
// Mirrors Pipecat's name+id pair (see FrameProcessor: "FooFrame#42").
type FrameMeta struct {
	ID   int64
	Name string
}

// FrameBase is embedded into every concrete frame type. It carries the
// per-frame ID and name and provides default ID()/Name()/String()
// implementations. Value-receiver methods are fine because Meta is set
// once at construction and never mutated.
type FrameBase struct {
	Meta FrameMeta
}

func (b FrameBase) ID() int64    { return b.Meta.ID }
func (b FrameBase) Name() string { return b.Meta.Name }
func (b FrameBase) String() string {
	if b.Meta.Name == "" {
		return fmt.Sprintf("<unnamed>#%d", b.Meta.ID)
	}
	return fmt.Sprintf("%s#%d", b.Meta.Name, b.Meta.ID)
}

// nextFrameID hands out monotonically increasing IDs. Package-scoped
// so IDs are globally unique within a process — useful for cross-task
// log correlation.
var nextFrameID atomic.Int64

// newFrameMeta builds a FrameMeta with a fresh ID and the given name.
// Constructors for each frame type call this. Frames constructed via
// struct literals (allowed in tests) get a zero-value Meta — their
// ID()/Name() return 0 and "".
func newFrameMeta(name string) FrameMeta {
	return FrameMeta{ID: nextFrameID.Add(1), Name: name}
}

// Frame is implemented by every frame flowing through the pipeline.
//
// ID + Name come from FrameBase (embedded in every concrete frame).
// Used in logs to correlate the same frame as it traverses processors.
//
// IsSystem returns true for frames that should be processed with priority
// (they bypass the per-processor data queue). System frames are handled
// inline by BaseProcessor's input loop, before any pending data frames.
//
// IsInterruptible returns false for frames that must survive
// BaseProcessor's procCh purge when InterruptFrame is processed. Do not
// use it as a playback-queue policy: PlaybackSink's queue can contain
// future, unplayed reconciliation frames, so it has its own stricter
// interrupt purge.
type Frame interface {
	ID() int64
	Name() string
	FrameType() FrameType
	IsSystem() bool
	IsInterruptible() bool
}

// BroadcastableFrame is implemented by frames that BaseProcessor.Broadcast
// can emit in both directions simultaneously. Clone must return a fresh,
// independent copy of the frame so the upstream and downstream copies
// don't share mutable state. Most broadcast frames are zero-field structs
// (BotStartedSpeakingFrame, BotStoppedSpeakingFrame) where Clone is a
// one-liner returning a constructor call (which assigns a new ID, since
// the two clones are distinct frame instances).
type BroadcastableFrame interface {
	Frame
	Clone() Frame
}

const (
	Audio FrameType = iota // iota auto-increments: 0, 1, 2, 3...
	Text
	Interrupt
	LLMResponseStart
	LLMResponseEnd
	Transcript
	End
	WordTimestamp
	TTSDone
	MetricsType
	TTSSpeak
	BotStartedSpeaking
	BotStoppedSpeaking
	LLMMessages
	Error
	LLMMessagesAppend
	STTConnect
	FunctionCallInProgress
	FunctionCallResult
)

type AudioFrame struct {
	FrameBase
	Data []byte
}

func NewAudioFrame(data []byte) AudioFrame {
	return AudioFrame{FrameBase: FrameBase{Meta: newFrameMeta("AudioFrame")}, Data: data}
}

func (f AudioFrame) FrameType() FrameType  { return Audio }
func (f AudioFrame) IsSystem() bool        { return false }
func (f AudioFrame) IsInterruptible() bool { return true }

type TextFrame struct {
	FrameBase
	Text string
}

func NewTextFrame(text string) TextFrame {
	return TextFrame{FrameBase: FrameBase{Meta: newFrameMeta("TextFrame")}, Text: text}
}

func (f TextFrame) FrameType() FrameType  { return Text }
func (f TextFrame) IsSystem() bool        { return false }
func (f TextFrame) IsInterruptible() bool { return true }

type InterruptFrame struct {
	FrameBase
}

func NewInterruptFrame() InterruptFrame {
	return InterruptFrame{FrameBase: FrameBase{Meta: newFrameMeta("InterruptFrame")}}
}

func (f InterruptFrame) FrameType() FrameType  { return Interrupt }
func (f InterruptFrame) IsSystem() bool        { return true }
func (f InterruptFrame) IsInterruptible() bool { return false }

type LLMResponseStartFrame struct {
	FrameBase
	StartedAt time.Time
}

func NewLLMResponseStartFrame(startedAt time.Time) LLMResponseStartFrame {
	return LLMResponseStartFrame{FrameBase: FrameBase{Meta: newFrameMeta("LLMResponseStartFrame")}, StartedAt: startedAt}
}

func (f LLMResponseStartFrame) FrameType() FrameType  { return LLMResponseStart }
func (f LLMResponseStartFrame) IsSystem() bool        { return false }
func (f LLMResponseStartFrame) IsInterruptible() bool { return true }

type LLMResponseEndFrame struct {
	FrameBase
}

func NewLLMResponseEndFrame() LLMResponseEndFrame {
	return LLMResponseEndFrame{FrameBase: FrameBase{Meta: newFrameMeta("LLMResponseEndFrame")}}
}

func (f LLMResponseEndFrame) FrameType() FrameType  { return LLMResponseEnd }
func (f LLMResponseEndFrame) IsSystem() bool        { return false }
func (f LLMResponseEndFrame) IsInterruptible() bool { return true }

type TranscriptFrame struct {
	FrameBase
	Text       string
	IsFinal    bool
	ResponseID int  // Groups tokens from the same STT websocket message.
	Finished   bool // Stream-level Soniox flag; not an utterance boundary.
}

func NewTranscriptFrame(text string, isFinal bool, responseID int, finished bool) TranscriptFrame {
	return TranscriptFrame{
		FrameBase:  FrameBase{Meta: newFrameMeta("TranscriptFrame")},
		Text:       text,
		IsFinal:    isFinal,
		ResponseID: responseID,
		Finished:   finished,
	}
}

func (f TranscriptFrame) FrameType() FrameType  { return Transcript }
func (f TranscriptFrame) IsSystem() bool        { return false }
func (f TranscriptFrame) IsInterruptible() bool { return true }

// EndFrame travels downstream through the pipeline as a regular data
// frame (IsSystem returns false) so processors observe it in pipeline
// order. It returns IsInterruptible=false so that if an InterruptFrame
// is processed while an EndFrame is queued behind it, the EndFrame
// survives the purge. This mirrors Pipecat's UninterruptibleFrame mixin
// on EndFrame.
type EndFrame struct {
	FrameBase
	Reason string
}

func NewEndFrame(reason string) EndFrame {
	return EndFrame{FrameBase: FrameBase{Meta: newFrameMeta("EndFrame")}, Reason: reason}
}

func (f EndFrame) FrameType() FrameType  { return End }
func (f EndFrame) IsSystem() bool        { return false }
func (f EndFrame) IsInterruptible() bool { return false }

type WordTimestampFrame struct {
	FrameBase
	Words []string
}

func NewWordTimestampFrame(words []string) WordTimestampFrame {
	return WordTimestampFrame{FrameBase: FrameBase{Meta: newFrameMeta("WordTimestampFrame")}, Words: words}
}

func (f WordTimestampFrame) FrameType() FrameType { return WordTimestamp }

// WordTimestampFrame is emitted only after the corresponding audio has
// actually played. Treat it as system-priority so an InterruptFrame cannot
// overtake or purge already-played assistant words before
// AssistantContextAggregator reconciles them.
func (f WordTimestampFrame) IsSystem() bool        { return true }
func (f WordTimestampFrame) IsInterruptible() bool { return false }

type TTSDoneFrame struct {
	FrameBase
}

func NewTTSDoneFrame() TTSDoneFrame {
	return TTSDoneFrame{FrameBase: FrameBase{Meta: newFrameMeta("TTSDoneFrame")}}
}

func (f TTSDoneFrame) FrameType() FrameType { return TTSDone }

// TTSDoneFrame is a post-playback structural signal. Keep it in the same
// priority class as WordTimestampFrame/BotStoppedSpeakingFrame so the played
// completion sequence preserves order at post-playback processors.
func (f TTSDoneFrame) IsSystem() bool        { return true }
func (f TTSDoneFrame) IsInterruptible() bool { return false }

type TTSSpeakFrame struct {
	FrameBase
	Text string
}

func NewTTSSpeakFrame(text string) TTSSpeakFrame {
	return TTSSpeakFrame{FrameBase: FrameBase{Meta: newFrameMeta("TTSSpeakFrame")}, Text: text}
}

func (f TTSSpeakFrame) FrameType() FrameType  { return TTSSpeak }
func (f TTSSpeakFrame) IsSystem() bool        { return false }
func (f TTSSpeakFrame) IsInterruptible() bool { return true }

type BotStartedSpeakingFrame struct {
	FrameBase
}

func NewBotStartedSpeakingFrame() BotStartedSpeakingFrame {
	return BotStartedSpeakingFrame{FrameBase: FrameBase{Meta: newFrameMeta("BotStartedSpeakingFrame")}}
}

func (f BotStartedSpeakingFrame) FrameType() FrameType { return BotStartedSpeaking }

// Pipecat models bot-speaking notifications as SystemFrame.
func (f BotStartedSpeakingFrame) IsSystem() bool        { return true }
func (f BotStartedSpeakingFrame) IsInterruptible() bool { return false }
func (f BotStartedSpeakingFrame) Clone() Frame          { return NewBotStartedSpeakingFrame() }

type BotStoppedSpeakingFrame struct {
	FrameBase
}

func NewBotStoppedSpeakingFrame() BotStoppedSpeakingFrame {
	return BotStoppedSpeakingFrame{FrameBase: FrameBase{Meta: newFrameMeta("BotStoppedSpeakingFrame")}}
}

func (f BotStoppedSpeakingFrame) FrameType() FrameType { return BotStoppedSpeaking }

// Pipecat models bot-speaking notifications as SystemFrame. This also keeps
// assistant commit timing ordered relative to played word timestamps.
func (f BotStoppedSpeakingFrame) IsSystem() bool        { return true }
func (f BotStoppedSpeakingFrame) IsInterruptible() bool { return false }
func (f BotStoppedSpeakingFrame) Clone() Frame          { return NewBotStoppedSpeakingFrame() }

type LLMMessagesFrame struct {
	FrameBase
	Messages []Message
}

func NewLLMMessagesFrame(messages []Message) LLMMessagesFrame {
	return LLMMessagesFrame{FrameBase: FrameBase{Meta: newFrameMeta("LLMMessagesFrame")}, Messages: messages}
}

func (f LLMMessagesFrame) FrameType() FrameType  { return LLMMessages }
func (f LLMMessagesFrame) IsSystem() bool        { return false }
func (f LLMMessagesFrame) IsInterruptible() bool { return true }

// ErrorFrame propagates an error upstream so the source can react
// (log + optionally end the task on Fatal). Mirrors Pipecat's ErrorFrame:
// pushed by BaseProcessor.PushError; routed to PipelineSourceProcessor
// which decides whether to call TaskContext.EndTask.
//
// ErrorFrame is a system frame (urgent priority) so it bubbles past any
// queued data frames. It survives interrupt purges because diagnostic
// errors should not be silently dropped.
type ErrorFrame struct {
	FrameBase
	Err       string // error message
	Fatal     bool   // if true, PipelineSource calls EndTask
	Processor string // name of the processor that pushed the error
}

func NewErrorFrame(processor, errMsg string, fatal bool) ErrorFrame {
	return ErrorFrame{
		FrameBase: FrameBase{Meta: newFrameMeta("ErrorFrame")},
		Err:       errMsg,
		Fatal:     fatal,
		Processor: processor,
	}
}

func (f ErrorFrame) FrameType() FrameType  { return Error }
func (f ErrorFrame) IsSystem() bool        { return true }
func (f ErrorFrame) IsInterruptible() bool { return false }

// LLMMessagesAppendFrame appends messages to the conversation context and
// optionally runs the LLM. Mirrors Pipecat's LLMMessagesAppendFrame.
// UserContextAggregator handles it: it appends Messages (if any) to its
// context, and when RunLLM is set it emits an LLMMessagesFrame for the
// current context. Pushing one with no Messages and RunLLM=true is how
// the bot takes the first turn (greet-first) from the initial context.
type LLMMessagesAppendFrame struct {
	FrameBase
	Messages []Message
	RunLLM   bool
}

func NewLLMMessagesAppendFrame(messages []Message, runLLM bool) LLMMessagesAppendFrame {
	return LLMMessagesAppendFrame{
		FrameBase: FrameBase{Meta: newFrameMeta("LLMMessagesAppendFrame")},
		Messages:  messages,
		RunLLM:    runLLM,
	}
}

func (f LLMMessagesAppendFrame) FrameType() FrameType  { return LLMMessagesAppend }
func (f LLMMessagesAppendFrame) IsSystem() bool        { return false }
func (f LLMMessagesAppendFrame) IsInterruptible() bool { return true }

// STTConnectFrame asks the STT processor to establish its provider
// websocket. Room transports enqueue it when a user joins so lazy STT
// connect stays in the normal pipeline path instead of a side callback.
type STTConnectFrame struct {
	FrameBase
	Reason string
	At     time.Time
}

func NewSTTConnectFrame(reason string, at time.Time) STTConnectFrame {
	if at.IsZero() {
		at = time.Now()
	}
	return STTConnectFrame{
		FrameBase: FrameBase{Meta: newFrameMeta("STTConnectFrame")},
		Reason:    reason,
		At:        at,
	}
}

func (f STTConnectFrame) FrameType() FrameType  { return STTConnect }
func (f STTConnectFrame) IsSystem() bool        { return true }
func (f STTConnectFrame) IsInterruptible() bool { return false }

type FunctionCallInProgressFrame struct {
	FrameBase
	FunctionName         string
	ToolCallID           string
	Arguments            map[string]any
	RawArguments         string
	CancelOnInterruption bool
}

func NewFunctionCallInProgressFrame(functionName, toolCallID string, arguments map[string]any, rawArguments string, cancelOnInterruption bool) FunctionCallInProgressFrame {
	return FunctionCallInProgressFrame{
		FrameBase:            FrameBase{Meta: newFrameMeta("FunctionCallInProgressFrame")},
		FunctionName:         functionName,
		ToolCallID:           toolCallID,
		Arguments:            arguments,
		RawArguments:         rawArguments,
		CancelOnInterruption: cancelOnInterruption,
	}
}

func (f FunctionCallInProgressFrame) FrameType() FrameType  { return FunctionCallInProgress }
func (f FunctionCallInProgressFrame) IsSystem() bool        { return false }
func (f FunctionCallInProgressFrame) IsInterruptible() bool { return false }

type FunctionCallResultFrame struct {
	FrameBase
	FunctionName string
	ToolCallID   string
	Arguments    map[string]any
	RawArguments string
	Result       string
	RunLLM       bool
}

func NewFunctionCallResultFrame(functionName, toolCallID string, arguments map[string]any, rawArguments, result string, runLLM bool) FunctionCallResultFrame {
	return FunctionCallResultFrame{
		FrameBase:    FrameBase{Meta: newFrameMeta("FunctionCallResultFrame")},
		FunctionName: functionName,
		ToolCallID:   toolCallID,
		Arguments:    arguments,
		RawArguments: rawArguments,
		Result:       result,
		RunLLM:       runLLM,
	}
}

func (f FunctionCallResultFrame) FrameType() FrameType  { return FunctionCallResult }
func (f FunctionCallResultFrame) IsSystem() bool        { return false }
func (f FunctionCallResultFrame) IsInterruptible() bool { return false }
