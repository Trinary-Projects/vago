package voicepipelinecore

import (
	"sync"
	"time"
)

// MetricLabel identifies the kind of measurement.
type MetricLabel string

const (
	MetricTTFB            MetricLabel = "ttfb"
	MetricProcessing      MetricLabel = "processing"
	MetricTextAggregation MetricLabel = "text_aggregation"
	MetricE2ELatency      MetricLabel = "e2e_latency"
	// MetricContextEnrich times a blocking pre-LLM context rewrite
	// (ContextEnricherProcessor). It is deliberately separate from the LLM's
	// own MetricTTFB: the enricher runs upstream of LLMProcessor, so
	// llm_ttfb_ms keeps measuring only the model's time to first token.
	MetricContextEnrich MetricLabel = "context_enrich"
)

// MetricsData is a single metric measurement.
type MetricsData struct {
	Processor string      `json:"processor"`
	Label     MetricLabel `json:"label"`
	ValueMs   float64     `json:"value_ms"` // milliseconds
}

// MetricsFrame carries metrics through the pipeline as a system frame.
// It is intercepted by the pipeline's Send function and never reaches processors.
type MetricsFrame struct {
	FrameBase
	Data       []MetricsData
	ResponseID int64
}

func NewMetricsFrame(data []MetricsData) MetricsFrame {
	return MetricsFrame{FrameBase: FrameBase{Meta: newFrameMeta("MetricsFrame")}, Data: data}
}

func (f MetricsFrame) FrameType() FrameType  { return MetricsType }
func (f MetricsFrame) IsSystem() bool        { return true }
func (f MetricsFrame) IsInterruptible() bool { return false }

// ProcessorMetrics is a lightweight helper for timing measurements.
// Embed in any processor that needs to emit metrics. Thread-safe.
type ProcessorMetrics struct {
	mu        sync.Mutex
	processor string
	timers    map[MetricLabel]time.Time
}

func NewProcessorMetrics(processor string) *ProcessorMetrics {
	return &ProcessorMetrics{
		processor: processor,
		timers:    make(map[MetricLabel]time.Time),
	}
}

// Start begins a timer for the given label using time.Now().
func (m *ProcessorMetrics) Start(label MetricLabel) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.timers[label] = time.Now()
}

// StartAt begins a timer for the given label using a provided timestamp.
func (m *ProcessorMetrics) StartAt(label MetricLabel, t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.timers[label] = t
}

// Stop ends the timer for the given label and returns a MetricsFrame.
// Returns nil if the timer was never started.
func (m *ProcessorMetrics) Stop(label MetricLabel) *MetricsFrame {
	m.mu.Lock()
	defer m.mu.Unlock()
	start, ok := m.timers[label]
	if !ok || start.IsZero() {
		return nil
	}
	valueMs := float64(time.Since(start).Microseconds()) / 1000.0
	delete(m.timers, label)
	mf := NewMetricsFrame([]MetricsData{{
		Processor: m.processor,
		Label:     label,
		ValueMs:   valueMs,
	}})
	return &mf
}

// Reset clears all pending timers (e.g., on interrupt).
func (m *ProcessorMetrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.timers = make(map[MetricLabel]time.Time)
}

type perTurnMetrics struct {
	mu      sync.Mutex
	current map[int64]TurnMetrics
}

func (m *perTurnMetrics) absorb(frame MetricsFrame) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil {
		m.current = make(map[int64]TurnMetrics)
	}
	current := m.current[frame.ResponseID]
	for _, d := range frame.Data {
		switch {
		case d.Processor == "llm" && d.Label == MetricTTFB:
			current.LLMTTFBMs = d.ValueMs
		case d.Processor == "llm" && d.Label == MetricProcessing:
			current.LLMProcessingMs = d.ValueMs
		case d.Processor == "tts" && d.Label == MetricTextAggregation:
			current.TTSTextAggregationMs = d.ValueMs
		case d.Processor == "tts" && d.Label == MetricTTFB:
			current.TTSTTFBMs = d.ValueMs
		case d.Processor == "playback" && d.Label == MetricE2ELatency:
			current.E2ELatencyMs = d.ValueMs
		}
	}
	m.current[frame.ResponseID] = current
}

func (m *perTurnMetrics) snapshotAndReset(id int64) TurnMetrics {
	if m == nil {
		return TurnMetrics{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := m.current[id]
	delete(m.current, id)
	return out
}
