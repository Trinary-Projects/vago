package voicepipelinecore

import "testing"

func TestPerTurnMetricsAbsorbSnapshotAndReset(t *testing.T) {
	m := &perTurnMetrics{}
	m.absorb(NewMetricsFrame([]MetricsData{
		{Processor: "llm", Label: MetricTTFB, ValueMs: 11},
		{Processor: "llm", Label: MetricProcessing, ValueMs: 22},
		{Processor: "tts", Label: MetricTextAggregation, ValueMs: 33},
		{Processor: "tts", Label: MetricTTFB, ValueMs: 44},
		{Processor: "playback", Label: MetricE2ELatency, ValueMs: 55},
	}))

	got := m.snapshotAndReset()
	if got.LLMTTFBMs != 11 ||
		got.LLMProcessingMs != 22 ||
		got.TTSTextAggregationMs != 33 ||
		got.TTSTTFBMs != 44 ||
		got.E2ELatencyMs != 55 {
		t.Fatalf("unexpected turn metrics: %+v", got)
	}

	if got := m.snapshotAndReset(); got != (TurnMetrics{}) {
		t.Fatalf("metrics should reset after snapshot, got %+v", got)
	}
}
