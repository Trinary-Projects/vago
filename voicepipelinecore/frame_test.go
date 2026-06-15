package voicepipelinecore

import (
	"testing"
)

// TestFrameMeta_AutoID verifies that constructor-built frames get
// monotonically increasing IDs and the right name.
func TestFrameMeta_AutoID(t *testing.T) {
	a := NewTextFrame("a")
	b := NewTextFrame("b")
	c := NewEndFrame("done")

	if a.ID() == 0 {
		t.Error("expected non-zero auto-ID for constructor-built frame")
	}
	if b.ID() <= a.ID() {
		t.Errorf("expected b.ID > a.ID, got a=%d b=%d", a.ID(), b.ID())
	}
	if c.ID() <= b.ID() {
		t.Errorf("expected c.ID > b.ID, got b=%d c=%d", b.ID(), c.ID())
	}
	if a.Name() != "TextFrame" {
		t.Errorf("expected Name=TextFrame, got %q", a.Name())
	}
	if c.Name() != "EndFrame" {
		t.Errorf("expected Name=EndFrame, got %q", c.Name())
	}
}

// TestFrameMeta_LiteralConstructionAllowed verifies that struct-literal
// construction (used in tests) still compiles and returns zero meta —
// the constructor is for production code that wants observability.
func TestFrameMeta_LiteralConstructionAllowed(t *testing.T) {
	lit := TextFrame{Text: "literal"}
	if lit.ID() != 0 {
		t.Errorf("expected zero ID for literal-constructed frame, got %d", lit.ID())
	}
	if lit.Name() != "" {
		t.Errorf("expected empty Name for literal-constructed frame, got %q", lit.Name())
	}
}

// TestFrameMeta_String verifies the human-readable form used in logs.
func TestFrameMeta_String(t *testing.T) {
	f := NewEndFrame("done")
	got := f.String()
	want := "EndFrame#"
	if len(got) < len(want) || got[:len(want)] != want {
		t.Errorf("expected String to start with %q, got %q", want, got)
	}
}

// TestErrorFrame_Construction verifies ErrorFrame is built with all
// the expected fields and is system-priority.
func TestErrorFrame_Construction(t *testing.T) {
	ef := NewErrorFrame("STT", "websocket dead", true)
	if ef.Processor != "STT" {
		t.Errorf("Processor: got %q want %q", ef.Processor, "STT")
	}
	if ef.Err != "websocket dead" {
		t.Errorf("Err: got %q", ef.Err)
	}
	if !ef.Fatal {
		t.Error("expected Fatal=true")
	}
	if !ef.IsSystem() {
		t.Error("ErrorFrame should be a system frame")
	}
	if ef.IsInterruptible() {
		t.Error("ErrorFrame should survive interrupt purges")
	}
}

func TestPostPlaybackFramesUseSystemPriority(t *testing.T) {
	frames := []struct {
		name  string
		frame Frame
	}{
		{name: "WordTimestampFrame", frame: NewWordTimestampFrame([]string{"hi"})},
		{name: "TTSDoneFrame", frame: NewTTSDoneFrame()},
		{name: "BotStartedSpeakingFrame", frame: NewBotStartedSpeakingFrame()},
		{name: "BotStoppedSpeakingFrame", frame: NewBotStoppedSpeakingFrame()},
	}

	for _, tc := range frames {
		if !tc.frame.IsSystem() {
			t.Errorf("%s should be system-priority", tc.name)
		}
		if tc.frame.IsInterruptible() {
			t.Errorf("%s should survive interrupt purges", tc.name)
		}
	}
}
