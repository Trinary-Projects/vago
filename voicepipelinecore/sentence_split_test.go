package voicepipelinecore

import "testing"

func TestSplitSentences(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		wantSentences []string
		wantRemainder string
	}{
		{
			name:          "empty string",
			input:         "",
			wantSentences: nil,
			wantRemainder: "",
		},
		{
			name:          "no terminator at all",
			input:         "just some text with no ending",
			wantSentences: nil,
			wantRemainder: "just some text with no ending",
		},
		{
			name:          "multi-sentence split",
			input:         "One. Two. Three.",
			wantSentences: []string{"One.", "Two.", "Three."},
			wantRemainder: "",
		},
		{
			name:          "remainder handling",
			input:         "Done. Not done yet",
			wantSentences: []string{"Done."},
			wantRemainder: "Not done yet",
		},
		{
			name:          "terminator run",
			input:         "Really?! Are you sure?",
			wantSentences: []string{"Really?!", "Are you sure?"},
			wantRemainder: "",
		},
		{
			name:          "closing delimiter after period",
			input:         `He said "stop." Then left.`,
			wantSentences: []string{`He said "stop."`, "Then left."},
			wantRemainder: "",
		},
		{
			name:          "closing bracket",
			input:         "(See note.) Continue now.",
			wantSentences: []string{"(See note.)", "Continue now."},
			wantRemainder: "",
		},
		{
			name:          "devanagari danda",
			input:         "यह ठीक है। अगला वाक्य।",
			wantSentences: []string{"यह ठीक है।", "अगला वाक्य।"},
			wantRemainder: "",
		},
		{
			name:          "devanagari double danda",
			input:         "समाप्त॥ शुरुआत",
			wantSentences: []string{"समाप्त॥"},
			wantRemainder: "शुरुआत",
		},
		{
			name:          "ellipsis",
			input:         "Wait… What now?",
			wantSentences: []string{"Wait…", "What now?"},
			wantRemainder: "",
		},
		{
			name:          "whitespace consumption between sentences",
			input:         "One.   Two.",
			wantSentences: []string{"One.", "Two."},
			wantRemainder: "",
		},
		{
			name:          "no complete sentence, trailing terminator run mid-scan",
			input:         "just text",
			wantSentences: nil,
			wantRemainder: "just text",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotSentences, gotRemainder := splitSentences(tc.input)
			if !equalStringSlices(gotSentences, tc.wantSentences) {
				t.Errorf("splitSentences(%q) sentences = %#v, want %#v", tc.input, gotSentences, tc.wantSentences)
			}
			if gotRemainder != tc.wantRemainder {
				t.Errorf("splitSentences(%q) remainder = %q, want %q", tc.input, gotRemainder, tc.wantRemainder)
			}
		})
	}
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestEndsWithSentenceTerminator(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"empty string", "", false},
		{"no terminator", "hello there", false},
		{"plain period", "Done.", true},
		{"trailing whitespace after period", "Done. ", true},
		{"trailing closing quote", `He said "stop."`, true},
		{"trailing closing paren and whitespace", "(See note.) ", true},
		{"ellipsis", "Wait…", true},
		{"devanagari danda", "यह ठीक है।", true},
		{"devanagari double danda", "समाप्त॥", true},
		{"mid-sentence, no trailing terminator (the failure case an ends-with test would miss)", "One. Sta", false},
		{"only whitespace", "   ", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := endsWithSentenceTerminator(tc.input); got != tc.want {
				t.Errorf("endsWithSentenceTerminator(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}
