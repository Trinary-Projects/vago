package disha

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/jaideep329/talk-go/voicepipelinecore"
)

func TestNewSTTProcessor(t *testing.T) {
	control := "control"
	test := "test"
	empty := ""
	padded := "  test  "
	unknown := "some_unrecognized_value"

	testCases := []struct {
		name         string
		flag         *string
		wantProvider string
		wantWarning  bool
	}{
		{name: "nil", wantProvider: "soniox"},
		{name: "empty", flag: &empty, wantProvider: "soniox"},
		{name: "control", flag: &control, wantProvider: "soniox"},
		{name: "test", flag: &test, wantProvider: "cartesia"},
		{name: "whitespace_trimmed", flag: &padded, wantProvider: "cartesia"},
		{name: "unknown_defaults_to_control", flag: &unknown, wantProvider: "soniox", wantWarning: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := log.New(&logs, "", 0)
			processor := newSTTProcessor(&voicepipelinecore.TaskContext{Logger: logger}, tc.flag, logger)
			t.Cleanup(processor.Stop)
			if tc.wantProvider == "cartesia" {
				if _, ok := processor.(*voicepipelinecore.CartesiaSTTProcessor); !ok {
					t.Fatalf("processor = %T, want *voicepipelinecore.CartesiaSTTProcessor", processor)
				}
			} else if _, ok := processor.(*voicepipelinecore.STTProcessor); !ok {
				t.Fatalf("processor = %T, want *voicepipelinecore.STTProcessor", processor)
			}
			wantLog := fmt.Sprintf("STT provider selected provider=%s variant=%q", tc.wantProvider, derefString(tc.flag))
			if !strings.Contains(logs.String(), wantLog) || strings.Count(logs.String(), "STT provider selected") != 1 {
				t.Errorf("selection log = %q, want exactly one %q", logs.String(), wantLog)
			}
			if got := strings.Contains(logs.String(), "unknown cartesia_call_stt_variant_flag"); got != tc.wantWarning {
				t.Errorf("warning present = %v, want %v", got, tc.wantWarning)
			}
		})
	}
}

func TestNewSTTProcessorNilLoggerSafe(t *testing.T) {
	unknown := "unrecognized"
	processor := newSTTProcessor(&voicepipelinecore.TaskContext{}, &unknown, nil)
	t.Cleanup(processor.Stop)
	if _, ok := processor.(*voicepipelinecore.STTProcessor); !ok {
		t.Fatalf("processor with nil logger = %T, want *voicepipelinecore.STTProcessor", processor)
	}
}

func TestCartesiaCallSTTVariantFlagDecode(t *testing.T) {
	for _, payload := range []string{`{}`, `{"cartesia_call_stt_variant_flag":null}`, `{"cartesia_call_stt_variant_flag":"test"}`} {
		t.Run(payload, func(t *testing.T) {
			var profile UserProfileData
			if err := json.Unmarshal([]byte(payload), &profile); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(payload, `"test"`) {
				if profile.CartesiaCallSTTVariantFlag == nil || *profile.CartesiaCallSTTVariantFlag != "test" {
					t.Fatal("test variant did not decode")
				}
			} else if profile.CartesiaCallSTTVariantFlag != nil {
				t.Fatal("missing/null flag must decode as nil")
			}
		})
	}
}
