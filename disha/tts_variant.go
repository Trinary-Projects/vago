package disha

import (
	"log"
	"strings"
)

// call_tts_variant_flag values from conversation_data.user_profile, and the
// Cartesia model ids each maps to. Voice id and language are unaffected by
// this flag and stay exactly as they are.
const (
	ttsModelControl = "sonic-3"
	ttsModelTest1   = "sonic-3.5"
	ttsModelTest2   = "sonic-3.6"
)

// resolveCartesiaModel picks the Cartesia TTS model id from
// call_tts_variant_flag, matching the loadSalesPrompt pattern of switching on
// a Disha A/B flag. nil/empty/"control" resolves to the current default;
// an unrecognized value logs a warning and falls back to the default rather
// than failing the call.
func resolveCartesiaModel(flag *string, logger *log.Logger) string {
	value := strings.TrimSpace(derefString(flag))
	switch value {
	case "", "control":
		return ttsModelControl
	case "test1":
		return ttsModelTest1
	case "test2":
		return ttsModelTest2
	default:
		if logger != nil {
			logger.Printf("disha: unknown call_tts_variant_flag %q, defaulting to %s\n", value, ttsModelControl)
		}
		return ttsModelControl
	}
}
