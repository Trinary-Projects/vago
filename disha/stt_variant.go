package disha

import (
	"log"
	"strings"

	"github.com/jaideep329/talk-go/voicepipelinecore"
)

// newSTTProcessor selects the provider from cartesia_call_stt_variant_flag.
// Like the TTS experiment, nil/empty/"control" and unknown values use control.
func newSTTProcessor(taskCtx *voicepipelinecore.TaskContext, flag *string, logger *log.Logger) voicepipelinecore.Processor {
	rawValue := derefString(flag)
	value := strings.TrimSpace(rawValue)
	provider := "soniox"
	switch value {
	case "", "control":
	case "test":
		provider = "cartesia"
	default:
		if logger != nil {
			logger.Printf("disha: unknown cartesia_call_stt_variant_flag %q, defaulting to %s\n", value, provider)
		}
	}
	if logger != nil {
		logger.Printf("disha: STT provider selected provider=%s variant=%q\n", provider, rawValue)
	}
	if provider == "cartesia" {
		return voicepipelinecore.NewCartesiaSTTProcessor(taskCtx)
	}
	return voicepipelinecore.NewSTTProcessor(taskCtx)
}
