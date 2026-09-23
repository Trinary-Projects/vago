package voicepipelinecore

// init runs once for the test binary. It redirects the STT and TTS
// dial URLs to a definitely-unreachable loopback so background connect
// goroutines fail fast (connection refused) instead of actually dialing
// Soniox/Cartesia. STT caps retries; most tests cancel via EndFrame
// before the cap is reached.
func init() {
	sttDialURL = "ws://127.0.0.1:1/test-no-soniox"
	cartesiaSTTDialURL = "ws://127.0.0.1:1/test-no-cartesia-stt"
	ttsDialURL = "ws://127.0.0.1:1/test-no-cartesia?key="
}
