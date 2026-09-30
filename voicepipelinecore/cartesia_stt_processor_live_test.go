package voicepipelinecore

import (
	"os"
	"testing"
	"time"
)

// TestCartesiaSTTLive is an opt-in integration test that exercises the
// production CartesiaSTTProcessor against the real Cartesia STT endpoint.
// It is never run as part of the default suite: init() in
// test_setup_test.go redirects cartesiaSTTDialURL to an unreachable
// loopback, so we must restore the real endpoint here explicitly.
//
// Run with:
//
//	set -a; source .env; set +a
//	CARTESIA_STT_LIVE_TEST=1 go test ./voicepipelinecore/ -run TestCartesiaSTTLive -v
func TestCartesiaSTTLive(t *testing.T) {
	if os.Getenv("CARTESIA_STT_LIVE_TEST") != "1" || os.Getenv("CARTESIA_API_KEY") == "" {
		t.Skip("set CARTESIA_STT_LIVE_TEST=1 and CARTESIA_API_KEY to run the live Cartesia STT test")
	}

	const clipPath = "/tmp/ink-probe/en16k.raw"
	pcm, err := os.ReadFile(clipPath)
	if err != nil {
		t.Skipf("skipping: could not read live test clip %s: %v", clipPath, err)
	}

	// Point at the real Cartesia endpoint; test_setup_test.go's init()
	// redirected this package var to an unreachable loopback for the rest
	// of the suite.
	oldURL := cartesiaSTTDialURL
	cartesiaSTTDialURL = "wss://api.cartesia.ai/stt/turns/websocket"
	t.Cleanup(func() { cartesiaSTTDialURL = oldURL })

	fix := newTestFixture(t)
	p, source, sink := startCartesiaSTTTest(t, fix)

	const frameBytes = 640 // 20ms of s16le/16kHz/mono.
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	firstAudioAt := time.Now()
	p.QueueFrame(NewSTTConnectFrame("user_joined", firstAudioAt), Downstream)

	sendFrame := func(chunk []byte) {
		buf := make([]byte, frameBytes)
		copy(buf, chunk)
		p.QueueFrame(NewAudioFrame(buf), Downstream)
		<-ticker.C
	}

	for off := 0; off < len(pcm); off += frameBytes {
		end := off + frameBytes
		if end > len(pcm) {
			end = len(pcm)
		}
		sendFrame(pcm[off:end])
	}

	// ~3s of trailing silence so the server has room to emit turn.end.
	silence := make([]byte, frameBytes)
	for i := 0; i < 150; i++ {
		sendFrame(silence)
	}

	p.QueueFrame(NewEndFrame("live-test-done"), Downstream)
	waitForFrameType[EndFrame](t, sink.Captured, 10*time.Second)
	stopProcessorsAndWait(t, fix, 10*time.Second, source, p, sink)

	frames := sink.Captured()
	errFrames := countFrames[ErrorFrame](source.Captured())
	var interims, finals int
	var lastFinalText string
	var lastFinalRID int
	var sawEndSharingFinalRID bool
	seenInterimRIDs := map[int]bool{}
	dupInterimRID := false

	for _, f := range frames {
		tf, ok := f.(TranscriptFrame)
		if !ok {
			continue
		}
		elapsed := time.Since(firstAudioAt).Milliseconds()
		t.Logf("t=%dms final=%v rid=%d text=%q", elapsed, tf.IsFinal, tf.ResponseID, tf.Text)
		if !tf.IsFinal {
			interims++
			if seenInterimRIDs[tf.ResponseID] {
				dupInterimRID = true
			}
			seenInterimRIDs[tf.ResponseID] = true
			continue
		}
		if tf.Text == "<end>" {
			if tf.ResponseID == lastFinalRID {
				sawEndSharingFinalRID = true
			}
			continue
		}
		finals++
		lastFinalText = tf.Text
		lastFinalRID = tf.ResponseID
	}

	t.Logf("upstream ErrorFrame count = %d", errFrames)

	if interims == 0 {
		t.Error("expected at least one interim (IsFinal=false) frame")
	}
	if finals == 0 || lastFinalText == "" {
		t.Error("expected at least one final frame with non-empty text")
	}
	if !sawEndSharingFinalRID {
		t.Error("expected a final <end> frame sharing the ResponseID of the preceding final text frame")
	}
	if dupInterimRID {
		t.Error("consecutive interim frames must have distinct ResponseIDs")
	}
}
