package voicepipelinecore

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lkmedia "github.com/livekit/media-sdk"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
	lkpcm "github.com/livekit/server-sdk-go/v2/pkg/media"
	"github.com/pion/webrtc/v4"
	pionmedia "github.com/pion/webrtc/v4/pkg/media"
	"gopkg.in/hraban/opus.v2"

	"github.com/jaideep329/talk-go/internal/perf"
	"github.com/jaideep329/talk-go/internal/sentryutil"
)

const (
	liveKitJoinRetries    = 3
	liveKitJoinRetryDelay = time.Second
	liveKitBotIdentity    = "talk-go-bot"

	liveKitOutCodecEnv         = "LIVEKIT_OUT_CODEC"
	liveKitOpusComplexityEnv   = "LIVEKIT_OPUS_COMPLEXITY"
	liveKitOpusBitrateEnv      = "LIVEKIT_OPUS_BITRATE"
	liveKitOpusMaxBandwidthEnv = "LIVEKIT_OPUS_MAX_BANDWIDTH"
)

type liveKitOutputCodec string

const (
	liveKitOutputCodecOpus liveKitOutputCodec = "opus"
	liveKitOutputCodecPCMU liveKitOutputCodec = "pcmu"
)

// LiveKitRoom owns the Go-native LiveKit media/signalling connection.
// It publishes bot PCM as an Opus microphone track, decodes remote Opus
// microphone tracks to 16kHz mono PCM for Soniox, and carries RTVI
// messages over LiveKit reliable data packets.
type LiveKitRoom struct {
	roomURL     string
	roomName    string
	taskCtx     *TaskContext
	audioSource *AudioSourceProcessor

	room     *lksdk.Room
	outTrack *lksdk.LocalTrack
	outCodec liveKitOutputCodec

	opusMu        sync.Mutex
	opusEncoder   *opus.Encoder
	opusEncodeBuf []byte

	audioTiming *audioTimingAggregator
	perfDiag    bool
	closed      atomic.Bool
	closedCh    chan struct{}
	greetOnce   sync.Once

	tracksMu sync.Mutex
	tracks   map[string]*lkpcm.PCMRemoteTrack
}

func JoinLiveKitRoom(roomURL, roomName, token string, taskCtx *TaskContext, audioSource *AudioSourceProcessor) (*LiveKitRoom, error) {
	roomURL = strings.TrimSpace(roomURL)
	roomName = strings.TrimSpace(roomName)
	token = strings.TrimSpace(token)
	if roomURL == "" {
		return nil, errors.New("livekit url is required")
	}
	if roomName == "" {
		return nil, errors.New("livekit room name is required")
	}
	if token == "" {
		return nil, errors.New("livekit token is required")
	}

	var lastErr error
	for attempt := 1; attempt <= liveKitJoinRetries; attempt++ {
		room, err := startLiveKitRoomAttempt(roomURL, roomName, token, taskCtx, audioSource)
		if err == nil {
			return room, nil
		}
		lastErr = err
		if taskCtx != nil && taskCtx.Logger != nil {
			taskCtx.Logger.Printf("LiveKit join attempt %d/%d failed: %v\n", attempt, liveKitJoinRetries, err)
		}
		if attempt < liveKitJoinRetries {
			done := taskDone(taskCtx)
			select {
			case <-time.After(liveKitJoinRetryDelay):
			case <-done:
				return nil, taskCtx.Ctx.Err()
			}
		}
	}
	err := fmt.Errorf("failed to join LiveKit room after %d attempts: %w", liveKitJoinRetries, lastErr)
	sentryutil.Capture(sentryutil.Event{
		Err:  err,
		Tags: map[string]string{"component": "livekit", "operation": "join"},
		Details: map[string]any{
			"room_url":  roomURL,
			"room_name": roomName,
			"attempts":  liveKitJoinRetries,
		},
	})
	return nil, err
}

func startLiveKitRoomAttempt(roomURL, roomName, token string, taskCtx *TaskContext, audioSource *AudioSourceProcessor) (*LiveKitRoom, error) {
	outCodec, err := liveKitOutputCodecFromEnv()
	if err != nil {
		return nil, err
	}
	r := &LiveKitRoom{
		roomURL:     roomURL,
		roomName:    roomName,
		taskCtx:     taskCtx,
		audioSource: audioSource,
		outCodec:    outCodec,
		perfDiag:    perf.Enabled(),
		closedCh:    make(chan struct{}),
		tracks:      make(map[string]*lkpcm.PCMRemoteTrack),
	}
	if r.perfDiag {
		r.audioTiming = newAudioTimingAggregator()
	}

	cb := lksdk.NewRoomCallback()
	cb.OnParticipantConnected = func(p *lksdk.RemoteParticipant) {
		r.markUserJoined(r.participantID(p))
	}
	cb.OnParticipantDisconnected = func(p *lksdk.RemoteParticipant) {
		participantID := r.participantID(p)
		r.markUserLeft(participantID)
		r.log("[%s] LiveKit participant %q left, requesting EndFrame", r.roomName, participantID)
		if r.taskCtx != nil {
			r.taskCtx.UIEvents.ServerMessage("Participant left", time.Now())
		}
		r.endTask(EndReasonClientDisconnect)
	}
	cb.OnDataPacket = func(data lksdk.DataPacket, params lksdk.DataReceiveParams) {
		switch packet := data.(type) {
		case *lksdk.UserDataPacket:
			r.handleAppMessageBytes(packet.Payload)
		}
	}
	cb.OnTrackSubscribed = func(track *webrtc.TrackRemote, publication *lksdk.RemoteTrackPublication, p *lksdk.RemoteParticipant) {
		if track == nil || track.Kind() != webrtc.RTPCodecTypeAudio || track.Codec().MimeType != webrtc.MimeTypeOpus {
			return
		}
		r.subscribeAudioTrack(track, p)
	}
	cb.OnTrackUnsubscribed = func(track *webrtc.TrackRemote, publication *lksdk.RemoteTrackPublication, p *lksdk.RemoteParticipant) {
		if track == nil {
			return
		}
		r.closeRemoteTrack(track.ID())
	}

	room, err := lksdk.ConnectToRoomWithToken(roomURL, token, cb)
	if err != nil {
		return nil, fmt.Errorf("connect livekit room: %w", err)
	}
	r.room = room

	outTrack, err := lksdk.NewLocalTrack(r.outCodecCapability())
	if err != nil {
		r.Disconnect()
		return nil, fmt.Errorf("create livekit %s track: %w", r.outCodec, err)
	}
	r.outTrack = outTrack
	if r.outCodec == liveKitOutputCodecOpus {
		// Let Pion bind the negotiated Opus clock/channels/fmtp. WebRTC Opus is
		// commonly advertised as audio/opus/48000/2 even when the payload is mono.
		if err := r.resetOpusEncoder(); err != nil {
			r.Disconnect()
			return nil, fmt.Errorf("create livekit opus encoder: %w", err)
		}
	}
	if _, err := room.LocalParticipant.PublishTrack(outTrack, &lksdk.TrackPublicationOptions{
		Name:   "talk-go-audio",
		Source: livekit.TrackSource_MICROPHONE,
	}); err != nil {
		r.Disconnect()
		return nil, fmt.Errorf("publish livekit %s track: %w", r.outCodec, err)
	}

	at := time.Now()
	localID := firstNonEmptyString(room.LocalParticipant.SID(), room.LocalParticipant.Identity())
	r.log("[%s] Bot joined LiveKit room participant_id=%s", roomName, localID)
	if taskCtx != nil {
		taskCtx.UIEvents.SetTransportSession("livekit", roomName, localID)
	}
	if taskCtx != nil && taskCtx.callEvents != nil {
		taskCtx.callEvents.fireBotJoined(at)
	}

	for _, participant := range room.GetRemoteParticipants() {
		r.markUserJoined(r.participantID(participant))
	}
	if r.perfDiag {
		r.goTracked(func() {
			r.monitorProcessUsage(os.Getpid())
		})
		r.goTracked(r.monitorAudioTiming)
	}
	return r, nil
}

func (r *LiveKitRoom) RoomURL() string {
	return r.roomURL
}

func (r *LiveKitRoom) RoomName() string {
	return r.roomName
}

func (r *LiveKitRoom) OutputSampleRate() int {
	if r != nil && r.outCodec == liveKitOutputCodecPCMU {
		return liveKitPCMUOutputRate
	}
	return defaultOutputSampleRate
}

func (r *LiveKitRoom) SendAppMessage(v interface{}) error {
	if r == nil || r.room == nil || r.closed.Load() {
		return nil
	}
	var raw []byte
	switch value := v.(type) {
	case []byte:
		raw = value
	case string:
		raw = []byte(value)
	default:
		var err error
		if r.perfDiagnosticsEnabled() {
			start := time.Now()
			raw, err = json.Marshal(value)
			r.recordAudioTiming("go_livekit_data_json_marshal", time.Since(start))
		} else {
			raw, err = json.Marshal(value)
		}
		if err != nil {
			return err
		}
	}
	if r.perfDiagnosticsEnabled() {
		start := time.Now()
		err := r.room.LocalParticipant.PublishDataPacket(lksdk.UserData(raw), lksdk.WithDataPublishReliable(true))
		r.recordAudioTiming("go_livekit_data_publish", time.Since(start))
		return err
	}
	return r.room.LocalParticipant.PublishDataPacket(lksdk.UserData(raw), lksdk.WithDataPublishReliable(true))
}

func (r *LiveKitRoom) WriteAudioPCM(pcm []byte) error {
	if r == nil || r.outTrack == nil || len(pcm) == 0 || r.closed.Load() {
		return nil
	}
	if r.outCodec == liveKitOutputCodecPCMU {
		return r.writeAudioPCMU(pcm)
	}
	return r.writeAudioOpus(pcm)
}

func (r *LiveKitRoom) writeAudioOpus(pcm []byte) error {
	if len(pcm)%2 != 0 {
		pcm = pcm[:len(pcm)-1]
	}
	r.opusMu.Lock()
	defer r.opusMu.Unlock()
	if r.opusEncoder == nil {
		if err := r.resetOpusEncoderLocked(); err != nil {
			return err
		}
	}
	var sample []int16
	if r.perfDiagnosticsEnabled() {
		start := time.Now()
		sample = pcmBytesToLiveKitSample(pcm)
		r.recordAudioTiming("go_livekit_out_pcm_bytes_to_samples", time.Since(start))

		start = time.Now()
		encodedLen, err := r.opusEncoder.Encode(sample, r.opusEncodeBuf)
		r.recordAudioTiming("go_livekit_out_opus_encode", time.Since(start))
		if err != nil {
			return err
		}

		start = time.Now()
		err = r.outTrack.WriteSample(pionmedia.Sample{Data: r.opusEncodeBuf[:encodedLen], Duration: playbackFrameDuration}, nil)
		r.recordAudioTiming("go_livekit_out_write_sample", time.Since(start))
		return err
	}
	sample = pcmBytesToLiveKitSample(pcm)
	encodedLen, err := r.opusEncoder.Encode(sample, r.opusEncodeBuf)
	if err != nil {
		return err
	}
	return r.outTrack.WriteSample(pionmedia.Sample{Data: r.opusEncodeBuf[:encodedLen], Duration: playbackFrameDuration}, nil)
}

func (r *LiveKitRoom) writeAudioPCMU(pcm []byte) error {
	if r.perfDiagnosticsEnabled() {
		start := time.Now()
		payload := pcm16BytesToPCMU(pcm)
		r.recordAudioTiming("go_livekit_out_pcm16_to_pcmu", time.Since(start))

		start = time.Now()
		err := r.outTrack.WriteSample(pionmedia.Sample{Data: payload, Duration: playbackFrameDuration}, nil)
		r.recordAudioTiming("go_livekit_out_write_sample", time.Since(start))
		return err
	}
	payload := pcm16BytesToPCMU(pcm)
	return r.outTrack.WriteSample(pionmedia.Sample{Data: payload, Duration: playbackFrameDuration}, nil)
}

func (r *LiveKitRoom) ClearAudioBuffer() {
	if r == nil {
		return
	}
	if r.outCodec == liveKitOutputCodecPCMU {
		return
	}
	r.opusMu.Lock()
	defer r.opusMu.Unlock()
	if err := r.resetOpusEncoderLocked(); err != nil {
		r.log("[%s] LiveKit opus encoder reset error: %v", r.roomName, err)
		sentryutil.Capture(sentryutil.Event{
			Err:  err,
			Tags: map[string]string{"component": "livekit", "operation": "opus_encoder_reset"},
		})
	}
}

func (r *LiveKitRoom) Disconnect() {
	if r == nil || !r.closed.CompareAndSwap(false, true) {
		return
	}
	close(r.closedCh)
	r.ClearAudioBuffer()
	r.tracksMu.Lock()
	for id, track := range r.tracks {
		track.Close()
		delete(r.tracks, id)
	}
	r.tracksMu.Unlock()
	if r.outTrack != nil {
		_ = r.outTrack.Close()
	}
	if r.room != nil {
		r.room.Disconnect()
	}
}

func (r *LiveKitRoom) subscribeAudioTrack(track *webrtc.TrackRemote, p *lksdk.RemoteParticipant) {
	participantID := ""
	if p != nil {
		participantID = r.participantID(p)
	}
	writer := &liveKitPCMWriter{room: r, participantID: participantID}
	var pcmTrack *lkpcm.PCMRemoteTrack
	var err error
	if r.perfDiagnosticsEnabled() {
		start := time.Now()
		pcmTrack, err = lkpcm.NewPCMRemoteTrack(
			track,
			writer,
			lkpcm.WithTargetSampleRate(16000),
			lkpcm.WithTargetChannels(1),
		)
		r.recordAudioTiming("go_livekit_in_pcm_remote_track_create", time.Since(start))
	} else {
		pcmTrack, err = lkpcm.NewPCMRemoteTrack(
			track,
			writer,
			lkpcm.WithTargetSampleRate(16000),
			lkpcm.WithTargetChannels(1),
		)
	}
	if err != nil {
		r.log("[%s] LiveKit audio track subscribe error participant=%q: %v", r.roomName, participantID, err)
		sentryutil.Capture(sentryutil.Event{
			Err:  err,
			Tags: map[string]string{"component": "livekit", "operation": "track_subscribe"},
			Details: map[string]any{
				"room_name":      r.roomName,
				"participant_id": participantID,
				"track_id":       track.ID(),
			},
		})
		return
	}
	r.tracksMu.Lock()
	r.tracks[track.ID()] = pcmTrack
	r.tracksMu.Unlock()
	r.log("[%s] LiveKit audio track subscribed participant=%q track=%s", r.roomName, participantID, track.ID())
}

func (r *LiveKitRoom) closeRemoteTrack(trackID string) {
	r.tracksMu.Lock()
	defer r.tracksMu.Unlock()
	if track := r.tracks[trackID]; track != nil {
		track.Close()
		delete(r.tracks, trackID)
	}
}

func (r *LiveKitRoom) handleAppMessageBytes(raw []byte) {
	if len(raw) == 0 {
		return
	}
	r.handleAppMessage(json.RawMessage(raw))
}

func (r *LiveKitRoom) handleAppMessage(raw json.RawMessage) {
	var msg dailyAppMessage
	var err error
	if r.perfDiagnosticsEnabled() {
		start := time.Now()
		err = json.Unmarshal(raw, &msg)
		r.recordAudioTiming("go_livekit_data_json_unmarshal", time.Since(start))
	} else {
		err = json.Unmarshal(raw, &msg)
	}
	if err != nil {
		r.log("[%s] LiveKit data message malformed: %v", r.roomName, err)
		return
	}
	if msg.Type == "end_call" {
		r.log("[%s] UI requested end_call via LiveKit data message", r.roomName)
		r.endTask(EndReasonClientDisconnect)
		return
	}
	if msg.Type == "ping" {
		r.sendLegacyPong()
		return
	}
	if msg.Label == rtviDebugLabel && msg.Type == "client-ready" {
		r.sendRTVIBotReady(msg.ID)
		return
	}
	if msg.Label != rtviDebugLabel || msg.Type != "client-message" {
		return
	}
	var data rtviClientMessageData
	if r.perfDiagnosticsEnabled() {
		start := time.Now()
		err = json.Unmarshal(msg.Data, &data)
		r.recordAudioTiming("go_livekit_rtvi_client_data_unmarshal", time.Since(start))
	} else {
		err = json.Unmarshal(msg.Data, &data)
	}
	if err != nil {
		r.log("[%s] RTVI client-message malformed: %v", r.roomName, err)
		return
	}
	switch data.T {
	case "ping":
		r.sendRTVIServerResponse(msg.ID, data.T, "pong")
	}
}

func (r *LiveKitRoom) sendLegacyPong() {
	if err := r.SendAppMessage(map[string]any{"type": "pong"}); err != nil {
		r.log("[%s] LiveKit pong publish error: %v", r.roomName, err)
		sentryutil.Capture(sentryutil.Event{
			Err:  err,
			Tags: map[string]string{"component": "livekit", "operation": "legacy_pong"},
		})
	}
}

func (r *LiveKitRoom) sendRTVIServerResponse(id, messageType string, data any) {
	id = rtviMessageID(id)
	resp := map[string]any{
		"label": rtviDebugLabel,
		"type":  "server-response",
		"id":    id,
		"data": map[string]any{
			"t": messageType,
			"d": data,
		},
	}
	if err := r.SendAppMessage(resp); err != nil {
		r.log("[%s] RTVI server-response publish error: %v", r.roomName, err)
		sentryutil.Capture(sentryutil.Event{
			Err:  err,
			Tags: map[string]string{"component": "livekit", "operation": "rtvi_server_response"},
			Details: map[string]any{
				"id":   id,
				"type": messageType,
			},
		})
	}
}

func (r *LiveKitRoom) sendRTVIBotReady(id string) {
	id = rtviMessageID(id)
	msg := map[string]any{
		"label": rtviDebugLabel,
		"type":  "bot-ready",
		"id":    id,
		"data": map[string]any{
			"version": rtviProtocolVersion,
			"about": map[string]any{
				"library": "talk-go",
			},
		},
	}
	if err := r.SendAppMessage(msg); err != nil {
		r.log("[%s] RTVI bot-ready publish error: %v", r.roomName, err)
		sentryutil.Capture(sentryutil.Event{
			Err:  err,
			Tags: map[string]string{"component": "livekit", "operation": "rtvi_bot_ready"},
			Details: map[string]any{
				"id": id,
			},
		})
	}
}

func (r *LiveKitRoom) markUserJoined(participantID string) {
	if participantID == "" || participantID == liveKitBotIdentity {
		return
	}
	at := time.Now()
	r.log("[%s] LiveKit participant %q joined", r.roomName, participantID)
	if r.taskCtx == nil {
		return
	}
	r.taskCtx.UIEvents.SetUserSessionID(participantID)
	if r.taskCtx.callStats != nil {
		r.taskCtx.callStats.MarkUserJoined(at)
	}
	if r.taskCtx.callEvents != nil {
		r.taskCtx.callEvents.fireUserJoined(at)
	}
	if r.audioSource != nil {
		r.greetOnce.Do(func() {
			r.log("[%s] User joined; starting bot greeting (first turn)", r.roomName)
			r.audioSource.QueueFrame(NewLLMMessagesAppendFrame(nil, true), Downstream)
		})
	}
}

func (r *LiveKitRoom) markUserLeft(participantID string) {
	if participantID == "" || r.taskCtx == nil || r.taskCtx.callStats == nil {
		return
	}
	r.taskCtx.callStats.MarkUserLeft(time.Now())
}

func (r *LiveKitRoom) endTask(reason EndReason) {
	if r.taskCtx != nil && r.taskCtx.EndTask != nil {
		r.taskCtx.EndTask(reason)
	}
}

func (r *LiveKitRoom) participantID(p *lksdk.RemoteParticipant) string {
	if p == nil {
		return ""
	}
	return firstNonEmptyString(p.SID(), p.Identity())
}

func (r *LiveKitRoom) recordAudioTiming(name string, elapsed time.Duration) {
	if r == nil || r.audioTiming == nil {
		return
	}
	r.audioTiming.record(name, elapsed)
}

func (r *LiveKitRoom) perfDiagnosticsEnabled() bool {
	return r != nil && r.perfDiag && r.audioTiming != nil
}

func (r *LiveKitRoom) monitorAudioTiming() {
	ticker := time.NewTicker(audioTimingLogInterval)
	defer ticker.Stop()

	done := taskDone(r.taskCtx)
	for {
		select {
		case <-ticker.C:
			r.emitAudioTiming()
		case <-r.closedCh:
			r.emitAudioTiming()
			return
		case <-done:
			r.emitAudioTiming()
			return
		}
	}
}

func (r *LiveKitRoom) emitAudioTiming() {
	if r == nil || r.audioTiming == nil {
		return
	}
	entries := r.audioTiming.snapshotAndReset()
	if len(entries) == 0 {
		return
	}
	raw, err := json.Marshal(audioTimingLog{
		Event:    "audio_timing",
		RoomName: r.roomName,
		Timings:  entries,
	})
	if err != nil {
		r.log("audio_timing marshal error: %v", err)
		return
	}
	r.log("audio_timing %s", raw)
}

func (r *LiveKitRoom) goTracked(fn func()) {
	if r.taskCtx != nil && r.taskCtx.wg != nil {
		r.taskCtx.wg.Add(1)
		go func() {
			defer r.taskCtx.wg.Done()
			fn()
		}()
		return
	}
	go fn()
}

func (r *LiveKitRoom) log(format string, args ...interface{}) {
	if r.taskCtx != nil && r.taskCtx.Logger != nil {
		r.taskCtx.Logger.Printf(format, args...)
	}
}

func (r *LiveKitRoom) resetOpusEncoder() error {
	r.opusMu.Lock()
	defer r.opusMu.Unlock()
	return r.resetOpusEncoderLocked()
}

func liveKitOutputCodecFromEnv() (liveKitOutputCodec, error) {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv(liveKitOutCodecEnv)))
	switch raw {
	case "", string(liveKitOutputCodecOpus):
		return liveKitOutputCodecOpus, nil
	case string(liveKitOutputCodecPCMU), "g711", "g711u", "mulaw", "mu-law", "ulaw", "u-law":
		return liveKitOutputCodecPCMU, nil
	default:
		return "", fmt.Errorf("%s must be opus or pcmu", liveKitOutCodecEnv)
	}
}

func (r *LiveKitRoom) outCodecCapability() webrtc.RTPCodecCapability {
	if r != nil && r.outCodec == liveKitOutputCodecPCMU {
		return webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMU, ClockRate: liveKitPCMUOutputRate}
	}
	return webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}
}

func (r *LiveKitRoom) resetOpusEncoderLocked() error {
	enc, err := opus.NewEncoder(defaultOutputSampleRate, 1, opus.AppVoIP)
	if err != nil {
		return err
	}
	if err := configureLiveKitOpusEncoder(enc); err != nil {
		return err
	}
	r.opusEncoder = enc
	if len(r.opusEncodeBuf) < liveKitOpusMaxPacket {
		r.opusEncodeBuf = make([]byte, liveKitOpusMaxPacket)
	}
	return nil
}

func configureLiveKitOpusEncoder(enc *opus.Encoder) error {
	if enc == nil {
		return nil
	}
	if raw := strings.TrimSpace(os.Getenv(liveKitOpusComplexityEnv)); raw != "" {
		complexity, err := strconv.Atoi(raw)
		if err != nil || complexity < 0 || complexity > 10 {
			return fmt.Errorf("%s must be an integer from 0 to 10", liveKitOpusComplexityEnv)
		}
		if err := enc.SetComplexity(complexity); err != nil {
			return fmt.Errorf("set %s=%d: %w", liveKitOpusComplexityEnv, complexity, err)
		}
	}
	if raw := strings.TrimSpace(os.Getenv(liveKitOpusBitrateEnv)); raw != "" {
		bitrate, err := strconv.Atoi(raw)
		if err != nil || bitrate < 500 || bitrate > 512000 {
			return fmt.Errorf("%s must be an integer from 500 to 512000 bits/s", liveKitOpusBitrateEnv)
		}
		if err := enc.SetBitrate(bitrate); err != nil {
			return fmt.Errorf("set %s=%d: %w", liveKitOpusBitrateEnv, bitrate, err)
		}
	}
	if raw := strings.TrimSpace(os.Getenv(liveKitOpusMaxBandwidthEnv)); raw != "" {
		bandwidth, err := parseLiveKitOpusBandwidth(raw)
		if err != nil {
			return err
		}
		if err := enc.SetMaxBandwidth(bandwidth); err != nil {
			return fmt.Errorf("set %s=%s: %w", liveKitOpusMaxBandwidthEnv, raw, err)
		}
	}
	return nil
}

func parseLiveKitOpusBandwidth(raw string) (opus.Bandwidth, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "narrowband", "narrow", "nb":
		return opus.Narrowband, nil
	case "mediumband", "medium", "mb":
		return opus.Mediumband, nil
	case "wideband", "wide", "wb":
		return opus.Wideband, nil
	case "superwideband", "super-wideband", "super_wideband", "superwide", "swb":
		return opus.SuperWideband, nil
	case "fullband", "full", "fb":
		return opus.Fullband, nil
	default:
		return 0, fmt.Errorf("%s must be one of narrowband, mediumband, wideband, superwideband, fullband", liveKitOpusMaxBandwidthEnv)
	}
}

type liveKitPCMWriter struct {
	room          *LiveKitRoom
	participantID string
	closed        atomic.Bool
}

func (w *liveKitPCMWriter) WriteSample(sample lkmedia.PCM16Sample) error {
	if w == nil || w.room == nil || w.room.audioSource == nil || w.closed.Load() || len(sample) == 0 {
		return nil
	}
	raw := make([]byte, sample.Size())
	if w.room.perfDiagnosticsEnabled() {
		start := time.Now()
		_, err := sample.CopyTo(raw)
		w.room.recordAudioTiming("go_livekit_in_pcm_sample_copy", time.Since(start))
		if err != nil {
			return err
		}

		start = time.Now()
		w.room.audioSource.PushPCM(raw, 16000, 1)
		w.room.recordAudioTiming("go_livekit_in_push_pcm", time.Since(start))
		return nil
	}
	if _, err := sample.CopyTo(raw); err != nil {
		return err
	}
	w.room.audioSource.PushPCM(raw, 16000, 1)
	return nil
}

func (w *liveKitPCMWriter) Close() error {
	if w != nil {
		w.closed.Store(true)
	}
	return nil
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func pcmBytesToLiveKitSample(pcm []byte) []int16 {
	sample := make([]int16, len(pcm)/2)
	for i := range sample {
		sample[i] = int16(binary.LittleEndian.Uint16(pcm[i*2:]))
	}
	return sample
}
