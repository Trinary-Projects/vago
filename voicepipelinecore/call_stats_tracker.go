package voicepipelinecore

import (
	"sync"
	"time"
)

// callStatsTracker tracks how long a non-bot participant was actually
// in the room and when their first audible frame arrived.
type callStatsTracker struct {
	mu sync.Mutex

	joinedAt     time.Time
	leftAt       time.Time
	firstAudioAt time.Time
	total        time.Duration
	present      bool
}

func newCallStatsTracker() *callStatsTracker {
	return &callStatsTracker{}
}

func (p *callStatsTracker) MarkUserJoined(at time.Time) {
	if p == nil {
		return
	}
	if at.IsZero() {
		at = time.Now()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.present {
		return
	}
	p.present = true
	p.joinedAt = at
	p.leftAt = time.Time{}
}

func (p *callStatsTracker) MarkUserLeft(at time.Time) {
	if p == nil {
		return
	}
	if at.IsZero() {
		at = time.Now()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.present {
		return
	}
	if p.joinedAt.IsZero() || at.Before(p.joinedAt) {
		p.leftAt = at
		p.present = false
		return
	}
	p.total += at.Sub(p.joinedAt)
	p.leftAt = at
	p.present = false
}

// MarkFirstUserAudio records the first audible audio frame. It returns
// true only for the first successful mark.
func (p *callStatsTracker) MarkFirstUserAudio(at time.Time) bool {
	if p == nil {
		return false
	}
	if at.IsZero() {
		at = time.Now()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.firstAudioAt.IsZero() {
		return false
	}
	p.firstAudioAt = at
	return true
}

func (p *callStatsTracker) FirstUserAudioFrameAt() time.Time {
	if p == nil {
		return time.Time{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.firstAudioAt
}

// Present reports whether a non-bot participant is currently joined.
// Mirrors Python's `len(transport.participants().keys()) > 1` check
// (base_pipeline_manager.py's dead-mic idle check): true only while a
// user is actually in the room, not merely "ever joined".
func (p *callStatsTracker) Present() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.present
}

func (p *callStatsTracker) TotalDurationSec(at time.Time) float64 {
	if p == nil {
		return 0
	}
	if at.IsZero() {
		at = time.Now()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	total := p.total
	if p.present && !p.joinedAt.IsZero() && at.After(p.joinedAt) {
		total += at.Sub(p.joinedAt)
	}
	return total.Seconds()
}
