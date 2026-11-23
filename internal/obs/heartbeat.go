package obs

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Heartbeats lets long-running loops prove they are alive. A loop that stops beating
// (stuck or crashed) makes readiness fail so the orchestrator can restart the process.
type Heartbeats struct {
	mu    sync.Mutex
	beats map[string]*beat
	now   func() time.Time
}

type beat struct {
	last   time.Time
	maxAge time.Duration
}

func NewHeartbeats() *Heartbeats { return &Heartbeats{beats: map[string]*beat{}, now: time.Now} }

// Register declares a component that must beat at least every maxAge and returns its
// beat function. It counts as having just beaten, giving it one period to start up.
func (h *Heartbeats) Register(name string, maxAge time.Duration) func() {
	h.mu.Lock()
	h.beats[name] = &beat{last: h.now(), maxAge: maxAge}
	h.mu.Unlock()
	return func() { h.Beat(name) }
}

func (h *Heartbeats) Beat(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if b, ok := h.beats[name]; ok {
		b.last = h.now()
	}
}

// Check reports an error naming every component that is overdue.
func (h *Heartbeats) Check(context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	var stale []string
	for name, b := range h.beats {
		if age := h.now().Sub(b.last); age > b.maxAge {
			stale = append(stale, fmt.Sprintf("%s silent for %s (limit %s)", name, age.Round(time.Second), b.maxAge))
		}
	}
	if len(stale) == 0 {
		return nil
	}
	sort.Strings(stale)
	return fmt.Errorf("stalled: %s", strings.Join(stale, "; "))
}
