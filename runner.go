package outrunner

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// RunnerPhase represents the current lifecycle phase of a runner.
type RunnerPhase int

const (
	RunnerProvisioning RunnerPhase = iota
	RunnerIdle
	RunnerRunning
	RunnerStopping
)

func (p RunnerPhase) String() string {
	switch p {
	case RunnerProvisioning:
		return "provisioning"
	case RunnerIdle:
		return "idle"
	case RunnerRunning:
		return "running"
	case RunnerStopping:
		return "stopping"
	default:
		return fmt.Sprintf("unknown(%d)", int(p))
	}
}

// RunnerState holds the full state of a single runner instance.
type RunnerState struct {
	Name      string
	RunnerID  int // from GenerateJitRunnerConfig().Runner.ID
	Phase     RunnerPhase
	CreatedAt time.Time
	StartedAt time.Time // when Start() completed (provisioning finished)

	done         chan struct{}
	doneOnce     sync.Once
	withdraw     chan struct{}
	withdrawOnce sync.Once
	lease        *groupLease
	cleaning     bool
	cancelJIT    func()
	jitCtx       context.Context
	withdrawing  bool
}

// SignalDone closes the done channel, signaling the runner goroutine to stop.
// Safe to call multiple times.
func (r *RunnerState) SignalDone() {
	r.doneOnce.Do(func() { close(r.done) })
}

func (r *RunnerState) SignalWithdraw() {
	r.withdrawOnce.Do(func() { close(r.withdraw) })
}
