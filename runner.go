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

// RunnerState holds one runner lifecycle and its local capacity lease.
type RunnerState struct {
	Name      string
	RunnerID  int
	Phase     RunnerPhase
	CreatedAt time.Time
	StartedAt time.Time

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

func (r *RunnerState) SignalDone() {
	r.doneOnce.Do(func() { close(r.done) })
}

func (r *RunnerState) SignalWithdraw() {
	r.withdrawOnce.Do(func() { close(r.withdraw) })
}
