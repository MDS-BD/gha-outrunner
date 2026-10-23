package outrunner

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
	"github.com/google/uuid"
)

// ScaleSetClient is the subset of scaleset.Client that Scaler uses.
// Extracted as an interface for testability.
type ScaleSetClient interface {
	GenerateJitRunnerConfig(ctx context.Context, setting *scaleset.RunnerScaleSetJitRunnerSetting, scaleSetID int) (*scaleset.RunnerScaleSetJitRunnerConfig, error)
	RemoveRunner(ctx context.Context, runnerID int64) error
}

// Scaler implements listener.Scaler by provisioning real runner environments.
// Each runner gets its own goroutine that manages the full lifecycle:
// provisioning, waiting for job completion, stopping, and deregistration.
type Scaler struct {
	logger      *slog.Logger
	client      ScaleSetClient
	scaleSetID  int
	maxRunners  int
	namePrefix  string
	poolName    string
	runner      *RunnerConfig
	provisioner Provisioner
	group       *GroupScheduler
	wakeCh      chan struct{}

	mu      sync.Mutex
	runners map[string]*RunnerState
	desired int
	jitErr  error

	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
	wg              sync.WaitGroup
}

var _ listener.Scaler = (*Scaler)(nil)

func NewScaler(logger *slog.Logger, client ScaleSetClient, scaleSetID, maxRunners int, namePrefix string, runner *RunnerConfig, prov Provisioner, group *GroupScheduler) *Scaler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Scaler{
		logger:          logger,
		client:          client,
		scaleSetID:      scaleSetID,
		maxRunners:      maxRunners,
		namePrefix:      namePrefix,
		poolName:        namePrefix,
		runner:          runner,
		provisioner:     prov,
		group:           group,
		runners:         make(map[string]*RunnerState),
		lifecycleCtx:    ctx,
		lifecycleCancel: cancel,
		wakeCh:          make(chan struct{}, 1),
	}
	if group != nil {
		group.SetWake(namePrefix, s.wakeCh)
		s.wg.Add(1)
		go s.wakeDispatcher()
	}
	return s
}

func (s *Scaler) wakeDispatcher() {
	defer s.wg.Done()
	for {
		select {
		case <-s.lifecycleCtx.Done():
			return
		case <-s.wakeCh:
			s.mu.Lock()
			if s.lifecycleCtx.Err() == nil {
				s.launchGroupedLocked()
			}
			s.mu.Unlock()
		}
	}
}

// HandleDesiredRunnerCount records demand. Grouped admission and JIT generation
// are asynchronous so message callbacks do not wait on GitHub or a host slot.
func (s *Scaler) HandleDesiredRunnerCount(ctx context.Context, count int) (int, error) {
	s.mu.Lock()
	if s.group != nil && s.lifecycleCtx.Err() != nil {
		current := len(s.runners)
		s.mu.Unlock()
		return current, nil
	}
	if s.jitErr != nil {
		current, err := len(s.runners), s.jitErr
		s.mu.Unlock()
		return current, err
	}
	if s.group != nil {
		s.desired = min(s.maxRunners, max(0, count))
		s.group.UpdateDemand(s.poolName, max(0, count))
		s.withdrawExcessLocked()
		s.launchGroupedLocked()
		current := len(s.runners)
		s.mu.Unlock()
		return current, nil
	}
	s.desired = min(s.maxRunners, count)
	s.logger.Debug("Desired runner count",
		slog.Int("requested", count),
		slog.Int("target", s.desired),
		slog.Int("current", len(s.runners)),
	)
	current, err := s.startUngroupedLocked(ctx)
	s.mu.Unlock()
	return current, err
}

func (s *Scaler) withdrawExcessLocked() {
	active := 0
	for _, state := range s.runners {
		if !state.withdrawing && !state.cleaning && state.Phase != RunnerStopping {
			active++
		}
	}
	for active > s.desired {
		var candidate *RunnerState
		for _, state := range s.runners {
			if state.withdrawing || state.cleaning || (state.Phase != RunnerProvisioning && state.Phase != RunnerIdle) {
				continue
			}
			candidate = state
			break
		}
		if candidate == nil {
			return
		}
		candidate.withdrawing = true
		if candidate.cancelJIT != nil {
			candidate.cancelJIT()
		}
		candidate.SignalWithdraw()
		active--
	}
}

func (s *Scaler) launchGroupedLocked() {
	if s.jitErr != nil || s.lifecycleCtx.Err() != nil {
		return
	}
	active := 0
	for _, state := range s.runners {
		if !state.cleaning {
			active++
		}
	}
	for active < s.desired && len(s.runners) < s.maxRunners {
		lease := s.group.acquire(s.poolName)
		if lease == nil {
			return
		}
		name := fmt.Sprintf("%s-%s", s.namePrefix, uuid.NewString()[:8])
		jitCtx, cancelJIT := context.WithCancel(s.lifecycleCtx)
		state := &RunnerState{
			Name: name, Phase: RunnerProvisioning, CreatedAt: time.Now(),
			done: make(chan struct{}), withdraw: make(chan struct{}), lease: lease,
			cancelJIT: cancelJIT, jitCtx: jitCtx,
		}
		s.runners[name] = state
		s.wg.Add(1)
		active++
		go s.runGroupedRunner(state)
	}
}

func (s *Scaler) runGroupedRunner(state *RunnerState) {
	defer s.wg.Done()
	name := state.Name
	jit, err := s.client.GenerateJitRunnerConfig(state.jitCtx,
		&scaleset.RunnerScaleSetJitRunnerSetting{Name: name}, s.scaleSetID)
	state.cancelJIT()
	if err != nil {
		s.logger.Error("Failed to generate JIT config", slog.String("name", name), slog.String("error", err.Error()))
		s.mu.Lock()
		if !state.withdrawing && s.lifecycleCtx.Err() == nil {
			s.jitErr = fmt.Errorf("generate JIT config: %w", err)
			s.group.BlockPool(s.poolName)
		}
		delete(s.runners, name)
		s.mu.Unlock()
		state.lease.Release()
		s.wake()
		return
	}
	s.mu.Lock()
	stillDesired := !state.withdrawing && s.lifecycleCtx.Err() == nil
	if stillDesired {
		state.RunnerID = jit.Runner.ID
		s.wg.Add(1)
	}
	s.mu.Unlock()
	if !stillDesired {
		ctx, cancel := s.cleanupContext(10 * time.Second)
		if err := s.client.RemoveRunner(ctx, int64(jit.Runner.ID)); err != nil {
			s.logger.Warn("Failed to deregister withdrawn runner", slog.String("name", name), slog.String("error", err.Error()))
		}
		cancel()
		s.removeRunner(name)
		state.lease.Release()
		s.wake()
		return
	}
	req := &RunnerRequest{Name: name, JITConfig: jit.EncodedJITConfig, Runner: s.runner}
	s.logger.Info("Spawning runner", slog.String("name", name), slog.Int("runnerID", state.RunnerID))
	go s.runRunnerLifecycle(state, req)
}

func (s *Scaler) startUngroupedLocked(ctx context.Context) (int, error) {
	for range s.desired - len(s.runners) {
		name := fmt.Sprintf("%s-%s", s.namePrefix, uuid.NewString()[:8])
		jit, err := s.client.GenerateJitRunnerConfig(ctx,
			&scaleset.RunnerScaleSetJitRunnerSetting{Name: name},
			s.scaleSetID,
		)
		if err != nil {
			return len(s.runners), fmt.Errorf("generate JIT config: %w", err)
		}
		state := &RunnerState{
			Name:      name,
			RunnerID:  jit.Runner.ID,
			Phase:     RunnerProvisioning,
			CreatedAt: time.Now(),
			done:      make(chan struct{}),
		}
		s.runners[name] = state
		req := &RunnerRequest{
			Name:      name,
			JITConfig: jit.EncodedJITConfig,
			Runner:    s.runner,
		}
		s.logger.Info("Spawning runner",
			slog.String("name", name),
			slog.Int("runnerID", state.RunnerID),
		)
		s.wg.Add(1)
		go s.runRunnerLifecycle(state, req)
	}
	return len(s.runners), nil
}

func (s *Scaler) HandleJobStarted(ctx context.Context, jobInfo *scaleset.JobStarted) error {
	s.mu.Lock()
	state, exists := s.runners[jobInfo.RunnerName]
	if exists {
		state.Phase = RunnerRunning
	}
	s.mu.Unlock()

	s.logger.Info("Job started",
		slog.String("runnerName", jobInfo.RunnerName),
		slog.Int64("requestId", jobInfo.RunnerRequestID),
	)
	return nil
}

func (s *Scaler) HandleJobCompleted(ctx context.Context, jobInfo *scaleset.JobCompleted) error {
	s.logger.Info("Job completed",
		slog.String("runnerName", jobInfo.RunnerName),
		slog.String("result", jobInfo.Result),
	)
	s.mu.Lock()
	if state, exists := s.runners[jobInfo.RunnerName]; exists {
		state.SignalDone()
		s.markCleaningLocked(state)
	}
	s.mu.Unlock()
	return nil
}

func (s *Scaler) markCleaningLocked(state *RunnerState) {
	if state.lease != nil && !state.cleaning {
		state.cleaning = true
		state.lease.MarkCleaning()
	}
}

// Shutdown cancels all runner goroutines and waits for them to finish.
func (s *Scaler) Shutdown(ctx context.Context) {
	s.mu.Lock()
	s.lifecycleCancel()
	for _, state := range s.runners {
		if state.cancelJIT != nil {
			state.cancelJIT()
		}
	}
	s.mu.Unlock()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		s.logger.Info("All runners shut down")
	case <-ctx.Done():
		s.logger.Warn("Shutdown timed out, some runners may not be fully cleaned up")
	}
}

// RunnerSnapshot is a point-in-time copy of a runner's state, safe to read
// without holding the scaler's mutex.
type RunnerSnapshot struct {
	Name      string
	RunnerID  int
	Phase     RunnerPhase
	CreatedAt time.Time
	StartedAt time.Time
}

// Runners returns a snapshot of current runner states.
func (s *Scaler) Runners() []RunnerSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshots := make([]RunnerSnapshot, 0, len(s.runners))
	for _, state := range s.runners {
		snapshots = append(snapshots, RunnerSnapshot{
			Name:      state.Name,
			RunnerID:  state.RunnerID,
			Phase:     state.Phase,
			CreatedAt: state.CreatedAt,
			StartedAt: state.StartedAt,
		})
	}
	return snapshots
}

// runRunnerLifecycle manages the full lifecycle of a single runner.
// Runs in its own goroutine. Handles provisioning, waiting, stopping,
// deregistration, and map cleanup.
func (s *Scaler) runRunnerLifecycle(state *RunnerState, req *RunnerRequest) {
	defer s.wg.Done()
	if state.lease == nil {
		defer s.removeRunner(state.Name)
	}
	name := state.Name

	// 1. Provision
	if err := s.provisioner.Start(s.lifecycleCtx, req); err != nil {
		s.logger.Error("Failed to start runner",
			slog.String("name", name),
			slog.String("error", err.Error()),
		)
		if state.lease == nil {
			s.deregisterRunner(name, state.RunnerID)
			return
		}
		s.markCleaning(state)
		stopCtx, cancel := s.cleanupContext(2 * time.Minute)
		stopErr := s.provisioner.Stop(stopCtx, name)
		cancel()
		if stopErr != nil {
			s.logger.Error("Failed to clean up partially started runner", slog.String("name", name), slog.String("error", stopErr.Error()))
			return
		}
		s.deregisterRunner(name, state.RunnerID)
		s.removeRunner(name)
		s.releaseRunnerLease(state)
		s.wake()
		return
	}

	s.mu.Lock()
	if state.Phase == RunnerRunning {
		// HandleJobStarted arrived during provisioning
		s.logger.Info("Runner provisioned (job already assigned)", slog.String("name", name))
	} else {
		state.Phase = RunnerIdle
		s.logger.Info("Runner provisioned", slog.String("name", name))
	}
	state.StartedAt = time.Now()
	s.mu.Unlock()
	withdrawn := false
	// Wait for completion, withdrawal, or shutdown.
	for {
		s.mu.Lock()
		withdrawCh := state.withdraw
		s.mu.Unlock()
		select {
		case <-withdrawCh:
			withdrawCtx, withdrawCancel := s.cleanupContext(10 * time.Second)
			err := s.client.RemoveRunner(withdrawCtx, int64(state.RunnerID))
			withdrawCancel()
			if err != nil {
				s.logger.Info("Runner withdrawal rejected; keeping runner available", slog.String("name", name), slog.String("error", err.Error()))
				s.mu.Lock()
				state.withdrawing = false
				state.withdraw = make(chan struct{})
				state.withdrawOnce = sync.Once{}
				if state.Phase != RunnerRunning {
					state.Phase = RunnerIdle
				}
				s.mu.Unlock()
				continue
			}
			withdrawn = true
			goto stop
		case <-state.done:
			s.logger.Debug("Runner signaled done", slog.String("name", name))
			goto stop
		case <-s.lifecycleCtx.Done():
			s.logger.Debug("Runner shutdown requested", slog.String("name", name))
			goto stop
		}
	}

stop:
	// 3. Stop
	s.logger.Info("Stopping runner", slog.String("name", name))
	s.markCleaning(state)
	s.mu.Lock()
	state.Phase = RunnerStopping
	s.mu.Unlock()

	stopCtx, cancel := s.cleanupContext(2 * time.Minute)
	stopErr := s.provisioner.Stop(stopCtx, name)
	cancel()
	if stopErr != nil {
		s.logger.Error("Failed to stop runner",
			slog.String("name", name),
			slog.String("error", stopErr.Error()),
		)
		if state.lease != nil {
			return
		}
	}

	// 4. Deregister from GitHub
	if !withdrawn {
		s.deregisterRunner(name, state.RunnerID)
	}
	s.removeRunner(name)
	if state.lease != nil {
		state.lease.Release()
	}
	s.wake()
}

func (s *Scaler) cleanupContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(s.lifecycleCtx), timeout)
}

func (s *Scaler) markCleaning(state *RunnerState) {
	s.mu.Lock()
	s.markCleaningLocked(state)
	s.mu.Unlock()
}

func (s *Scaler) releaseRunnerLease(state *RunnerState) {
	state.lease.Release()
}

func (s *Scaler) removeRunner(name string) {
	s.mu.Lock()
	delete(s.runners, name)
	s.mu.Unlock()
}

func (s *Scaler) wake() {
	if s.lifecycleCtx.Err() != nil {
		return
	}
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

func (s *Scaler) deregisterRunner(name string, runnerID int) {
	if runnerID == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.client.RemoveRunner(ctx, int64(runnerID)); err != nil {
		s.logger.Warn("Failed to deregister runner",
			slog.String("name", name),
			slog.Int("runnerID", runnerID),
			slog.String("error", err.Error()),
		)
	}
}
