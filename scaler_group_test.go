package outrunner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/actions/scaleset"
)

const groupedTestDeadline = 5 * time.Second

type groupedTestPool struct {
	name string
	max  int
}

func newReadyGroupedTestScheduler(t *testing.T, max int, pools ...groupedTestPool) *GroupScheduler {
	t.Helper()
	group, err := NewGroupScheduler(max)
	if err != nil {
		t.Fatal(err)
	}
	for _, pool := range pools {
		if err := group.RegisterPool(pool.name, pool.max); err != nil {
			t.Fatal(err)
		}
	}
	// A successful zero-capacity poll from every pool acknowledges the initial
	// offers before any grouped scaler is allowed to request a lease.
	for _, pool := range pools {
		offer, poll := group.beginPoll(pool.name, func(error) {})
		if offer != 0 {
			t.Fatalf("initial offer for pool %q = %d, want zero", pool.name, offer)
		}
		group.finishPoll(pool.name, poll, &scaleset.RunnerScaleSetMessage{
			Statistics: &scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: 0},
		}, nil)
	}
	return group
}

func newGroupedTestScaler(group *GroupScheduler, pool groupedTestPool, client ScaleSetClient, provisioner Provisioner) *Scaler {
	return NewScaler(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		client, 1, pool.max, pool.name,
		&RunnerConfig{Docker: &DockerImage{Image: "test:latest"}},
		provisioner, group,
	)
}

type groupedTestClient struct {
	nextID       atomic.Int64
	generateFn   func(context.Context, *scaleset.RunnerScaleSetJitRunnerSetting, int) (*scaleset.RunnerScaleSetJitRunnerConfig, error)
	removeFn     func(context.Context, int64) error
	generateCall atomic.Int32
	removeCall   atomic.Int32
}

func (c *groupedTestClient) GenerateJitRunnerConfig(ctx context.Context, setting *scaleset.RunnerScaleSetJitRunnerSetting, scaleSetID int) (*scaleset.RunnerScaleSetJitRunnerConfig, error) {
	c.generateCall.Add(1)
	if c.generateFn != nil {
		return c.generateFn(ctx, setting, scaleSetID)
	}
	return groupedTestJIT(setting, int(c.nextID.Add(1))), nil
}

func (c *groupedTestClient) RemoveRunner(ctx context.Context, runnerID int64) error {
	c.removeCall.Add(1)
	if c.removeFn != nil {
		return c.removeFn(ctx, runnerID)
	}
	return nil
}

func groupedTestJIT(setting *scaleset.RunnerScaleSetJitRunnerSetting, id int) *scaleset.RunnerScaleSetJitRunnerConfig {
	return &scaleset.RunnerScaleSetJitRunnerConfig{
		Runner:           &scaleset.RunnerReference{ID: id, Name: setting.Name},
		EncodedJITConfig: "test-jit-config",
	}
}

type groupedTestProvisioner struct {
	startFn    func(context.Context, *RunnerRequest) error
	stopFn     func(context.Context, string) error
	started    chan string
	stopCalled chan string
	startCall  atomic.Int32
	stopCall   atomic.Int32
}

func (p *groupedTestProvisioner) Start(ctx context.Context, req *RunnerRequest) error {
	p.startCall.Add(1)
	if p.started != nil {
		p.started <- req.Name
	}
	if p.startFn != nil {
		return p.startFn(ctx, req)
	}
	return nil
}

func (p *groupedTestProvisioner) Stop(ctx context.Context, name string) error {
	p.stopCall.Add(1)
	if p.stopCalled != nil {
		p.stopCalled <- name
	}
	if p.stopFn != nil {
		return p.stopFn(ctx, name)
	}
	return nil
}

func (*groupedTestProvisioner) Close() error { return nil }

type groupedTestReportedStopError struct {
	reported chan struct{}
	once     sync.Once
}

func (e *groupedTestReportedStopError) Error() string {
	e.once.Do(func() { close(e.reported) })
	return "stop failed"
}

func receiveGroupedTestValue[T any](t *testing.T, ch <-chan T, description string) T {
	t.Helper()
	timer := time.NewTimer(groupedTestDeadline)
	defer timer.Stop()
	select {
	case value := <-ch:
		return value
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
		var zero T
		return zero
	}
}

func waitGroupedTestCondition(t *testing.T, description string, condition func() bool) {
	t.Helper()
	timer := time.NewTimer(groupedTestDeadline)
	defer timer.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-timer.C:
			t.Fatalf("timed out waiting for %s", description)
			return
		default:
			runtime.Gosched()
		}
	}
}

func groupedTestPoolCounts(group *GroupScheduler, name string) (leases, cleaning int) {
	group.mu.Lock()
	defer group.mu.Unlock()
	pool := group.pools[name]
	if pool == nil {
		return 0, 0
	}
	return pool.leases, pool.cleaning
}

func groupedTestTotalLeases(group *GroupScheduler) int {
	group.mu.Lock()
	defer group.mu.Unlock()
	total := 0
	for _, pool := range group.pools {
		total += pool.leases
	}
	return total
}

func groupedTestHasRunner(scaler *Scaler, name string) bool {
	for _, runner := range scaler.Runners() {
		if runner.Name == name {
			return true
		}
	}
	return false
}

func shutdownGroupedTestScaler(t *testing.T, scaler *Scaler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), groupedTestDeadline)
	defer cancel()
	done := make(chan struct{})
	go func() {
		scaler.Shutdown(ctx)
		close(done)
	}()
	receiveGroupedTestValue(t, done, "scaler shutdown")
}

func TestGroupedScalerHoldsCapacityThroughStopAndWakesOtherPool(t *testing.T) {
	poolA := groupedTestPool{name: "repo-a", max: 1}
	poolB := groupedTestPool{name: "repo-b", max: 1}
	group := newReadyGroupedTestScheduler(t, 1, poolA, poolB)

	stopGate := make(chan struct{})
	var releaseStop sync.Once
	release := func() { releaseStop.Do(func() { close(stopGate) }) }
	clientA, clientB := &groupedTestClient{}, &groupedTestClient{}
	provA := &groupedTestProvisioner{
		started:    make(chan string, 1),
		stopCalled: make(chan string, 1),
		stopFn: func(context.Context, string) error {
			<-stopGate
			return nil
		},
	}
	provB := &groupedTestProvisioner{started: make(chan string, 1)}
	scalerA := newGroupedTestScaler(group, poolA, clientA, provA)
	scalerB := newGroupedTestScaler(group, poolB, clientB, provB)
	t.Cleanup(func() {
		release()
		shutdownGroupedTestScaler(t, scalerB)
		shutdownGroupedTestScaler(t, scalerA)
	})

	if _, err := scalerA.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	runnerA := receiveGroupedTestValue(t, provA.started, "repo-a runner start")
	if _, err := scalerB.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if got := len(scalerB.Runners()); got != 0 {
		t.Fatalf("repo-b runners before repo-a stop = %d, want 0", got)
	}

	if err := scalerA.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: runnerA, Result: "succeeded"}); err != nil {
		t.Fatal(err)
	}
	// The completion message also carries the reduced assigned-job count.
	if _, err := scalerA.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if got := receiveGroupedTestValue(t, provA.stopCalled, "repo-a Stop"); got != runnerA {
		t.Fatalf("Stop called for %q, want %q", got, runnerA)
	}
	if got := groupedTestTotalLeases(group); got != 1 {
		t.Fatalf("group leases while repo-a Stop is blocked = %d, want 1", got)
	}
	if got := len(scalerB.Runners()); got != 0 {
		t.Fatalf("repo-b runners while repo-a Stop is blocked = %d, want 0", got)
	}
	release()
	waitGroupedTestCondition(t, "repo-a runner removal", func() bool { return !groupedTestHasRunner(scalerA, runnerA) })
	waitGroupedTestCondition(t, "repo-b runner start", func() bool { return len(scalerB.Runners()) == 1 })
	if got := receiveGroupedTestValue(t, provB.started, "repo-b runner start"); got == "" {
		t.Fatal("repo-b started an empty runner name")
	}
	if got := groupedTestTotalLeases(group); got != 1 {
		t.Fatalf("group leases after repo-b starts = %d, want 1", got)
	}
}

func TestGroupedScalerFailedStopRetainsReservation(t *testing.T) {
	poolA := groupedTestPool{name: "repo-a", max: 2}
	poolB := groupedTestPool{name: "repo-b", max: 2}
	poolC := groupedTestPool{name: "repo-c", max: 2}
	group := newReadyGroupedTestScheduler(t, 2, poolA, poolB, poolC)

	stopGate := make(chan struct{})
	var releaseStop sync.Once
	release := func() { releaseStop.Do(func() { close(stopGate) }) }
	clientA, clientB, clientC := &groupedTestClient{}, &groupedTestClient{}, &groupedTestClient{}
	provA := &groupedTestProvisioner{started: make(chan string, 1), stopCalled: make(chan string, 1)}
	provA.stopFn = func(context.Context, string) error {
		<-stopGate
		return errors.New("stop failed")
	}
	provB := &groupedTestProvisioner{started: make(chan string, 1)}
	provC := &groupedTestProvisioner{started: make(chan string, 1)}
	scalerA := newGroupedTestScaler(group, poolA, clientA, provA)
	scalerB := newGroupedTestScaler(group, poolB, clientB, provB)
	scalerC := newGroupedTestScaler(group, poolC, clientC, provC)
	t.Cleanup(func() {
		release()
		shutdownGroupedTestScaler(t, scalerC)
		shutdownGroupedTestScaler(t, scalerB)
		shutdownGroupedTestScaler(t, scalerA)
	})

	if _, err := scalerA.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	runnerA := receiveGroupedTestValue(t, provA.started, "repo-a runner start")
	if _, err := scalerB.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	receiveGroupedTestValue(t, provB.started, "repo-b runner start")
	if _, err := scalerC.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if len(scalerC.Runners()) != 0 {
		t.Fatal("repo-c acquired a runner while the group was full")
	}

	if err := scalerA.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: runnerA, Result: "succeeded"}); err != nil {
		t.Fatal(err)
	}
	if got := receiveGroupedTestValue(t, provA.stopCalled, "repo-a Stop"); got != runnerA {
		t.Fatalf("Stop called for %q, want %q", got, runnerA)
	}
	release()
	// Shutdown joins the failed lifecycle, making the failed Stop's retained
	// reservation observable before repo-c is offered another admission turn.
	shutdownGroupedTestScaler(t, scalerA)
	if !groupedTestHasRunner(scalerA, runnerA) {
		t.Fatal("failed Stop removed repo-a's runner state")
	}
	if leases, _ := groupedTestPoolCounts(group, poolA.name); leases != 1 {
		t.Fatalf("repo-a leases after failed Stop = %d, want 1", leases)
	}
	if got := groupedTestTotalLeases(group); got != 2 {
		t.Fatalf("group leases after failed Stop = %d, want 2", got)
	}

	// A fresh demand callback is a deterministic admission attempt after the
	// failed Stop has fully returned; no third runner may be provisioned.
	if _, err := scalerC.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if got := len(scalerC.Runners()); got != 0 {
		t.Fatalf("repo-c runners after failed Stop = %d, want 0", got)
	}
	if got := provC.startCall.Load(); got != 0 {
		t.Fatalf("repo-c provisioned %d runners after failed Stop", got)
	}
}

func TestGroupedScalerWithdrawalRejectedForRunningJobDoesNotStop(t *testing.T) {
	pool := groupedTestPool{name: "repo-a", max: 1}
	group := newReadyGroupedTestScheduler(t, 1, pool)
	removeStarted := make(chan int64, 1)
	removeResult := make(chan error, 1)
	client := &groupedTestClient{}
	client.removeFn = func(ctx context.Context, runnerID int64) error {
		if client.removeCall.Load() == 1 {
			removeStarted <- runnerID
			select {
			case err := <-removeResult:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	prov := &groupedTestProvisioner{started: make(chan string, 1), stopCalled: make(chan string, 1)}
	scaler := newGroupedTestScaler(group, pool, client, prov)
	t.Cleanup(func() { shutdownGroupedTestScaler(t, scaler) })

	if _, err := scaler.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	runnerName := receiveGroupedTestValue(t, prov.started, "runner start")
	waitGroupedTestCondition(t, "runner becoming idle", func() bool {
		for _, runner := range scaler.Runners() {
			if runner.Name == runnerName {
				return runner.Phase == RunnerIdle
			}
		}
		return false
	})
	if _, err := scaler.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if got := receiveGroupedTestValue(t, removeStarted, "withdrawal RemoveRunner"); got == 0 {
		t.Fatal("withdrawal used runner ID zero")
	}
	if err := scaler.HandleJobStarted(context.Background(), &scaleset.JobStarted{RunnerName: runnerName}); err != nil {
		t.Fatal(err)
	}
	removeResult <- scaleset.JobStillRunningError
	waitGroupedTestCondition(t, "rejected withdrawal to restore the running runner", func() bool {
		scaler.mu.Lock()
		defer scaler.mu.Unlock()
		state := scaler.runners[runnerName]
		return state != nil && !state.withdrawing && state.Phase == RunnerRunning
	})
	if got := prov.stopCall.Load(); got != 0 {
		t.Fatalf("Stop called %d times after JobStillRunningError, want 0", got)
	}
	select {
	case name := <-prov.stopCalled:
		t.Fatalf("Stop(%q) called for the still-running job", name)
	default:
	}

	if err := scaler.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: runnerName, Result: "succeeded"}); err != nil {
		t.Fatal(err)
	}
	if got := receiveGroupedTestValue(t, prov.stopCalled, "Stop after job completion"); got != runnerName {
		t.Fatalf("Stop called for %q after job completion, want %q", got, runnerName)
	}
	waitGroupedTestCondition(t, "completed runner removal", func() bool { return len(scaler.Runners()) == 0 })
	if got := prov.stopCall.Load(); got != 1 {
		t.Fatalf("Stop calls = %d, want exactly one after job completion", got)
	}
}

func TestGroupedScalerCancelsQueuedJITWithoutLateProvisioning(t *testing.T) {
	for _, mode := range []string{"demand-drop", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			pool := groupedTestPool{name: "repo-a", max: 1}
			group := newReadyGroupedTestScheduler(t, 1, pool)
			jitStarted := make(chan context.Context, 1)
			jitRelease := make(chan struct{})
			var releaseJIT sync.Once
			release := func() { releaseJIT.Do(func() { close(jitRelease) }) }
			client := &groupedTestClient{}
			client.generateFn = func(ctx context.Context, setting *scaleset.RunnerScaleSetJitRunnerSetting, _ int) (*scaleset.RunnerScaleSetJitRunnerConfig, error) {
				jitStarted <- ctx
				<-jitRelease // Intentionally return success even after cancellation.
				return groupedTestJIT(setting, 1), nil
			}
			removeStarted := make(chan int64, 1)
			client.removeFn = func(_ context.Context, runnerID int64) error {
				removeStarted <- runnerID
				return nil
			}
			prov := &groupedTestProvisioner{started: make(chan string, 1)}
			scaler := newGroupedTestScaler(group, pool, client, prov)
			t.Cleanup(func() {
				release()
				shutdownGroupedTestScaler(t, scaler)
			})

			if _, err := scaler.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
				t.Fatal(err)
			}
			jitCtx := receiveGroupedTestValue(t, jitStarted, "JIT generation")
			if mode == "demand-drop" {
				if _, err := scaler.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
					t.Fatal(err)
				}
			}
			shutdownDone := make(chan struct{})
			if mode == "shutdown" {
				go func() {
					scaler.Shutdown(context.Background())
					close(shutdownDone)
				}()
			}
			receiveGroupedTestValue(t, jitCtx.Done(), "JIT context cancellation")
			release()
			if runnerID := receiveGroupedTestValue(t, removeStarted, "late JIT deregistration"); runnerID != 1 {
				t.Fatalf("late JIT removed runner ID %d, want 1", runnerID)
			}
			if mode == "demand-drop" {
				shutdownGroupedTestScaler(t, scaler)
			} else {
				receiveGroupedTestValue(t, shutdownDone, "shutdown after canceled JIT")
			}
			if got := prov.startCall.Load(); got != 0 {
				t.Fatalf("provisioned %d runners from a canceled JIT response", got)
			}
			if got := len(scaler.Runners()); got != 0 {
				t.Fatalf("runner states after canceled JIT cleanup = %d, want 0", got)
			}
			if leases, _ := groupedTestPoolCounts(group, pool.name); leases != 0 {
				t.Fatalf("lease count after canceled JIT cleanup = %d, want 0", leases)
			}
		})
	}
}

func TestGroupedScalerHonorsPoolAndCrossRepositoryCaps(t *testing.T) {
	t.Run("pool cap", func(t *testing.T) {
		pool := groupedTestPool{name: "repo-a", max: 1}
		group := newReadyGroupedTestScheduler(t, 3, pool)
		startGate := make(chan struct{})
		client := &groupedTestClient{}
		prov := &groupedTestProvisioner{started: make(chan string, 3)}
		prov.startFn = func(ctx context.Context, _ *RunnerRequest) error {
			select {
			case <-startGate:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		scaler := newGroupedTestScaler(group, pool, client, prov)
		t.Cleanup(func() {
			close(startGate)
			shutdownGroupedTestScaler(t, scaler)
		})

		if _, err := scaler.HandleDesiredRunnerCount(context.Background(), 3); err != nil {
			t.Fatal(err)
		}
		receiveGroupedTestValue(t, prov.started, "pool-limited runner start")
		if got := len(scaler.Runners()); got != 1 {
			t.Fatalf("runners with pool max 1 and demand 3 = %d, want 1", got)
		}
		if got := client.generateCall.Load(); got != 1 {
			t.Fatalf("JIT calls with pool max 1 and demand 3 = %d, want 1", got)
		}
	})

	t.Run("shared cap across distinct clients", func(t *testing.T) {
		poolA := groupedTestPool{name: "repo-a", max: 2}
		poolB := groupedTestPool{name: "repo-b", max: 2}
		poolC := groupedTestPool{name: "repo-c", max: 2}
		group := newReadyGroupedTestScheduler(t, 2, poolA, poolB, poolC)
		startGate := make(chan struct{})
		newClient := func() *groupedTestClient { return &groupedTestClient{} }
		clientA, clientB, clientC := newClient(), newClient(), newClient()
		provA := &groupedTestProvisioner{started: make(chan string, 2)}
		provB := &groupedTestProvisioner{started: make(chan string, 2)}
		provC := &groupedTestProvisioner{started: make(chan string, 2)}
		for _, prov := range []*groupedTestProvisioner{provA, provB, provC} {
			prov.startFn = func(ctx context.Context, _ *RunnerRequest) error {
				select {
				case <-startGate:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		scalerA := newGroupedTestScaler(group, poolA, clientA, provA)
		scalerB := newGroupedTestScaler(group, poolB, clientB, provB)
		scalerC := newGroupedTestScaler(group, poolC, clientC, provC)
		t.Cleanup(func() {
			close(startGate)
			shutdownGroupedTestScaler(t, scalerC)
			shutdownGroupedTestScaler(t, scalerB)
			shutdownGroupedTestScaler(t, scalerA)
		})

		for _, scaler := range []*Scaler{scalerA, scalerB, scalerC} {
			if _, err := scaler.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
				t.Fatal(err)
			}
		}
		receiveGroupedTestValue(t, provA.started, "repo-a aggregate-cap runner start")
		receiveGroupedTestValue(t, provB.started, "repo-b aggregate-cap runner start")
		if got := len(scalerA.Runners()) + len(scalerB.Runners()) + len(scalerC.Runners()); got != 2 {
			t.Fatalf("runners across distinct clients with group cap 2 = %d, want 2", got)
		}
		if got := groupedTestTotalLeases(group); got != 2 {
			t.Fatalf("leases across distinct clients with group cap 2 = %d, want 2", got)
		}
		if got := len(scalerC.Runners()); got != 0 {
			t.Fatalf("repo-c runners after aggregate cap was reached = %d, want 0", got)
		}
		if got := clientA.generateCall.Load() + clientB.generateCall.Load() + clientC.generateCall.Load(); got != 2 {
			t.Fatalf("JIT calls across distinct clients at group cap 2 = %d, want 2", got)
		}
	})
}

func TestGroupedScalerCompletionCleaningPreservesNeededRunner(t *testing.T) {
	pool := groupedTestPool{name: "repo-a", max: 2}
	group := newReadyGroupedTestScheduler(t, 2, pool)
	stopGate := make(chan struct{})
	var releaseStop sync.Once
	release := func() { releaseStop.Do(func() { close(stopGate) }) }
	var completedRunner string
	prov := &groupedTestProvisioner{started: make(chan string, 2), stopCalled: make(chan string, 2)}
	prov.stopFn = func(_ context.Context, name string) error {
		if name == completedRunner {
			<-stopGate
		}
		return nil
	}
	scaler := newGroupedTestScaler(group, pool, &groupedTestClient{}, prov)
	t.Cleanup(func() {
		release()
		shutdownGroupedTestScaler(t, scaler)
	})

	if _, err := scaler.HandleDesiredRunnerCount(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	first := receiveGroupedTestValue(t, prov.started, "first runner start")
	second := receiveGroupedTestValue(t, prov.started, "second runner start")
	waitGroupedTestCondition(t, "both runners becoming idle", func() bool {
		idle := 0
		for _, runner := range scaler.Runners() {
			if runner.Phase == RunnerIdle {
				idle++
			}
		}
		return idle == 2
	})
	completedRunner = first
	if err := scaler.HandleJobStarted(context.Background(), &scaleset.JobStarted{RunnerName: completedRunner}); err != nil {
		t.Fatal(err)
	}
	if err := scaler.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: completedRunner, Result: "succeeded"}); err != nil {
		t.Fatal(err)
	}
	// Handle the new authoritative demand while the just-completed runner is
	// still retained for cleaning; the other idle runner remains needed.
	if _, err := scaler.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if got := receiveGroupedTestValue(t, prov.stopCalled, "completed runner cleanup"); got != completedRunner {
		t.Fatalf("Stop called for %q, want completed runner %q", got, completedRunner)
	}
	scaler.mu.Lock()
	retained := scaler.runners[second]
	withdrawing := retained == nil || retained.withdrawing
	scaler.mu.Unlock()
	if withdrawing {
		t.Fatalf("still-needed runner %q was withdrawn after completed runner cleaning", second)
	}
	if got := prov.stopCall.Load(); got != 1 {
		t.Fatalf("Stop calls while completed-runner cleanup is blocked = %d, want 1", got)
	}
	release()
	waitGroupedTestCondition(t, "completed runner removal", func() bool { return !groupedTestHasRunner(scaler, completedRunner) })
	if !groupedTestHasRunner(scaler, second) {
		t.Fatalf("still-needed runner %q was removed", second)
	}
}

func TestGroupedScalerFailedStopLeavesOtherPoolSlotAvailable(t *testing.T) {
	pool := groupedTestPool{name: "repo-a", max: 2}
	group := newReadyGroupedTestScheduler(t, 2, pool)
	stopGate := make(chan struct{})
	var releaseStop sync.Once
	release := func() { releaseStop.Do(func() { close(stopGate) }) }
	stopFailureReported := make(chan struct{})
	var failedRunner string
	prov := &groupedTestProvisioner{started: make(chan string, 2), stopCalled: make(chan string, 2)}
	prov.stopFn = func(_ context.Context, name string) error {
		if name == failedRunner {
			<-stopGate
			return &groupedTestReportedStopError{reported: stopFailureReported}
		}
		return nil
	}
	scaler := newGroupedTestScaler(group, pool, &groupedTestClient{}, prov)
	t.Cleanup(func() {
		release()
		shutdownGroupedTestScaler(t, scaler)
	})

	if _, err := scaler.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	failedRunner = receiveGroupedTestValue(t, prov.started, "initial runner start")
	waitGroupedTestCondition(t, "initial runner becoming idle", func() bool {
		for _, runner := range scaler.Runners() {
			if runner.Name == failedRunner {
				return runner.Phase == RunnerIdle
			}
		}
		return false
	})
	if err := scaler.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: failedRunner, Result: "succeeded"}); err != nil {
		t.Fatal(err)
	}
	if got := receiveGroupedTestValue(t, prov.stopCalled, "initial runner Stop"); got != failedRunner {
		t.Fatalf("Stop called for %q, want %q", got, failedRunner)
	}
	release()
	// Error() signals that Stop has failed; wait for its caller to return so
	// the retained lease is stable before asking for a runner in the free slot.
	receiveGroupedTestValue(t, stopFailureReported, "failed Stop result")
	waitGroupedTestCondition(t, "quarantined runner retained after failed Stop", func() bool {
		return groupedTestHasRunner(scaler, failedRunner)
	})
	if _, err := scaler.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	otherRunner := receiveGroupedTestValue(t, prov.started, "second runner using free pool slot")
	if otherRunner == failedRunner {
		t.Fatal("provisioning reused the quarantined runner name")
	}
	if leases, cleaning := groupedTestPoolCounts(group, pool.name); leases != 2 || cleaning != 1 {
		t.Fatalf("pool reservations after provisioning into the free slot = leases %d, cleaning %d; want 2 and 1", leases, cleaning)
	}

	if got := groupedTestTotalLeases(group); got != 2 {
		t.Fatalf("pool leases after using the free slot = %d, want 2", got)
	}
	if got := len(scaler.Runners()); got != 2 {
		t.Fatalf("runners after using the free slot = %d, want quarantined and new runner", got)
	}
	if !groupedTestHasRunner(scaler, failedRunner) || !groupedTestHasRunner(scaler, otherRunner) {
		t.Fatalf("runner states do not contain both quarantined %q and new %q runners", failedRunner, otherRunner)
	}
	if got := prov.startCall.Load(); got != 2 {
		t.Fatalf("runner starts = %d, want exactly 2 with no dirty reuse", got)
	}
}
