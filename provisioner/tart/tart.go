package tart

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	outrunner "github.com/NetwindHQ/gha-outrunner"
)

const tartCleanupTimeout = 2 * time.Minute

type vmInventory struct {
	Source  string `json:"Source"`
	Name    string `json:"Name"`
	Running bool   `json:"Running"`
	State   string `json:"State"`
}

type vmInventoryCommand struct {
	Source  string `json:"Source"`
	Name    string `json:"Name"`
	Running *bool  `json:"Running"`
	State   string `json:"State"`
}

const maxStderrLog = 1024

// tailBuffer is an io.Writer that keeps the last maxStderrLog bytes of output.
type tailBuffer struct {
	buf []byte
	max int
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.buf = append(t.buf, p...)
	if len(t.buf) > t.max {
		t.buf = t.buf[len(t.buf)-t.max:]
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	return string(t.buf)
}

// stderrLog writes stderr to both a file and an in-memory tail buffer.
type stderrLog struct {
	file *os.File
	tail *tailBuffer
	w    io.Writer
}

func newStderrLog(name, label string) (*stderrLog, error) {
	f, err := os.CreateTemp("", fmt.Sprintf("outrunner-%s-%s-*.log", label, name))
	if err != nil {
		return nil, fmt.Errorf("create stderr log: %w", err)
	}
	tail := &tailBuffer{max: maxStderrLog}
	return &stderrLog{file: f, tail: tail, w: io.MultiWriter(f, tail)}, nil
}

func (s *stderrLog) Write(p []byte) (int, error) { return s.w.Write(p) }
func (s *stderrLog) Path() string                { return s.file.Name() }
func (s *stderrLog) Tail() string                { return strings.TrimSpace(s.tail.String()) }

func (s *stderrLog) Close() {
	_ = s.file.Close()
}

// CleanupFile removes the log file (call on success when there's nothing to debug).
func (s *stderrLog) CleanupFile() {
	_ = s.file.Close()
	_ = os.Remove(s.file.Name())
}

func (t *Provisioner) reportExit(name string, err error) {
	t.mu.Lock()
	lifecycle := t.running[name]
	if lifecycle != nil {
		select {
		case lifecycle.done <- err:
		default:
		}
	}
	t.mu.Unlock()
}

// Done reports the first exit of the Tart VM or runner process for name.
func (t *Provisioner) Done(name string) <-chan error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if lifecycle := t.running[name]; lifecycle != nil {
		return lifecycle.done
	}
	done := make(chan error, 1)
	done <- fmt.Errorf("Tart runner %q is not managed", name)
	return done
}

type vmLifecycle struct {
	cancel    context.CancelFunc
	done      chan error
	startDone chan struct{}
	stopping  bool
	vmCreated bool
	wg        sync.WaitGroup
}

type stopReservation struct {
	done chan struct{}
	err  error
}

// Provisioner creates ephemeral Tart VMs as GitHub Actions runners.
// Uses `tart exec` via the guest agent for command execution.
type Provisioner struct {
	logger    *slog.Logger
	mu        sync.Mutex
	cleanupMu sync.Mutex
	running   map[string]*vmLifecycle
	stopping  map[string]*stopReservation
	bases     map[string]struct{}
	prefixes  []string
}

func New(logger *slog.Logger, baseNames, prefixes []string) *Provisioner {
	bases := make(map[string]struct{}, len(baseNames))
	for _, name := range baseNames {
		bases[name] = struct{}{}
	}
	return &Provisioner{
		logger:   logger,
		running:  make(map[string]*vmLifecycle),
		stopping: make(map[string]*stopReservation),
		bases:    bases,
		prefixes: slices.Clone(prefixes),
	}

}

func (t *Provisioner) isBase(name string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.bases[name]
	return ok
}

func (t *Provisioner) Start(ctx context.Context, req *outrunner.RunnerRequest) (retErr error) {
	if req == nil || req.Runner == nil || req.Runner.Tart == nil {
		name := "<nil>"
		if req != nil {
			name = req.Name
		}
		return fmt.Errorf("no tart config for runner %s", name)
	}
	if !isDisposableName(req.Name, t.prefixes) {
		return fmt.Errorf("runner VM name %q is not an owned Tart clone name", req.Name)
	}
	if t.isBase(req.Name) {
		return fmt.Errorf("runner name %q conflicts with a configured Tart base image", req.Name)
	}

	runCtx, cancel := context.WithCancel(ctx)
	lifecycle := &vmLifecycle{
		cancel:    cancel,
		done:      make(chan error, 1),
		startDone: make(chan struct{}),
	}
	t.mu.Lock()
	if _, stopping := t.stopping[req.Name]; stopping {
		t.mu.Unlock()
		cancel()
		return fmt.Errorf("runner VM %q is being stopped", req.Name)
	}
	if _, exists := t.running[req.Name]; exists {
		t.mu.Unlock()
		cancel()
		return fmt.Errorf("runner VM %q is already managed", req.Name)
	}
	t.running[req.Name] = lifecycle
	t.mu.Unlock()
	defer func() {
		if retErr != nil {
			t.finishStart(req.Name, lifecycle, retErr)
			cancel()
		}
		close(lifecycle.startDone)
	}()

	preflightCtx, preflightCancel := context.WithTimeout(runCtx, tartCleanupTimeout)
	preflight, err := t.localInventory(preflightCtx)
	preflightCancel()
	if err != nil {
		t.discardUncreatedStart(req.Name, lifecycle)
		return fmt.Errorf("verify Tart clone destination: %w", err)
	}
	if _, exists := findLocalVM(preflight, req.Name); exists {
		t.discardUncreatedStart(req.Name, lifecycle)
		return fmt.Errorf("Tart clone destination %q already exists", req.Name)
	}

	img := req.Runner.Tart
	t.mu.Lock()
	if t.running[req.Name] != lifecycle || lifecycle.stopping {
		t.mu.Unlock()
		return fmt.Errorf("runner VM %q is being stopped", req.Name)
	}
	// The destination was confirmed absent above. Claim the generated name
	// before clone so Stop can safely remove a partial clone on failure.
	lifecycle.vmCreated = true
	t.mu.Unlock()
	t.logger.Debug("Cloning VM", slog.String("image", img.Image), slog.String("name", req.Name))
	if out, err := exec.CommandContext(runCtx, "tart", "clone", img.Image, req.Name).CombinedOutput(); err != nil {
		cloneErr := fmt.Errorf("tart clone: %w: %s", err, out)
		return errors.Join(cloneErr, t.cleanupFailedStart(req.Name, lifecycle))
	}

	if runCtx.Err() != nil {
		return errors.Join(runCtx.Err(), t.cleanupFailedStart(req.Name, lifecycle))
	}
	if out, err := exec.CommandContext(runCtx, "tart", "set", req.Name,
		"--cpu", strconv.Itoa(img.CPUs),
		"--memory", strconv.Itoa(img.MemoryMB),
	).CombinedOutput(); err != nil {
		setErr := fmt.Errorf("tart set: %w: %s", err, out)
		return errors.Join(setErr, t.cleanupFailedStart(req.Name, lifecycle))
	}

	if !t.addProcess(req.Name, lifecycle) {
		return fmt.Errorf("runner VM %q is being stopped", req.Name)
	}
	go func() {
		defer lifecycle.wg.Done()
		t.logger.Debug("Starting VM", slog.String("name", req.Name))
		args := []string{"run", "--no-graphics"}
		for _, m := range img.Mounts {
			dir := m.Name + ":" + m.Source
			if m.ReadOnly {
				dir += ":ro"
			}
			args = append(args, "--dir="+dir)
		}
		args = append(args, req.Name)
		t.runAndLog(runCtx, req.Name, "vm", args)
	}()

	t.logger.Debug("Waiting for guest agent", slog.String("name", req.Name))
	if err := t.waitForAgent(runCtx, req.Name, 3*time.Minute); err != nil {
		waitErr := fmt.Errorf("guest agent not ready: %w", err)
		return errors.Join(waitErr, t.cleanupFailedStart(req.Name, lifecycle))
	}

	runnerCmd := img.RunnerCmd
	if runnerCmd == "" {
		runnerCmd = "/actions-runner/run.sh"
	}
	t.logger.Info("Starting runner in VM",
		slog.String("name", req.Name),
		slog.String("cmd", runnerCmd),
	)
	if !t.addProcess(req.Name, lifecycle) {
		return fmt.Errorf("runner VM %q is being stopped", req.Name)
	}
	go func() {
		defer lifecycle.wg.Done()
		t.runAndLog(runCtx, req.Name, "runner",
			[]string{"exec", req.Name, runnerCmd, "--jitconfig", req.JITConfig})
	}()

	t.logger.Info("Runner started in VM", slog.String("name", req.Name))
	return nil
}

func (t *Provisioner) discardUncreatedStart(name string, lifecycle *vmLifecycle) {
	t.mu.Lock()
	if t.running[name] == lifecycle {
		delete(t.running, name)
	}
	t.mu.Unlock()
}

func (t *Provisioner) addProcess(name string, lifecycle *vmLifecycle) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.running[name] != lifecycle || lifecycle.stopping {
		return false
	}
	lifecycle.wg.Add(1)
	return true
}
func (t *Provisioner) cleanupFailedStart(name string, lifecycle *vmLifecycle) error {
	t.mu.Lock()
	if lifecycle.stopping {
		t.mu.Unlock()
		return nil
	}
	lifecycle.stopping = true
	lifecycle.cancel()
	t.mu.Unlock()
	lifecycle.wg.Wait()

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), tartCleanupTimeout)
	defer cleanupCancel()
	if err := t.removeVM(cleanupCtx, name); err != nil {
		return err
	}
	t.mu.Lock()
	lifecycle.vmCreated = false
	if t.running[name] == lifecycle {
		delete(t.running, name)
	}
	t.mu.Unlock()
	return nil
}

// runAndLog runs a tart command, streaming stderr to a temp file.
// On failure it logs the last 1KB of stderr inline plus the full log path.
// On success it removes the log file.
func (t *Provisioner) runAndLog(ctx context.Context, name, label string, args []string) {
	stderr, err := newStderrLog(name, label)
	if err != nil {
		t.logger.Warn("Failed to create stderr log, running without capture",
			slog.String("name", name),
			slog.String("error", err.Error()),
		)
	}
	cmd := exec.CommandContext(ctx, "tart", args...)
	if stderr != nil {
		cmd.Stderr = stderr
	}
	if err := cmd.Start(); err != nil {
		if stderr != nil {
			stderr.CleanupFile()
		}
		if ctx.Err() == nil {
			t.logger.Error("Failed to start tart "+label,
				slog.String("name", name),
				slog.String("error", err.Error()),
			)
			t.reportExit(name, fmt.Errorf("start tart %s: %w", label, err))
		}
		return
	}
	err = cmd.Wait()
	if stderr != nil {
		if err != nil && ctx.Err() == nil {
			stderr.Close()
			t.logger.Error("Tart "+label+" exited with error",
				slog.String("name", name),
				slog.String("error", err.Error()),
				slog.String("stderr", stderr.Tail()),
				slog.String("log", stderr.Path()),
			)
		} else {
			stderr.CleanupFile()
		}
	}
	if ctx.Err() == nil {
		if err != nil {
			t.reportExit(name, fmt.Errorf("tart %s exited: %w", label, err))
		} else {
			t.reportExit(name, nil)
		}
	}
}

func (t *Provisioner) Stop(ctx context.Context, name string) error {
	if !isDisposableName(name, t.prefixes) {
		return fmt.Errorf("refusing to stop unmanaged Tart VM %q", name)
	}
	if t.isBase(name) {
		return fmt.Errorf("refusing to stop configured Tart base image %q", name)
	}

	t.mu.Lock()
	if reservation := t.stopping[name]; reservation != nil {
		t.mu.Unlock()
		select {
		case <-reservation.done:
			return reservation.err
		case <-ctx.Done():
			return fmt.Errorf("wait for Tart stop %q: %w", name, ctx.Err())
		}
	}
	lifecycle := t.running[name]
	reservation := &stopReservation{done: make(chan struct{})}
	if lifecycle == nil {
		t.stopping[name] = reservation
		t.mu.Unlock()
		err := t.confirmAbsent(ctx, name)
		t.finishStopReservation(name, reservation, err)
		return err
	}
	t.stopping[name] = reservation
	lifecycle.stopping = true
	lifecycle.cancel()
	t.mu.Unlock()

	select {
	case <-lifecycle.startDone:
	case <-ctx.Done():
		err := fmt.Errorf("wait for Tart startup before stopping %q: %w", name, ctx.Err())
		t.finishStopReservation(name, reservation, err)
		return err
	}

	finished := make(chan struct{})
	go func() {
		lifecycle.wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-ctx.Done():
		err := fmt.Errorf("wait for Tart processes before stopping %q: %w", name, ctx.Err())
		t.finishStopReservation(name, reservation, err)
		return err
	}

	var err error
	if lifecycle.vmCreated {
		err = t.removeVM(ctx, name)
	} else {
		err = t.confirmAbsent(ctx, name)
	}
	t.finishStopReservation(name, reservation, err)
	return err
}

func (t *Provisioner) confirmAbsent(ctx context.Context, name string) error {
	checkCtx, cancel := context.WithTimeout(ctx, tartCleanupTimeout)
	defer cancel()
	inventory, err := t.localInventory(checkCtx)
	if err != nil {
		return fmt.Errorf("verify Tart VM %q absence: %w", name, err)
	}
	if _, exists := findLocalVM(inventory, name); exists {
		return fmt.Errorf("refusing to stop unmanaged existing Tart VM %q", name)
	}
	return nil
}

func (t *Provisioner) finishStopReservation(name string, reservation *stopReservation, err error) {
	t.mu.Lock()
	reservation.err = err
	if t.stopping[name] == reservation {
		delete(t.stopping, name)
	}
	if err == nil {
		delete(t.running, name)
	}
	close(reservation.done)
	t.mu.Unlock()
}

func (t *Provisioner) finishStart(name string, lifecycle *vmLifecycle, err error) {
	select {
	case lifecycle.done <- err:
	default:
	}
}

func (t *Provisioner) Close() error {
	var names []string
	t.mu.Lock()
	for name := range t.running {
		names = append(names, name)
	}
	t.mu.Unlock()
	var errs []error
	for _, name := range names {
		ctx, cancel := context.WithTimeout(context.Background(), tartCleanupTimeout)
		if err := t.Stop(ctx, name); err != nil {
			errs = append(errs, err)
		}
		cancel()
	}
	return errors.Join(errs...)
}

// Reconcile removes only named disposable clones from all configured Tart pools.
func (t *Provisioner) Reconcile(ctx context.Context, prefixes, baseNames []string) error {
	t.mu.Lock()
	bases := make(map[string]struct{}, len(baseNames)+len(t.bases))
	for name := range t.bases {
		bases[name] = struct{}{}
	}
	for _, name := range baseNames {
		bases[name] = struct{}{}
		t.bases[name] = struct{}{}
	}
	t.mu.Unlock()
	t.cleanupMu.Lock()
	defer t.cleanupMu.Unlock()
	inventory, err := t.localInventory(ctx)
	if err != nil {
		return fmt.Errorf("list local Tart VMs: %w", err)
	}
	var errs []error
	for _, vm := range inventory {
		if vm.Name == "" || vm.Source != "local" || !isDisposableName(vm.Name, prefixes) {
			continue
		}
		if _, protected := bases[vm.Name]; protected {
			t.logger.Warn("Skipping configured Tart base during reconciliation", slog.String("name", vm.Name))
			continue
		}
		cleanupCtx, cancel := context.WithTimeout(ctx, tartCleanupTimeout)
		if err := t.removeVMLocked(cleanupCtx, vm.Name); err != nil {
			errs = append(errs, err)
		}
		cancel()
	}
	if len(errs) == 0 {
		verifyCtx, cancel := context.WithTimeout(ctx, tartCleanupTimeout)
		defer cancel()
		remaining, err := t.localInventory(verifyCtx)
		if err != nil {
			errs = append(errs, fmt.Errorf("verify Tart reconciliation: %w", err))
		} else {
			for _, vm := range remaining {
				if vm.Source != "local" || !isDisposableName(vm.Name, prefixes) {
					continue
				}
				if _, protected := bases[vm.Name]; protected {
					continue
				}
				if vm.Running {
					errs = append(errs, fmt.Errorf("Tart clone %q is still running after reconciliation", vm.Name))
				} else {
					errs = append(errs, fmt.Errorf("Tart clone %q still exists after reconciliation", vm.Name))
				}
			}
		}
	}
	return errors.Join(errs...)
}
func isDisposableName(name string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if prefix == "" || !strings.HasPrefix(name, prefix) || len(name) != len(prefix)+8 {
			continue
		}
		for _, r := range name[len(prefix):] {
			if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
				return false
			}
		}
		return true
	}
	return false
}

func (t *Provisioner) localInventory(ctx context.Context) ([]vmInventory, error) {
	out, err := exec.CommandContext(ctx, "tart", "list", "--source", "local", "--format", "json").Output()
	if err != nil {
		return nil, fmt.Errorf("tart list: %w", err)
	}
	var records []vmInventoryCommand
	if err := json.Unmarshal(out, &records); err != nil {
		return nil, fmt.Errorf("decode tart list: %w", err)
	}
	inventory := make([]vmInventory, 0, len(records))
	for _, record := range records {
		if record.Source != "local" || record.Name == "" || record.Running == nil || record.State == "" {
			return nil, fmt.Errorf("incomplete Tart VM inventory record: source=%q name=%q state=%q", record.Source, record.Name, record.State)
		}
		inventory = append(inventory, vmInventory{
			Source: record.Source, Name: record.Name, Running: *record.Running, State: record.State,
		})
	}
	return inventory, nil
}

func (t *Provisioner) removeVM(ctx context.Context, name string) error {
	if t.isBase(name) {
		return fmt.Errorf("refusing to remove configured Tart base image %q", name)
	}
	if !isDisposableName(name, t.prefixes) {
		return fmt.Errorf("refusing to remove unmanaged Tart VM %q", name)
	}
	t.cleanupMu.Lock()
	defer t.cleanupMu.Unlock()
	return t.removeVMLocked(ctx, name)
}
func (t *Provisioner) removeVMLocked(ctx context.Context, name string) error {
	inventory, err := t.localInventory(ctx)
	if err != nil {
		return fmt.Errorf("check Tart clone %q: %w", name, err)
	}
	vm, exists := findLocalVM(inventory, name)
	if !exists {
		return nil
	}
	if vm.Running || vm.State == "suspended" {
		if out, err := exec.CommandContext(ctx, "tart", "stop", name).CombinedOutput(); err != nil {
			verify, verifyErr := t.localInventory(ctx)
			if verifyErr != nil {
				return errors.Join(fmt.Errorf("stop Tart VM %q: %w: %s", name, err, out), verifyErr)
			}
			current, stillExists := findLocalVM(verify, name)
			if !stillExists {
				return nil
			}
			if current.Running || current.State == "running" {
				return fmt.Errorf("stop Tart VM %q: %w: %s", name, err, out)
			}
		}
	}
	verify, verifyErr := t.localInventory(ctx)
	if verifyErr != nil {
		return fmt.Errorf("verify stopped Tart VM %q: %w", name, verifyErr)
	}
	vm, exists = findLocalVM(verify, name)
	if !exists {
		return nil
	}
	if vm.Running || vm.State == "running" {
		return fmt.Errorf("Tart VM %q remains running after stop", name)
	}
	if vm.State != "stopped" {
		return fmt.Errorf("refusing to delete Tart VM %q in state %q", name, vm.State)
	}
	if out, err := exec.CommandContext(ctx, "tart", "delete", name).CombinedOutput(); err != nil {
		verify, verifyErr := t.localInventory(ctx)
		if verifyErr != nil {
			return errors.Join(fmt.Errorf("delete Tart clone %q: %w: %s", name, err, out), verifyErr)
		}
		if _, stillExists := findLocalVM(verify, name); stillExists {
			return fmt.Errorf("delete Tart clone %q: %w: %s", name, err, out)
		}
	}
	verify, err = t.localInventory(ctx)
	if err != nil {
		return fmt.Errorf("verify Tart clone deletion %q: %w", name, err)
	}
	if _, stillExists := findLocalVM(verify, name); stillExists {
		return fmt.Errorf("Tart clone %q still exists after deletion", name)
	}
	return nil
}

func findLocalVM(inventory []vmInventory, name string) (vmInventory, bool) {
	for _, vm := range inventory {
		if vm.Source == "local" && vm.Name == name {
			return vm, true
		}
	}
	return vmInventory{}, false
}

func (t *Provisioner) waitForAgent(ctx context.Context, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout after %v", timeout)
		}

		err := exec.CommandContext(ctx, "tart", "exec", name, "echo", "ok").Run()
		if err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
