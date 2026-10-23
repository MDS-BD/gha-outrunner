package tart

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	outrunner "github.com/NetwindHQ/gha-outrunner"
)

const (
	maxStderrLog   = 1024
	cleanupTimeout = 30 * time.Second
)

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

// Provisioner creates ephemeral Tart VMs as GitHub Actions runners.
// Uses `tart exec` via the guest agent for command execution.
type provisionedVM struct {
	cancel      context.CancelFunc
	startupDone chan struct{}
	processes   sync.WaitGroup
	done        chan error
	exitOnce    sync.Once
	owned       bool
	exitErr     error
	stopping    bool
	stopDone    chan struct{}
	stopErr     error
}

// Provisioner creates ephemeral Tart VMs as GitHub Actions runners.
// Uses `tart exec` via the guest agent for command execution.
type Provisioner struct {
	logger  *slog.Logger
	mu      sync.Mutex
	closed  bool
	running map[string]*provisionedVM
}

func New(logger *slog.Logger) *Provisioner {
	return &Provisioner{
		logger:  logger,
		running: make(map[string]*provisionedVM),
	}
}

func (t *Provisioner) Start(ctx context.Context, req *outrunner.RunnerRequest) error {
	if req == nil || req.Runner == nil || req.Runner.Tart == nil {
		return fmt.Errorf("no tart config for runner")
	}
	if req.Name == "" {
		return fmt.Errorf("no Tart VM name")
	}
	img := req.Runner.Tart
	runCtx, cancel := context.WithCancel(ctx)
	vm := &provisionedVM{
		cancel:      cancel,
		startupDone: make(chan struct{}),
		done:        make(chan error, 1),
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		cancel()
		return fmt.Errorf("Tart provisioner is closed")
	}
	if _, exists := t.running[req.Name]; exists {
		t.mu.Unlock()
		cancel()
		return fmt.Errorf("Tart VM %q is already managed", req.Name)
	}
	t.running[req.Name] = vm
	t.mu.Unlock()
	defer func() {
		t.mu.Lock()
		close(vm.startupDone)
		t.mu.Unlock()
	}()

	abort := func(cause error) error {
		cancel()
		if !vm.owned {
			t.removeManaged(req.Name, vm)
			return cause
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cleanupCancel()
		if err := t.stopOwned(cleanupCtx, req.Name, vm); err != nil {
			return errors.Join(cause, fmt.Errorf("clean up failed Tart start %q: %w", req.Name, err))
		}
		t.removeManaged(req.Name, vm)
		return cause
	}

	// 1. Claim an absent destination before clone so crash-partial clones are recoverable.
	t.logger.Debug("Cloning VM", slog.String("image", img.Image), slog.String("name", req.Name))
	inventory, err := listVMs(runCtx)
	if err != nil {
		return abort(fmt.Errorf("check Tart clone destination: %w", err))
	}
	if _, exists, err := findVM(inventory, req.Name); err != nil {
		return abort(err)
	} else if exists {
		return abort(fmt.Errorf("Tart VM %q already exists", req.Name))
	}
	if err := t.updateManifest(req.Name, cloneReserved); err != nil {
		return abort(fmt.Errorf("reserve Tart VM ownership: %w", err))
	}
	vm.owned = true
	if out, err := exec.CommandContext(runCtx, "tart", "clone", img.Image, req.Name).CombinedOutput(); err != nil {
		cloneErr := fmt.Errorf("tart clone: %w: %s", err, out)
		checkCtx, checkCancel := context.WithTimeout(context.Background(), cleanupTimeout)
		current, inventoryErr := listVMs(checkCtx)
		checkCancel()
		if inventoryErr != nil {
			return abort(errors.Join(cloneErr, fmt.Errorf("check Tart clone after failure: %w", inventoryErr)))
		}
		_, exists, findErr := findVM(current, req.Name)
		if findErr != nil {
			return abort(errors.Join(cloneErr, findErr))
		}
		if exists && strings.Contains(strings.ToLower(string(out)), "already exists") {
			if err := t.updateManifest(req.Name, cloneForeign); err != nil {
				return abort(errors.Join(cloneErr, fmt.Errorf("mark colliding Tart VM as foreign: %w", err)))
			}
			vm.owned = false
			cancel()
			t.removeManaged(req.Name, vm)
			return cloneErr
		}
		return abort(cloneErr)
	}
	if err := t.updateManifest(req.Name, cloneOwned); err != nil {
		return abort(fmt.Errorf("record Tart clone ownership: %w", err))
	}

	// 2. Set resources
	if out, err := exec.CommandContext(runCtx, "tart", "set", req.Name,
		"--cpu", strconv.Itoa(img.CPUs),
		"--memory", strconv.Itoa(img.MemoryMB),
	).CombinedOutput(); err != nil {
		return abort(fmt.Errorf("tart set: %w: %s", err, out))
	}

	// 3. Run in background (tart run is blocking)
	t.mu.Lock()
	if t.closed || runCtx.Err() != nil {
		t.mu.Unlock()
		return abort(fmt.Errorf("Tart provisioner closed during startup"))
	}
	vm.processes.Add(1)
	go func() {
		defer vm.processes.Done()
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
		err := t.runAndLog(runCtx, req.Name, "vm", args)
		t.notifyExit(vm, err, runCtx)
	}()
	t.mu.Unlock()

	// 4. Wait for guest agent
	t.logger.Debug("Waiting for guest agent", slog.String("name", req.Name))
	if err := t.waitForAgent(runCtx, req.Name, 3*time.Minute); err != nil {
		return abort(fmt.Errorf("guest agent not ready: %w", err))
	}
	if err := runCtx.Err(); err != nil {
		return abort(fmt.Errorf("start Tart runner: %w", err))
	}

	// 5. Start runner via tart exec
	runnerCmd := img.RunnerCmd
	if runnerCmd == "" {
		runnerCmd = "/actions-runner/run.sh"
	}
	t.logger.Info("Starting runner in VM",
		slog.String("name", req.Name),
		slog.String("cmd", runnerCmd),
	)
	t.mu.Lock()
	if t.closed || runCtx.Err() != nil {
		t.mu.Unlock()
		return abort(fmt.Errorf("Tart provisioner closed during startup"))
	}
	vm.processes.Add(1)
	go func() {
		defer vm.processes.Done()
		err := t.runAndLog(runCtx, req.Name, "runner",
			[]string{"exec", req.Name, runnerCmd, "--jitconfig", req.JITConfig})
		t.notifyExit(vm, err, runCtx)
	}()
	t.mu.Unlock()
	t.logger.Info("Runner started in VM", slog.String("name", req.Name))
	return nil
}

// runAndLog runs a tart command, streaming stderr to a temp file.
// On failure it logs the last 1KB of stderr inline plus the full log path.
// On success it removes the log file.
func (t *Provisioner) runAndLog(ctx context.Context, name, label string, args []string) error {
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
		}
		return err
	}
	err = cmd.Wait()
	if err != nil && ctx.Err() == nil {
		attrs := []any{
			slog.String("name", name),
			slog.String("error", err.Error()),
		}
		if stderr != nil {
			stderr.Close()
			attrs = append(attrs,
				slog.String("stderr", stderr.Tail()),
				slog.String("log", stderr.Path()),
			)
		}
		t.logger.Error("Tart "+label+" exited with error", attrs...)
	} else if stderr != nil {
		stderr.CleanupFile()
	}
	return err

}

func (t *Provisioner) notifyExit(vm *provisionedVM, err error, ctx context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if ctx.Err() != nil || vm.stopping || t.closed {
		return
	}
	vm.exitOnce.Do(func() {
		vm.exitErr = err
		vm.done <- err
		close(vm.done)
	})
}

// Done reports the runner process's first exit after successful startup.
func (t *Provisioner) Done(name string) <-chan error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if vm, ok := t.running[name]; ok {
		return vm.done
	}
	done := make(chan error, 1)
	if t.closed {
		done <- nil
	} else {
		done <- fmt.Errorf("Tart VM %q is not managed", name)
	}
	close(done)
	return done

}

// Reconcile removes only orphaned clones recorded by this provisioner.
func (t *Provisioner) Reconcile(ctx context.Context, prefixes, protectedBases []string) error {
	manifest, path, err := t.ownedManifest()
	if err != nil {
		return err
	}
	inventory, err := listVMs(ctx)
	if err != nil {
		return err
	}
	protected := make(map[string]struct{}, len(protectedBases))
	for _, base := range protectedBases {
		protected[base] = struct{}{}
	}
	var cleanupErrs []error
	for name, state := range manifest.Clones {
		_, exists, err := findVM(inventory, name)
		if err != nil {
			cleanupErrs = append(cleanupErrs, err)
			continue
		}
		if !exists {
			if err := t.removeManifestEntry(path, name); err != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("remove absent Tart ownership %q: %w", name, err))
			}
			continue
		}
		if state == cloneForeign {
			t.logger.Warn("Preserving Tart VM recorded as a foreign collision", slog.String("name", name))
			continue
		}
		if _, isBase := protected[name]; isBase {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("refusing to clean owned Tart VM %q because it is a configured base image", name))
			t.logger.Error("Refusing to clean configured Tart base image", slog.String("name", name))
			continue
		}
		t.logger.Info("Cleaning up owned orphaned VM", slog.String("name", name))
		cleanupCtx, cancel := context.WithTimeout(ctx, cleanupTimeout)
		if err := t.deleteOwned(cleanupCtx, name, path); err != nil {
			cleanupErrs = append(cleanupErrs, err)
		}
		cancel()
	}
	for _, vm := range inventory {
		if _, known := manifest.Clones[vm.Name]; known {
			continue
		}
		for _, prefix := range prefixes {
			if strings.HasPrefix(vm.Name, prefix) {
				t.logger.Warn("Preserving Tart VM without ownership record; remove it manually if it is an orphan",
					slog.String("name", vm.Name),
					slog.String("prefix", prefix),
				)
				break
			}
		}
	}
	return errors.Join(cleanupErrs...)
}

func (t *Provisioner) deleteOwned(ctx context.Context, name, manifestPath string) error {
	// Stop the VM (idempotent)
	if out, err := exec.CommandContext(ctx, "tart", "stop", name).CombinedOutput(); err != nil {
		checkCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		inventory, listErr := listVMs(checkCtx)
		cancel()
		if listErr != nil {
			return errors.Join(fmt.Errorf("stop owned Tart VM %q: %w: %s", name, err, strings.TrimSpace(string(out))), listErr)
		}
		if vm, exists, findErr := findVM(inventory, name); findErr != nil {
			return findErr
		} else if exists && *vm.Running {
			return fmt.Errorf("stop owned Tart VM %q: %w: %s", name, err, strings.TrimSpace(string(out)))
		}
	}
	// Delete the clone
	if out, err := exec.CommandContext(ctx, "tart", "delete", name).CombinedOutput(); err != nil {
		checkCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		inventory, listErr := listVMs(checkCtx)
		cancel()
		if listErr != nil {
			return errors.Join(fmt.Errorf("delete owned Tart VM %q: %w: %s", name, err, strings.TrimSpace(string(out))), listErr)
		}
		if _, exists, findErr := findVM(inventory, name); findErr != nil {
			return findErr
		} else if exists {
			return fmt.Errorf("delete owned Tart VM %q: %w: %s", name, err, strings.TrimSpace(string(out)))
		}
	}
	checkCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	inventory, err := listVMs(checkCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("verify deleted Tart VM %q: %w", name, err)
	}
	if _, exists, err := findVM(inventory, name); err != nil {
		return err
	} else if exists {
		return fmt.Errorf("Tart VM %q remains after deletion", name)
	}
	return t.removeManifestEntry(manifestPath, name)
}

func (t *Provisioner) stopOwned(ctx context.Context, name string, vm *provisionedVM) error {
	vm.cancel()
	processesDone := make(chan struct{})
	go func() {
		vm.processes.Wait()
		close(processesDone)
	}()
	select {
	case <-processesDone:
	case <-ctx.Done():
		return ctx.Err()
	}

	manifest, path, err := t.ownedManifest()
	if err != nil {
		return err
	}
	state, owned := manifest.Clones[name]
	if !owned || state == cloneForeign {
		return nil
	}
	return t.deleteOwned(ctx, name, path)
}

func (t *Provisioner) Stop(ctx context.Context, name string) error {
	t.mu.Lock()
	vm, ok := t.running[name]
	if !ok {
		t.mu.Unlock()
		return nil
	}
	if vm.stopping {
		stopDone := vm.stopDone
		t.mu.Unlock()
		select {
		case <-stopDone:
			return vm.stopErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	vm.stopping = true
	vm.stopDone = make(chan struct{})
	stopDone := vm.stopDone
	vm.cancel()
	t.mu.Unlock()

	var err error
	select {
	case <-vm.startupDone:
		err = t.stopOwned(ctx, name, vm)
	case <-ctx.Done():
		err = ctx.Err()
	}
	t.mu.Lock()
	vm.stopErr = err
	vm.stopping = false
	if err == nil && t.running[name] == vm {
		delete(t.running, name)
	}

	close(stopDone)
	t.mu.Unlock()
	return err
}

func (t *Provisioner) Close() error {
	t.mu.Lock()
	t.closed = true
	vms := make(map[string]*provisionedVM, len(t.running))
	for name, vm := range t.running {
		vm.cancel()
		vms[name] = vm
	}
	t.mu.Unlock()
	var cleanupErrs []error
	for name := range vms {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		if err := t.Stop(cleanupCtx, name); err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("close Tart VM %q: %w", name, err))
		}
		cancel()
	}
	return errors.Join(cleanupErrs...)
}

func (t *Provisioner) removeManaged(name string, vm *provisionedVM) {
	t.mu.Lock()
	if t.running[name] == vm {
		delete(t.running, name)
	}
	t.mu.Unlock()
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
