package tart

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

const (
	manifestVersion = 1
	cloneReserved   = "reserved"
	cloneOwned      = "owned"
	cloneForeign    = "foreign"
)

type tartVM struct {
	Source  string `json:"Source"`
	Name    string `json:"Name"`
	Running *bool  `json:"Running"`
	State   string `json:"State"`
}

type cloneManifest struct {
	Version int               `json:"version"`
	Clones  map[string]string `json:"clones"`
}

var manifestLocks sync.Map

func tartHome() (string, error) {
	home := os.Getenv("TART_HOME")
	if home == "" {
		userHome, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("find home directory: %w", err)
		}
		home = filepath.Join(userHome, ".tart")
	}
	abs, err := filepath.Abs(home)
	if err != nil {
		return "", fmt.Errorf("resolve Tart home: %w", err)
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return "", fmt.Errorf("create Tart home: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", fmt.Errorf("resolve Tart home symlinks: %w", err)
	}
	return resolved, nil
}

func ownershipManifestPath() (string, error) {
	home, err := tartHome()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".outrunner-owned-clones.json"), nil
}

// AcquireHost prevents multiple outrunner controllers sharing a Tart home from
// reconciling or creating clones concurrently. The caller must hold the lock
// until all Tart provisioners have closed.
func AcquireHost() (io.Closer, error) {
	home, err := tartHome()
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filepath.Join(home, ".outrunner-host.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open Tart host lock: %w", err)
	}
	if err := lockHostFile(file); err != nil {
		_ = file.Close()
		if isHostLockHeld(err) {
			return nil, fmt.Errorf("another outrunner holds the Tart host lock: %w", err)
		}
		return nil, fmt.Errorf("lock Tart host: %w", err)
	}
	return &hostLock{file: file}, nil

}

type hostLock struct {
	file *os.File
	once sync.Once
	err  error
}

func (l *hostLock) Close() error {
	l.once.Do(func() {
		unlockErr := unlockHostFile(l.file)
		closeErr := l.file.Close()
		l.err = errors.Join(unlockErr, closeErr)
	})
	return l.err
}

func listVMs(ctx context.Context) ([]tartVM, error) {
	out, err := exec.CommandContext(ctx, "tart", "list", "--source", "local", "--format", "json").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("tart list: %w: %s", err, strings.TrimSpace(string(out)))
	}
	var vms []tartVM
	if err := json.Unmarshal(out, &vms); err != nil {
		return nil, fmt.Errorf("decode tart list JSON: %w", err)
	}
	for _, vm := range vms {
		if vm.Name == "" || vm.Source != "local" {
			return nil, fmt.Errorf("decode Tart local inventory JSON: entry is missing Name or has source %q", vm.Source)
		}
		if vm.Running == nil || vm.State == "" {
			return nil, fmt.Errorf("decode Tart local inventory JSON: entry %q is missing Running or State", vm.Name)
		}
	}
	return vms, nil
}

func findVM(vms []tartVM, name string) (tartVM, bool, error) {
	var found tartVM
	present := false
	for _, vm := range vms {
		if vm.Name != name {
			continue
		}
		if present {
			return tartVM{}, false, fmt.Errorf("multiple Tart inventory entries have the name %q", name)
		}
		found = vm
		present = true
	}
	return found, present, nil
}

func manifestLock(path string) *sync.Mutex {
	lock, _ := manifestLocks.LoadOrStore(path, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

func readManifest(path string) (cloneManifest, error) {
	manifest := cloneManifest{Version: manifestVersion, Clones: make(map[string]string)}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return manifest, nil
	}
	if err != nil {
		return cloneManifest{}, fmt.Errorf("read Tart ownership manifest: %w", err)
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return cloneManifest{}, fmt.Errorf("decode Tart ownership manifest: %w", err)
	}
	if manifest.Version != manifestVersion || manifest.Clones == nil {
		return cloneManifest{}, fmt.Errorf("decode Tart ownership manifest: unsupported or incomplete manifest")
	}
	for name, state := range manifest.Clones {
		if name == "" || (state != cloneReserved && state != cloneOwned && state != cloneForeign) {
			return cloneManifest{}, fmt.Errorf("decode Tart ownership manifest: invalid clone entry %q", name)
		}
	}
	return manifest, nil
}

func writeManifest(path string, manifest cloneManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode Tart ownership manifest: %w", err)
	}
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, ".outrunner-owned-clones-*.tmp")
	if err != nil {
		return fmt.Errorf("create Tart ownership manifest temporary file: %w", err)
	}
	tmpPath := file.Name()
	defer os.Remove(tmpPath)
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return fmt.Errorf("secure Tart ownership manifest: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("write Tart ownership manifest: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync Tart ownership manifest: %w", err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("close Tart ownership manifest: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace Tart ownership manifest: %w", err)
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open Tart ownership directory: %w", err)
	}
	defer dirFile.Close()
	if err := dirFile.Sync(); err != nil {
		return fmt.Errorf("sync Tart ownership directory: %w", err)
	}
	return nil

}

func (t *Provisioner) updateManifest(name, state string) error {
	path, err := ownershipManifestPath()
	if err != nil {
		return err
	}
	lock := manifestLock(path)
	lock.Lock()
	defer lock.Unlock()
	manifest, err := readManifest(path)
	if err != nil {
		return err
	}
	previous, exists := manifest.Clones[name]
	switch state {
	case cloneReserved:
		if exists {
			return fmt.Errorf("Tart VM %q already has ownership state %q", name, previous)
		}
		manifest.Clones[name] = cloneReserved
	case cloneOwned:
		if !exists {
			return fmt.Errorf("Tart VM %q has no ownership reservation", name)
		}
		manifest.Clones[name] = cloneOwned
	case cloneForeign:
		if !exists {
			return fmt.Errorf("Tart VM %q has no ownership reservation", name)
		}
		manifest.Clones[name] = cloneForeign

	case "":
		if !exists {
			return nil
		}
		delete(manifest.Clones, name)
	default:
		return fmt.Errorf("invalid Tart ownership state %q", state)
	}
	return writeManifest(path, manifest)
}

func (t *Provisioner) ownedManifest() (cloneManifest, string, error) {
	path, err := ownershipManifestPath()
	if err != nil {
		return cloneManifest{}, "", err
	}
	lock := manifestLock(path)
	lock.Lock()
	defer lock.Unlock()
	manifest, err := readManifest(path)
	return manifest, path, err
}

func (t *Provisioner) removeManifestEntry(path, name string) error {
	lock := manifestLock(path)
	lock.Lock()
	defer lock.Unlock()
	manifest, err := readManifest(path)
	if err != nil {
		return err
	}
	if _, ok := manifest.Clones[name]; !ok {
		return nil
	}
	delete(manifest.Clones, name)
	return writeManifest(path, manifest)
}
