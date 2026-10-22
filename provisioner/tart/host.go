package tart

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

const hostLockName = ".outrunner-host.lock"

// AcquireHost prevents multiple controllers from mutating the same Tart store.
// The caller must hold the returned file until all Tart provisioners have shut down.
func AcquireHost() (*os.File, error) {
	home := os.Getenv("TART_HOME")
	if home == "" {
		userHome, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("resolve Tart home: %w", err)
		}
		home = filepath.Join(userHome, ".tart")
	}
	home, err := filepath.Abs(filepath.Clean(home))
	if err != nil {
		return nil, fmt.Errorf("resolve Tart home: %w", err)
	}
	if err := os.MkdirAll(home, 0o700); err != nil {
		return nil, fmt.Errorf("create Tart home: %w", err)
	}
	file, err := os.OpenFile(filepath.Join(home, hostLockName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open Tart host lock: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if err == unix.EWOULDBLOCK || err == unix.EAGAIN {
			return nil, fmt.Errorf("Tart store %q is already owned by another outrunner controller", home)
		}
		return nil, fmt.Errorf("acquire Tart host lock: %w", err)
	}
	return file, nil
}
