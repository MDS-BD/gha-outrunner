//go:build !darwin && !linux

package tart

import (
	"fmt"
	"os"
)

func lockHostFile(file *os.File) error {
	return fmt.Errorf("Tart host locking is unsupported on this platform")
}

func isHostLockHeld(err error) bool {
	return false
}
func unlockHostFile(file *os.File) error {
	return fmt.Errorf("Tart host locking is unsupported on this platform")
}
