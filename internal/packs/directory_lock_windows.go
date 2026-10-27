//go:build windows

package packs

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

type DirectoryLock struct {
	files      []*os.File
	overlapped []windows.Overlapped
}

func AcquireDirectoryLock(dir string) (*DirectoryLock, error) {
	legacyDir := filepath.Join(dir, legacyStateDir)
	legacyExists := false
	if _, err := os.Stat(legacyDir); err == nil {
		legacyExists = true
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	paths := []string{filepath.Join(dir, neutralStateDir, "owner.lock")}
	if legacyExists {
		paths = append([]string{filepath.Join(legacyDir, "owner.lock")}, paths...)
	}
	for _, path := range paths {
		if err := RejectSymlinks(dir, path); err != nil {
			return nil, err
		}
	}
	l := &DirectoryLock{}
	for _, path := range paths {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			_ = l.Close()
			return nil, err
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			_ = l.Close()
			return nil, err
		}
		var overlapped windows.Overlapped
		flags := uint32(windows.LOCKFILE_EXCLUSIVE_LOCK | windows.LOCKFILE_FAIL_IMMEDIATELY)
		if err = windows.LockFileEx(windows.Handle(f.Fd()), flags, 0, 1, 0, &overlapped); err != nil {
			_ = f.Close()
			_ = l.Close()
			return nil, fmt.Errorf("parser packs directory is already in use: %w", err)
		}
		l.files = append(l.files, f)
		l.overlapped = append(l.overlapped, overlapped)
	}
	if err := migrateLegacyState(dir); err != nil {
		_ = l.Close()
		return nil, err
	}
	return l, nil
}

func (l *DirectoryLock) Close() error {
	if l == nil {
		return nil
	}
	var first error
	for i, f := range l.files {
		_ = windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, &l.overlapped[i])
		if err := f.Close(); first == nil {
			first = err
		}
	}
	return first
}
