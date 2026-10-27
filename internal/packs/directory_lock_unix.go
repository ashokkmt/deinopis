//go:build !windows

package packs

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

type DirectoryLock struct{ files []*os.File }

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
	var files []*os.File
	for _, path := range paths {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			closeFiles(files)
			return nil, err
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			closeFiles(files)
			return nil, err
		}
		if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			_ = f.Close()
			closeFiles(files)
			return nil, fmt.Errorf("parser packs directory is already in use: %w", err)
		}
		files = append(files, f)
	}
	if err := migrateLegacyState(dir); err != nil {
		closeFiles(files)
		return nil, err
	}
	return &DirectoryLock{files: files}, nil
}

func (l *DirectoryLock) Close() error {
	if l == nil {
		return nil
	}
	var first error
	for _, f := range l.files {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		if err := f.Close(); first == nil {
			first = err
		}
	}
	return first
}

func closeFiles(files []*os.File) {
	for _, f := range files {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}
}
