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
	path := filepath.Join(dir, neutralStateDir, "owner.lock")
	if err := RejectSymlinks(dir, path); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("parser packs directory is already in use: %w", err)
	}
	return &DirectoryLock{files: []*os.File{f}}, nil
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
