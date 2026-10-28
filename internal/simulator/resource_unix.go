//go:build darwin || linux

package simulator

import (
	"fmt"
	"runtime"
	"syscall"
)

func processMetrics() (float64, uint64, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(0, &usage); err != nil {
		return 0, 0, err
	}
	cpu := float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
	rss := uint64(usage.Maxrss)
	if runtime.GOOS == "linux" {
		rss *= 1024
	}
	if rss == 0 {
		return cpu, 0, fmt.Errorf("operating system reported zero peak RSS")
	}
	return cpu, rss, nil
}
