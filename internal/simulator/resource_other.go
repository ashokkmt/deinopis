//go:build !darwin && !linux

package simulator

import "errors"

func processMetrics() (float64, uint64, error) {
	return 0, 0, errors.New("process CPU/RSS metrics are unsupported on this operating system")
}
