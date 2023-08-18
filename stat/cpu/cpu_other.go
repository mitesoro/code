//go:build !linux

package cpu

// RefreshCpu returns cpu usage, always returns 0 on systems other than linux.
func RefreshCpu() uint64 {
	return 0
}
