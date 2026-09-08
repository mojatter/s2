package s2

import (
	"fmt"
	"math"
	"path"
	"strings"
)

// MustInt64 converts a uint64 to int64, panicking if the value exceeds math.MaxInt64.
func MustInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		panic(fmt.Sprintf("numconv: uint64 value %d overflows int64", v))
	}
	return int64(v)
}

// MustUint64 converts an int64 to uint64, panicking if the value is negative.
func MustUint64(v int64) uint64 {
	if v < 0 {
		panic(fmt.Sprintf("numconv: int64 value %d is negative", v))
	}
	return uint64(v)
}

// Key joins a storage prefix and an object name.
func Key(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return path.Join(prefix, name)
}

// RelName is the inverse of Key: it strips the storage prefix, leaving a name
// that does not carry it untouched.
func RelName(prefix, name string) string {
	if prefix == "" {
		return name
	}
	dir := path.Clean(prefix) + "/"
	if !strings.HasPrefix(name, dir) {
		return name
	}
	return name[len(dir):]
}
