// Package numconv contains internal helpers for converting between Go's
// signed and unsigned integer types when dealing with stdlib APIs that
// disagree on signedness (notably os.FileInfo.Size and io.Seeker offsets).
//
// These helpers panic on overflow rather than silently truncating; they
// are intended for paths where the input cannot legitimately exceed the
// destination range, so a panic indicates a programmer error.
package numconv

import (
	"fmt"
	"math"
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
