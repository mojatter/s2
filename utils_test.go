package s2

import (
	"math"
	"testing"
)

func TestMustInt64(t *testing.T) {
	testCases := []struct {
		caseName string
		input    uint64
		want     int64
	}{
		{"zero", 0, 0},
		{"one", 1, 1},
		{"max int64", math.MaxInt64, math.MaxInt64},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			got := MustInt64(tc.input)
			if got != tc.want {
				t.Errorf("MustInt64(%d) = %d, want %d", tc.input, got, tc.want)
			}
		})
	}
}

func TestMustInt64Panic(t *testing.T) {
	testCases := []struct {
		caseName string
		input    uint64
	}{
		{"MaxInt64 + 1", math.MaxInt64 + 1},
		{"MaxUint64", math.MaxUint64},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("MustInt64(%d) did not panic", tc.input)
				}
			}()
			MustInt64(tc.input)
		})
	}
}

func TestMustUint64(t *testing.T) {
	testCases := []struct {
		caseName string
		input    int64
		want     uint64
	}{
		{"zero", 0, 0},
		{"one", 1, 1},
		{"max int64", math.MaxInt64, math.MaxInt64},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			got := MustUint64(tc.input)
			if got != tc.want {
				t.Errorf("MustUint64(%d) = %d, want %d", tc.input, got, tc.want)
			}
		})
	}
}

func TestMustUint64Panic(t *testing.T) {
	testCases := []struct {
		caseName string
		input    int64
	}{
		{"minus one", -1},
		{"min int64", math.MinInt64},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("MustUint64(%d) did not panic", tc.input)
				}
			}()
			MustUint64(tc.input)
		})
	}
}
