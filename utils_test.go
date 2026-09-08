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

func TestKey(t *testing.T) {
	testCases := []struct {
		caseName string
		prefix   string
		name     string
		want     string
	}{
		{"no prefix", "", "a.txt", "a.txt"},
		{"no prefix keeps the trailing slash", "", "dir/", "dir/"},
		{"with prefix", "data", "a.txt", "data/a.txt"},
		{"prefix with a trailing slash", "data/", "a.txt", "data/a.txt"},
		{"nested prefix", "data/sub", "a.txt", "data/sub/a.txt"},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			if got := Key(tc.prefix, tc.name); got != tc.want {
				t.Errorf("Key(%q, %q) = %q, want %q", tc.prefix, tc.name, got, tc.want)
			}
		})
	}
}

func TestRelName(t *testing.T) {
	testCases := []struct {
		caseName string
		prefix   string
		name     string
		want     string
	}{
		{"no prefix", "", "a.txt", "a.txt"},
		{"strips the prefix", "data", "data/a.txt", "a.txt"},
		{"prefix with a trailing slash", "data/", "data/a.txt", "a.txt"},
		{"unnormalized prefix", "data//sub", "data/sub/a.txt", "a.txt"},
		{"name without the prefix is untouched", "data", "data2/a.txt", "data2/a.txt"},
		{"name equal to the prefix is untouched", "data", "data", "data"},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			if got := RelName(tc.prefix, tc.name); got != tc.want {
				t.Errorf("RelName(%q, %q) = %q, want %q", tc.prefix, tc.name, got, tc.want)
			}
		})
	}
}
