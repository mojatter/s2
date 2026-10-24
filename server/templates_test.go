package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsPreviewable(t *testing.T) {
	testCases := []struct {
		caseName string
		cfg      *Config
		name     string
		size     uint64
		want     bool
	}{
		{caseName: "zero config text under default", cfg: &Config{}, name: "a.txt", size: 10, want: true},
		{caseName: "zero config text at default", cfg: &Config{}, name: "a.txt", size: DefaultMaxPreviewSize, want: true},
		{caseName: "zero config text over default", cfg: &Config{}, name: "a.txt", size: DefaultMaxPreviewSize + 1, want: false},
		{caseName: "explicit limit text at", cfg: &Config{MaxPreviewSize: 5}, name: "a.txt", size: 5, want: true},
		{caseName: "explicit limit text over", cfg: &Config{MaxPreviewSize: 5}, name: "a.txt", size: 6, want: false},
		{caseName: "image ignores limit", cfg: &Config{MaxPreviewSize: 5}, name: "a.png", size: 1 << 30, want: true},
		{caseName: "unknown extension", cfg: &Config{}, name: "a.bin", size: 1, want: false},
		{caseName: "uppercase extension", cfg: &Config{}, name: "A.TXT", size: 1, want: true},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			fn := templateFuncs(tc.cfg)["isPreviewable"].(func(string, uint64) bool)
			assert.Equal(t, tc.want, fn(tc.name, tc.size))
		})
	}
}
