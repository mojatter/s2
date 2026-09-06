package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContentTypeByExt(t *testing.T) {
	// ContentTypeByExt tries mime.TypeByExtension first, then falls
	// back to a built-in switch. The OS MIME database differs between
	// macOS and Linux, so we only assert on properties that hold
	// regardless of the platform.
	//
	// The empty result for an unrecognized extension is what lets the S3
	// API answer with no Content-Type at all (see setContentType in the
	// s3api package).
	testCases := []struct {
		caseName     string
		ext          string
		wantEmpty    bool   // extension is not recognized at all
		wantContains string // result must contain this substring otherwise
	}{
		{caseName: "Go source returns text", ext: ".go", wantContains: "text/"},
		{caseName: "JSON", ext: ".json", wantContains: "json"},
		{caseName: "CSS", ext: ".css", wantContains: "css"},
		{caseName: "PNG image", ext: ".png", wantContains: "image/png"},
		{caseName: "WebP image", ext: ".webp", wantContains: "image/webp"},
		{caseName: "Wasm binary", ext: ".wasm", wantContains: "wasm"},
		{caseName: "unknown extension", ext: ".nopesuchtype", wantEmpty: true},
		{caseName: "empty extension", ext: "", wantEmpty: true},
		{caseName: "bare dot", ext: ".", wantEmpty: true},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			got := ContentTypeByExt(tc.ext)
			if tc.wantEmpty {
				assert.Empty(t, got)
				return
			}
			assert.Contains(t, got, tc.wantContains)
		})
	}
}
