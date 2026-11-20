package s2

import (
	"errors"
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
		{"with prefix keeps the trailing slash", "data", "dir/", "data/dir/"},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			if got := Key(tc.prefix, tc.name); got != tc.want {
				t.Errorf("Key(%q, %q) = %q, want %q", tc.prefix, tc.name, got, tc.want)
			}
		})
	}
}

func TestJoinURI(t *testing.T) {
	testCases := []struct {
		caseName string
		uri      string
		prefix   string
		want     string
	}{
		{"no prefix", "s3://b/", "", "s3://b/"},
		{"adds the trailing slash", "s3://b", "", "s3://b/"},
		{"nested prefix", "s3://b/p/", "a/b", "s3://b/p/a/b/"},
		{"prefix with a trailing slash", "gs://b/", "a/", "gs://b/a/"},
		{"escapes the prefix", "file:///d/", "x y", "file:///d/x%20y/"},
		{"takes a percent sign as is", "file:///d/", "x%y", "file:///d/x%25y/"},
		{"keeps an escaped path", "file:///d%20e/", "x", "file:///d%20e/x/"},
		{"keeps the host and port", "http://h:9/base/", "bk", "http://h:9/base/bk/"},
		{"drops the query", "https://h/b/?sv=x", "p", "https://h/b/p/"},
		{"drops an empty query", "https://h/b/?", "p", "https://h/b/p/"},
		{"drops the fragment", "https://h/b/#f", "p", "https://h/b/p/"},
		{"drops the credentials", "https://u:pw@h/b/", "p", "https://h/b/p/"},
		{"empty URI", "", "a", ""},
		{"opaque URI", "mailto:x", "p", ""},
		{"host and port without a scheme", "localhost:9000", "p", ""},
		{"no scheme", "rel/path", "p", ""},
		{"scheme-relative", "//h/b/", "p", ""},
		{"URI that does not parse", "%zz", "a", ""},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			if got := JoinURI(tc.uri, tc.prefix); got != tc.want {
				t.Errorf("JoinURI(%q, %q) = %q, want %q", tc.uri, tc.prefix, got, tc.want)
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

func TestValidateName(t *testing.T) {
	testCases := []struct {
		caseName string
		input    string
		wantErr  bool
	}{
		{"plain", "a.txt", false},
		{"nested", "a/b.txt", false},
		// One trailing "/" names a directory marker.
		{"marker", "a/b/", false},
		{"top-level marker", "a/", false},
		{"two trailing slashes", "a//", true},
		{"root marker", "./", true},
		{"dot inside a name", "a..b", false},
		{"leading dots in a name", "..a", false},
		{"a dotfile", ".keep", false},
		{"empty", "", true},
		{"dot", ".", true},
		{"parent", "..", true},
		{"escapes", "../other/secret.txt", true},
		{"escapes with a trailing slash", "../other/", true},
		{"escapes after cleaning", "a/../../other", true},
		{"cleans to the root", "a/..", true},
		{"rooted escape", "/../other/secret.txt", true},
		{"rooted escape with extra slashes", "//../other/", true},
		{"rooted", "/a.txt", true},
		// These resolve to a different name than they spell, which is how a
		// policy on the spelled name is evaded.
		{"dot element", "./private/secret.txt", true},
		{"dot element inside", "a/./b", true},
		{"empty element", "a//b", true},
		{"only slashes", "//", true},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			err := ValidateName(tc.input)
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidName) {
					t.Errorf("ValidateName(%q) = %v, want ErrInvalidName", tc.input, err)
				}
				return
			}
			if err != nil {
				t.Errorf("ValidateName(%q) = %v, want nil", tc.input, err)
			}
		})
	}
}

func TestValidatePrefix(t *testing.T) {
	testCases := []struct {
		caseName string
		input    string
		wantErr  bool
	}{
		{"unset selects everything", "", false},
		{"a name", "a/b.txt", false},
		{"a directory", "a/b/", false},
		{"the root itself", ".", true},
		{"the root with a slash", "./", true},
		{"escapes", "../other", true},
		{"escapes with a slash", "../other/", true},
		{"resolves elsewhere", "./a", true},
		{"two trailing slashes", "a//", true},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			err := ValidatePrefix(tc.input)
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidName) {
					t.Errorf("ValidatePrefix(%q) = %v, want ErrInvalidName", tc.input, err)
				}
				return
			}
			if err != nil {
				t.Errorf("ValidatePrefix(%q) = %v, want nil", tc.input, err)
			}
		})
	}
}
