package s2

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ConfigTestSuite struct {
	suite.Suite
}

func TestConfigTestSuite(t *testing.T) {
	suite.Run(t, &ConfigTestSuite{})
}

func (s *ConfigTestSuite) TestParseRoot() {
	testCases := []struct {
		caseName   string
		root       string
		wantName   string
		wantPrefix string
	}{
		{
			caseName:   "name only",
			root:       "my-bucket",
			wantName:   "my-bucket",
			wantPrefix: "",
		},
		{
			caseName:   "name with prefix",
			root:       "my-bucket/some/prefix",
			wantName:   "my-bucket",
			wantPrefix: "some/prefix",
		},
		{
			caseName:   "slashes trimmed",
			root:       "/my-bucket/pfx/",
			wantName:   "my-bucket",
			wantPrefix: "pfx",
		},
		{
			caseName:   "single prefix segment",
			root:       "my-bucket/data",
			wantName:   "my-bucket",
			wantPrefix: "data",
		},
		{
			// A backend spells a full key two ways -- joined with the object
			// name, or concatenated to keep that name verbatim -- and they
			// agree only while the prefix is already clean.
			caseName:   "prefix cleaned",
			root:       "my-bucket/data//objects/./",
			wantName:   "my-bucket",
			wantPrefix: "data/objects",
		},
		{
			// "." joins away but is a live element in a listing prefix, so a
			// storage rooted on it would write keys it could never list.
			caseName:   "prefix resolving to the root",
			root:       "my-bucket/a/..",
			wantName:   "my-bucket",
			wantPrefix: "",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			name, prefix := ParseRoot(tc.root)
			s.Equal(tc.wantName, name)
			s.Equal(tc.wantPrefix, prefix)
		})
	}
}
