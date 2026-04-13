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
	}

	for _, tc := range testCases {
		s.Run(tc.caseName, func() {
			name, prefix := ParseRoot(tc.root)
			s.Equal(tc.wantName, name)
			s.Equal(tc.wantPrefix, prefix)
		})
	}
}
