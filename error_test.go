package s2

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrNotExistMessage(t *testing.T) {
	err := fmt.Errorf("%w: test-object", ErrNotExist)
	assert.Equal(t, "s2: object not exist: test-object", err.Error())
}

func TestErrNotExist_Is(t *testing.T) {
	testCases := []struct {
		caseName string
		err      error
		want     bool
	}{
		{
			caseName: "wrapped",
			err:      fmt.Errorf("%w: test", ErrNotExist),
			want:     true,
		},
		{
			caseName: "double wrapped",
			err:      fmt.Errorf("outer: %w", fmt.Errorf("%w: test", ErrNotExist)),
			want:     true,
		},
		{
			caseName: "direct sentinel",
			err:      ErrNotExist,
			want:     true,
		},
		{
			caseName: "other error",
			err:      errors.New("some other error"),
			want:     false,
		},
		{
			caseName: "nil error",
			err:      nil,
			want:     false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			assert.Equal(t, tc.want, errors.Is(tc.err, ErrNotExist))
		})
	}
}
