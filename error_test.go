package s2

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrNotExist_Error(t *testing.T) {
	err := &ErrNotExist{Name: "test-object"}
	assert.Equal(t, "not exist: test-object", err.Error())
}

func TestIsNotExist(t *testing.T) {
	testCases := []struct {
		caseName string
		err      error
		want     bool
	}{
		{
			caseName: "not exist",
			err:      &ErrNotExist{Name: "test"},
			want:     true,
		},
		{
			caseName: "wrapped not exist",
			err:      errors.Join(errors.New("wrap"), &ErrNotExist{Name: "test"}),
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
			assert.Equal(t, tc.want, IsNotExist(tc.err))
		})
	}
}
