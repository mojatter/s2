package s2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

type nilStorage struct {
	Storage
}

var _ Storage = (*nilStorage)(nil)

func TestNewStorage(t *testing.T) {
	testType := Type("test")
	RegisterNewStorageFunc(testType, func(ctx context.Context, cfg Config) (Storage, error) {
		return &nilStorage{}, nil
	})
	defer UnregisterNewStorageFunc(testType)

	testCases := []struct {
		caseName string
		ctx      context.Context
		cfg      Config
		wantErr  string
	}{
		{
			caseName: "typical",
			cfg: Config{
				Type: testType,
			},
		},
		{
			caseName: "unknown type",
			cfg: Config{
				Type: "UNKNOWN",
			},
			wantErr: "s2: unknown storage type: UNKNOWN",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			_, err := NewStorage(tc.ctx, tc.cfg)
			if tc.wantErr != "" {
				assert.EqualError(t, err, tc.wantErr)
				return
			}
			assert.NoError(t, err)
		})
	}
}
