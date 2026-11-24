package s2test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/mojatter/s2"
)

func TestStorageDelegatorOpen(t *testing.T) {
	errGet := errors.New("get failed")
	testCases := []struct {
		caseName string
		d        *StorageDelegator
		want     string
		wantErr  error
	}{
		{
			caseName: "OpenFunc opens without Get",
			d: &StorageDelegator{
				OpenFunc: func(context.Context, string) (io.ReadCloser, error) {
					return io.NopCloser(strings.NewReader("by name")), nil
				},
				GetFunc: func(context.Context, string) (s2.Object, error) {
					t.Error("GetFunc called with OpenFunc set")
					return nil, nil
				},
			},
			want: "by name",
		},
		{
			caseName: "without OpenFunc opens what Get returns",
			d: &StorageDelegator{
				GetFunc: func(_ context.Context, name string) (s2.Object, error) {
					return s2.NewObjectBytes(name, []byte("via get")), nil
				},
			},
			want: "via get",
		},
		{
			caseName: "without OpenFunc passes on Get's error",
			d: &StorageDelegator{
				GetFunc: func(context.Context, string) (s2.Object, error) {
					return nil, errGet
				},
			},
			wantErr: errGet,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			rc, err := tc.d.Open(context.Background(), "k")
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) || rc != nil {
					t.Fatalf("Open = (%v, %v), want (nil, %v)", rc, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Open failed: %v", err)
			}
			defer rc.Close()

			got, err := io.ReadAll(rc)
			if err != nil {
				t.Fatalf("reading failed: %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("Open read %q, want %q", got, tc.want)
			}
		})
	}
}
