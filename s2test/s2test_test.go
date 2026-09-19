package s2test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/mojatter/s2"
	_ "github.com/mojatter/s2/fs" // registers memfs
)

// etagStorage wraps a real Storage and replaces the ETag every Object reports,
// standing in for a backend whose ETag is not the body MD5. etag is evaluated
// on each ETag() call, like a real Object that reads its body on demand.
type etagStorage struct {
	s2.Storage
	etag func(obj s2.Object, gen int) string
	gen  int // bumped by PutMetadata, for a backend that rotates the ETag
}

func (s *etagStorage) Get(ctx context.Context, name string) (s2.Object, error) {
	obj, err := s.Storage.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	return etagObject{obj, s}, nil
}

func (s *etagStorage) List(ctx context.Context, opts s2.ListOptions) (s2.ListResult, error) {
	res, err := s.Storage.List(ctx, opts)
	if err != nil {
		return res, err
	}
	for i, obj := range res.Objects {
		res.Objects[i] = etagObject{obj, s}
	}
	return res, nil
}

func (s *etagStorage) PutMetadata(ctx context.Context, name string, md s2.Metadata) error {
	s.gen++
	return s.Storage.PutMetadata(ctx, name, md)
}

type etagObject struct {
	s2.Object
	strg *etagStorage
}

func (o etagObject) ETag() string { return o.strg.etag(o.Object, o.strg.gen) }

// opaqueETag derives the ETag from the body, as a real opaque ETag does, but
// not as its MD5.
func opaqueETag(obj s2.Object, _ int) string {
	rc, err := obj.Open()
	if err != nil {
		return ""
	}
	defer func() { _ = rc.Close() }()

	h := sha256.New()
	if _, err := io.Copy(h, rc); err != nil {
		return ""
	}
	return `"opaque-` + hex.EncodeToString(h.Sum(nil)) + `"`
}

// constantETag answers one value for every object. It is quoted, non-empty and
// consistent between Get and List, so only the overwrite check rejects it.
func constantETag(s2.Object, int) string { return `"constant"` }

func emptyETag(s2.Object, int) string { return "" }

// quotedEmptyETag is syntactically a quoted tag with nothing in it.
func quotedEmptyETag(s2.Object, int) string { return `""` }

// unquotedETag reaches the client verbatim in the ETag header, where it is not
// a valid RFC 7232 entity tag.
func unquotedETag(obj s2.Object, gen int) string {
	return strings.Trim(opaqueETag(obj, gen), `"`)
}

// innerQuoteETag is quoted at both ends but carries a DQUOTE inside, which
// etagc excludes; a client truncates it there.
func innerQuoteETag(obj s2.Object, gen int) string {
	return `"a"` + strings.Trim(opaqueETag(obj, gen), `"`) + `"`
}

// rotatingETag mints a new ETag on each PutMetadata, as azblob's SetMetadata
// and s3's self-CopyObject do, and reports the backend's own until the first.
func rotatingETag(obj s2.Object, gen int) string {
	if gen == 0 {
		return obj.ETag()
	}
	return `"gen-` + strconv.Itoa(gen) + "-" + obj.Name() + `"`
}

func newMemFS(t *testing.T) s2.Storage {
	t.Helper()
	strg, err := s2.NewStorage(t.Context(), s2.Config{Type: s2.TypeMemFS})
	if err != nil {
		t.Fatal(err)
	}
	return strg
}

func newETagFS(t *testing.T, etag func(s2.Object, int) string) s2.Storage {
	t.Helper()
	return &etagStorage{Storage: newMemFS(t), etag: etag}
}

func TestWithOpaqueETag(t *testing.T) {
	testCases := []struct {
		caseName string
		run      func(ctx context.Context, strg s2.Storage, opts ...Option) error
	}{
		{caseName: "TestStorageGetPut", run: TestStorageGetPut},
		{caseName: "TestStoragePutMetadata", run: TestStoragePutMetadata},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			ctx := t.Context()
			if err := tc.run(ctx, newMemFS(t)); err != nil {
				t.Errorf("a body-MD5 backend must pass without options: %v", err)
			}
			if err := tc.run(ctx, newETagFS(t, opaqueETag)); err == nil {
				t.Error("an opaque-ETag backend must fail without WithOpaqueETag")
			}
			if err := tc.run(ctx, newETagFS(t, opaqueETag), WithOpaqueETag()); err != nil {
				t.Errorf("WithOpaqueETag: %v", err)
			}
		})
	}
}

// WithOpaqueETag relaxes the ETag's form, not the guarantee that there is one.
func TestWithOpaqueETagRejects(t *testing.T) {
	type rejectCase struct {
		caseName string
		run      func(ctx context.Context, strg s2.Storage, opts ...Option) error
		etag     func(s2.Object, int) string
		wantErr  string
	}

	// The form assertions both helpers share.
	formCases := []rejectCase{
		{caseName: "empty", etag: emptyETag, wantErr: "ETag() is empty"},
		{caseName: "quoted but empty", etag: quotedEmptyETag, wantErr: "want a non-empty quoted entity tag"},
		{caseName: "unquoted", etag: unquotedETag, wantErr: "want a non-empty quoted entity tag"},
		{caseName: "quote inside", etag: innerQuoteETag, wantErr: "want no quote inside the entity tag"},
	}
	helpers := []rejectCase{
		{caseName: "TestStorageGetPut", run: TestStorageGetPut},
		{caseName: "TestStoragePutMetadata", run: TestStoragePutMetadata},
	}

	// Only TestStorageGetPut overwrites an existing key.
	testCases := []rejectCase{
		{
			caseName: "TestStorageGetPut/one value for every object",
			run:      TestStorageGetPut,
			etag:     constantETag,
			wantErr:  "after overwriting with a different body",
		},
	}
	for _, h := range helpers {
		for _, f := range formCases {
			testCases = append(testCases, rejectCase{h.caseName + "/" + f.caseName, h.run, f.etag, f.wantErr})
		}
	}

	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			err := tc.run(t.Context(), newETagFS(t, tc.etag), WithOpaqueETag())
			if err == nil {
				t.Fatal("WithOpaqueETag must not accept this backend")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("want an error containing %q, got: %v", tc.wantErr, err)
			}
		})
	}
}

func TestWithOpaqueETagAllowsRotationOnPutMetadata(t *testing.T) {
	if err := TestStoragePutMetadata(t.Context(), newETagFS(t, rotatingETag), WithOpaqueETag()); err != nil {
		t.Errorf("PutMetadata may rotate an opaque ETag: %v", err)
	}
	err := TestStoragePutMetadata(t.Context(), newETagFS(t, rotatingETag))
	if err == nil {
		t.Fatal("a rotated ETag must fail without WithOpaqueETag")
	}
	if !strings.Contains(err.Error(), "after PutMetadata") {
		t.Errorf("want the stability error, got: %v", err)
	}
}
