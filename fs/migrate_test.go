package fs

import (
	"context"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/mojatter/s2"
	"github.com/mojatter/wfs"
	"github.com/mojatter/wfs/memfs"
	"github.com/mojatter/wfs/osfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedFile is a file to write, or a directory to create when name ends in "/".
type seedFile struct {
	name, body string
}

// wrappedStorage is any storage that is not the fs one itself.
type wrappedStorage struct {
	s2.Storage
}

func TestMigrateMeta(t *testing.T) {
	const oldMeta, newMeta = `{"etag":"\"old\"","metadata":{}}`, `{"etag":"\"new\"","metadata":{}}`
	testCases := []struct {
		caseName string
		// seed is written in order, so a later file is newer.
		seed    []seedFile
		want    map[string]string
		wantErr error
	}{
		{
			caseName: "written through the root",
			seed:     []seedFile{{"photos/a.txt", "body"}, {".meta/photos/a.txt", oldMeta}},
			want:     map[string]string{"photos/a.txt": "body", "photos/.meta/a.txt": oldMeta},
		},
		{
			caseName: "written through a sub",
			seed:     []seedFile{{"b/photos/a.txt", "body"}, {"b/.meta/photos/a.txt", oldMeta}},
			want:     map[string]string{"b/photos/a.txt": "body", "b/photos/.meta/a.txt": oldMeta},
		},
		{
			caseName: "deeply nested",
			seed:     []seedFile{{"a/b/c/d.txt", "body"}, {".meta/a/b/c/d.txt", oldMeta}},
			want:     map[string]string{"a/b/c/d.txt": "body", "a/b/c/.meta/d.txt": oldMeta},
		},
		{
			caseName: "top-level metadata files stay",
			seed:     []seedFile{{"a.txt", "body"}, {".meta/a.txt", oldMeta}},
			want:     map[string]string{"a.txt": "body", ".meta/a.txt": oldMeta},
		},
		{
			caseName: "an orphan stays, as it may be a pre-v0.18.1 key",
			seed:     []seedFile{{"kept.txt", "body"}, {".meta/gone/a.txt", oldMeta}},
			want:     map[string]string{"kept.txt": "body", ".meta/gone/a.txt": oldMeta},
		},
		{
			caseName: "a metadata file of a directory stays",
			seed:     []seedFile{{"photos/a/x.txt", "body"}, {".meta/photos/a", oldMeta}},
			want:     map[string]string{"photos/a/x.txt": "body", ".meta/photos/a": oldMeta},
		},
		{
			caseName: "a pre-v0.18.1 key holding .meta stays with its metadata file",
			seed:     []seedFile{{"photos/.meta/a.txt", "key"}, {".meta/photos/.meta/a.txt", oldMeta}},
			want:     map[string]string{"photos/.meta/a.txt": "key", ".meta/photos/.meta/a.txt": oldMeta},
		},
		{
			caseName: "the new location wins when newer",
			seed:     []seedFile{{"photos/a.txt", "body"}, {".meta/photos/a.txt", oldMeta}, {"photos/.meta/a.txt", newMeta}},
			want:     map[string]string{"photos/a.txt": "body", "photos/.meta/a.txt": newMeta},
		},
		{
			caseName: "a legacy metadata file wins when newer",
			seed:     []seedFile{{"photos/a.txt", "body"}, {"photos/.meta/a.txt", oldMeta}, {".meta/photos/a.txt", newMeta}},
			want:     map[string]string{"photos/a.txt": "body", "photos/.meta/a.txt": newMeta},
		},
		{
			caseName: "of two legacy metadata files the newer wins",
			seed:     []seedFile{{"b/photos/a.txt", "body"}, {".meta/b/photos/a.txt", oldMeta}, {"b/.meta/photos/a.txt", newMeta}},
			want:     map[string]string{"b/photos/a.txt": "body", "b/photos/.meta/a.txt": newMeta},
		},
		{
			caseName: "of two legacy metadata files the newer wins, the other order",
			seed:     []seedFile{{"b/photos/a.txt", "body"}, {"b/.meta/photos/a.txt", oldMeta}, {".meta/b/photos/a.txt", newMeta}},
			want:     map[string]string{"b/photos/a.txt": "body", "b/photos/.meta/a.txt": newMeta},
		},
		{
			caseName: "an empty directory at the new location gives way, legacy newer",
			seed:     []seedFile{{"a/x", "body"}, {"a/.meta/x/", ""}, {".meta/a/x", oldMeta}},
			want:     map[string]string{"a/x": "body", "a/.meta/x": oldMeta},
		},
		{
			caseName: "an empty directory at the new location gives way, legacy older",
			seed:     []seedFile{{"a/x", "body"}, {".meta/a/x", oldMeta}, {"a/.meta/x/", ""}},
			want:     map[string]string{"a/x": "body", "a/.meta/x": oldMeta},
		},
		{
			caseName: "a full directory at the new location is reported, the rest migrates",
			seed:     []seedFile{{"a/x", "body"}, {"a/.meta/x/junk", "j"}, {".meta/a/x", oldMeta}, {"z/y", "body"}, {".meta/z/y", newMeta}},
			want:     map[string]string{"a/x": "body", "a/.meta/x/junk": "j", ".meta/a/x": oldMeta, "z/y": "body", "z/.meta/y": newMeta},
			wantErr:  ErrMetaBlocked,
		},
		{
			caseName: "a file at the new location that is not a metadata file is reported, both stay",
			seed:     []seedFile{{"img/logo.png", "png"}, {".meta/img/logo.png", oldMeta}, {"img/.meta/logo.png", "\x89PNG"}},
			want:     map[string]string{"img/logo.png": "png", ".meta/img/logo.png": oldMeta, "img/.meta/logo.png": "\x89PNG"},
			wantErr:  ErrMetaBlocked,
		},
		{
			caseName: "a legacy file that is not a metadata file stays, older than the real one",
			seed:     []seedFile{{"x/y/z", "body"}, {"x/.meta/y/z", "\x89PNG"}, {".meta/x/y/z", oldMeta}},
			want:     map[string]string{"x/y/z": "body", "x/.meta/y/z": "\x89PNG", "x/y/.meta/z": oldMeta},
		},
		{
			caseName: "a legacy file that is not a metadata file stays, newer than the real one",
			seed:     []seedFile{{"x/y/z", "body"}, {".meta/x/y/z", oldMeta}, {"x/.meta/y/z", "\x89PNG"}},
			want:     map[string]string{"x/y/z": "body", "x/.meta/y/z": "\x89PNG", "x/y/.meta/z": oldMeta},
		},
		{
			caseName: "a file named .meta blocks the move",
			seed:     []seedFile{{"photos/a.txt", "body"}, {"photos/.meta", "key"}, {".meta/photos/a.txt", oldMeta}},
			want:     map[string]string{"photos/a.txt": "body", "photos/.meta": "key", ".meta/photos/a.txt": oldMeta},
			wantErr:  ErrMetaBlocked,
		},
	}
	fsyses := []struct {
		caseName string
		newFS    func(t *testing.T) fs.FS
	}{
		{caseName: "memfs", newFS: func(*testing.T) fs.FS { return memfs.New() }},
		{caseName: "osfs", newFS: func(t *testing.T) fs.FS { return osfs.DirFS(t.TempDir()) }},
	}
	for _, fc := range fsyses {
		for _, tc := range testCases {
			t.Run(fc.caseName+"/"+tc.caseName, func(t *testing.T) {
				ctx := context.Background()
				fsys := fc.newFS(t)
				for i, f := range tc.seed {
					if dir, ok := strings.CutSuffix(f.name, "/"); ok {
						require.NoError(t, wfs.MkdirAll(fsys, dir, fs.ModePerm))
					} else {
						_, err := wfs.WriteFile(fsys, f.name, []byte(f.body), fs.ModePerm)
						require.NoError(t, err)
					}
					if i < len(tc.seed)-1 {
						time.Sleep(10 * time.Millisecond)
					}
				}
				strg := NewStorageFS(s2.Config{}, fsys)

				// Twice: a rerun finds nothing left and changes nothing.
				for range 2 {
					ok, err := MigrateMeta(ctx, strg)
					if tc.wantErr != nil {
						require.ErrorIs(t, err, tc.wantErr)
					} else {
						require.NoError(t, err)
					}
					assert.True(t, ok)
					assert.Equal(t, tc.want, readAllFiles(t, fsys))
				}
			})
		}
	}
}

func TestMigrateMetaSkips(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		caseName string
		strg     s2.Storage
		wantOK   bool
	}{
		{caseName: "a wrapper", strg: wrappedStorage{NewStorageMem(s2.Config{})}, wantOK: false},
		{caseName: "a missing root", strg: NewStorageDir(t.TempDir() + "/missing"), wantOK: true},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			ok, err := MigrateMeta(ctx, tc.strg)
			require.NoError(t, err)
			assert.Equal(t, tc.wantOK, ok)
		})
	}
}

// cancelOnOpen cancels when name is opened, so the walk sees the context end part-way.
type cancelOnOpen struct {
	fs.FS
	name   string
	cancel context.CancelFunc
}

func (c cancelOnOpen) Open(name string) (fs.File, error) {
	if name == c.name {
		c.cancel()
	}
	return c.FS.Open(name)
}

func TestMigrateMetaCancelled(t *testing.T) {
	const oldMeta = `{"etag":"\"old\"","metadata":{}}`
	testCases := []struct {
		caseName string
		seed     map[string]string
		// cancelAt is opened when the context ends; empty cancels before the walk.
		cancelAt string
	}{
		{caseName: "before the walk", seed: map[string]string{"photos/a.txt": "body"}},
		{caseName: "inside a .meta", seed: map[string]string{"photos/a.txt": "body", ".meta/photos/a.txt": oldMeta}, cancelAt: ".meta/photos"},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			mem := memfs.New()
			for name, body := range tc.seed {
				_, err := wfs.WriteFile(mem, name, []byte(body), fs.ModePerm)
				require.NoError(t, err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var fsys fs.FS = mem
			if tc.cancelAt == "" {
				cancel()
			} else {
				fsys = cancelOnOpen{FS: mem, name: tc.cancelAt, cancel: cancel}
			}
			_, err := MigrateMeta(ctx, NewStorageFS(s2.Config{}, fsys))
			require.ErrorIs(t, err, context.Canceled)
			assert.Equal(t, tc.seed, readAllFiles(t, mem), "nothing moved")
		})
	}
}

// readAllFiles maps every regular file under fsys to its body, and every empty directory but the root to "<dir>/".
func readAllFiles(t *testing.T, fsys fs.FS) map[string]string {
	t.Helper()
	got := map[string]string{}
	require.NoError(t, fs.WalkDir(fsys, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			entries, err := fs.ReadDir(fsys, name)
			if name != "." && len(entries) == 0 {
				got[name+"/"] = ""
			}
			return err
		}
		data, err := fs.ReadFile(fsys, name)
		got[name] = string(data)
		return err
	}))
	return got
}
