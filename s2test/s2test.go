// Package s2test is a conformance suite for s2.Storage implementations.
//
// The suite assumes a backend whose ETag is the MD5 of the body. A configuration
// that answers something else, such as an s3 root using SSE-KMS or SSE-C, fails
// every ETag assertion even though the implementation is correct.
package s2test

import (
	"bytes"
	"context"
	"crypto/md5" // #nosec G501 -- S3-compatible ETag
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"

	"github.com/mojatter/s2"
)

// TestStorageList is a test helper for validating s2.Storage list operations.
// It exercises the flat-listing path of Storage.List (Recursive: false) for a
// particular prefix against the expected direct object names, including both
// resume paths: the After continuation token and the StartAfter key.
// The expected array should only contain names of objects immediately beneath the prefix.
// expectedPrefixes is an optional list of expected common prefixes (subdirectories).
func TestStorageList(ctx context.Context, strg s2.Storage, prefix string, expected ...string) error {
	return TestStorageListWithPrefixes(ctx, strg, prefix, nil, expected...)
}

// TestStorageListWithPrefixes is like TestStorageList but also validates common prefixes.
func TestStorageListWithPrefixes(ctx context.Context, strg s2.Storage, prefix string, expectedPrefixes []string, expected ...string) error {
	sort.Strings(expected)
	sort.Strings(expectedPrefixes)
	var errs []string
	errorf := func(format string, args ...any) {
		errs = append(errs, fmt.Sprintf(format, args...))
	}

	checkMatch := func(name string, objs []s2.Object, want []string) {
		if len(objs) != len(want) {
			errorf("%s: got %d objects, expected %d", name, len(objs), len(want))
			return
		}
		for i, obj := range objs {
			if obj.Name() != want[i] {
				errorf("%s: object at index %d has name %q, expected %q", name, i, obj.Name(), want[i])
			}
		}
	}

	checkPrefixes := func(name string, got []string, want []string) {
		if len(want) == 0 {
			return
		}
		sort.Strings(got)
		if len(got) != len(want) {
			errorf("%s: got %d prefixes %v, expected %d %v", name, len(got), got, len(want), want)
			return
		}
		for i, g := range got {
			if g != want[i] {
				errorf("%s: prefix at index %d is %q, expected %q", name, i, g, want[i])
			}
		}
	}

	res, err := strg.List(ctx, s2.ListOptions{Prefix: prefix})
	if err != nil {
		errorf("List(prefix=%q) failed: %v", prefix, err)
	} else {
		checkMatch(fmt.Sprintf("List(prefix=%q)", prefix), res.Objects, expected)
		checkPrefixes(fmt.Sprintf("List(prefix=%q) prefixes", prefix), res.CommonPrefixes, expectedPrefixes)
	}

	if len(expected) > 1 {
		limit := len(expected) / 2
		res1, err := strg.List(ctx, s2.ListOptions{Prefix: prefix, Limit: limit})
		if err != nil {
			errorf("List(prefix=%q, limit=%d) failed: %v", prefix, limit, err)
		} else if n := len(res1.Objects); n == 0 || n > limit {
			// Limit caps entries, so CommonPrefixes may take part of the page.
			errorf("List(prefix=%q, limit=%d) returned %d objects", prefix, limit, n)
		} else {
			// NextAfter fed back as After.
			if res1.NextAfter == "" {
				errorf("List(prefix=%q, limit=%d) returned no NextAfter", prefix, limit)
			} else {
				res2, err := strg.List(ctx, s2.ListOptions{Prefix: prefix, After: res1.NextAfter})
				if err != nil {
					errorf("List(prefix=%q, after=<token>) failed: %v", prefix, err)
				} else {
					var combined []s2.Object
					combined = append(combined, res1.Objects...)
					combined = append(combined, res2.Objects...)
					checkMatch(fmt.Sprintf("List Pagination (prefix %q, limit %d, after token)", prefix, limit), combined, expected)
				}
			}

			// The caller-chosen resume key.
			last := res1.Objects[len(res1.Objects)-1].Name()
			res2, err := strg.List(ctx, s2.ListOptions{Prefix: prefix, StartAfter: last})
			if err != nil {
				errorf("List(prefix=%q, start-after=%q) failed: %v", prefix, last, err)
			} else {
				var combined []s2.Object
				combined = append(combined, res1.Objects...)
				combined = append(combined, res2.Objects...)
				checkMatch(fmt.Sprintf("List StartAfter (prefix %q, limit %d, start-after %q)", prefix, limit, last), combined, expected)
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("TestStorageList found %d errors:\n\t%s", len(errs), strings.Join(errs, "\n\t"))
	}
	return nil
}

// TestStorageListRecursive is a test helper for validating s2.Storage recursive list operations.
// It exercises the recursive path of Storage.List (Recursive: true) — including
// a prefix-filter case and both resume paths, the After continuation token
// and the StartAfter key — against the expected object names.
// The provided expected array must be the comprehensive list of object names in the storage.
func TestStorageListRecursive(ctx context.Context, strg s2.Storage, expected ...string) error {
	sort.Strings(expected)
	var errs []string
	errorf := func(format string, args ...any) {
		errs = append(errs, fmt.Sprintf(format, args...))
	}

	checkMatch := func(name string, objs []s2.Object, want []string) {
		if len(objs) != len(want) {
			errorf("%s: got %d objects, expected %d", name, len(objs), len(want))
			return
		}
		for i, obj := range objs {
			if obj.Name() != want[i] {
				errorf("%s: object at index %d has name %q, expected %q", name, i, obj.Name(), want[i])
			}
		}
	}

	// 1. Check List(Recursive: true)
	res, err := strg.List(ctx, s2.ListOptions{Recursive: true})
	if err != nil {
		errorf("List(recursive) failed: %v", err)
	} else {
		checkMatch("List(recursive)", res.Objects, expected)
	}

	// 2. Test prefix filtering
	if len(expected) > 0 {
		for _, name := range expected {
			if idx := strings.LastIndex(name, "/"); idx > 0 {
				prefix := name[:idx+1]
				var want []string
				for _, e := range expected {
					if strings.HasPrefix(e, prefix) {
						want = append(want, e)
					}
				}
				if len(want) > 0 && len(want) < len(expected) {
					res, err := strg.List(ctx, s2.ListOptions{Prefix: prefix, Recursive: true})
					if err != nil {
						errorf("List(prefix=%q, recursive) failed: %v", prefix, err)
					} else {
						checkMatch(fmt.Sprintf("List(prefix=%q, recursive)", prefix), res.Objects, want)
					}
					break // test one prefix
				}
			}
		}
	}

	// 3. Test both resume paths
	if len(expected) > 1 {
		limit := len(expected) / 2
		res1, err := strg.List(ctx, s2.ListOptions{Limit: limit, Recursive: true})
		if err != nil {
			errorf("List recursive pagination failed: %v", err)
		} else if len(res1.Objects) != limit {
			errorf("List(limit=%d, recursive) returned %d objects", limit, len(res1.Objects))
		} else {
			// NextAfter fed back as After.
			if res1.NextAfter == "" {
				errorf("List(limit=%d, recursive) returned no NextAfter", limit)
			} else {
				res2, err := strg.List(ctx, s2.ListOptions{After: res1.NextAfter, Recursive: true})
				if err != nil {
					errorf("List(after=<token>, recursive) failed: %v", err)
				} else {
					var combined []s2.Object
					combined = append(combined, res1.Objects...)
					combined = append(combined, res2.Objects...)
					checkMatch(fmt.Sprintf("Pagination combined (limit %d, after token)", limit), combined, expected)
				}
			}

			// The caller-chosen resume key.
			last := res1.Objects[len(res1.Objects)-1].Name()
			res2, err := strg.List(ctx, s2.ListOptions{StartAfter: last, Recursive: true})
			if err != nil {
				errorf("List(start-after=%q, recursive) failed: %v", last, err)
			} else {
				var combined []s2.Object
				combined = append(combined, res1.Objects...)
				combined = append(combined, res2.Objects...)
				checkMatch(fmt.Sprintf("StartAfter combined (limit %d, start-after %q)", limit, last), combined, expected)
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("TestStorageListRecursive found %d errors:\n\t%s", len(errs), strings.Join(errs, "\n\t"))
	}
	return nil
}

// TestStorageListPaging writes its own fixture, then lists it flat and
// recursively (on a Sub storage), covering both resume paths.
func TestStorageListPaging(ctx context.Context, strg s2.Storage) error {
	const dir = "s2test-list"
	names := []string{
		dir + "/a.txt",
		dir + "/b.txt",
		dir + "/c.txt",
		dir + "/d.txt",
		dir + "/sub/e.txt",
	}
	for _, name := range names {
		if err := strg.Put(ctx, s2.NewObjectBytes(name, []byte("x"))); err != nil {
			return fmt.Errorf("Put(%q) failed: %w", name, err)
		}
	}

	if err := TestStorageListWithPrefixes(ctx, strg, dir, []string{dir + "/sub/"},
		dir+"/a.txt", dir+"/b.txt", dir+"/c.txt", dir+"/d.txt"); err != nil {
		return err
	}

	// A Sub storage isolates the recursive listing from other objects.
	sub, err := strg.Sub(ctx, dir)
	if err != nil {
		return fmt.Errorf("Sub(%q) failed: %w", dir, err)
	}
	return TestStorageListRecursive(ctx, sub, "a.txt", "b.txt", "c.txt", "d.txt", "sub/e.txt")
}

// TestStorageGetPut validates that Put writes an object and Get reads it back with its metadata, Content-Type and ETag.
func TestStorageGetPut(ctx context.Context, strg s2.Storage) error {
	var errs []string
	errorf := func(format string, args ...any) {
		errs = append(errs, fmt.Sprintf(format, args...))
	}

	name := "s2test-getput.txt"
	body := []byte("s2test content")
	obj := s2.NewObjectBytes(name, body, s2.WithContentType("text/plain"))
	obj.Metadata().Set("testkey", "test-val")

	if err := strg.Put(ctx, obj); err != nil {
		return fmt.Errorf("Put(%q) failed: %w", name, err)
	}

	got, err := strg.Get(ctx, name)
	if err != nil {
		return fmt.Errorf("Get(%q) failed: %w", name, err)
	}
	if got.Name() != name {
		errorf("Get(%q).Name() = %q", name, got.Name())
	}
	if got.Length() != uint64(len(body)) {
		errorf("Get(%q).Length() = %d, want %d", name, got.Length(), len(body))
	}

	rc, err := got.Open()
	if err != nil {
		return fmt.Errorf("Get(%q).Open() failed: %w", name, err)
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("ReadAll failed: %w", err)
	}
	if string(b) != string(body) {
		errorf("body = %q, want %q", string(b), string(body))
	}

	v, ok := got.Metadata().Get("testkey")
	if !ok {
		errorf("metadata key %q not found", "testkey")
	} else if v != "test-val" {
		errorf("metadata %q = %q, want %q", "testkey", v, "test-val")
	}

	if ct := got.ContentType(); ct != "text/plain" {
		errorf("Get(%q).ContentType() = %q, want %q", name, ct, "text/plain")
	}

	// The ETag is the body's MD5, and listed objects report the same one.
	etag := got.ETag()
	if want := quotedMD5(body); etag != want {
		errorf("Get(%q).ETag() = %q, want %q", name, etag, want)
	}
	res, err := strg.List(ctx, s2.ListOptions{Recursive: true})
	if err != nil {
		return fmt.Errorf("List(%q) failed: %w", name, err)
	}
	listed := false
	for _, obj := range res.Objects {
		if obj.Name() != name {
			continue
		}
		listed = true
		if obj.ETag() != etag {
			errorf("List(%q).ETag() = %q, want %q", name, obj.ETag(), etag)
		}
	}
	if !listed {
		errorf("List(%q) did not return the object", name)
	}

	// A body larger than one upload block must still get its MD5 as the ETag.
	large := "s2test-getput-large.bin"
	largeBody := bytes.Repeat([]byte("s2test"), (2<<20)/6+1)
	if err := strg.Put(ctx, s2.NewObjectBytes(large, largeBody)); err != nil {
		return fmt.Errorf("Put(%q) failed: %w", large, err)
	}
	gotLarge, err := strg.Get(ctx, large)
	if err != nil {
		return fmt.Errorf("Get(%q) failed: %w", large, err)
	}
	if etag, want := gotLarge.ETag(), quotedMD5(largeBody); etag != want {
		errorf("Get(%q).ETag() = %q, want %q", large, etag, want)
	}
	if err := strg.Delete(ctx, large); err != nil {
		return fmt.Errorf("Delete(%q) failed: %w", large, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("TestStorageGetPut found %d errors:\n\t%s", len(errs), strings.Join(errs, "\n\t"))
	}
	return nil
}

// TestStorageGetNotExist validates that Get returns ErrNotExist for missing objects.
func TestStorageGetNotExist(ctx context.Context, strg s2.Storage) error {
	_, err := strg.Get(ctx, "s2test-does-not-exist.txt")
	if err == nil {
		return fmt.Errorf("Get for non-existent object should return error")
	}
	if !errors.Is(err, s2.ErrNotExist) {
		return fmt.Errorf("Get for non-existent object returned %v, want ErrNotExist", err)
	}
	return nil
}

// TestStorageExists validates the Exists method.
func TestStorageExists(ctx context.Context, strg s2.Storage) error {
	name := "s2test-exists.txt"
	if err := strg.Put(ctx, s2.NewObjectBytes(name, []byte("x"))); err != nil {
		return fmt.Errorf("Put(%q) failed: %w", name, err)
	}

	ok, err := strg.Exists(ctx, name)
	if err != nil {
		return fmt.Errorf("Exists(%q) failed: %w", name, err)
	}
	if !ok {
		return fmt.Errorf("Exists(%q) = false, want true", name)
	}

	ok, err = strg.Exists(ctx, "s2test-does-not-exist.txt")
	if err != nil {
		return fmt.Errorf("Exists(non-existent) failed: %w", err)
	}
	if ok {
		return fmt.Errorf("Exists(non-existent) = true, want false")
	}
	return nil
}

// TestStorageCopyMove validates Copy and Move operations.
func TestStorageCopyMove(ctx context.Context, strg s2.Storage) error {
	var errs []string
	errorf := func(format string, args ...any) {
		errs = append(errs, fmt.Sprintf(format, args...))
	}

	src := "s2test-copymove-src.txt"
	body := []byte("copy move content")
	if err := strg.Put(ctx, s2.NewObjectBytes(src, body)); err != nil {
		return fmt.Errorf("Put(%q) failed: %w", src, err)
	}

	// Copy
	dst := "s2test-copymove-dst.txt"
	if err := strg.Copy(ctx, src, dst); err != nil {
		return fmt.Errorf("Copy(%q, %q) failed: %w", src, dst, err)
	}

	got, err := strg.Get(ctx, dst)
	if err != nil {
		errorf("Get(%q) after Copy failed: %v", dst, err)
	} else {
		rc, err := got.Open()
		if err != nil {
			errorf("Open(%q) after Copy failed: %v", dst, err)
		} else {
			b, _ := io.ReadAll(rc)
			_ = rc.Close()
			if string(b) != string(body) {
				errorf("Copy body = %q, want %q", string(b), string(body))
			}
		}
	}

	// Source should still exist after Copy
	if ok, err := strg.Exists(ctx, src); err != nil {
		errorf("Exists(%q) after Copy failed: %v", src, err)
	} else if !ok {
		errorf("source %q should still exist after Copy", src)
	}

	// Move
	moved := "s2test-copymove-moved.txt"
	if err := s2.Move(ctx, strg, src, moved); err != nil {
		return fmt.Errorf("Move(%q, %q) failed: %w", src, moved, err)
	}

	// Source should be gone after Move
	if ok, err := strg.Exists(ctx, src); err != nil {
		errorf("Exists(%q) after Move failed: %v", src, err)
	} else if ok {
		errorf("source %q should not exist after Move", src)
	}

	// Destination should exist after Move
	got, err = strg.Get(ctx, moved)
	if err != nil {
		errorf("Get(%q) after Move failed: %v", moved, err)
	} else {
		rc, err := got.Open()
		if err != nil {
			errorf("Open(%q) after Move failed: %v", moved, err)
		} else {
			b, _ := io.ReadAll(rc)
			_ = rc.Close()
			if string(b) != string(body) {
				errorf("Move body = %q, want %q", string(b), string(body))
			}
		}
	}

	// Copy/Move of a missing source must report ErrNotExist.
	missing := "s2test-copymove-missing.txt"
	if err := strg.Copy(ctx, missing, "s2test-copymove-missing-dst.txt"); !errors.Is(err, s2.ErrNotExist) {
		errorf("Copy(%q, ...) on missing source returned %v, want ErrNotExist", missing, err)
	}
	if err := s2.Move(ctx, strg, missing, "s2test-copymove-missing-dst.txt"); !errors.Is(err, s2.ErrNotExist) {
		errorf("Move(%q, ...) on missing source returned %v, want ErrNotExist", missing, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("TestStorageCopyMove found %d errors:\n\t%s", len(errs), strings.Join(errs, "\n\t"))
	}
	return nil
}

// TestStorageDelete validates Delete and DeleteRecursive operations.
func TestStorageDelete(ctx context.Context, strg s2.Storage) error {
	var errs []string
	errorf := func(format string, args ...any) {
		errs = append(errs, fmt.Sprintf(format, args...))
	}

	// Single delete
	name := "s2test-delete.txt"
	if err := strg.Put(ctx, s2.NewObjectBytes(name, []byte("delete me"))); err != nil {
		return fmt.Errorf("Put(%q) failed: %w", name, err)
	}
	if err := strg.Delete(ctx, name); err != nil {
		return fmt.Errorf("Delete(%q) failed: %w", name, err)
	}
	if ok, _ := strg.Exists(ctx, name); ok {
		errorf("Delete(%q): object still exists", name)
	}

	// Deleting a non-existent object is a no-op and must not error.
	if err := strg.Delete(ctx, name); err != nil {
		errorf("Delete(%q) on already-deleted object returned %v, want nil", name, err)
	}

	// Recursive delete
	files := []string{
		"s2test-delrec/a.txt",
		"s2test-delrec/b.txt",
		"s2test-delrec/sub/c.txt",
	}
	for _, f := range files {
		if err := strg.Put(ctx, s2.NewObjectBytes(f, []byte("x"))); err != nil {
			return fmt.Errorf("Put(%q) failed: %w", f, err)
		}
	}
	if err := strg.DeleteRecursive(ctx, "s2test-delrec"); err != nil {
		return fmt.Errorf("DeleteRecursive(%q) failed: %w", "s2test-delrec", err)
	}
	for _, f := range files {
		if ok, _ := strg.Exists(ctx, f); ok {
			errorf("DeleteRecursive: %q still exists", f)
		}
	}

	// A trailing slash confines the prefix to that directory, sparing names that merely share it.
	nested := []string{"s2test-dir/a.txt", "s2test-dir/sub/b.txt"}
	sibling := "s2test-dir-sibling/c.txt"
	for _, f := range append(nested, sibling) {
		if err := strg.Put(ctx, s2.NewObjectBytes(f, []byte("x"))); err != nil {
			return fmt.Errorf("Put(%q) failed: %w", f, err)
		}
	}
	if err := strg.DeleteRecursive(ctx, "s2test-dir/"); err != nil {
		return fmt.Errorf("DeleteRecursive(%q) failed: %w", "s2test-dir/", err)
	}
	for _, f := range nested {
		if ok, _ := strg.Exists(ctx, f); ok {
			errorf("DeleteRecursive(%q): %q still exists", "s2test-dir/", f)
		}
	}
	if ok, _ := strg.Exists(ctx, "s2test-dir"); ok {
		errorf("DeleteRecursive(%q): the directory itself still exists", "s2test-dir/")
	}
	if ok, _ := strg.Exists(ctx, sibling); !ok {
		errorf("DeleteRecursive(%q) removed %q, which only shares the prefix", "s2test-dir/", sibling)
	}
	_ = strg.Delete(ctx, sibling)

	if len(errs) > 0 {
		return fmt.Errorf("TestStorageDelete found %d errors:\n\t%s", len(errs), strings.Join(errs, "\n\t"))
	}
	return nil
}

// TestStoragePutMetadata validates PutMetadata replaces the user metadata and leaves the body, Content-Type and ETag alone.
func TestStoragePutMetadata(ctx context.Context, strg s2.Storage) error {
	name := "s2test-putmeta.txt"
	body := []byte("metadata test")
	if err := strg.Put(ctx, s2.NewObjectBytes(name, body, s2.WithContentType("text/csv"), s2.WithMetadata(s2.Metadata{"stale": "x"}))); err != nil {
		return fmt.Errorf("Put(%q) failed: %w", name, err)
	}

	md := s2.Metadata{"author": "s2test", "version": "1"}
	if err := strg.PutMetadata(ctx, name, md); err != nil {
		return fmt.Errorf("PutMetadata(%q) failed: %w", name, err)
	}

	got, err := strg.Get(ctx, name)
	if err != nil {
		return fmt.Errorf("Get(%q) after PutMetadata failed: %w", name, err)
	}

	// Body should be unchanged
	rc, err := got.Open()
	if err != nil {
		return fmt.Errorf("Open(%q) failed: %w", name, err)
	}
	b, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(b) != string(body) {
		return fmt.Errorf("body changed after PutMetadata: got %q, want %q", string(b), string(body))
	}

	// Metadata should be updated
	v, ok := got.Metadata().Get("author")
	if !ok || v != "s2test" {
		return fmt.Errorf("metadata 'author' = %q (ok=%v), want 's2test'", v, ok)
	}
	v, ok = got.Metadata().Get("version")
	if !ok || v != "1" {
		return fmt.Errorf("metadata 'version' = %q (ok=%v), want '1'", v, ok)
	}

	if v, ok := got.Metadata().Get("stale"); ok {
		return fmt.Errorf("metadata 'stale' = %q after PutMetadata; it must replace, not merge", v)
	}

	// Only the user metadata is replaced.
	if ct := got.ContentType(); ct != "text/csv" {
		return fmt.Errorf("ContentType() after PutMetadata = %q, want %q", ct, "text/csv")
	}
	if etag, want := got.ETag(), quotedMD5(body); etag != want {
		return fmt.Errorf("ETag() after PutMetadata = %q, want %q", etag, want)
	}

	return nil
}

func quotedMD5(body []byte) string {
	sum := md5.Sum(body) // #nosec G401 -- S3-compatible ETag
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// TestStorageNameEscape validates that a name or prefix reaching outside the
// storage root reaches nothing. It writes a sibling of a Sub prefix, then
// drives every name-taking method of that Sub with "../<sibling>": each must
// fail or report nothing, and the sibling must survive untouched.
func TestStorageNameEscape(ctx context.Context, strg s2.Storage) error {
	var errs []string
	errorf := func(format string, args ...any) {
		errs = append(errs, fmt.Sprintf(format, args...))
	}

	const (
		dir     = "s2test-escape"
		sibling = "s2test-escape-sibling.txt"
		body    = "untouched"
	)
	// Both spellings: path.Clean absorbs a ".." against a leading "/", so the
	// rooted form escapes a guard that only looks at the cleaned name.
	escapes := []string{"../" + sibling, "/../" + sibling}
	if err := strg.Put(ctx, s2.NewObjectBytes(sibling, []byte(body))); err != nil {
		return fmt.Errorf("Put(%q) failed: %w", sibling, err)
	}
	if err := strg.Put(ctx, s2.NewObjectBytes(dir+"/inner.txt", []byte("inner"))); err != nil {
		return fmt.Errorf("Put(%q) failed: %w", dir+"/inner.txt", err)
	}
	sub, err := strg.Sub(ctx, dir)
	if err != nil {
		return fmt.Errorf("Sub(%q) failed: %w", dir, err)
	}
	// Decoys inside the Sub, under the names an escaping one folds to when a
	// backend cleans the path one level instead of rejecting it. Without them
	// a folding backend passes: nothing outside the root was touched. Each
	// carries a Content-Type and metadata, because a fold can land on a
	// backend's sidecar and take those alone.
	decoys := []string{sibling, "decoy.txt"}
	for _, name := range decoys {
		obj := s2.NewObjectBytes(name, []byte(body),
			s2.WithContentType("text/plain"),
			s2.WithMetadata(s2.Metadata{"decoy": "yes"}))
		if err := sub.Put(ctx, obj); err != nil {
			return fmt.Errorf("Sub(%q).Put(%q) failed: %w", dir, name, err)
		}
	}

	// Reads must not reach it. The error value is the backend's own.
	for _, escape := range escapes {
		if obj, err := sub.Get(ctx, escape); err == nil && obj != nil {
			errorf("Sub(%q).Get(%q) reached the object outside the root", dir, escape)
		}
		if ok, err := sub.Exists(ctx, escape); err == nil && ok {
			errorf("Sub(%q).Exists(%q) = true", dir, escape)
		}
		for _, prefix := range []string{escape, path.Dir(escape) + "/"} {
			if res, err := sub.List(ctx, s2.ListOptions{Prefix: prefix, Recursive: true}); err == nil && len(res.Objects) > 0 {
				errorf("Sub(%q).List(prefix=%q) returned %d objects", dir, prefix, len(res.Objects))
			}
		}
		// A Sub of a Sub must not escape either.
		if nested, err := sub.Sub(ctx, path.Dir(escape)); err == nil && nested != nil {
			if res, err := nested.List(ctx, s2.ListOptions{Recursive: true}); err == nil && len(res.Objects) > 0 {
				errorf("Sub(%q).Sub(%q) listed %d objects outside the root", dir, path.Dir(escape), len(res.Objects))
			}
		}
	}

	// Writes must not reach it either. Each is checked against the body below.
	// An escaping name is driven as the source as well as the destination: a
	// backend that validates only the destination still copies the outside
	// object in.
	copied, moved := "s2test-escape-copied.txt", "s2test-escape-moved.txt"
	for _, escape := range escapes {
		_ = sub.Put(ctx, s2.NewObjectBytes(escape, []byte("overwritten")))
		_ = sub.PutMetadata(ctx, escape, s2.Metadata{"escaped": "yes"})
		_ = sub.Copy(ctx, "inner.txt", escape)
		_ = sub.Copy(ctx, escape, copied)
		_ = s2.Move(ctx, sub, escape, moved)
		_ = sub.Delete(ctx, escape)
		_ = sub.DeleteRecursive(ctx, path.Dir(escape)+"/")
		if u, err := sub.SignedURL(ctx, s2.SignedURLOptions{Name: escape}); err == nil && u != "" {
			errorf("Sub(%q).SignedURL(%q) signed a name outside the root", dir, escape)
		}
	}
	// Nothing from outside may have landed inside under a name of its own.
	for _, name := range []string{copied, moved} {
		if ok, err := sub.Exists(ctx, name); err == nil && ok {
			errorf("Sub(%q).Exists(%q) = true: the object outside the root was brought in", dir, name)
			_ = sub.Delete(ctx, name)
		}
	}

	// A trailing "/" names nothing either: it folds to the object beside it,
	// and a backend that joins before it validates takes that object's
	// sidecar with it.
	for _, name := range decoys {
		_ = sub.Delete(ctx, name+"/")
		_ = sub.PutMetadata(ctx, name+"/", s2.Metadata{"escaped": "yes"})
	}
	// "/" is a spelling of the root rather than a name, so nothing is there.
	if ok, err := sub.Exists(ctx, "/"); err == nil && ok {
		errorf("Sub(%q).Exists(%q) = true", dir, "/")
	}

	// The decoys must be intact too -- body, Content-Type and metadata: a
	// backend that folds an escaping name by one level lands on them rather
	// than outside the root.
	for _, name := range decoys {
		got, err := sub.Get(ctx, name)
		if err != nil {
			errorf("Sub(%q).Get(%q) did not survive the escape attempts: %v", dir, name, err)
			continue
		}
		if err := checkDecoy(got, body); err != nil {
			errorf("Sub(%q).Get(%q): %v", dir, name, err)
		}
	}

	// An empty prefix means everything inside the Sub, not everything whose
	// name starts with it: the root sibling is spelled dir+"-sibling.txt", and
	// a backend that joins without restoring the separator reaches it.
	if err := sub.DeleteRecursive(ctx, ""); err != nil {
		errorf("Sub(%q).DeleteRecursive(%q) failed: %v", dir, "", err)
	}

	got, err := strg.Get(ctx, sibling)
	if err != nil {
		errorf("%q did not survive the escape attempts: %v", sibling, err)
	} else {
		rc, err := got.Open()
		if err != nil {
			errorf("Open(%q) failed: %v", sibling, err)
		} else {
			b, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil {
				errorf("reading %q failed: %v", sibling, err)
			} else if string(b) != body {
				errorf("%q is now %q, want %q", sibling, string(b), body)
			}
		}
		if v, ok := got.Metadata()["escaped"]; ok {
			errorf("%q gained metadata from outside the root: escaped=%q", sibling, v)
		}
	}

	if err := strg.Delete(ctx, sibling); err != nil {
		errorf("Delete(%q) failed: %v", sibling, err)
	}
	// With the trailing slash: DeleteRecursive matches the prefix by string,
	// and the caller may hold keys that merely start with dir -- this helper's
	// own sibling is named that way on purpose.
	if err := strg.DeleteRecursive(ctx, dir+"/"); err != nil {
		errorf("DeleteRecursive(%q) failed: %v", dir+"/", err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("TestStorageNameEscape found %d errors:\n\t%s", len(errs), strings.Join(errs, "\n\t"))
	}
	return nil
}

// checkDecoy reports how obj differs from the decoy TestStorageNameEscape wrote.
func checkDecoy(obj s2.Object, body string) error {
	rc, err := obj.Open()
	if err != nil {
		return fmt.Errorf("Open failed: %w", err)
	}
	defer func() { _ = rc.Close() }()

	b, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("read failed: %w", err)
	}
	if string(b) != body {
		return fmt.Errorf("body is %q, want %q", string(b), body)
	}
	if ct := obj.ContentType(); ct != "text/plain" {
		return fmt.Errorf("ContentType() is %q, want %q", ct, "text/plain")
	}
	if v := obj.Metadata()["decoy"]; v != "yes" {
		return fmt.Errorf("metadata decoy=%q, want %q", v, "yes")
	}
	return nil
}
