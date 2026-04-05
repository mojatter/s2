package s2test

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/mojatter/s2"
)

// TestStorageList is a test helper for validating s2.Storage list operations.
// It tests the List and ListAfter methods of an s2.Storage implementation
// for a particular prefix against the expected direct object names.
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

	objs, prefixes, err := strg.List(ctx, prefix, 0)
	if err != nil {
		errorf("List(%q, 0) failed: %v", prefix, err)
	} else {
		checkMatch(fmt.Sprintf("List(%q, 0)", prefix), objs, expected)
		checkPrefixes(fmt.Sprintf("List(%q, 0) prefixes", prefix), prefixes, expectedPrefixes)
	}

	if len(expected) > 1 {
		limit := len(expected) / 2
		objs1, _, err := strg.List(ctx, prefix, limit)
		if err != nil {
			errorf("List(%q, %d) failed: %v", prefix, limit, err)
		} else if len(objs1) != limit {
			errorf("List(%q, %d) returned %d objects", prefix, limit, len(objs1))
		} else {
			last := objs1[len(objs1)-1].Name()
			objs2, _, err := strg.ListAfter(ctx, prefix, 0, last)
			if err != nil {
				errorf("ListAfter(%q, 0, %q) failed: %v", prefix, last, err)
			} else {
				var combined []s2.Object
				combined = append(combined, objs1...)
				combined = append(combined, objs2...)
				checkMatch(fmt.Sprintf("List Pagination (prefix %q, limit %d, after %q)", prefix, limit, last), combined, expected)
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("TestStorageList found %d errors:\n\t%s", len(errs), strings.Join(errs, "\n\t"))
	}
	return nil
}

// TestStorageListRecursive is a test helper for validating s2.Storage recursive list operations.
// It tests the ListRecursive and ListRecursiveAfter methods
// of an s2.Storage implementation against the expected object names.
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

	// 1. Check ListRecursive("", 0)
	objs, err := strg.ListRecursive(ctx, "", 0)
	if err != nil {
		errorf("ListRecursive(\"\", 0) failed: %v", err)
	} else {
		checkMatch("ListRecursive(\"\", 0)", objs, expected)
	}

	// 2. Test prefix filtering
	if len(expected) > 0 {
		// Find a prefix that matches a subset
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
					objs, err := strg.ListRecursive(ctx, prefix, 0)
					if err != nil {
						errorf("ListRecursive(%q, 0) failed: %v", prefix, err)
					} else {
						checkMatch(fmt.Sprintf("ListRecursive(%q, 0)", prefix), objs, want)
					}
					break // test one prefix
				}
			}
		}
	}

	// 3. Test Pagination (ListRecursive & ListRecursiveAfter)
	if len(expected) > 1 {
		limit := len(expected) / 2
		objs1, err := strg.ListRecursive(ctx, "", limit)
		if err != nil {
			errorf("ListRecursive pagination failed: %v", err)
		} else if len(objs1) != limit {
			errorf("ListRecursive(\"\", %d) returned %d objects", limit, len(objs1))
		} else {
			last := objs1[len(objs1)-1].Name()
			objs2, err := strg.ListRecursiveAfter(ctx, "", 0, last)
			if err != nil {
				errorf("ListRecursiveAfter(\"\", 0, %q) failed: %v", last, err)
			} else {
				var combined []s2.Object
				combined = append(combined, objs1...)
				combined = append(combined, objs2...)
				checkMatch(fmt.Sprintf("Pagination combined (limit %d, after %q)", limit, last), combined, expected)
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("TestStorageListRecursive found %d errors:\n\t%s", len(errs), strings.Join(errs, "\n\t"))
	}
	return nil
}

// TestStorageGetPut validates that Put writes an object and Get reads it back correctly.
func TestStorageGetPut(ctx context.Context, strg s2.Storage) error {
	var errs []string
	errorf := func(format string, args ...any) {
		errs = append(errs, fmt.Sprintf(format, args...))
	}

	name := "s2test-getput.txt"
	body := []byte("s2test content")
	obj := s2.NewObjectBytes(name, body)
	obj.Metadata().Put("test-key", "test-val")

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
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("ReadAll failed: %w", err)
	}
	if string(b) != string(body) {
		errorf("body = %q, want %q", string(b), string(body))
	}

	v, ok := got.Metadata().Get("test-key")
	if !ok {
		errorf("metadata key %q not found", "test-key")
	} else if v != "test-val" {
		errorf("metadata %q = %q, want %q", "test-key", v, "test-val")
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
	if !s2.IsNotExist(err) {
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
			rc.Close()
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
	if err := strg.Move(ctx, src, moved); err != nil {
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
			rc.Close()
			if string(b) != string(body) {
				errorf("Move body = %q, want %q", string(b), string(body))
			}
		}
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

	if len(errs) > 0 {
		return fmt.Errorf("TestStorageDelete found %d errors:\n\t%s", len(errs), strings.Join(errs, "\n\t"))
	}
	return nil
}

// TestStoragePutMetadata validates PutMetadata updates metadata without changing the object body.
func TestStoragePutMetadata(ctx context.Context, strg s2.Storage) error {
	name := "s2test-putmeta.txt"
	body := []byte("metadata test")
	if err := strg.Put(ctx, s2.NewObjectBytes(name, body)); err != nil {
		return fmt.Errorf("Put(%q) failed: %w", name, err)
	}

	md := s2.MetadataMap{"author": "s2test", "version": "1"}
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
	rc.Close()
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

	return nil
}
