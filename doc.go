// Package s2 is a lightweight object storage abstraction with multiple
// backends and an embeddable S3-compatible server.
//
// # Overview
//
// s2 provides a single Storage interface that all backends implement, and
// a small set of value types ([Object], [Metadata], [Config]) for moving
// data through it. The same interface drives a local-filesystem backend
// (osfs), an in-memory backend (memfs), and an AWS S3 / S3-compatible
// backend (s3). Backends register themselves via blank import:
//
//	import (
//	    _ "github.com/mojatter/s2/fs" // osfs + memfs
//	    _ "github.com/mojatter/s2/s3" // AWS S3
//	)
//
//	strg, err := s2.NewStorage(ctx, s2.Config{Type: s2.TypeOSFS, Root: "/var/data"})
//
// # Errors
//
// Operations that report a missing object wrap the sentinel [ErrNotExist].
// Detect them with errors.Is:
//
//	if _, err := strg.Get(ctx, "foo.txt"); errors.Is(err, s2.ErrNotExist) {
//	    // not found
//	}
//
// [NewStorage] wraps [ErrUnknownType] when the requested backend has not
// been registered.
//
// # Concurrency
//
// All Storage implementations shipped with s2 are safe for concurrent use
// by multiple goroutines. Methods that mutate state (Put, Delete,
// PutMetadata, ...) are independently atomic per object: a single Put is
// either fully visible to subsequent reads or not visible at all. Multiple
// concurrent Puts to the same object resolve to one of the writes — there
// is no defined ordering — but no torn writes are exposed.
//
// PutMetadata is NOT atomic with Put. Calling Put followed by PutMetadata
// leaves a window during which the object exists with whatever metadata
// Put itself wrote.
//
// # Atomicity per backend
//
// The following table summarizes the atomicity guarantees of each
// operation across the bundled backends.
//
//	Operation             | osfs                  | memfs                 | s3
//	----------------------|-----------------------|-----------------------|-----------------------
//	Put                   | atomic (temp+rename)  | atomic                | atomic (server-side)
//	Copy                  | atomic per dst        | atomic per dst        | atomic (server-side)
//	Move (via [Move])     | atomic (rename)       | non-atomic            | non-atomic
//	Delete                | atomic                | atomic                | atomic
//	DeleteRecursive       | best-effort, partial  | best-effort, partial  | best-effort, partial
//
// "Best-effort, partial" means the operation deletes objects one at a time
// (or in pages, for s3) and may leave some objects behind on error.
//
// # Stability
//
// s2 follows semantic versioning. Until v1.0.0 the public API may change
// between minor versions; breaking changes are documented in the release
// notes. After v1.0.0, the package will only break compatibility on a
// major-version bump.
package s2
