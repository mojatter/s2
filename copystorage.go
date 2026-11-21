package s2

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// CopyStorageOptions controls a CopyStorage call.
type CopyStorageOptions struct {
	// DstDir is prepended to every copied name on dst; empty keeps the names as they are.
	DstDir string
	// SrcOptions selects the objects on src, as List does; Limit is the page size and every page is copied.
	SrcOptions ListOptions
	// Overwrite replaces an object that already exists on dst; false skips it.
	Overwrite bool
	// ObjectDone, if set, is called for each object counted in the result, in listing order; not for the object a failing call stops at.
	ObjectDone func(CopyStorageObjectResult)
}

// CopyStorageStatus is what CopyStorage did with one object, matching a count in CopyStorageResult.
type CopyStorageStatus string

const (
	// CopyStorageCopied is an object counted in CopyStorageResult.Copied.
	CopyStorageCopied CopyStorageStatus = "copied"
	// CopyStorageSkipped is an object counted in CopyStorageResult.Skipped.
	CopyStorageSkipped CopyStorageStatus = "skipped"
	// CopyStorageUnsupported is an object counted in CopyStorageResult.Unsupported.
	CopyStorageUnsupported CopyStorageStatus = "unsupported"
	// CopyStorageRejected is an object counted in CopyStorageResult.Rejected.
	CopyStorageRejected CopyStorageStatus = "rejected"
)

// CopyStorageObjectResult is what CopyStorage did with one object.
type CopyStorageObjectResult struct {
	// Name is the object's name on src.
	Name string
	// DstName is the copy's name on dst; empty when Name fails ValidateName.
	DstName string
	// Status is the count the object went into.
	Status CopyStorageStatus
	// Err is why the object was unsupported or rejected, nil otherwise; a rejection by name wraps ErrInvalidName.
	Err error
}

// CopyStorageResult reports what a CopyStorage call did, also when it returns an error.
type CopyStorageResult struct {
	// Copied is the number of objects written to dst.
	Copied int64
	// Skipped is the number of objects that already existed on dst while Overwrite was false.
	Skipped int64
	// Unsupported is the number of objects src lists but cannot read; they are skipped.
	Unsupported int64
	// Rejected is the number of objects src or dst refuses by name (ErrInvalidName), or whose copy lands in the selection.
	Rejected int64
	// ResumeOptions is nil when the call finished; otherwise pass it as SrcOptions to continue.
	// Its StartAfter ends the range already processed; with Overwrite, objects past it may be written again, and an invalid name past it is counted again.
	ResumeOptions *ListOptions
}

// CopyStorage copies the objects opts.SrcOptions selects on src to dst, keeping each object's name, Content-Type and metadata.
// It stops at the first error, keeping what was copied. With Overwrite false, the check for an existing object is atomic
// against a concurrent writer only when dst != src and dst's Uploader honours IfNoneMatch.
// When dst == src, or their URIs share a scheme and host, a dst and DstDir that place copies inside the selection are refused,
// and with such URIs an object whose copy would still land there is counted in Rejected.
// A dst that is another view of src in a way the URIs do not show is not detected, and a recursive copy can then loop.
func CopyStorage(ctx context.Context, src, dst Storage, opts CopyStorageOptions) (res CopyStorageResult, err error) {
	last := ""
	defer func() {
		if err != nil {
			res.ResumeOptions = resumeOptions(opts.SrcOptions, last)
		}
	}()

	if err := ValidatePrefix(opts.DstDir); err != nil {
		return res, err
	}
	if err := ValidatePrefix(opts.SrcOptions.Prefix); err != nil {
		return res, err
	}
	same := sameStorage(src, dst)
	if same && strings.HasPrefix(Key(opts.DstDir, opts.SrcOptions.Prefix), opts.SrcOptions.Prefix) {
		return res, fmt.Errorf("s2: CopyStorage onto the same storage: DstDir %q overlaps Prefix %q", opts.DstDir, opts.SrcOptions.Prefix)
	}
	srcAt, dstAt, views := uriPaths(src, dst)
	views = views && !same
	prefix := opts.SrcOptions.Prefix
	if !opts.SrcOptions.Recursive {
		// A non-recursive listing reads Prefix as a directory and stops at the next "/".
		prefix = dirPrefix(prefix)
	}
	sel := srcAt + prefix
	// listed reports whether the listing would return a key at p.
	listed := func(p string) bool {
		rest, ok := strings.CutPrefix(p, sel)
		return ok && (opts.SrcOptions.Recursive || !strings.Contains(rest, "/"))
	}
	if views {
		// Every copy lands under out, so out inside the selection would list the copies again.
		out := dstAt + dirPrefix(opts.DstDir) + prefix
		if listed(out) {
			return res, fmt.Errorf("s2: CopyStorage: DstDir %q on %s overlaps Prefix %q on %s",
				opts.DstDir, JoinURI(dst.URI(), ""), opts.SrcOptions.Prefix, JoinURI(src.URI(), ""))
		}
	}
	t := tally{res: &res, done: opts.ObjectDone}
	lopts := opts.SrcOptions
	for {
		page, err := src.List(ctx, lopts)
		if err != nil {
			return res, err
		}
		for _, obj := range page.Objects {
			// Key would fold an invalid name onto another object's, and List refuses it as StartAfter.
			if err := ValidateName(obj.Name()); err != nil {
				t.add(obj.Name(), "", CopyStorageRejected, err)
				continue
			}
			dstName := Key(opts.DstDir, obj.Name())
			// A copy inside the selection would be listed again, or change the objects being copied.
			if views && listed(dstAt+dstName) {
				t.add(obj.Name(), dstName, CopyStorageRejected, fmt.Errorf("s2: CopyStorage: copy %q on %s lands in Prefix %q on %s",
					dstName, JoinURI(dst.URI(), ""), opts.SrcOptions.Prefix, JoinURI(src.URI(), "")))
				last = obj.Name()
				continue
			}
			if same {
				err = copyWithin(ctx, src, obj.Name(), dstName, opts.Overwrite, t)
			} else {
				err = copyAcross(ctx, src, dst, obj.Name(), dstName, opts.Overwrite, t)
			}
			if err != nil {
				return res, err
			}
			last = obj.Name()
		}
		if page.NextAfter == "" {
			return res, nil
		}
		if page.NextAfter == lopts.After {
			return res, fmt.Errorf("s2: List returned a NextAfter that does not advance: %q", lopts.After)
		}
		lopts.After = page.NextAfter
	}
}

// resumeOptions returns opts resuming after last, or opts unchanged when nothing finished.
func resumeOptions(opts ListOptions, last string) *ListOptions {
	if last != "" {
		opts.After = ""
		opts.StartAfter = last
	}
	return &opts
}

// sameStorage reports whether a and b are the same value; an incomparable pair is not.
func sameStorage(a, b Storage) (same bool) {
	defer func() {
		if recover() != nil {
			same = false
		}
	}()

	return a == b
}

// uriPaths returns the unescaped URI paths of src and dst, each ending in "/", when both have a URI with the same scheme and host.
func uriPaths(src, dst Storage) (srcAt, dstAt string, ok bool) {
	su, sok := parseURI(src)
	du, dok := parseURI(dst)
	// An s3:// host is a bucket name, whose case matters.
	if !sok || !dok || su.Scheme != du.Scheme || su.Host != du.Host {
		return "", "", false
	}
	return su.Path, du.Path, true
}

// parseURI parses s's URI as JoinURI cleans it; JoinURI leaves out one it cannot extend, such as "custom:a/".
func parseURI(s Storage) (*url.URL, bool) {
	u, err := url.Parse(JoinURI(s.URI(), ""))
	if err != nil || u.Scheme == "" {
		return nil, false
	}
	return u, true
}

// dirPrefix returns dir with a trailing "/", or "" for no dir.
func dirPrefix(dir string) string {
	if dir = strings.TrimSuffix(dir, "/"); dir != "" {
		return dir + "/"
	}
	return ""
}

// copyWithin copies name to dstName on s with Storage.Copy.
func copyWithin(ctx context.Context, s Storage, name, dstName string, overwrite bool, t tally) error {
	if !overwrite {
		exists, err := hasObject(ctx, s, dstName)
		if err != nil {
			return t.rejected(name, dstName, err)
		}
		if exists {
			t.add(name, dstName, CopyStorageSkipped, nil)
			return nil
		}
	}
	if err := s.Copy(ctx, name, dstName); err != nil {
		return t.rejected(name, dstName, err)
	}
	t.add(name, dstName, CopyStorageCopied, nil)
	return nil
}

// copyAcross reads name from src with Get, for the metadata a listing may lack, and writes it to dst as dstName.
func copyAcross(ctx context.Context, src, dst Storage, name, dstName string, overwrite bool, t tally) error {
	if !overwrite {
		// Checked first so an existing object is skipped without sending its body.
		exists, err := hasObject(ctx, dst, dstName)
		if err != nil {
			return t.rejected(name, dstName, err)
		}
		if exists {
			t.add(name, dstName, CopyStorageSkipped, nil)
			return nil
		}
	}
	obj, err := src.Get(ctx, name)
	if errors.Is(err, errors.ErrUnsupported) {
		t.add(name, dstName, CopyStorageUnsupported, err)
		return nil
	}
	if err != nil {
		return t.rejected(name, dstName, err)
	}
	out := renamedObject{Object: obj, name: dstName}
	if overwrite {
		err = dst.Put(ctx, out)
	} else {
		err = putIfAbsent(ctx, dst, out)
	}
	switch {
	case errors.Is(err, ErrPreconditionFailed):
		t.add(name, dstName, CopyStorageSkipped, nil)
		return nil
	case err != nil && !errors.Is(err, ErrUnknownETag):
		// ErrUnknownETag means the object was stored.
		return t.rejected(name, dstName, err)
	}
	t.add(name, dstName, CopyStorageCopied, nil)
	return nil
}

// hasObject reports whether s holds an object at name, as Get answers; Exists also reports a directory or prefix.
func hasObject(ctx context.Context, s Storage, name string) (bool, error) {
	_, err := s.Get(ctx, name)
	switch {
	case err == nil, errors.Is(err, errors.ErrUnsupported):
		return true, nil
	case errors.Is(err, ErrNotExist):
		return false, nil
	}
	return false, err
}

// tally counts each object in res and reports it.
type tally struct {
	res  *CopyStorageResult
	done func(CopyStorageObjectResult)
}

// add counts one object under status, then reports it.
func (t tally) add(name, dstName string, status CopyStorageStatus, err error) {
	switch status {
	case CopyStorageCopied:
		t.res.Copied++
	case CopyStorageSkipped:
		t.res.Skipped++
	case CopyStorageUnsupported:
		t.res.Unsupported++
	case CopyStorageRejected:
		t.res.Rejected++
	}
	if t.done != nil {
		t.done(CopyStorageObjectResult{Name: name, DstName: dstName, Status: status, Err: err})
	}
}

// rejected counts err as rejected when it wraps ErrInvalidName, and returns any other error.
func (t tally) rejected(name, dstName string, err error) error {
	if errors.Is(err, ErrInvalidName) {
		t.add(name, dstName, CopyStorageRejected, err)
		return nil
	}
	return err
}

// putIfAbsent writes obj unless an object holds its name, atomically when dst's Uploader honours IfNoneMatch.
func putIfAbsent(ctx context.Context, dst Storage, obj Object) error {
	_, err := Upload(ctx, dst, obj, UploadOptions{IfNoneMatch: "*"})
	// ErrUnknownETag means obj was stored, even when the read-back wraps ErrUnsupported.
	if errors.Is(err, errors.ErrUnsupported) && !errors.Is(err, ErrUnknownETag) {
		return dst.Put(ctx, obj)
	}
	return err
}

// renamedObject is an Object stored under another name.
type renamedObject struct {
	Object
	name string
}

func (o renamedObject) Name() string { return o.name }
