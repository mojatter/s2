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
	// Its StartAfter ends the range already processed; with Overwrite, objects past it may be written again.
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
	lopts := opts.SrcOptions
	for {
		page, err := src.List(ctx, lopts)
		if err != nil {
			return res, err
		}
		for _, obj := range page.Objects {
			// Key would fold an invalid name onto another object's, and List refuses it as StartAfter.
			if ValidateName(obj.Name()) != nil {
				res.Rejected++
				continue
			}
			dstName := Key(opts.DstDir, obj.Name())
			// A copy inside the selection would be listed again, or change the objects being copied.
			if views && listed(dstAt+dstName) {
				res.Rejected++
				last = obj.Name()
				continue
			}
			if same {
				err = copyWithin(ctx, src, obj.Name(), dstName, opts.Overwrite, &res)
			} else {
				err = copyAcross(ctx, src, dst, obj.Name(), dstName, opts.Overwrite, &res)
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
func copyWithin(ctx context.Context, s Storage, name, dstName string, overwrite bool, res *CopyStorageResult) error {
	if !overwrite {
		exists, err := hasObject(ctx, s, dstName)
		if err != nil {
			return countRejected(err, res)
		}
		if exists {
			res.Skipped++
			return nil
		}
	}
	if err := s.Copy(ctx, name, dstName); err != nil {
		return countRejected(err, res)
	}
	res.Copied++
	return nil
}

// copyAcross reads name from src with Get, for the metadata a listing may lack, and writes it to dst as dstName.
func copyAcross(ctx context.Context, src, dst Storage, name, dstName string, overwrite bool, res *CopyStorageResult) error {
	if !overwrite {
		// Checked first so an existing object is skipped without sending its body.
		exists, err := hasObject(ctx, dst, dstName)
		if err != nil {
			return countRejected(err, res)
		}
		if exists {
			res.Skipped++
			return nil
		}
	}
	obj, err := src.Get(ctx, name)
	if errors.Is(err, errors.ErrUnsupported) {
		res.Unsupported++
		return nil
	}
	if err != nil {
		return countRejected(err, res)
	}
	out := renamedObject{Object: obj, name: dstName}
	if overwrite {
		err = dst.Put(ctx, out)
	} else {
		err = putIfAbsent(ctx, dst, out)
	}
	switch {
	case errors.Is(err, ErrPreconditionFailed):
		res.Skipped++
		return nil
	case err != nil && !errors.Is(err, ErrUnknownETag):
		// ErrUnknownETag means the object was stored.
		return countRejected(err, res)
	}
	res.Copied++
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

// countRejected counts err in Rejected when it wraps ErrInvalidName, and returns any other error.
func countRejected(err error, res *CopyStorageResult) error {
	if errors.Is(err, ErrInvalidName) {
		res.Rejected++
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
