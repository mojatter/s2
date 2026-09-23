package s2

import "errors"

// ErrNotExist is returned when an operation targets an object that does not
// exist. Backends wrap this with the missing object's name via fmt.Errorf, so
// callers should detect it with errors.Is rather than direct equality:
//
//	if errors.Is(err, s2.ErrNotExist) {
//	    // handle missing object
//	}
var ErrNotExist = errors.New("s2: object not exist")

// ErrRequiredConfigRoot is returned by NewStorage implementations when
// Config.Root is empty. Root identifies the bucket, container, or directory
// that the storage operates on and is always required.
var ErrRequiredConfigRoot = errors.New("s2: required config.root")

// ErrUnknownType is returned by NewStorage when no plugin is registered for
// the requested Type. Detect with errors.Is:
//
//	if errors.Is(err, s2.ErrUnknownType) {
//	    // unknown backend
//	}
var ErrUnknownType = errors.New("s2: unknown storage type")

// ErrUnknownETag is returned by Upload when the object was stored but its ETag
// could not be learned, because the read-back that would have supplied it
// failed. The write happened; only UploadResult.ETag is missing. The read-back's
// own error is wrapped alongside, so errors.Is still finds ErrNotExist under it.
var ErrUnknownETag = errors.New("s2: object stored but its etag is unknown")

// ErrInvalidName is returned for a name or prefix a storage refuses: one that
// would resolve to something other than it spells, as "../other" does, or one
// the backend keeps for its own state. Detect with errors.Is:
//
//	if errors.Is(err, s2.ErrInvalidName) {
//	    // reject the caller's name
//	}
var ErrInvalidName = errors.New("s2: invalid name")
