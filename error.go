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
