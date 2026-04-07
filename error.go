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

// ErrUnknownType is returned by NewStorage when no plugin is registered for
// the requested Type. Detect with errors.Is:
//
//	if errors.Is(err, s2.ErrUnknownType) {
//	    // unknown backend
//	}
var ErrUnknownType = errors.New("s2: unknown storage type")
