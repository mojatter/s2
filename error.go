package s2

import "errors"

// ErrUnknownType represents an error that the storage type is unknown.
type ErrUnknownType struct {
	Type Type
}

func (e *ErrUnknownType) Error() string {
	return "unknown type: " + string(e.Type)
}

// IsUnknownType returns true if the error is a unknown type error.
func IsUnknownType(err error) bool {
	var e *ErrUnknownType
	return errors.As(err, &e)
}

// ErrNotExist represents an error that the object does not exist.
type ErrNotExist struct {
	Name string
}

func (e *ErrNotExist) Error() string {
	return "not exist: " + e.Name
}

// IsNotExist returns true if the error is a not exist error.
func IsNotExist(err error) bool {
	var e *ErrNotExist
	return errors.As(err, &e)
}
