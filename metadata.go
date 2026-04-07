package s2

import "maps"

// Metadata holds object metadata as case-sensitive key/value pairs.
//
// Metadata mirrors the http.Header / url.Values pattern: it is a named map
// type with helper methods, so callers can use the standard map operations
// (range, len, indexing, literal initialization) directly when convenient
// and the methods when ergonomics matter.
//
// The zero value is nil and is read-safe; writes to a nil Metadata panic.
// Use make(s2.Metadata) or a literal s2.Metadata{...} to obtain a writable
// instance.
type Metadata map[string]string

// Get returns the value associated with key. The boolean reports whether
// the key is present, distinguishing a missing entry from an empty value.
func (m Metadata) Get(key string) (string, bool) {
	v, ok := m[key]
	return v, ok
}

// Set assigns value to key, overwriting any existing entry.
func (m Metadata) Set(key, value string) {
	m[key] = value
}

// Delete removes the entry for key. Calling Delete on a missing key is a no-op.
func (m Metadata) Delete(key string) {
	delete(m, key)
}

// Clone returns a deep copy of the metadata. The result is independent of
// the receiver: mutating one does not affect the other. Cloning a nil
// Metadata returns nil.
func (m Metadata) Clone() Metadata {
	if m == nil {
		return nil
	}
	return maps.Clone(m)
}
