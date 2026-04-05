package s2

// Metadata is an interface that represents metadata of an object.
type Metadata interface {
	// Len returns the number of metadata entries.
	Len() int
	// Keys returns all metadata keys.
	Keys() []string
	// Get returns the value of the specified metadata key.
	Get(key string) (string, bool)
	// Put sets the value for the specified metadata key.
	Put(key, value string)
	// ToMap converts the metadata to a map.
	ToMap() map[string]string
}

// MetadataMap implements Metadata interface.
type MetadataMap map[string]string

var _ = (Metadata)(MetadataMap{})

func (m MetadataMap) Len() int {
	return len(m)
}

func (m MetadataMap) Keys() []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func (m MetadataMap) Get(key string) (string, bool) {
	val, ok := m[key]
	return val, ok
}

func (m MetadataMap) Put(key, value string) {
	m[key] = value
}

func (m MetadataMap) ToMap() map[string]string {
	return m
}
