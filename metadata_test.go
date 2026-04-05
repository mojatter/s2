package s2

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetadataMap(t *testing.T) {
	m := MetadataMap{
		"key1": "value1",
		"key2": "value2",
	}

	assert.Equal(t, 2, m.Len())

	keys := m.Keys()
	assert.Len(t, keys, 2)
	assert.Contains(t, keys, "key1")
	assert.Contains(t, keys, "key2")

	val, ok := m.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", val)

	val, ok = m.Get("nonexistent")
	assert.False(t, ok)
	assert.Equal(t, "", val)

	m.Put("key3", "value3")
	assert.Equal(t, 3, m.Len())
	val, ok = m.Get("key3")
	assert.True(t, ok)
	assert.Equal(t, "value3", val)

	toMap := m.ToMap()
	assert.Equal(t, 3, len(toMap))
	assert.Equal(t, "value1", toMap["key1"])
}
