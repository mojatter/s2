package s2

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetadata(t *testing.T) {
	m := Metadata{
		"key1": "value1",
		"key2": "value2",
	}

	assert.Equal(t, 2, len(m))

	val, ok := m.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", val)

	val, ok = m.Get("nonexistent")
	assert.False(t, ok)
	assert.Equal(t, "", val)

	m.Set("key3", "value3")
	assert.Equal(t, 3, len(m))
	val, ok = m.Get("key3")
	assert.True(t, ok)
	assert.Equal(t, "value3", val)

	m.Delete("key3")
	_, ok = m.Get("key3")
	assert.False(t, ok)
}

func TestMetadataClone(t *testing.T) {
	t.Run("nil clone returns nil", func(t *testing.T) {
		var m Metadata
		assert.Nil(t, m.Clone())
	})

	t.Run("clone is independent", func(t *testing.T) {
		m := Metadata{"a": "1"}
		c := m.Clone()
		c.Set("a", "2")
		v, _ := m.Get("a")
		assert.Equal(t, "1", v)
	})
}
