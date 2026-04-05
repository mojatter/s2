package s2

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewObject(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "s2test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	tempFile := filepath.Join(tempDir, "test.txt")
	err = os.WriteFile(tempFile, []byte("hello"), 0644)
	require.NoError(t, err)

	ctx := context.Background()
	obj, err := NewObject(ctx, tempFile)
	require.NoError(t, err)
	assert.Equal(t, tempFile, obj.Name())
	assert.Equal(t, uint64(5), obj.Length())
	assert.NotZero(t, obj.LastModified())

	rc, err := obj.Open()
	require.NoError(t, err)
	defer rc.Close()

	content, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(content))

	// Test non-existent file
	_, err = NewObject(ctx, filepath.Join(tempDir, "nonexistent"))
	assert.Error(t, err)

	// Test directory
	_, err = NewObject(ctx, tempDir)
	assert.Error(t, err)
	var errNotExist *ErrNotExist
	assert.ErrorAs(t, err, &errNotExist)
}

func TestNewObjectReader(t *testing.T) {
	body := io.NopCloser(os.Stdin) // just a reader
	obj := NewObjectReader("test", body, 100)
	assert.Equal(t, "test", obj.Name())
	assert.Equal(t, uint64(100), obj.Length())

	rc, err := obj.Open()
	require.NoError(t, err)
	assert.Equal(t, body, rc)
}

func TestNewObjectBytes(t *testing.T) {
	data := []byte("hello world")
	obj := NewObjectBytes("test.bin", data)
	assert.Equal(t, "test.bin", obj.Name())
	assert.Equal(t, uint64(len(data)), obj.Length())

	rc, err := obj.Open()
	require.NoError(t, err)
	defer rc.Close()

	content, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, data, content)

	md := obj.Metadata()
	assert.NotNil(t, md)
	md.Put("foo", "bar")
	assert.Equal(t, "bar", obj.Metadata().ToMap()["foo"])
}

func TestObjectOptions(t *testing.T) {
	t.Run("WithMetadata on NewObjectBytes", func(t *testing.T) {
		md := MetadataMap{"author": "alice", "version": "1"}
		obj := NewObjectBytes("meta.txt", []byte("data"), WithMetadata(md))

		v, ok := obj.Metadata().Get("author")
		assert.True(t, ok)
		assert.Equal(t, "alice", v)

		v, ok = obj.Metadata().Get("version")
		assert.True(t, ok)
		assert.Equal(t, "1", v)
	})

	t.Run("WithLastModified on NewObjectBytes", func(t *testing.T) {
		ts := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
		obj := NewObjectBytes("time.txt", []byte("data"), WithLastModified(ts))
		assert.Equal(t, ts, obj.LastModified())
	})

	t.Run("WithMetadata on NewObjectReader", func(t *testing.T) {
		md := MetadataMap{"key": "val"}
		body := io.NopCloser(os.Stdin)
		obj := NewObjectReader("r.txt", body, 0, WithMetadata(md))

		v, ok := obj.Metadata().Get("key")
		assert.True(t, ok)
		assert.Equal(t, "val", v)
	})

	t.Run("WithMetadata on NewObject", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "s2test-opt")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		f := filepath.Join(tempDir, "opt.txt")
		require.NoError(t, os.WriteFile(f, []byte("hello"), 0644))

		md := MetadataMap{"key": "val"}
		obj, err := NewObject(context.Background(), f, WithMetadata(md))
		require.NoError(t, err)

		v, ok := obj.Metadata().Get("key")
		assert.True(t, ok)
		assert.Equal(t, "val", v)
	})

	t.Run("multiple options", func(t *testing.T) {
		ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		md := MetadataMap{"a": "b"}
		obj := NewObjectBytes("multi.txt", []byte("x"), WithMetadata(md), WithLastModified(ts))

		assert.Equal(t, ts, obj.LastModified())
		v, ok := obj.Metadata().Get("a")
		assert.True(t, ok)
		assert.Equal(t, "b", v)
	})
}

func TestOpenRange(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "s2test-range")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	f := filepath.Join(tempDir, "range.txt")
	require.NoError(t, os.WriteFile(f, []byte("Hello, World!"), 0644))

	ctx := context.Background()
	obj, err := NewObject(ctx, f)
	require.NoError(t, err)

	t.Run("full range", func(t *testing.T) {
		rc, err := obj.OpenRange(0, obj.Length())
		require.NoError(t, err)
		defer rc.Close()
		b, _ := io.ReadAll(rc)
		assert.Equal(t, "Hello, World!", string(b))
	})

	t.Run("partial from middle", func(t *testing.T) {
		// Re-create obj since Open consumes body for non-file objects
		obj, err := NewObject(ctx, f)
		require.NoError(t, err)
		rc, err := obj.OpenRange(7, 5)
		require.NoError(t, err)
		defer rc.Close()
		b, _ := io.ReadAll(rc)
		assert.Equal(t, "World", string(b))
	})

	t.Run("partial from start", func(t *testing.T) {
		obj, err := NewObject(ctx, f)
		require.NoError(t, err)
		rc, err := obj.OpenRange(0, 5)
		require.NoError(t, err)
		defer rc.Close()
		b, _ := io.ReadAll(rc)
		assert.Equal(t, "Hello", string(b))
	})

	t.Run("non-seeker fallback", func(t *testing.T) {
		// NewObjectBytes body is a bytes.Reader wrapped in NopCloser (seeker),
		// but NewObjectReader with a plain reader uses the fallback path.
		data := []byte("ABCDEFGHIJ")
		pr, pw := io.Pipe()
		go func() {
			pw.Write(data)
			pw.Close()
		}()
		obj := NewObjectReader("pipe.txt", pr, uint64(len(data)))
		rc, err := obj.OpenRange(3, 4)
		require.NoError(t, err)
		defer rc.Close()
		b, _ := io.ReadAll(rc)
		assert.Equal(t, "DEFG", string(b))
	})

	t.Run("not found", func(t *testing.T) {
		badObj := NewObjectReader("nonexistent.txt", nil, 10)
		// body is nil, so Open() falls back to os.Open which fails
		_, err := badObj.OpenRange(0, 5)
		assert.Error(t, err)
	})
}
