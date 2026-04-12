package gcs

import (
	"io"
	"testing"
	"time"

	"github.com/mojatter/s2"
	"github.com/stretchr/testify/suite"
)

type ObjectTestSuite struct {
	suite.Suite
	client *mockGCSClient
}

func TestObjectTestSuite(t *testing.T) {
	suite.Run(t, &ObjectTestSuite{})
}

func (s *ObjectTestSuite) SetupTest() {
	s.client = newMockGCSClient()
	s.client.put("my-bucket", "test.txt", []byte("hello"), map[string]string{"foo": "bar"})
}

func (s *ObjectTestSuite) TestObjectProperties() {
	now := time.Now()
	obj := &object{
		client:       s.client,
		bucket:       "my-bucket",
		prefix:       "",
		name:         "test.txt",
		length:       uint64(5),
		lastModified: now,
		metadata:     s2.Metadata{"existing": "true"},
	}

	s.Equal("test.txt", obj.Name())
	s.Equal(uint64(5), obj.Length())
	s.Equal(now, obj.LastModified())
	val, ok := obj.Metadata().Get("existing")
	s.True(ok)
	s.Equal("true", val)
}

func (s *ObjectTestSuite) TestObjectOpen() {
	obj := &object{
		client: s.client,
		bucket: "my-bucket",
		prefix: "",
		name:   "test.txt",
	}

	rc, err := obj.Open()
	s.Require().NoError(err)
	defer rc.Close()

	body, err := io.ReadAll(rc)
	s.Require().NoError(err)
	s.Equal("hello", string(body))
}

func (s *ObjectTestSuite) TestObjectMetadataNil() {
	obj := &object{
		client: s.client,
		bucket: "my-bucket",
		name:   "test.txt",
	}

	md := obj.Metadata()
	s.NotNil(md)
	s.Equal(0, len(md))
}

func (s *ObjectTestSuite) TestObjectOpenRange() {
	s.Run("typical", func() {
		obj := &object{
			client: s.client,
			bucket: "my-bucket",
			name:   "test.txt",
			length: 5,
		}
		rc, err := obj.OpenRange(0, 5)
		s.Require().NoError(err)
		defer rc.Close()

		body, _ := io.ReadAll(rc)
		s.Equal("hello", string(body))
	})

	s.Run("not found", func() {
		obj := &object{
			client: s.client,
			bucket: "my-bucket",
			name:   "not-found.txt",
			length: 5,
		}
		_, err := obj.OpenRange(0, 5)
		s.Error(err)
		s.ErrorIs(err, s2.ErrNotExist)
	})
}

func (s *ObjectTestSuite) TestObjectOpenNotFound() {
	obj := &object{
		client: s.client,
		bucket: "my-bucket",
		prefix: "",
		name:   "not-found.txt",
	}

	rc, err := obj.Open()
	s.Require().Error(err)
	s.Nil(rc)
	s.Require().ErrorIs(err, s2.ErrNotExist)
}
