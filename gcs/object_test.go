package gcs

import (
	"context"
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

	// Metadata() is a pure read: a List result keeps the nil the backend gave
	// it. Storage.Get is what guarantees a writable map.
	s.Nil(obj.Metadata())
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

// Open and OpenRange read the version Get or List saw, or fail.
func (s *ObjectTestSuite) TestObjectOpenSinceGet() {
	testCases := []struct {
		caseName string
		change   func(m *mockGCSClient)
		// ignoreGenerationMatch reads as an endpoint that does not honour ifGenerationMatch.
		ignoreGenerationMatch bool
		wantBody              string
		wantErr               error
	}{
		{caseName: "unchanged", wantBody: "short"},
		{caseName: "replaced", change: func(m *mockGCSClient) { m.put("my-bucket", "k", []byte("a longer body"), nil) }, wantErr: s2.ErrModified},
		{caseName: "replaced on an endpoint that ignores ifGenerationMatch", change: func(m *mockGCSClient) { m.put("my-bucket", "k", []byte("a longer body"), nil) }, ignoreGenerationMatch: true, wantErr: s2.ErrModified},
		{caseName: "deleted", change: func(m *mockGCSClient) { m.del("my-bucket", "k") }, wantErr: s2.ErrNotExist},
		{caseName: "deleted and recreated", change: func(m *mockGCSClient) {
			m.del("my-bucket", "k")
			m.put("my-bucket", "k", []byte("a longer body"), nil)
		}, wantErr: s2.ErrModified},
	}
	for _, tc := range testCases {
		for _, via := range []string{"Get", "List"} {
			s.Run(tc.caseName+" via "+via, func() {
				m := newMockGCSClient()
				m.ignoreGenerationMatch = tc.ignoreGenerationMatch
				m.put("my-bucket", "k", []byte("short"), nil)
				strg := &gcsStorage{client: m, bucket: "my-bucket"}
				var obj s2.Object
				if via == "Get" {
					got, err := strg.Get(context.Background(), "k")
					s.Require().NoError(err)
					obj = got
				} else {
					res, err := strg.List(context.Background(), s2.ListOptions{})
					s.Require().NoError(err)
					s.Require().Len(res.Objects, 1)
					obj = res.Objects[0]
				}
				if tc.change != nil {
					tc.change(m)
				}

				// The mid-object range reads from offset 1, so the conditions are checked away from the start too.
				opens := []struct {
					caseName string
					open     func() (io.ReadCloser, error)
					offset   int
				}{
					{caseName: "Open", open: obj.Open},
					{caseName: "OpenRange", open: func() (io.ReadCloser, error) { return obj.OpenRange(0, obj.Length()) }},
					{caseName: "OpenRange mid", open: func() (io.ReadCloser, error) { return obj.OpenRange(1, obj.Length()-1) }, offset: 1},
				}
				for _, op := range opens {
					s.Run(op.caseName, func() {
						rc, err := op.open()
						if tc.wantErr != nil {
							s.ErrorIs(err, tc.wantErr)
							return
						}
						s.Require().NoError(err)
						b, err := io.ReadAll(rc)
						_ = rc.Close()
						s.Require().NoError(err)
						s.Equal(tc.wantBody[op.offset:], string(b))
					})
				}
			})
		}
	}
}
