package s3api

import (
	"crypto/md5" // #nosec G501 -- MD5 is used here only to mirror S3 multipart ETag semantics under test.
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mojatter/s2"
)

func TestPartsReader(t *testing.T) {
	testCases := []struct {
		caseName string
		bodies   []string
	}{
		{caseName: "single part", bodies: []string{"hello"}},
		{caseName: "two parts", bodies: []string{"foo", "barbaz"}},
		{caseName: "empty part in middle", bodies: []string{"a", "", "b"}},
		{caseName: "many parts", bodies: []string{"1", "22", "333", "4444", "55555"}},
		{caseName: "no parts", bodies: nil},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			parts := make([]s2.Object, len(tc.bodies))
			var want string
			var wantMD5s []byte
			for i, body := range tc.bodies {
				parts[i] = s2.NewObjectBytes("part", []byte(body))
				want += body
				h := md5.Sum([]byte(body)) // #nosec G401
				wantMD5s = append(wantMD5s, h[:]...)
			}

			pr := &partsReader{parts: parts}
			got, err := io.ReadAll(pr)
			require.NoError(t, err)
			assert.Equal(t, want, string(got))
			assert.Equal(t, wantMD5s, pr.partMD5s)
			assert.NoError(t, pr.Close())
		})
	}
}

func TestPartsReader_SmallBuffer(t *testing.T) {
	// Read byte-by-byte to exercise the "part exhausted mid-buffer" path.
	parts := []s2.Object{
		s2.NewObjectBytes("a", []byte("abc")),
		s2.NewObjectBytes("b", []byte("de")),
	}
	pr := &partsReader{parts: parts}

	var got []byte
	buf := make([]byte, 1)
	for {
		n, err := pr.Read(buf)
		got = append(got, buf[:n]...)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}
	assert.Equal(t, "abcde", string(got))
	assert.Len(t, pr.partMD5s, 2*md5.Size)
}
