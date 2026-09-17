package azblob

import (
	"bytes"
	"crypto/md5" // #nosec G501 -- checks Content-MD5
	"io"
	"testing"
	"testing/iotest"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
)

func TestMD5Reader(t *testing.T) {
	testCases := []struct {
		caseName string
		body     []byte
	}{
		{caseName: "empty", body: []byte{}},
		{caseName: "small", body: []byte("hello")},
		{caseName: "several reads", body: bytes.Repeat([]byte("abc"), 10000)},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			headers := &blob.HTTPHeaders{}
			r := &md5Reader{r: iotest.HalfReader(bytes.NewReader(tc.body)), h: md5.New(), headers: headers}

			var got []byte
			if len(tc.body) > 0 {
				first := make([]byte, 1)
				n, err := r.Read(first)
				if err != nil {
					t.Fatal(err)
				}
				if headers.BlobContentMD5 != nil {
					t.Fatalf("BlobContentMD5 set before EOF")
				}
				got = append(got, first[:n]...)
			}
			rest, err := io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, rest...)

			if !bytes.Equal(got, tc.body) {
				t.Fatalf("body = %d bytes, want %d", len(got), len(tc.body))
			}
			want := md5.Sum(tc.body) // #nosec G401 -- checks Content-MD5
			if !bytes.Equal(headers.BlobContentMD5, want[:]) {
				t.Fatalf("BlobContentMD5 = %x, want %x", headers.BlobContentMD5, want)
			}
		})
	}
}
