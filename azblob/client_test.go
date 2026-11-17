package azblob

import (
	"bytes"
	"context"
	"crypto/md5" // #nosec G501 -- checks Content-MD5
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func TestCopyBlobSource(t *testing.T) {
	testCases := []struct {
		caseName string
		key      string
	}{
		{caseName: "plain", key: "dir/plain.txt"},
		{caseName: "space", key: "sp ace.txt"},
		{caseName: "hash", key: "hash#.txt"},
		{caseName: "question mark", key: "q?x.txt"},
		{caseName: "bare percent", key: "pct%zz.txt"},
		{caseName: "percent escape", key: "pct%41.txt"},
		{caseName: "plus", key: "plus+.txt"},
		{caseName: "non-ASCII", key: "dir/日本語.txt"},
		{caseName: "directory marker", key: "d+ir/"},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			var got string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r.Header.Get("x-ms-copy-source")
				w.Header().Set("x-ms-copy-status", "success")
				w.WriteHeader(http.StatusAccepted)
			}))
			defer srv.Close()

			c, err := newSDKClient(&AzblobConfig{ConnectionString: "DefaultEndpointsProtocol=http;AccountName=acct;AccountKey=a2V5;BlobEndpoint=" + srv.URL + "/acct;"})
			if err != nil {
				t.Fatal(err)
			}
			if err := c.copyBlob(context.Background(), "ctr", tc.key, "dst"); err != nil {
				t.Fatal(err)
			}
			u, err := url.Parse(got)
			if err != nil {
				t.Fatalf("x-ms-copy-source %q: %v", got, err)
			}
			if u.RawQuery != "" || u.Fragment != "" || u.Path != "/acct/ctr/"+tc.key {
				t.Errorf("x-ms-copy-source %q names path %q (query %q, fragment %q), want %q", got, u.Path, u.RawQuery, u.Fragment, "/acct/ctr/"+tc.key)
			}
		})
	}
}
