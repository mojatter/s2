package azblob

import (
	"bytes"
	"context"
	"crypto/md5" // #nosec G501 -- checks Content-MD5
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"testing/iotest"
	"time"

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
		sas      string
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
		{caseName: "SAS connection string", key: "dir/plain.txt", sas: "sv=2020-02-10&sp=rw&sig=c2ln"},
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

			conn := "DefaultEndpointsProtocol=http;AccountName=acct;AccountKey=a2V5;BlobEndpoint=" + srv.URL + "/acct;"
			if tc.sas != "" {
				conn = "BlobEndpoint=" + srv.URL + "/acct;SharedAccessSignature=" + tc.sas
			}
			c, err := newSDKClient(&AzblobConfig{ConnectionString: conn})
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
			if u.RawQuery != tc.sas || u.Fragment != "" || u.Path != "/acct/ctr/"+tc.key {
				t.Errorf("x-ms-copy-source %q names path %q (query %q, fragment %q), want %q (query %q)", got, u.Path, u.RawQuery, u.Fragment, "/acct/ctr/"+tc.key, tc.sas)
			}
		})
	}
}

func TestCopyBlobWait(t *testing.T) {
	testCases := []struct {
		caseName string
		start    string
		props    []string
		cancel   bool
		wantErr  bool
	}{
		{caseName: "success at start", start: "success"},
		{caseName: "pending then success", start: "pending", props: []string{"pending", "success"}},
		{caseName: "pending then failed", start: "pending", props: []string{"failed"}, wantErr: true},
		{caseName: "aborted at start", start: "aborted", wantErr: true},
		{caseName: "canceled while pending", start: "pending", cancel: true, wantErr: true},
		{caseName: "pending then no status", start: "pending", props: []string{""}},
	}
	defer func(d time.Duration) { copyPollInterval = d }(copyPollInterval)

	copyPollInterval = time.Millisecond
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var polls int
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut {
					if tc.cancel {
						cancel()
					}
					w.Header().Set("x-ms-copy-status", tc.start)
					w.WriteHeader(http.StatusAccepted)
					return
				}
				if polls >= len(tc.props) {
					t.Errorf("poll %d past the %d planned", polls+1, len(tc.props))
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				if tc.props[polls] != "" {
					w.Header().Set("x-ms-copy-status", tc.props[polls])
				}
				polls++
			}))
			defer srv.Close()

			c, err := newSDKClient(&AzblobConfig{ConnectionString: "DefaultEndpointsProtocol=http;AccountName=acct;AccountKey=a2V5;BlobEndpoint=" + srv.URL + "/acct;"})
			if err != nil {
				t.Fatal(err)
			}
			err = c.copyBlob(ctx, "ctr", "src", "dst")
			if (err != nil) != tc.wantErr {
				t.Errorf("copyBlob: %v, want error %v", err, tc.wantErr)
			}
			if tc.cancel && !errors.Is(err, context.Canceled) {
				t.Errorf("copyBlob: %v, want context.Canceled", err)
			}
			if polls != len(tc.props) {
				t.Errorf("polled %d times, want %d", polls, len(tc.props))
			}
		})
	}
}

func TestSignedURLEscape(t *testing.T) {
	testCases := []struct {
		caseName string
		key      string
		wantPath string
	}{
		{caseName: "plain", key: "dir/plain.txt", wantPath: "/ctr/dir/plain.txt"},
		{caseName: "space", key: "sp ace.txt", wantPath: "/ctr/sp%20ace.txt"},
		{caseName: "hash", key: "hash#.txt", wantPath: "/ctr/hash%23.txt"},
		{caseName: "question mark", key: "q?x.txt", wantPath: "/ctr/q%3Fx.txt"},
		{caseName: "bare percent", key: "pct%zz.txt", wantPath: "/ctr/pct%25zz.txt"},
		{caseName: "percent escape", key: "pct%41.txt", wantPath: "/ctr/pct%2541.txt"},
		{caseName: "plus", key: "plus+.txt", wantPath: "/ctr/plus+.txt"},
		{caseName: "reserved under a directory", key: "dir/q?x.txt", wantPath: "/ctr/dir/q%3Fx.txt"},
		{caseName: "non-ASCII", key: "dir/日本語.txt", wantPath: "/ctr/dir/%E6%97%A5%E6%9C%AC%E8%AA%9E.txt"},
		{caseName: "directory marker", key: "d+ir/", wantPath: "/ctr/d+ir/"},
	}
	c, err := newSDKClient(&AzblobConfig{AccountName: "acct", AccountKey: "a2V5"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			got, err := c.signedURL("ctr", tc.key, "GET", time.Now().Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			u, err := url.Parse(got)
			if err != nil {
				t.Fatalf("signed URL %q: %v", got, err)
			}
			if u.Path != "/ctr/"+tc.key || u.EscapedPath() != tc.wantPath || u.Fragment != "" || u.Query().Get("sig") == "" {
				t.Errorf("signed URL %q names path %q (escaped %q, fragment %q, sig %q), want %q as %q", got, u.Path, u.EscapedPath(), u.Fragment, u.Query().Get("sig"), "/ctr/"+tc.key, tc.wantPath)
			}
		})
	}
}
