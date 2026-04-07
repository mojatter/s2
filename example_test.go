package s2_test

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/mojatter/s2"
	_ "github.com/mojatter/s2/fs" // register osfs/memfs backends
)

// ExampleNewStorage shows how to construct an in-memory Storage. The blank
// import of github.com/mojatter/s2/fs registers both osfs and memfs.
func ExampleNewStorage() {
	ctx := context.Background()

	strg, err := s2.NewStorage(ctx, s2.Config{Type: s2.TypeMemFS})
	if err != nil {
		panic(err)
	}
	fmt.Println(strg.Type())
	// Output: memfs
}

// ExampleStorage_Put_bytes writes a small object from a byte slice and
// reads it back via Open.
func ExampleStorage_Put_bytes() {
	ctx := context.Background()
	strg, _ := s2.NewStorage(ctx, s2.Config{Type: s2.TypeMemFS})

	if err := strg.Put(ctx, s2.NewObjectBytes("greeting.txt", []byte("hello, s2!"))); err != nil {
		panic(err)
	}

	obj, err := strg.Get(ctx, "greeting.txt")
	if err != nil {
		panic(err)
	}
	rc, _ := obj.Open()
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	fmt.Println(string(body))
	// Output: hello, s2!
}

// ExampleStorage_Put_metadata attaches custom metadata at write time.
// Metadata supplied via WithMetadata is persisted as part of Put — no
// separate PutMetadata call is needed for this case.
func ExampleStorage_Put_metadata() {
	ctx := context.Background()
	strg, _ := s2.NewStorage(ctx, s2.Config{Type: s2.TypeMemFS})

	obj := s2.NewObjectBytes(
		"report.txt",
		[]byte("payload"),
		s2.WithMetadata(s2.Metadata{"author": "alice", "version": "1"}),
	)
	if err := strg.Put(ctx, obj); err != nil {
		panic(err)
	}

	got, _ := strg.Get(ctx, "report.txt")
	author, _ := got.Metadata().Get("author")
	fmt.Println(author)
	// Output: alice
}

// ExampleStorage_List_flat lists immediate children at a given prefix and
// distinguishes objects from common (directory-like) prefixes.
func ExampleStorage_List_flat() {
	ctx := context.Background()
	strg, _ := s2.NewStorage(ctx, s2.Config{Type: s2.TypeMemFS})

	for _, key := range []string{"a.txt", "b.txt", "sub/c.txt"} {
		_ = strg.Put(ctx, s2.NewObjectBytes(key, []byte("x")))
	}

	res, err := strg.List(ctx, s2.ListOptions{})
	if err != nil {
		panic(err)
	}
	for _, o := range res.Objects {
		fmt.Println("obj:", o.Name())
	}
	for _, p := range res.CommonPrefixes {
		fmt.Println("dir:", p)
	}
	// Output:
	// obj: a.txt
	// obj: b.txt
	// dir: sub
}

// ExampleStorage_List_recursive walks the entire namespace below a prefix.
func ExampleStorage_List_recursive() {
	ctx := context.Background()
	strg, _ := s2.NewStorage(ctx, s2.Config{Type: s2.TypeMemFS})

	for _, key := range []string{"a.txt", "sub/b.txt", "sub/deep/c.txt"} {
		_ = strg.Put(ctx, s2.NewObjectBytes(key, []byte("x")))
	}

	res, _ := strg.List(ctx, s2.ListOptions{Recursive: true})
	for _, o := range res.Objects {
		fmt.Println(o.Name())
	}
	// Output:
	// a.txt
	// sub/b.txt
	// sub/deep/c.txt
}

// ExampleErrNotExist demonstrates how to detect a missing object using
// errors.Is. Backends wrap [s2.ErrNotExist] with the missing key, so
// direct equality (err == s2.ErrNotExist) is not reliable — use errors.Is.
func ExampleErrNotExist() {
	ctx := context.Background()
	strg, _ := s2.NewStorage(ctx, s2.Config{Type: s2.TypeMemFS})

	_, err := strg.Get(ctx, "missing.txt")
	if errors.Is(err, s2.ErrNotExist) {
		fmt.Println("not found")
	}
	// Output: not found
}

// ExampleMove uses the free function s2.Move, which transparently picks the
// fastest path: backends that implement the optional s2.Mover interface
// (e.g. osfs via filesystem rename) get an atomic rename, others fall back
// to Copy + Delete.
func ExampleMove() {
	ctx := context.Background()
	strg, _ := s2.NewStorage(ctx, s2.Config{Type: s2.TypeMemFS})
	_ = strg.Put(ctx, s2.NewObjectBytes("src.txt", []byte("hi")))

	if err := s2.Move(ctx, strg, "src.txt", "dst.txt"); err != nil {
		panic(err)
	}

	_, err := strg.Get(ctx, "src.txt")
	fmt.Println("src missing:", errors.Is(err, s2.ErrNotExist))

	got, _ := strg.Get(ctx, "dst.txt")
	rc, _ := got.Open()
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	fmt.Println("dst body:", string(body))
	// Output:
	// src missing: true
	// dst body: hi
}
