package main

import (
	"log"
	"os"

	_ "github.com/mojatter/s2/fs" // Register FS storage
	"github.com/mojatter/s2/server"

	_ "github.com/mojatter/s2/server/handlers"
	_ "github.com/mojatter/s2/server/handlers/buckets"
	_ "github.com/mojatter/s2/server/handlers/buckets/objects"
	_ "github.com/mojatter/s2/server/handlers/s3api"
)

func main() {
	if err := server.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
