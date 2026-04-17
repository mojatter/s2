// Package s2env loads JSON configuration files and initializes all storage
// backends. Importing this package registers every built-in backend via
// side-effect imports.
//
// The core types and functions now live in the parent s2 package.
// This package re-exports them for backward compatibility.
package s2env

import (
	"context"
	"io"

	"github.com/mojatter/s2"
	_ "github.com/mojatter/s2/azblob"
	_ "github.com/mojatter/s2/fs"
	_ "github.com/mojatter/s2/gcs"
	_ "github.com/mojatter/s2/s3"
)

// Configs is an alias for [s2.Configs].
type Configs = s2.Configs

// Storages is an alias for [s2.Storages].
type Storages = s2.Storages

// LoadConfigs parses JSON formatted data from the specified io.Reader into Configs.
//
// Deprecated: Use [s2.LoadConfigs] instead.
func LoadConfigs(r io.Reader) (Configs, error) {
	return s2.LoadConfigs(r)
}

// LoadConfigsFile reads the file at the given path and parses it as a JSON format
// into Configs.
//
// Deprecated: Use [s2.LoadConfigsFile] instead.
func LoadConfigsFile(filename string) (Configs, error) {
	return s2.LoadConfigsFile(filename)
}

// Load helps to simplify loading the Configs from a specific JSON file and
// fully initializing all nested Storage instances into the resulting Storages structure.
//
// Deprecated: Use [s2.Load] instead.
func Load(ctx context.Context, filename string) (Storages, error) {
	return s2.Load(ctx, filename)
}
