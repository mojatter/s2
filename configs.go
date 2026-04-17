package s2

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Configs is a map of logical names to Config objects.
type Configs map[string]Config

// Storages is a map of logical names to Storage instances.
type Storages map[string]Storage

// LoadConfigs parses JSON formatted data from the specified io.Reader into Configs.
func LoadConfigs(r io.Reader) (Configs, error) {
	var c Configs
	if err := json.NewDecoder(r).Decode(&c); err != nil {
		return nil, fmt.Errorf("decode configs: %w", err)
	}
	return c, nil
}

// LoadConfigsFile reads the file at the given path and parses it as a JSON format
// into Configs.
func LoadConfigsFile(filename string) (Configs, error) {
	f, err := os.Open(filepath.Clean(filename))
	if err != nil {
		return nil, fmt.Errorf("open config file: %w", err)
	}
	defer func() { _ = f.Close() }()

	return LoadConfigs(f)
}

// Storages iterates over the parsed configs, invokes NewStorage for each,
// and returns a new Storages map representing the initialized storage environments.
func (c Configs) Storages(ctx context.Context) (Storages, error) {
	storages := make(Storages, len(c))
	for name, cfg := range c {
		strg, err := NewStorage(ctx, cfg)
		if err != nil {
			return nil, err
		}
		storages[name] = strg
	}
	return storages, nil
}

// Load helps to simplify loading the Configs from a specific JSON file and
// fully initializing all nested Storage instances into the resulting Storages structure.
func Load(ctx context.Context, filename string) (Storages, error) {
	c, err := LoadConfigsFile(filename)
	if err != nil {
		return nil, err
	}
	return c.Storages(ctx)
}
