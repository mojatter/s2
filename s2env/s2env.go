package s2env

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/mojatter/s2"
	_ "github.com/mojatter/s2/fs"
	_ "github.com/mojatter/s2/s3"
)

// Configs is a map of logical names to s2.Config objects.
type Configs map[string]s2.Config

// Storages is a map of logical names to s2.Storage instances.
type Storages map[string]s2.Storage

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
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open config file: %w", err)
	}
	defer f.Close()
	return LoadConfigs(f)
}

// Storages iterates over the parsed configs, invokes s2.NewStorage for each,
// and returns a new Configs map representing the initialized storage environments.
func (c Configs) Storages(ctx context.Context) (Storages, error) {
	storages := make(Storages, len(c))
	for name, cfg := range c {
		strg, err := s2.NewStorage(ctx, cfg)
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
