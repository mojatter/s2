package s2env

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mojatter/s2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfigs(t *testing.T) {
	jsonData := `{
		"local-dev": {
			"type": "memfs",
			"root": ""
		},
		"public-assets": {
			"type": "osfs",
			"root": "/tmp/assets",
			"signed_url": "http://localhost:8080/assets"
		}
	}`

	configs, err := LoadConfigs(strings.NewReader(jsonData))
	require.NoError(t, err)

	require.Len(t, configs, 2)
	assert.Equal(t, s2.TypeMemFS, configs["local-dev"].Type)
	assert.Equal(t, s2.TypeOSFS, configs["public-assets"].Type)
	assert.Equal(t, "/tmp/assets", configs["public-assets"].Root)
	assert.Equal(t, "http://localhost:8080/assets", configs["public-assets"].SignedURL)
}

func TestLoadConfigsFileError(t *testing.T) {
	_, err := LoadConfigsFile("non-existent-file.json")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open config file")
}

func TestConfigsStorages(t *testing.T) {
	configs := Configs{
		"local": s2.Config{
			Type: s2.TypeMemFS,
		},
	}

	storages, err := configs.Storages(context.Background())
	require.NoError(t, err)
	require.Len(t, storages, 1)

	strg, ok := storages["local"]
	require.True(t, ok)
	assert.NotNil(t, strg)
	assert.Equal(t, s2.TypeMemFS, strg.Type())
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	configFile := filepath.Join(dir, "configs.json")

	jsonData := `{
		"mem": {
			"type": "memfs"
		}
	}`
	err := os.WriteFile(configFile, []byte(jsonData), 0644)
	require.NoError(t, err)

	storages, err := Load(context.Background(), configFile)
	require.NoError(t, err)

	require.Len(t, storages, 1)
	assert.Equal(t, s2.TypeMemFS, storages["mem"].Type())
}
