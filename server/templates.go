package server

import (
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mojatter/s2"
)

//go:embed templates/*
var templatesFS embed.FS

var (
	templatesMux  sync.Mutex
	templateNames = []string{
		"index.html",
		"buckets/objects.html",
	}
)

func RegisterTemplate(name string) {
	templatesMux.Lock()
	defer templatesMux.Unlock()

	if !slices.Contains(templateNames, name) {
		templateNames = append(templateNames, name)
	}
}

func loadTemplates(cfg *Config) (*template.Template, error) {
	sub, err := fs.Sub(templatesFS, "templates")
	if err != nil {
		return nil, err
	}
	t := template.New("").Funcs(templateFuncs(cfg))
	for _, name := range templateNames {
		if _, err := subTemplate(sub, t, name); err != nil {
			return nil, err
		}
	}
	return t, nil
}

func subTemplate(sub fs.FS, t *template.Template, name string) (*template.Template, error) {
	b, err := fs.ReadFile(sub, name)
	if err != nil {
		return nil, err
	}
	return t.New(name).Parse(string(b))
}

// previewableExts is the set of file extensions that can be previewed in the Web Console.
var previewableExts = map[string]bool{
	// Images
	".png": true, ".jpg": true, ".jpeg": true, ".gif": true, ".webp": true, ".svg": true, ".bmp": true, ".ico": true,
	// Video
	".mp4": true, ".webm": true, ".ogg": true,
	// Audio
	".mp3": true, ".wav": true, ".aac": true, ".flac": true,
	// PDF
	".pdf": true,
	// Text / Code
	".txt": true, ".md": true, ".json": true, ".xml": true, ".csv": true, ".log": true,
	".yaml": true, ".yml": true, ".toml": true, ".ini": true, ".cfg": true, ".conf": true,
	".html": true, ".css": true, ".js": true, ".ts": true,
	".go": true, ".py": true, ".rb": true, ".rs": true, ".java": true,
	".c": true, ".h": true, ".cpp": true, ".sh": true, ".sql": true,
	".makefile": true, ".dockerfile": true,
}

// textPreviewExts is the subset of previewableExts that are rendered as text.
// These are subject to the MaxPreviewSize limit.
var textPreviewExts = map[string]bool{
	".txt": true, ".md": true, ".json": true, ".xml": true, ".csv": true, ".log": true,
	".yaml": true, ".yml": true, ".toml": true, ".ini": true, ".cfg": true, ".conf": true,
	".html": true, ".css": true, ".js": true, ".ts": true,
	".go": true, ".py": true, ".rb": true, ".rs": true, ".java": true,
	".c": true, ".h": true, ".cpp": true, ".sh": true, ".sql": true,
	".makefile": true, ".dockerfile": true,
}

func templateFuncs(cfg *Config) template.FuncMap {
	return template.FuncMap{
		"formatSize": func(size uint64) string {
			const unit = 1024
			if size < unit {
				return fmt.Sprintf("%d B", size)
			}
			div, exp := uint64(unit), 0
			for n := size / unit; n >= unit; n /= unit {
				div *= unit
				exp++
			}
			return fmt.Sprintf("%.1f %cB", float64(size)/float64(div), "KMGTPE"[exp])
		},
		"formatTime": func(t time.Time) string {
			return t.Format("2006-01-02 15:04")
		},
		"baseName": path.Base,
		"isPreviewable": func(name string, size uint64) bool {
			ext := strings.ToLower(path.Ext(name))
			if !previewableExts[ext] {
				return false
			}
			if textPreviewExts[ext] && s2.MustInt64(size) > cfg.MaxPreviewSize {
				return false
			}
			return true
		},
	}
}
