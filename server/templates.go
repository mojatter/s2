package server

import (
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/mojatter/s2"
)

//go:embed templates/*
var templatesFS embed.FS

// loadTemplates parses every file under templates/ into a single
// template set, keyed by its path relative to templates/.
func loadTemplates(cfg *Config) (*template.Template, error) {
	sub, err := fs.Sub(templatesFS, "templates")
	if err != nil {
		return nil, err
	}
	t := template.New("").Funcs(templateFuncs(cfg))
	err = fs.WalkDir(sub, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		b, err := fs.ReadFile(sub, name)
		if err != nil {
			return err
		}
		_, err = t.New(name).Parse(string(b))
		return err
	})
	if err != nil {
		return nil, err
	}
	return t, nil
}

// The ext sets below use []string + slices.Contains. map[string]bool (O(1))
// and sorted []string + slices.BinarySearch (O(log N)) were both considered,
// but with <30 entries per set the lookup cost is negligible and plain
// slices read cleanest.

// imageExts is the set of file extensions recognized as images for gallery view.
var imageExts = []string{
	".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".bmp", ".ico",
}

// videoExts is the set of file extensions recognized as video for preview.
var videoExts = []string{
	".mp4", ".webm", ".ogg",
}

// audioExts is the set of file extensions recognized as audio for preview.
var audioExts = []string{
	".mp3", ".wav", ".aac", ".flac",
}

// textPreviewExts is the set of file extensions rendered as text in the
// preview pane. These are subject to the MaxPreviewSize limit.
var textPreviewExts = []string{
	".txt", ".md", ".json", ".xml", ".csv", ".log",
	".yaml", ".yml", ".toml", ".ini", ".cfg", ".conf",
	".html", ".css", ".js", ".ts",
	".go", ".py", ".rb", ".rs", ".java",
	".c", ".h", ".cpp", ".sh", ".sql",
	".makefile", ".dockerfile",
}

// PreviewType returns the preview category for the given file extension:
// "image", "video", "audio", "pdf", "text", or "" (unsupported).
func PreviewType(ext string) string {
	ext = strings.ToLower(ext)
	switch {
	case slices.Contains(imageExts, ext):
		return "image"
	case slices.Contains(videoExts, ext):
		return "video"
	case slices.Contains(audioExts, ext):
		return "audio"
	case ext == ".pdf":
		return "pdf"
	case slices.Contains(textPreviewExts, ext):
		return "text"
	default:
		return ""
	}
}

// templateFuncs returns the FuncMap exposed to every template loaded
// by loadTemplates.
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
		"trimPrefix": func(prefix, key string) string {
			if prefix == "" {
				return key
			}
			return strings.TrimPrefix(key, prefix+"/")
		},
		"isImage": func(name string) bool {
			return slices.Contains(imageExts, strings.ToLower(path.Ext(name)))
		},
		"isPreviewable": func(name string, size uint64) bool {
			pt := PreviewType(strings.ToLower(path.Ext(name)))
			if pt == "" {
				return false
			}
			if pt == "text" && s2.MustInt64(size) > cfg.MaxPreviewSize {
				return false
			}
			return true
		},
		"dict": func(values ...any) (map[string]any, error) {
			if len(values)%2 != 0 {
				return nil, fmt.Errorf("dict: odd number of args")
			}
			m := make(map[string]any, len(values)/2)
			for i := 0; i < len(values); i += 2 {
				key, ok := values[i].(string)
				if !ok {
					return nil, fmt.Errorf("dict: non-string key at position %d", i)
				}
				m[key] = values[i+1]
			}
			return m, nil
		},
	}
}
