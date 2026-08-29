package buckets

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/server"
	"github.com/mojatter/s2/server/middleware"
)

// Breadcrumb represents a single path segment in the console navigation.
type Breadcrumb struct {
	Name   string
	Prefix string
}

// objectsData loads the data needed to render the objects view.
// Callers are responsible for choosing the full-page vs fragment renderer.
func objectsData(ctx context.Context, s *server.Server, bucket, prefix, search string) (map[string]any, error) {
	strg, err := s.Buckets.Get(ctx, bucket)
	if err != nil {
		return nil, err
	}

	var (
		objs     []s2.Object
		prefixes []string
	)
	if search != "" {
		listPrefix := search
		if prefix != "" {
			listPrefix = prefix + "/" + search
		}
		res, err := strg.List(ctx, s2.ListOptions{Prefix: listPrefix, Recursive: true})
		if err != nil {
			return nil, err
		}
		objs = server.FilterKeep(res.Objects)
	} else {
		res, err := strg.List(ctx, s2.ListOptions{Prefix: prefix})
		if err != nil {
			return nil, err
		}
		objs = server.FilterKeep(res.Objects)
		prefixes = res.CommonPrefixes
	}

	var breadcrumbs []Breadcrumb
	parts := strings.Split(strings.Trim(prefix, "/"), "/")
	current := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		if current == "" {
			current = part
		} else {
			current = path.Join(current, part)
		}
		breadcrumbs = append(breadcrumbs, Breadcrumb{
			Name:   part,
			Prefix: current + "/",
		})
	}

	parentPrefix := ""
	if len(breadcrumbs) > 1 {
		parentPrefix = breadcrumbs[len(breadcrumbs)-2].Prefix
	}

	return map[string]any{
		"BucketName":    bucket,
		"Objects":       objs,
		"Prefixes":      prefixes,
		"CurrentPrefix": prefix,
		"ParentPrefix":  parentPrefix,
		"Breadcrumbs":   breadcrumbs,
		"HasParent":     prefix != "" && prefix != "/",
		"Search":        search,
	}, nil
}

// writeObjectsFragment renders the objects.html fragment for htmx partial swaps.
// Used by both handleObjects (on HX-Request) and the mutating POST/DELETE
// handlers to re-render the list after a state change.
func writeObjectsFragment(ctx context.Context, w http.ResponseWriter, s *server.Server, bucket, prefix, search string) {
	data, err := objectsData(ctx, s, bucket, prefix, search)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	var buf bytes.Buffer
	if err := s.Template.ExecuteTemplate(&buf, "console/buckets/objects.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = buf.WriteTo(w)
}

func handleObjects(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	name := r.PathValue("name")
	prefix := strings.TrimRight(r.URL.Query().Get("prefix"), "/")
	search := r.URL.Query().Get("search")

	if r.Header.Get("HX-Request") == "true" {
		writeObjectsFragment(ctx, w, s, name, prefix, search)
		return
	}

	data, err := objectsData(ctx, s, name, prefix, search)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err := s.RenderConsoleIndex(ctx, w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func handleCreateFolder(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	name := r.PathValue("name")
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	prefix := r.FormValue("prefix")
	folderName := r.FormValue("folder_name")
	if folderName == "" {
		http.Error(w, "folder name is required", http.StatusBadRequest)
		return
	}

	key := path.Join(prefix, folderName)
	if !server.DenyUnlessAllowedS3Action(w, server.UserFromContext(ctx), server.ActionPutObject, name, key) {
		return
	}
	if err := s.Buckets.CreateFolder(ctx, name, key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeObjectsFragment(ctx, w, s, name, prefix, "")
}

func handleUploadFile(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	name := r.PathValue("name")
	prefix := r.FormValue("prefix")

	maxSize := s.Config.EffectiveMaxUploadSize()
	r.Body = http.MaxBytesReader(w, r.Body, maxSize)

	file, header, err := r.FormFile("file")
	if err != nil {
		if err.Error() == "http: request body too large" {
			http.Error(w, fmt.Sprintf("File too large (max %d MB)", maxSize/(1<<20)), http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer func() { _ = file.Close() }()

	strg, err := s.Buckets.Get(ctx, name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	key := path.Join(prefix, header.Filename)
	if !server.DenyUnlessAllowedS3Action(w, server.UserFromContext(ctx), server.ActionPutObject, name, key) {
		return
	}
	obj := s2.NewObjectReader(key, file, s2.MustUint64(header.Size))
	if err := strg.Put(ctx, obj); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeObjectsFragment(ctx, w, s, name, prefix, "")
}

func handleDeleteObject(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	name := r.PathValue("name")
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	strg, err := s.Buckets.Get(ctx, name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if strings.HasSuffix(key, "/") {
		user := server.UserFromContext(ctx)
		if user != nil && user.Policy != nil {
			// ConsoleAction defers authorization for a recursive folder
			// delete; check and delete each descendant per page instead of
			// buffering all keys first, to bound memory. Processing stops
			// at the first denied key rather than skipping it and scanning
			// the rest of the prefix to completion -- a caller with no
			// delete permission at all under a huge prefix would otherwise
			// force a full pagination sweep for no benefit. The refreshed
			// fragment below still reflects whatever was deleted before
			// the stop, unlike an old 403 short-circuit that would leave
			// the console showing already-deleted objects (htmx doesn't
			// swap on a 4xx response).
			//
			// TODO: stopping early is silent -- the response is always a
			// plain 200, so the user has no way to tell that the folder
			// was only partially cleared, or why it stopped where it did.
			//
			// .keep markers aren't filtered out here (unlike elsewhere):
			// they're deleted like any other object under the same
			// wildcard grant, so an authorized folder doesn't linger empty.
			after := ""
		pagination:
			for {
				res, err := strg.List(ctx, s2.ListOptions{Prefix: key, Recursive: true, After: after})
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				for _, obj := range res.Objects {
					if !server.AllowedS3Action(user, server.ActionDeleteObject, name, obj.Name()) {
						break pagination
					}
					if err := strg.Delete(ctx, obj.Name()); err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}
				}
				if res.NextAfter == "" {
					break
				}
				after = res.NextAfter
			}
		} else if err := strg.DeleteRecursive(ctx, key); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		if err := strg.Delete(ctx, key); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	prefix := r.URL.Query().Get("prefix")
	writeObjectsFragment(ctx, w, s, name, prefix, "")
}

func init() {
	server.RegisterConsoleHandleFunc("GET /buckets/{name}", middleware.BasicAuth(handleObjects))
	server.RegisterConsoleHandleFunc("POST /buckets/{name}/folders", middleware.BasicAuth(handleCreateFolder))
	server.RegisterConsoleHandleFunc("POST /buckets/{name}/upload", middleware.BasicAuth(handleUploadFile))
	server.RegisterConsoleHandleFunc("DELETE /buckets/{name}/objects", middleware.BasicAuth(handleDeleteObject))
}
