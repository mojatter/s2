package console

import (
	"bytes"
	"net/http"

	"github.com/mojatter/s2/server"
	"github.com/mojatter/s2/server/middleware"
)

// scriptTemplates lists the {{define "scripts:..."}} blocks to render.
// Populated automatically by RegisterTemplateWithScripts.
var scriptTemplates []string

// RegisterTemplateWithScripts registers a template that contains a
// {{define "scripts:<name>"}} block. It registers both the template
// itself (via server.RegisterTemplate) and the scripts block for the
// GET /scripts endpoint.
func RegisterTemplateWithScripts(name string) {
	server.RegisterTemplate(name)
	scriptTemplates = append(scriptTemplates, "scripts:"+name)
}

func handleScripts(s *server.Server, w http.ResponseWriter, r *http.Request) {
	var buf bytes.Buffer
	for _, name := range scriptTemplates {
		if err := s.Template.ExecuteTemplate(&buf, name, nil); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

func init() {
	server.RegisterConsoleHandleFunc("GET /scripts", middleware.BasicAuth(handleScripts))
}
