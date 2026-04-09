package server

import (
	"context"
	"flag"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"
)

const (
	cmd   = "s2-server"
	desc  = "S2 is a simple object storage server."
	usage = cmd + " [flags]"
)

// helpExamples is appended verbatim to the -help output so new users can see
// a few complete command lines instead of having to synthesize one from the
// flag list alone.
const helpExamples = `Examples:
  # Run with defaults: stores data in ./data, listens on :9000.
  s2-server

  # Override the listen address and storage root for a one-off run.
  s2-server -listen :8080 -root /tmp/s2

  # Create initial buckets on startup (both flag and env var work).
  s2-server -buckets assets,uploads
  S2_SERVER_BUCKETS=assets,uploads s2-server

  # Load persistent settings from a config file, then override one field.
  s2-server -f ./s2.json -listen :9001

  # Print the version and exit.
  s2-server -v
`

// version is set at build time via -ldflags.
// Falls back to the module version embedded by go install.
var version = "dev"

func init() {
	if version == "dev" {
		if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
			version = info.Main.Version
		}
	}
}

var (
	handlersMux sync.Mutex
	// handlers holds every route registered via the legacy
	// RegisterHandleFunc API. These still back the single listener
	// returned by Handler().
	handlers = map[string]HandlerFunc{}
	// s3Handlers and consoleHandlers hold routes registered via the
	// RegisterS3HandleFunc and RegisterConsoleHandleFunc APIs respectively.
	// They are served by S3Handler() and ConsoleHandler() so that a future
	// version can bind each set of routes to its own listener. In this
	// release both registries are empty by default, so Handler() and the
	// existing single-listener Start() keep the same behavior.
	s3Handlers      = map[string]HandlerFunc{}
	consoleHandlers = map[string]HandlerFunc{}
)

// Flags holds the parsed command-line arguments for s2-server. Pointer-typed
// fields distinguish "explicitly set" from "left at default"; only explicitly
// set flags override values loaded from file/env.
type Flags struct {
	isVersion  bool
	isHelp     bool
	configFile string
	listen     *string
	root       *string
	buckets    *string
}

func initFlags(args []string) (*Flags, error) {
	var (
		f       Flags
		listen  string
		root    string
		buckets string
	)
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	fs.BoolVar(&f.isVersion, "v", false, "print version")
	fs.BoolVar(&f.isHelp, "h", false, "help for "+cmd)
	fs.StringVar(&f.configFile, "f", os.Getenv(EnvS2ServerConfig), "configuration file (also "+EnvS2ServerConfig+")")
	fs.StringVar(&listen, "listen", "", "listen address, e.g. :9000 (overrides config file and "+EnvS2ServerListen+")")
	fs.StringVar(&root, "root", "", "storage root path (overrides config file and "+EnvS2ServerRoot+")")
	fs.StringVar(&buckets, "buckets", "", "comma-separated list of buckets to create on startup (overrides config file and "+EnvS2ServerBuckets+")")
	if err := fs.Parse(args[1:]); err != nil {
		return nil, fmt.Errorf("failed to parse flags: %w", err)
	}
	// Record only flags that were explicitly set so Run can apply them with
	// the correct precedence (default < file < env < flag).
	fs.Visit(func(fl *flag.Flag) {
		switch fl.Name {
		case "listen":
			f.listen = &listen
		case "root":
			f.root = &root
		case "buckets":
			f.buckets = &buckets
		}
	})
	if f.isVersion {
		fmt.Println(version)
	}
	if f.isHelp {
		fmt.Fprintf(os.Stderr, "%s\n\nUsage:\n  %s\n\n", desc, usage)
		fmt.Fprintln(os.Stderr, "Flags:")
		fs.PrintDefaults()
		fmt.Fprint(os.Stderr, "\n", helpExamples)
	}
	return &f, nil
}

func Run(args []string) error {
	f, err := initFlags(args)
	if err != nil {
		return err
	}
	if f.isVersion || f.isHelp {
		return nil
	}

	cfg := DefaultConfig()
	if f.configFile != "" {
		if err := cfg.LoadFile(f.configFile); err != nil {
			return err
		}
	}
	if err := cfg.LoadEnv(); err != nil {
		return err
	}
	// Flags have the highest precedence: default < file < env < flag.
	if f.listen != nil {
		cfg.Listen = *f.listen
	}
	if f.root != nil {
		cfg.Root = *f.root
	}
	if f.buckets != nil {
		cfg.Buckets = splitBucketList(*f.buckets)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv, err := NewServer(ctx, cfg)
	if err != nil {
		return err
	}
	for _, name := range cfg.Buckets {
		if ok, _ := srv.Buckets.Exists(name); ok {
			continue
		}
		if err := srv.Buckets.Create(ctx, name); err != nil {
			return fmt.Errorf("create initial bucket %q: %w", name, err)
		}
		slog.Info("Created initial bucket", "name", name)
	}

	slog.Info("Listening", "addr", cfg.Listen)
	return srv.Start(ctx)
}

// Server is a web server that provides a Web Console and S3-compatible API for S2.
type Server struct {
	Config    *Config
	Template  *template.Template
	Buckets   *Buckets
	StartedAt time.Time // server start time, used as epoch for upload ID generation
}

// NewServer creates a new server with the specified configuration.
func NewServer(ctx context.Context, cfg *Config) (*Server, error) {
	tmpl, err := loadTemplates(cfg)
	if err != nil {
		return nil, err
	}
	buckets, err := newBuckets(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Server{
		Config:    cfg,
		Template:  tmpl,
		Buckets:   buckets,
		StartedAt: time.Now(),
	}, nil
}

// Handler builds and returns the HTTP handler without starting a listener.
//
// It serves every route from the legacy RegisterHandleFunc registry plus
// every route from both the S3 and Console registries, so that existing
// callers keep working while new code migrates to the split APIs. This is
// also what the single-listener Start() uses.
func (s *Server) Handler() http.Handler {
	return s.buildMux(handlers, s3Handlers, consoleHandlers)
}

// S3Handler builds an HTTP handler that serves only the routes registered
// via RegisterS3HandleFunc. It is exposed so that a future release can
// bind it to a dedicated S3 listener; today it is unused when callers
// still register via the legacy API.
func (s *Server) S3Handler() http.Handler {
	return s.buildMux(s3Handlers)
}

// ConsoleHandler builds an HTTP handler that serves only the routes
// registered via RegisterConsoleHandleFunc. Returns nil when no console
// routes have been registered, which lets the caller decide whether to
// start a second listener at all.
func (s *Server) ConsoleHandler() http.Handler {
	if len(consoleHandlers) == 0 {
		return nil
	}
	return s.buildMux(consoleHandlers)
}

// buildMux composes the given registries into a single ServeMux. A pattern
// registered in more than one registry panics, same as the registration
// APIs themselves.
func (s *Server) buildMux(registries ...map[string]HandlerFunc) http.Handler {
	mux := http.NewServeMux()
	seen := map[string]struct{}{}
	for _, reg := range registries {
		for pattern, handler := range reg {
			if _, dup := seen[pattern]; dup {
				panic("s2: handler already registered for " + pattern)
			}
			seen[pattern] = struct{}{}
			mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
				handler(s, w, r)
			})
		}
	}
	return mux
}

// Start starts the server and shuts it down gracefully when ctx is cancelled.
func (s *Server) Start(ctx context.Context) error {
	srv := &http.Server{
		Addr:              s.Config.Listen,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 30 * time.Second,
	}
	slog.Info("Server listening", "addr", s.Config.Listen)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		slog.Info("Shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
		return nil
	}
}

type HandlerFunc func(srv *Server, w http.ResponseWriter, r *http.Request)

// RegisterHandleFunc registers a handler into the legacy single-listener
// registry. New code should prefer RegisterS3HandleFunc or
// RegisterConsoleHandleFunc so that a future release can bind the S3 API
// and the Web Console to separate listeners; the legacy registry is kept
// so existing callers continue to work without change.
func RegisterHandleFunc(pattern string, handler HandlerFunc) {
	registerInto(handlers, "", pattern, handler)
}

// RegisterS3HandleFunc registers a handler that will be served by
// S3Handler(). Patterns use Go 1.22 ServeMux syntax.
func RegisterS3HandleFunc(pattern string, handler HandlerFunc) {
	registerInto(s3Handlers, "S3 ", pattern, handler)
}

// RegisterConsoleHandleFunc registers a handler that will be served by
// ConsoleHandler(). Patterns use Go 1.22 ServeMux syntax.
func RegisterConsoleHandleFunc(pattern string, handler HandlerFunc) {
	registerInto(consoleHandlers, "console ", pattern, handler)
}

func registerInto(reg map[string]HandlerFunc, label, pattern string, handler HandlerFunc) {
	handlersMux.Lock()
	defer handlersMux.Unlock()

	if _, exists := reg[pattern]; exists {
		panic("s2: " + label + "handler already registered for " + pattern)
	}
	reg[pattern] = handler
}
