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
	"slices"
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

// registryMux guards access to the package-level registries populated
// via RegisterS3HandleFunc, RegisterConsoleHandleFunc, and
// registerHttpServerFactory.
var registryMux sync.Mutex

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
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
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

// buildMux composes the given registry into an http.ServeMux, then wraps
// it with the provided middlewares in order: the first middleware wraps
// the mux, the second wraps that result, and so on.
func (s *Server) buildMux(registry map[string]HandlerFunc, middlewares ...func(http.Handler) http.Handler) http.Handler {
	mux := http.NewServeMux()
	for pattern, handler := range registry {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			handler(s, w, r)
		})
	}
	var h http.Handler = mux
	for _, mw := range middlewares {
		h = mw(h)
	}
	return h
}

// httpServerFactory builds an *http.Server for s. Returning nil causes
// (*Server).Start to skip this factory entirely (no server is started).
type httpServerFactory func(s *Server) *http.Server

// httpServerFactories holds the factory list consumed by (*Server).Start.
var httpServerFactories []httpServerFactory

// registerHttpServerFactory appends fn to the list of HTTP server
// factories invoked by (*Server).Start.
func registerHttpServerFactory(fn httpServerFactory) {
	registryMux.Lock()
	defer registryMux.Unlock()

	httpServerFactories = append(httpServerFactories, fn)
}

// Start launches each HTTP server produced by the registered factories.
// Factories returning nil are skipped. All running servers are shut
// down gracefully when ctx is cancelled or any listener dies.
func (s *Server) Start(ctx context.Context) error {
	registryMux.Lock()
	factories := slices.Clone(httpServerFactories)
	registryMux.Unlock()

	var httpServers []*http.Server
	for _, fn := range factories {
		if httpServer := fn(s); httpServer != nil {
			httpServers = append(httpServers, httpServer)
		}
	}

	errCh := make(chan error, len(httpServers))
	for _, httpServer := range httpServers {
		go func() {
			errCh <- httpServer.ListenAndServe()
		}()
	}

	shutdownAll := func() error {
		slog.Info("Shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var firstErr error
		for _, httpServer := range httpServers {
			if err := httpServer.Shutdown(shutdownCtx); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("shutdown %s: %w", httpServer.Addr, err)
			}
		}
		return firstErr
	}

	select {
	case err := <-errCh:
		_ = shutdownAll()
		return err
	case <-ctx.Done():
		return shutdownAll()
	}
}

// HandlerFunc is the signature for S3 API and Web Console route
// handlers registered via RegisterS3HandleFunc / RegisterConsoleHandleFunc.
type HandlerFunc func(srv *Server, w http.ResponseWriter, r *http.Request)

// registerHandler adds handler to reg under pattern, panicking if
// pattern is already registered. label is prefixed to the panic
// message to identify which registry ("S3 " or "console ") was hit.
func registerHandler(reg map[string]HandlerFunc, label, pattern string, handler HandlerFunc) {
	registryMux.Lock()
	defer registryMux.Unlock()

	if _, exists := reg[pattern]; exists {
		panic("s2: " + label + "handler already registered for " + pattern)
	}
	reg[pattern] = handler
}
