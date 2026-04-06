package server

import (
	"context"
	"flag"
	"fmt"
	"html/template"
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
	handlers    = map[string]HandlerFunc{}
)

type Flags struct {
	isVersion  bool
	isHelp     bool
	configFile string
}

func initFlags(args []string) (*Flags, error) {
	var f Flags
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	fs.BoolVar(&f.isVersion, "v", false, "print version")
	fs.BoolVar(&f.isHelp, "h", false, "help for "+cmd)
	fs.StringVar(&f.configFile, "f", os.Getenv(EnvS2ServerConfig), "configuration file")
	if err := fs.Parse(args[1:]); err != nil {
		return nil, fmt.Errorf("failed to parse flags: %w", err)
	}
	if f.isVersion {
		fmt.Println(version)
	}
	if f.isHelp {
		fmt.Fprintf(os.Stderr, "%s\n\nUsage:\n  %s\n\n", desc, usage)
		fmt.Fprintln(os.Stderr, "Flags:")
		fs.PrintDefaults()
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv, err := NewServer(ctx, cfg)
	if err != nil {
		return err
	}
	fmt.Printf("Listening on %s\n", cfg.Listen)
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
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	for pattern, handler := range handlers {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			handler(s, w, r)
		})
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
	fmt.Printf("Server listening on %s\n", s.Config.Listen)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		fmt.Println("Shutting down...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
		return nil
	}
}

type HandlerFunc func(srv *Server, w http.ResponseWriter, r *http.Request)

func RegisterHandleFunc(pattern string, handler HandlerFunc) {
	handlersMux.Lock()
	defer handlersMux.Unlock()

	if _, exists := handlers[pattern]; exists {
		panic("s2: handler already registered for " + pattern)
	}
	handlers[pattern] = handler
}
