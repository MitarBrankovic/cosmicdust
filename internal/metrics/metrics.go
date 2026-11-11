// Package metrics serves the agent's self-metrics in the Prometheus text
// format, the format the metrics side of an edge stack (cAdvisor,
// node-exporter) already speaks, so the agent is scraped next to them. Values
// are read from counters the parts keep anyway, at scrape time: between scrapes
// the metrics cost nothing.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewRegistry returns a registry with the Go runtime and process collectors
// and cosmicdust_agent_build_info.
func NewRegistry(version string) *prometheus.Registry {
	r := prometheus.NewRegistry()
	build := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "cosmicdust_agent", Name: "build_info",
		Help:        "Always 1; the labels say which build runs.",
		ConstLabels: prometheus.Labels{"version": version, "goversion": runtime.Version()},
	})
	build.Set(1)
	r.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		build,
	)
	return r
}

// Options configure the HTTP endpoint.
type Options struct {
	Addr     string              // e.g. ":9101"
	Gatherer prometheus.Gatherer // what /metrics serves
	Health   func() error        // /healthz answers 200 when it returns nil
	Pprof    bool                // serve /debug/pprof/ too, for profiling under load
	Logger   *slog.Logger        // defaults to slog.Default()
}

// Handler routes /metrics, /healthz and, if asked, /debug/pprof/.
func Handler(o Options) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(o.Gatherer, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if o.Health != nil {
			if err := o.Health(); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = fmt.Fprintln(w, err)
				return
			}
		}
		_, _ = fmt.Fprintln(w, "ok")
	})
	if o.Pprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	return mux
}

// Server is the HTTP endpoint as an agent component.
type Server struct {
	opts Options
	log  *slog.Logger
}

// NewServer returns the endpoint; it listens only once Run starts.
func NewServer(o Options) *Server {
	log := o.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Server{opts: o, log: log}
}

func (s *Server) Name() string { return "http" }

// Run listens on the address and serves until ctx ends. A listen failure, a
// port already taken, is returned at once, which stops the agent at start
// rather than leaving it running without its metrics.
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.opts.Addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: Handler(s.opts), ReadHeaderTimeout: 5 * time.Second}
	s.log.Info("http listening", "addr", ln.Addr().String(), "pprof", s.opts.Pprof)
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	select {
	case err := <-served:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		return err
	}
	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
