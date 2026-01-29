package server

import (
	"context"
	"net/http"
	"time"
)

// Config collects HTTP server knobs.
type Config struct {
	Addr          string
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
	ShutdownGrace time.Duration
}

// Server wraps an http.Server with graceful shutdown helpers.
type Server struct {
	httpServer *http.Server
	mux        *http.ServeMux
	cfg        Config
}

// New spins up a server with the provided config.
func New(cfg Config) *Server {
	mux := http.NewServeMux()
	srv := &http.Server{
		Addr:         cfg.Addr,
		Handler:      mux,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	return &Server{
		httpServer: srv,
		mux:        mux,
		cfg:        cfg,
	}
}

// Mux exposes the underlying mux for route registrations.
func (s *Server) Mux() *http.ServeMux {
	return s.mux
}

// Run starts the HTTP server and blocks until the context is done or the server errors.
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)

	go func() {
		errCh <- s.httpServer.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownGrace)
		defer cancel()
		_ = s.httpServer.Shutdown(shutdownCtx)
		return ctx.Err()
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}
