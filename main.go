package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
	"github.com/excalibase/watcher-go/internal/health"
	mysqllistener "github.com/excalibase/watcher-go/internal/mysql"
	natsPublisher "github.com/excalibase/watcher-go/internal/nats"
	pglistener "github.com/excalibase/watcher-go/internal/postgres"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "watcher",
	Short: "excalibase-watcher — CDC agent for PostgreSQL and MySQL",
	Long:  "Streams Change Data Capture events from PostgreSQL WAL and MySQL binlog to NATS JetStream.",
	RunE: func(cmd *cobra.Command, args []string) error {
		return run()
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "config.yaml", "config file path")
}

func run() error {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service := cdc.NewService()
	defer service.Shutdown()

	pub, stopPub, err := startNATS(ctx, cfg, service)
	if err != nil {
		return err
	}
	if stopPub != nil {
		defer stopPub()
	}

	stopPG, err := startPostgres(ctx, cfg, service)
	if err != nil {
		return err
	}
	if stopPG != nil {
		defer stopPG()
	}

	stopMySQL, err := startMySQL(ctx, cfg, service, pub)
	if err != nil {
		return err
	}
	if stopMySQL != nil {
		defer stopMySQL()
	}

	server := startHTTPServer(cfg, service)

	slog.Info("excalibase-watcher started")
	waitForShutdown()

	slog.Info("shutting down...")
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Warn("HTTP server shutdown error", "error", err)
	}

	return nil
}

func startNATS(ctx context.Context, cfg *config.Config, service *cdc.Service) (*natsPublisher.Publisher, func(), error) {
	if !cfg.NATS.Enabled {
		return nil, nil, nil
	}
	pub := natsPublisher.NewPublisher(cfg.NATS, service)
	if err := pub.Start(ctx); err != nil {
		return nil, nil, fmt.Errorf("starting NATS publisher: %w", err)
	}
	slog.Info("NATS JetStream publisher started")
	return pub, pub.Stop, nil
}

func startPostgres(ctx context.Context, cfg *config.Config, service *cdc.Service) (func(), error) {
	if !cfg.Postgres.Enabled {
		return nil, nil
	}
	pgListener, err := pglistener.NewListener(cfg.Postgres, service)
	if err != nil {
		return nil, fmt.Errorf("creating postgres listener: %w", err)
	}
	if err := pgListener.Start(ctx); err != nil {
		return nil, fmt.Errorf("starting postgres listener: %w", err)
	}
	service.MarkRunning()
	slog.Info("PostgreSQL CDC listener started")
	return pgListener.Stop, nil
}

func startMySQL(ctx context.Context, cfg *config.Config, service *cdc.Service, pub *natsPublisher.Publisher) (func(), error) {
	if !cfg.MySQL.Enabled {
		return nil, nil
	}
	mysqlListener, err := mysqllistener.NewListener(cfg.MySQL, service)
	if err != nil {
		return nil, fmt.Errorf("creating mysql listener: %w", err)
	}
	if pub != nil {
		mysqlListener.SetAckedLSNProvider(pub.LastAckedLSN)
	}
	if err := mysqlListener.Start(ctx); err != nil {
		return nil, fmt.Errorf("starting mysql listener: %w", err)
	}
	service.MarkRunning()
	slog.Info("MySQL CDC listener started")
	return mysqlListener.Stop, nil
}

func startHTTPServer(cfg *config.Config, service *cdc.Service) *http.Server {
	checker := health.NewChecker(service.IsRunning, service.TotalSubscriberCount)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", checker.HealthHandler)
	mux.HandleFunc("/readyz", checker.ReadyHandler)
	if cfg.Metrics.Enabled {
		mux.Handle("/metrics", promhttp.Handler())
	}

	server := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Health.BindAddress, cfg.Health.Port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	go func() {
		slog.Info("HTTP server starting", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "error", err)
		}
	}()

	return server
}

func waitForShutdown() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
