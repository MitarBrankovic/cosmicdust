// Command cosmicdust-server is the control plane's service for the logs and lifecycle
// events of edge nodes: it receives the Parquet files nodes send over NATS or
// gRPC, keeps them in an archive split by application, indexes them in
// Elasticsearch, and deletes both after each application's retention. It is
// configured from the environment, as the c12s services are; see
// internal/server/configs.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/c12s/cosmicdust/internal/server/configs"
	"github.com/c12s/cosmicdust/internal/server/startup"
)

func main() {
	config, err := configs.NewFromEnv()
	if err != nil {
		log.Fatalln(err)
	}
	app, err := startup.NewAppWithConfig(config)
	if err != nil {
		log.Fatalln(err)
	}
	if err := app.Start(); err != nil {
		app.Logger().Error("cannot start", "err", err)
		os.Exit(1)
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGTERM, syscall.SIGINT)
	<-shutdown

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	app.GracefulStop(ctx)
}
