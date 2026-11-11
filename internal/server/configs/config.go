// Package configs reads the service's configuration from the environment,
// as the c12s services do, and the retention periods from a file.
package configs

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config is the service's configuration.
type Config struct {
	NatsAddress   string // NATS_ADDRESS: "nats:4222" or a nats:// URL; empty: no NATS
	SubjectPrefix string // SUBJECT_PREFIX: the nodes' subject prefix, default "cosmicdust"
	GrpcAddress   string // GRPC_ADDRESS: listen address of the gRPC server, default ":50051"; empty: none
	HttpAddress   string // HTTP_ADDRESS: /metrics and /healthz, default ":8080"; empty: none
	DataDir       string // DATA_DIR: inbox, archive and state, default "data"

	EsAddress     string // ES_ADDRESS: Elasticsearch, e.g. http://elasticsearch:9200; empty: archive only
	EsUsername    string // ES_USERNAME, ES_PASSWORD: basic authentication
	EsPassword    string
	EsAPIKey      string // ES_API_KEY: an API key instead
	EsIndexPrefix string // ES_INDEX_PREFIX: data streams are <prefix>-logs-<app>, ...; default "cosmicdust"
	EsReplicas    int    // ES_REPLICAS: replicas of the data streams' indices; -1 (default): the cluster's

	RetentionFile string        // RETENTION_FILE: retention periods per application; empty: the defaults
	PartialIdle   time.Duration // PARTIAL_IDLE: drop an upload nobody continued for this long, default 10m
	CleanupEvery  time.Duration // CLEANUP_EVERY: how often the archive is swept, default 1h
	DedupWindow   time.Duration // DEDUP_WINDOW: how long a received file is remembered, to recognise it sent again; default 7d
	CompactAfter  time.Duration // COMPACT_AFTER: merge an application's hour of the archive once it ended this long ago; default 2h
	CompactEvery  time.Duration // COMPACT_EVERY: how often the archive is looked at for hours to merge; default 10m, 0: never
	LogLevel      string        // LOG_LEVEL: debug, info, warn or error; default info

	Retention *Retention
}

// NewFromEnv reads the configuration from the environment.
func NewFromEnv() (*Config, error) {
	c := &Config{
		NatsAddress:   os.Getenv("NATS_ADDRESS"),
		SubjectPrefix: env("SUBJECT_PREFIX", "cosmicdust"),
		GrpcAddress:   env("GRPC_ADDRESS", ":50051"),
		HttpAddress:   env("HTTP_ADDRESS", ":8080"),
		DataDir:       env("DATA_DIR", "data"),
		EsAddress:     os.Getenv("ES_ADDRESS"),
		EsUsername:    os.Getenv("ES_USERNAME"),
		EsPassword:    os.Getenv("ES_PASSWORD"),
		EsAPIKey:      os.Getenv("ES_API_KEY"),
		EsIndexPrefix: env("ES_INDEX_PREFIX", "cosmicdust"),
		RetentionFile: os.Getenv("RETENTION_FILE"),
		LogLevel:      env("LOG_LEVEL", "info"),
	}
	var errs []error
	var err error
	if c.EsReplicas, err = strconv.Atoi(env("ES_REPLICAS", "-1")); err != nil {
		errs = append(errs, fmt.Errorf("ES_REPLICAS: %w", err))
	}
	if c.PartialIdle, err = ParseDuration(env("PARTIAL_IDLE", "10m")); err != nil {
		errs = append(errs, fmt.Errorf("PARTIAL_IDLE: %w", err))
	}
	if c.CompactAfter, err = ParseDuration(env("COMPACT_AFTER", "2h")); err != nil {
		errs = append(errs, fmt.Errorf("COMPACT_AFTER: %w", err))
	}
	if c.CompactEvery, err = ParseDuration(env("COMPACT_EVERY", "10m")); err != nil {
		errs = append(errs, fmt.Errorf("COMPACT_EVERY: %w", err))
	}
	if c.DedupWindow, err = ParseDuration(env("DEDUP_WINDOW", "7d")); err != nil {
		errs = append(errs, fmt.Errorf("DEDUP_WINDOW: %w", err))
	}
	if c.CleanupEvery, err = ParseDuration(env("CLEANUP_EVERY", "1h")); err != nil {
		errs = append(errs, fmt.Errorf("CLEANUP_EVERY: %w", err))
	}
	c.Retention = DefaultRetention()
	if c.RetentionFile != "" {
		if c.Retention, err = LoadRetention(c.RetentionFile); err != nil {
			errs = append(errs, err)
		}
	}
	if c.NatsAddress == "" && c.GrpcAddress == "" {
		errs = append(errs, errors.New("neither NATS_ADDRESS nor GRPC_ADDRESS: nothing to receive on"))
	}
	return c, errors.Join(errs...)
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}
