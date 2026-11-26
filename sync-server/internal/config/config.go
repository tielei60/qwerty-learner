package config

import (
	"os"
	"path/filepath"
	"strconv"
)

// Config holds runtime configuration for the embedded NATS + SQLite sync server.
type Config struct {
	DataDir      string
	SQLitePath   string
	NATSStoreDir string

	NATSHost string
	NATSPort int

	WSHost string
	WSPort int

	// AuthToken secures both NATS TCP and WebSocket connections.
	AuthToken string
}

// Load reads configuration from environment variables with sensible local defaults.
func Load() Config {
	dataDir := getEnv("SYNC_DATA_DIR", "./data")
	return Config{
		DataDir:      dataDir,
		SQLitePath:   getEnv("SYNC_SQLITE_PATH", filepath.Join(dataDir, "shared.db")),
		NATSStoreDir: getEnv("SYNC_NATS_STORE_DIR", filepath.Join(dataDir, "jetstream")),
		NATSHost:     getEnv("SYNC_NATS_HOST", "127.0.0.1"),
		NATSPort:     getEnvInt("SYNC_NATS_PORT", 4222),
		WSHost:       getEnv("SYNC_WS_HOST", "0.0.0.0"),
		WSPort:       getEnvInt("SYNC_WS_PORT", 9322),
		AuthToken:    getEnv("SYNC_AUTH_TOKEN", "qwerty-dev-token"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return parsed
}
