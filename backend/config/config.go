package config

import (
	"fmt"
	"os"
)

type Config struct {

	// Database configuration
	DBHost string

	DBPort string

	DBUser string

	DBPassword string

	DBName string

	// ZeroMQ configuration for provision notifications
	ZMQHost string

	ZMQPort string

	// ZeroMQ configuration for metrics
	MetricsHost string

	MetricsPort string

	// ZeroMQ configuration for poller
	PollerConfigHost string

	PollerConfigPort string

	// ReportDB configuration
	ReportDBHost string

	ReportDBPort string

	// Server configuration
	ServerPort string
}

// NewConfig creates a new configuration instance
func NewConfig() *Config {
	return &Config{

		// Database configuration
		DBHost: getEnv("DB_HOST", "localhost"),

		DBPort: getEnv("DB_PORT", "5432"),

		DBUser: getEnv("DB_USER", "postgres"),

		DBPassword: getEnv("DB_PASSWORD", "postgres"),

		DBName: getEnv("DB_NAME", "networkdb"),

		// ZeroMQ configuration for provision notifications
		ZMQHost: getEnv("ZMQ_HOST", "127.0.0.1"),

		ZMQPort: getEnv("ZMQ_PORT", "5555"),

		MetricsHost: getEnv("METRICS_HOST", "127.0.0.1"),

		MetricsPort: getEnv("METRICS_PORT", "5556"),

		PollerConfigHost: getEnv("POLLER_CONFIG_HOST", "127.0.0.1"),

		PollerConfigPort: getEnv("POLLER_CONFIG_PORT", "5557"),

		ReportDBHost: getEnv("REPORTDB_HOST", "127.0.0.1"),

		ReportDBPort: getEnv("REPORTDB_PORT", "8008"), // Query port

		ServerPort: getEnv("PORT", "8080"),
	}
}

func (c *Config) GetDBConnectionString() string {

	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		c.DBHost, c.DBPort, c.DBUser, c.DBPassword, c.DBName)

}

func (c *Config) GetZMQConnectionString() string {

	return fmt.Sprintf("tcp://%s:%s", c.ZMQHost, c.ZMQPort)

}

func (c *Config) GetMetricsConnectionString() string {

	return fmt.Sprintf("tcp://%s:%s", c.MetricsHost, c.MetricsPort)

}

func (c *Config) GetPollerConfigConnectionString() string {

	return fmt.Sprintf("tcp://%s:%s", c.PollerConfigHost, c.PollerConfigPort)

}

func (c *Config) GetReportDBConnectionString() string {

	return fmt.Sprintf("tcp://%s:%s", c.ReportDBHost, c.ReportDBPort)

}

func (c *Config) GetServerPort() string {
	return c.ServerPort
}

func getEnv(key, defaultValue string) string {

	if value, exists := os.LookupEnv(key); exists {
		return value
	}

	return defaultValue
}
