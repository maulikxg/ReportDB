package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"log"
	"github.com/your-username/v1/backend/handlers"
	"github.com/your-username/v1/backend/models"
)

type Config struct {
	DBHost     string `mapstructure:"db_host"`
	DBPort     int    `mapstructure:"db_port"`
	DBUser     string `mapstructure:"db_user"`
	DBPassword string `mapstructure:"db_password"`
	DBName     string `mapstructure:"db_name"`
	ServerPort int    `mapstructure:"server_port"`
}

func initConfig() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config")

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("error reading config file: %v", err)
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unable to decode config: %v", err)
	}

	return &cfg, nil
}

func initDB(cfg *Config) (*gorm.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	// Auto-migrate the models
	err = db.AutoMigrate(
		&models.Device{},
		&models.Metric{},
		&models.CPUMetric{},
		&models.MemoryMetric{},
		&models.DiskMetric{},
		&models.NetworkMetric{},
		&models.ProcessMetric{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to migrate database: %v", err)
	}

	return db, nil
}

func setupRouter(db *gorm.DB) *gin.Engine {
	r := gin.Default()
	
	metricsHandler := handlers.NewMetricsHandler(db)

	// API routes
	v1 := r.Group("/api/v1")
	{
		// Metrics endpoints
		v1.POST("/metrics", metricsHandler.StoreMetrics)
		v1.GET("/devices", metricsHandler.GetDevices)
		v1.GET("/devices/:deviceId/metrics", metricsHandler.GetDeviceMetrics)
		v1.GET("/devices/:deviceId/latest", metricsHandler.GetLatestMetrics)
	}

	return r
}

func main() {
	// Load configuration
	cfg, err := initConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize database connection
	db, err := initDB(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	// Setup router
	r := setupRouter(db)

	// Start server
	addr := fmt.Sprintf(":%d", cfg.ServerPort)
	if err := r.Run(addr); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
} 