package main

import (
	"log"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"v1/backend/config"
	"v1/backend/database"
	"v1/backend/metrics"
	"v1/backend/poller"
	"v1/backend/reportdb"
	"v1/backend/routes"
	"v1/backend/utils"
)

func main() {

	if err := godotenv.Load(); err != nil {

		log.Println("Warning: No .env file found, using defaults")

	}

	cfg := config.NewConfig()

	// Initialize main database
	db, err := database.New(cfg.GetDBConnectionString())

	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	defer db.Close()

	// Initialize database tables
	if err := db.InitializeTables(); err != nil {
		log.Fatalf("Failed to initialize database tables: %v", err)
	}

	// Initialize ReportDB client

	reportClient, err := reportdb.New()

	if err != nil {

		log.Printf("Warning: Failed to initialize ReportDB client: %v", err)

		log.Println("Continuing without ReportDB client - metrics queries will not work")

		reportClient = &reportdb.Client{}

	} else {

		defer reportClient.Close()

	}

	// Initialize metrics receiver
	metricsReceiver, err := metrics.NewReceiver(cfg.GetMetricsConnectionString())

	if err != nil {

		log.Printf("Warning: Failed to initialize metrics receiver: %v", err)

		log.Println("Continuing without metrics receiver - metrics will not be received")

		metricsReceiver = &metrics.Receiver{}

	} else {

		defer metricsReceiver.Close()

		metricsReceiver.ProcessMetrics(reportClient)
	}

	// Initialize poller configurer
	pollerConfigurer, err := poller.NewConfigurer(cfg.GetPollerConfigConnectionString())

	if err != nil {

		log.Printf("Warning: Failed to initialize poller configurer: %v", err)

		log.Println("Continuing without poller configurer - poller config will not be sent")

		pollerConfigurer = &poller.Configurer{}

	} else {

		defer pollerConfigurer.Close()

	}

	// Initialize provision notifier
	notifier, err := utils.NewProvisionNotifier(cfg.GetZMQConnectionString())

	if err != nil {

		log.Printf("Warning: Failed to initialize ZMQ notifier: %v", err)

		log.Println("Continuing without ZMQ notifier - provisions will not be sent to poller")

		notifier = &utils.ProvisionNotifier{}

	} else {

		defer notifier.Close()

	}

	// Initialize repositories for discovery queue
	credRepo := database.NewCredentialRepository(db)

	discRepo := database.NewDiscoveryRepository(db)

	provRepo := database.NewProvisionRepository(db)

	// Initialize discovery queue
	discoveryQueue := utils.NewDiscoveryQueue(credRepo, discRepo, provRepo, 5)

	defer discoveryQueue.Stop()

	// Initialize router
	router := gin.Default()

	// Configure CORS
	router.Use(cors.New(cors.Config{

		AllowOrigins: []string{"*"},

		AllowMethods: []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},

		AllowHeaders: []string{"Origin", "Content-Type", "Accept", "Authorization"},

		ExposeHeaders: []string{"Content-Length"},

		AllowCredentials: true,

		MaxAge: 12 * time.Hour,
	}))

	// Configure routes
	routes.SetupRoutes(

		router,

		db,

		notifier,

		discoveryQueue,

		reportClient,

		pollerConfigurer,
	)

	// Start the server
	port := cfg.GetServerPort()

	log.Printf("Server starting at :%s", port)

	if err := router.Run(":" + port); err != nil {
		log.Fatal("Server exited with error: ", err)
	}

}
