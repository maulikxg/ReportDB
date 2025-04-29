package routes

import (
	"github.com/gin-gonic/gin"
	"v1/backend/controllers"
	"v1/backend/database"
	"v1/backend/poller"
	"v1/backend/reportdb"
	"v1/backend/utils"
)

// SetupRoutes configures all API routes
func SetupRoutes(

	router *gin.Engine,

	db *database.Database,

	notifier *utils.ProvisionNotifier,

	discoveryQueue *utils.DiscoveryQueue,

	reportClient *reportdb.Client,

	pollerConfigurer *poller.Configurer,

) {

	credRepo := database.NewCredentialRepository(db)

	discRepo := database.NewDiscoveryRepository(db)

	provRepo := database.NewProvisionRepository(db)

	credController := controllers.NewCredentialController(credRepo)

	discController := controllers.NewDiscoveryController(discRepo, credRepo, discoveryQueue)

	provController := controllers.NewProvisionController(provRepo, credRepo, discRepo, notifier, pollerConfigurer)

	metricsController := controllers.NewMetricsController(reportClient)

	api := router.Group("/api/v1")

	credentials := api.Group("/credentials")

	{
		credentials.POST("", credController.Create)

		credentials.GET("", credController.GetAll)

		credentials.GET("/:id", credController.GetByID)

		credentials.PUT("/:id", credController.Update)

		credentials.DELETE("/:id", credController.Delete)

	}

	discoveries := api.Group("/discoveries")

	{
		discoveries.POST("", discController.Create)

		discoveries.GET("", discController.GetAll)

		discoveries.GET("/:id", discController.GetByID)

		discoveries.DELETE("/:id", discController.Delete)

	}

	provisions := api.Group("/provisions")

	{

		provisions.POST("", provController.Create)

		provisions.GET("", provController.GetAll)

		provisions.GET("/:id", provController.GetByID)

		provisions.GET("/discovery/:discovery_id", provController.GetByDiscoveryID)

		provisions.PUT("/:id/status", provController.UpdateStatus)

		provisions.DELETE("/:id", provController.Delete)

	}

	metrics := api.Group("/metrics")

	{

		metrics.GET("/device/:id", metricsController.GetLatestMetrics)

		metrics.GET("/device/:id/counter/:counter_id", metricsController.GetMetricsByCounter)

		metrics.GET("/device/:id/range", metricsController.GetMetricsRange)
		
	}
}
