package controllers

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"v1/backend/reportdb"
)

type MetricsController struct {
	reportClient *reportdb.Client
}

func NewMetricsController(reportClient *reportdb.Client) *MetricsController {

	return &MetricsController{

		reportClient: reportClient,
	}

}

// GetLatestMetrics retrieves the latest metrics for a device
func (c *MetricsController) GetLatestMetrics(ctx *gin.Context) {

	// Parse object ID
	objectID, err := strconv.ParseUint(ctx.Param("id"), 10, 32)

	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid object ID format"})

		return

	}

	// Get CPU metrics
	cpuMetrics, err := c.reportClient.GetLatestMetrics(uint32(objectID), 1) // 1 = CPU

	if err != nil {

		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get CPU metrics: " + err.Error()})

		return

	}

	// Get memory metrics
	memoryMetrics, err := c.reportClient.GetLatestMetrics(uint32(objectID), 2) // 2 = Memory

	if err != nil {

		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get memory metrics: " + err.Error()})

		return

	}

	// Get disk metrics
	diskMetrics, err := c.reportClient.GetLatestMetrics(uint32(objectID), 3) // 3 = Disk

	if err != nil {

		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get disk metrics: " + err.Error()})

		return

	}

	// Return all metrics
	ctx.JSON(http.StatusOK, gin.H{

		"object_id": objectID,

		"cpu": cpuMetrics,

		"memory": memoryMetrics,

		"disk": diskMetrics,
	})
}

// GetMetricsByCounter retrieves metrics for a specific counter
func (c *MetricsController) GetMetricsByCounter(ctx *gin.Context) {

	// Parse object ID
	objectID, err := strconv.ParseUint(ctx.Param("id"), 10, 32)

	if err != nil {

		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid object ID format"})

		return

	}

	// Parse counter ID
	counterID, err := strconv.ParseUint(ctx.Param("counter_id"), 10, 16)

	if err != nil {

		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid counter ID format"})

		return

	}

	// Get metrics
	metrics, err := c.reportClient.GetLatestMetrics(uint32(objectID), uint16(counterID))

	if err != nil {

		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get metrics: " + err.Error()})

		return

	}

	// Return metrics
	ctx.JSON(http.StatusOK, gin.H{

		"object_id": objectID,

		"counter_id": counterID,

		"metrics": metrics,
	})
}

// GetMetricsRange retrieves metrics for a specific time range
func (c *MetricsController) GetMetricsRange(ctx *gin.Context) {

	// Parse object ID
	objectID, err := strconv.ParseUint(ctx.Param("id"), 10, 32)

	if err != nil {

		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid object ID format"})

		return

	}

	// Parse counter ID
	counterIDStr := ctx.DefaultQuery("counter_id", "1")

	counterID, err := strconv.ParseUint(counterIDStr, 10, 16)

	if err != nil {

		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid counter ID format"})

		return

	}

	// Parse time range
	fromStr := ctx.DefaultQuery("from", "")

	toStr := ctx.DefaultQuery("to", "")

	var from, to time.Time

	if fromStr == "" {

		// Default to 24 hours ago
		from = time.Now().Add(-24 * time.Hour)

	} else {

		fromUnix, err := strconv.ParseInt(fromStr, 10, 64)

		if err != nil {

			ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'from' timestamp"})

			return

		}

		from = time.Unix(fromUnix, 0)
	}

	if toStr == "" {

		// Default to now
		to = time.Now()

	} else {

		toUnix, err := strconv.ParseInt(toStr, 10, 64)

		if err != nil {

			ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'to' timestamp"})

			return
		}

		to = time.Unix(toUnix, 0)

	}

	// Get metrics
	metrics, err := c.reportClient.GetMetricsRange(uint32(objectID), uint16(counterID), from, to)

	if err != nil {

		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get metrics: " + err.Error()})

		return

	}

	// Return metrics
	ctx.JSON(http.StatusOK, gin.H{

		"object_id": objectID,

		"counter_id": counterID,

		"from": from.Unix(),

		"to": to.Unix(),

		"metrics": metrics,
	})
}
