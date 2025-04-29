package handlers

import (
	"github.com/gin-gonic/gin"
	"github.com/your-username/v1/backend/models"
	"net/http"
	"time"
)

// GetDevices returns all monitored devices
func (h *MetricsHandler) GetDevices(c *gin.Context) {
	var devices []models.Device
	if err := h.db.Find(&devices).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch devices"})
		return
	}
	c.JSON(http.StatusOK, devices)
}

// GetDeviceMetrics returns metrics for a specific device within a time range
func (h *MetricsHandler) GetDeviceMetrics(c *gin.Context) {
	deviceID := c.Param("deviceId")
	metricType := c.DefaultQuery("type", "all")
	from := c.DefaultQuery("from", time.Now().Add(-1*time.Hour).Format(time.RFC3339))
	to := c.DefaultQuery("to", time.Now().Format(time.RFC3339))

	fromTime, err := time.Parse(time.RFC3339, from)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'from' timestamp"})
		return
	}

	toTime, err := time.Parse(time.RFC3339, to)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid 'to' timestamp"})
		return
	}

	var device models.Device
	if err := h.db.Where("hostname = ?", deviceID).First(&device).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Device not found"})
		return
	}

	query := h.db.Where("device_id = ? AND created_at BETWEEN ? AND ?", device.ID, fromTime, toTime)

	switch metricType {
	case "cpu":
		var metrics []models.CPUMetric
		if err := query.Find(&metrics).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch CPU metrics"})
			return
		}
		c.JSON(http.StatusOK, metrics)
	case "memory":
		var metrics []models.MemoryMetric
		if err := query.Find(&metrics).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch memory metrics"})
			return
		}
		c.JSON(http.StatusOK, metrics)
	case "disk":
		var metrics []models.DiskMetric
		if err := query.Find(&metrics).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch disk metrics"})
			return
		}
		c.JSON(http.StatusOK, metrics)
	case "network":
		var metrics []models.NetworkMetric
		if err := query.Find(&metrics).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch network metrics"})
			return
		}
		c.JSON(http.StatusOK, metrics)
	case "process":
		var metrics []models.ProcessMetric
		if err := query.Find(&metrics).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch process metrics"})
			return
		}
		c.JSON(http.StatusOK, metrics)
	default:
		var metrics []models.Metric
		if err := query.Find(&metrics).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch metrics"})
			return
		}
		c.JSON(http.StatusOK, metrics)
	}
}

// GetLatestMetrics returns the most recent metrics for a device
func (h *MetricsHandler) GetLatestMetrics(c *gin.Context) {
	deviceID := c.Param("deviceId")

	var device models.Device
	if err := h.db.Where("hostname = ?", deviceID).First(&device).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Device not found"})
		return
	}

	var cpu models.CPUMetric
	var memory models.MemoryMetric
	var disks []models.DiskMetric
	var network []models.NetworkMetric
	var processes []models.ProcessMetric

	h.db.Where("device_id = ?", device.ID).Order("created_at DESC").First(&cpu)
	h.db.Where("device_id = ?", device.ID).Order("created_at DESC").First(&memory)
	h.db.Where("device_id = ?", device.ID).Order("created_at DESC").Limit(10).Find(&disks)
	h.db.Where("device_id = ?", device.ID).Order("created_at DESC").Limit(5).Find(&network)
	h.db.Where("device_id = ?", device.ID).Order("created_at DESC").Limit(10).Find(&processes)

	c.JSON(http.StatusOK, gin.H{
		"device": device,
		"metrics": gin.H{
			"cpu":       cpu,
			"memory":    memory,
			"disk":      disks,
			"network":   network,
			"processes": processes,
		},
	})
} 