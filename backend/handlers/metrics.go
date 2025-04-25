package handlers

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"net/http"
	"time"
	"github.com/your-username/v1/backend/models"
)

type MetricsHandler struct {
	db *gorm.DB
}

func NewMetricsHandler(db *gorm.DB) *MetricsHandler {
	return &MetricsHandler{db: db}
}

type MetricsPayload struct {
	Timestamp time.Time         `json:"timestamp"`
	DeviceID  string           `json:"device_id"`
	CPU       CPUMetrics       `json:"cpu"`
	Memory    MemoryMetrics    `json:"memory"`
	Disk      []DiskMetrics    `json:"disk"`
	Network   []NetworkMetrics `json:"network"`
	Processes []ProcessMetrics `json:"processes"`
}

func (h *MetricsHandler) StoreMetrics(c *gin.Context) {
	var payload MetricsPayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Start a transaction
	tx := h.db.Begin()
	if tx.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to start transaction"})
		return
	}

	// Get or create device
	var device models.Device
	if err := tx.Where("hostname = ?", payload.DeviceID).FirstOrCreate(&device, models.Device{
		Hostname: payload.DeviceID,
	}).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to process device"})
		return
	}

	// Update device status
	device.LastSeen = payload.Timestamp
	device.Status = "online"
	if err := tx.Save(&device).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update device"})
		return
	}

	// Store CPU metrics
	cpuMetric := models.CPUMetric{}
	cpuMetric.DeviceID = device.ID
	cpuMetric.CreatedAt = payload.Timestamp
	cpuMetric.Type = "cpu"
	cpuMetric.Usage = payload.CPU.Usage
	cpuMetric.LoadAvg1 = payload.CPU.LoadAvg1
	cpuMetric.LoadAvg5 = payload.CPU.LoadAvg5
	cpuMetric.LoadAvg15 = payload.CPU.LoadAvg15
	
	if err := tx.Create(&cpuMetric).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to store CPU metrics"})
		return
	}

	// Store Memory metrics
	memMetric := models.MemoryMetric{}
	memMetric.DeviceID = device.ID
	memMetric.CreatedAt = payload.Timestamp
	memMetric.Type = "memory"
	memMetric.Total = payload.Memory.Total
	memMetric.Used = payload.Memory.Used
	memMetric.Free = payload.Memory.Free
	memMetric.UsagePerc = payload.Memory.UsagePerc

	if err := tx.Create(&memMetric).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to store Memory metrics"})
		return
	}

	// Store Disk metrics
	for _, disk := range payload.Disk {
		diskMetric := models.DiskMetric{}
		diskMetric.DeviceID = device.ID
		diskMetric.CreatedAt = payload.Timestamp
		diskMetric.Type = "disk"
		diskMetric.Mount = disk.Mount
		diskMetric.Total = disk.Total
		diskMetric.Used = disk.Used
		diskMetric.Free = disk.Free
		diskMetric.UsagePerc = disk.UsagePerc

		if err := tx.Create(&diskMetric).Error; err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to store Disk metrics"})
			return
		}
	}

	// Store Network metrics
	for _, net := range payload.Network {
		netMetric := models.NetworkMetric{}
		netMetric.DeviceID = device.ID
		netMetric.CreatedAt = payload.Timestamp
		netMetric.Type = "network"
		netMetric.Interface = net.Interface
		netMetric.RxBytes = net.RxBytes
		netMetric.TxBytes = net.TxBytes
		netMetric.RxPackets = net.RxPackets
		netMetric.TxPackets = net.TxPackets

		if err := tx.Create(&netMetric).Error; err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to store Network metrics"})
			return
		}
	}

	// Store Process metrics
	for _, proc := range payload.Processes {
		procMetric := models.ProcessMetric{}
		procMetric.DeviceID = device.ID
		procMetric.CreatedAt = payload.Timestamp
		procMetric.Type = "process"
		procMetric.PID = proc.PID
		procMetric.Name = proc.Name
		procMetric.CPU = proc.CPU
		procMetric.Memory = proc.Memory
		procMetric.Command = proc.Command

		if err := tx.Create(&procMetric).Error; err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to store Process metrics"})
			return
		}
	}

	// Store raw metrics
	rawJSON, _ := json.Marshal(payload)
	rawMetric := models.Metric{
		DeviceID:  device.ID,
		CreatedAt: payload.Timestamp,
		Type:      "raw",
		Raw:       rawJSON,
	}

	if err := tx.Create(&rawMetric).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to store raw metrics"})
		return
	}

	// Commit transaction
	if err := tx.Commit().Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to commit transaction"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success"})
} 