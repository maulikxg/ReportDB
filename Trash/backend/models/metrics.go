package models

import (
	"time"
	"gorm.io/gorm"
)

type Device struct {
	gorm.Model
	Hostname    string `gorm:"uniqueIndex"`
	LastSeen    time.Time
	Status      string
	Description string
}

type Metric struct {
	ID        uint      `gorm:"primarykey"`
	CreatedAt time.Time
	DeviceID  uint
	Device    Device `gorm:"foreignKey:DeviceID"`
	Type      string // cpu, memory, disk, network, process
	Value     float64
	Labels    map[string]string `gorm:"type:jsonb"`
	Raw       []byte           `gorm:"type:jsonb"` // Store the complete raw metrics JSON
}

type CPUMetric struct {
	Metric
	Usage     float64
	LoadAvg1  float64
	LoadAvg5  float64
	LoadAvg15 float64
}

type MemoryMetric struct {
	Metric
	Total     uint64
	Used      uint64
	Free      uint64
	UsagePerc float64
}

type DiskMetric struct {
	Metric
	Mount     string
	Total     uint64
	Used      uint64
	Free      uint64
	UsagePerc float64
}

type NetworkMetric struct {
	Metric
	Interface string
	RxBytes   uint64
	TxBytes   uint64
	RxPackets uint64
	TxPackets uint64
}

type ProcessMetric struct {
	Metric
	PID     int
	Name    string
	CPU     float64
	Memory  float64
	Command string
} 