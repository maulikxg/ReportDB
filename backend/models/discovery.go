package models

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"
)

// Status constants for discovery
const (
	StatusPending = "pending"
	StatusRunning = "running"
	StatusSuccess = "success"
	StatusFailed  = "failed"
)

// IntArray is a custom type for integer arrays in Postgres
type IntArray []int

// Value implements the driver.Valuer interface
func (a IntArray) Value() (driver.Value, error) {
	return json.Marshal(a)
}

// Scan implements the sql.Scanner interface
func (a *IntArray) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	
	return json.Unmarshal(b, a)
}

// DiscoveryProfile represents a network discovery task
type DiscoveryProfile struct {
	DiscoveryID     int       `json:"discovery_id"`
	CredentialIDs   IntArray  `json:"credential_id"`
	IP              string    `json:"ip,omitempty"`
	IPRange         string    `json:"ip_range,omitempty"`
	DiscoveryStatus string    `json:"discovery_status"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// NewDiscoveryProfile creates a new discovery profile
func NewDiscoveryProfile(credentialIDs []int, ip, ipRange string) *DiscoveryProfile {
	now := time.Now()
	return &DiscoveryProfile{
		CredentialIDs:   credentialIDs,
		IP:              ip,
		IPRange:         ipRange,
		DiscoveryStatus: StatusPending,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
} 