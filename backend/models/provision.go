package models

import (
	"time"
)

// Provision represents a provisioned device
type Provision struct {
	ObjectID      int       `json:"object_id"`
	IP            string    `json:"ip"`
	CredentialID  int       `json:"credential_id"`
	DiscoveryID   int       `json:"discovery_id"`
	IsProvisioned bool      `json:"is_provisioned"`
	CreatedAt     time.Time `json:"created_at"`
}

// NewProvision creates a new provision record
func NewProvision(ip string, credentialID, discoveryID int) *Provision {
	return &Provision{
		IP:            ip,
		CredentialID:  credentialID,
		DiscoveryID:   discoveryID,
		IsProvisioned: false,
		CreatedAt:     time.Now(),
	}
} 