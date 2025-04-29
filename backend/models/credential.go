package models

import (
	"time"
)

// CredentialProfile represents a set of credentials for network devices
type CredentialProfile struct {
	CredentialID int       `json:"credential_id"`
	Username     string    `json:"username"`
	Password     string    `json:"password"`
	Port         int       `json:"port"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// NewCredentialProfile creates a new credential profile with defaults
func NewCredentialProfile(username, password string, port int) *CredentialProfile {

	now := time.Now()

	return &CredentialProfile{

		Username: username,

		Password: password,

		Port: port,

		CreatedAt: now,

		UpdatedAt: now,
	}
}
