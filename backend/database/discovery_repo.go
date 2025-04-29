package database

import (
	"encoding/json"
	"v1/backend/models"
)

type DiscoveryRepository struct {
	db *Database
}

func NewDiscoveryRepository(db *Database) *DiscoveryRepository {

	return &DiscoveryRepository{db: db}

}

// Create inserts a new discovery profile
func (r *DiscoveryRepository) Create(d *models.DiscoveryProfile) (int, error) {

	var id int

	credIDs, err := json.Marshal(d.CredentialIDs)

	if err != nil {
		return 0, err
	}

	query := `
		INSERT INTO discovery_profile (credential_id, ip, ip_range, discovery_status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING discovery_id
	`
	err = r.db.QueryRow(
		query,
		credIDs,
		d.IP,
		d.IPRange,
		d.DiscoveryStatus,
		d.CreatedAt,
		d.UpdatedAt,
	).Scan(&id)

	return id, err
}

// GetByID retrieves a discovery profile by ID
func (r *DiscoveryRepository) GetByID(id int) (*models.DiscoveryProfile, error) {
	query := `
		SELECT discovery_id, credential_id, ip, ip_range, discovery_status, created_at, updated_at
		FROM discovery_profile
		WHERE discovery_id = $1
	`
	d := &models.DiscoveryProfile{}

	var credIDsBytes []byte

	err := r.db.QueryRow(query, id).Scan(
		&d.DiscoveryID,
		&credIDsBytes,
		&d.IP,
		&d.IPRange,
		&d.DiscoveryStatus,
		&d.CreatedAt,
		&d.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(credIDsBytes, &d.CredentialIDs); err != nil {
		return nil, err
	}

	return d, nil
}

// GetAll retrieves all discovery profiles
func (r *DiscoveryRepository) GetAll() ([]*models.DiscoveryProfile, error) {
	query := `
		SELECT discovery_id, credential_id, ip, ip_range, discovery_status, created_at, updated_at
		FROM discovery_profile
		ORDER BY discovery_id
	`
	rows, err := r.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	discoveries := make([]*models.DiscoveryProfile, 0)
	for rows.Next() {
		d := &models.DiscoveryProfile{}
		var credIDsBytes []byte

		if err := rows.Scan(
			&d.DiscoveryID,
			&credIDsBytes,
			&d.IP,
			&d.IPRange,
			&d.DiscoveryStatus,
			&d.CreatedAt,
			&d.UpdatedAt,
		); err != nil {
			return nil, err
		}

		if err := json.Unmarshal(credIDsBytes, &d.CredentialIDs); err != nil {
			return nil, err
		}

		discoveries = append(discoveries, d)
	}

	return discoveries, rows.Err()
}

// UpdateStatus updates the status of a discovery profile
func (r *DiscoveryRepository) UpdateStatus(id int, status string) error {
	query := `
		UPDATE discovery_profile
		SET discovery_status = $1, updated_at = CURRENT_TIMESTAMP
		WHERE discovery_id = $2
	`
	_, err := r.db.Exec(query, status, id)
	return err
}

// Delete removes a discovery profile
func (r *DiscoveryRepository) Delete(id int) error {
	query := "DELETE FROM discovery_profile WHERE discovery_id = $1"

	_, err := r.db.Exec(query, id)

	return err
}
