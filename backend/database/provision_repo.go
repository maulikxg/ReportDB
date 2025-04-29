package database

import (
	"v1/backend/models"
)

// ProvisionRepository handles database operations for provisions
type ProvisionRepository struct {
	db *Database
}

// NewProvisionRepository creates a new provision repository
func NewProvisionRepository(db *Database) *ProvisionRepository {
	return &ProvisionRepository{db: db}
}

// Create inserts a new provision record
func (r *ProvisionRepository) Create(p *models.Provision) (int, error) {
	var id int
	query := `
		INSERT INTO provision (ip, credential_id, discovery_id, is_provisioned, created_at)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING object_id
	`
	err := r.db.QueryRow(
		query,
		p.IP,
		p.CredentialID,
		p.DiscoveryID,
		p.IsProvisioned,
		p.CreatedAt,
	).Scan(&id)
	
	return id, err
}

// GetByID retrieves a provision by ID
func (r *ProvisionRepository) GetByID(id int) (*models.Provision, error) {
	query := `
		SELECT object_id, ip, credential_id, discovery_id, is_provisioned, created_at
		FROM provision
		WHERE object_id = $1
	`
	p := &models.Provision{}
	err := r.db.QueryRow(query, id).Scan(
		&p.ObjectID,
		&p.IP,
		&p.CredentialID,
		&p.DiscoveryID,
		&p.IsProvisioned,
		&p.CreatedAt,
	)
	
	return p, err
}

// GetAll retrieves all provisions
func (r *ProvisionRepository) GetAll() ([]*models.Provision, error) {
	query := `
		SELECT object_id, ip, credential_id, discovery_id, is_provisioned, created_at
		FROM provision
		ORDER BY object_id
	`
	rows, err := r.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	provisions := make([]*models.Provision, 0)
	for rows.Next() {
		p := &models.Provision{}
		if err := rows.Scan(
			&p.ObjectID,
			&p.IP,
			&p.CredentialID,
			&p.DiscoveryID,
			&p.IsProvisioned,
			&p.CreatedAt,
		); err != nil {
			return nil, err
		}
		provisions = append(provisions, p)
	}

	return provisions, rows.Err()
}

// GetByDiscoveryID retrieves all provisions for a discovery
func (r *ProvisionRepository) GetByDiscoveryID(discoveryID int) ([]*models.Provision, error) {
	query := `
		SELECT object_id, ip, credential_id, discovery_id, is_provisioned, created_at
		FROM provision
		WHERE discovery_id = $1
		ORDER BY object_id
	`
	rows, err := r.db.Query(query, discoveryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	provisions := make([]*models.Provision, 0)
	for rows.Next() {
		p := &models.Provision{}
		if err := rows.Scan(
			&p.ObjectID,
			&p.IP,
			&p.CredentialID,
			&p.DiscoveryID,
			&p.IsProvisioned,
			&p.CreatedAt,
		); err != nil {
			return nil, err
		}
		provisions = append(provisions, p)
	}

	return provisions, rows.Err()
}

// UpdateProvisionStatus updates the provisioning status
func (r *ProvisionRepository) UpdateProvisionStatus(id int, isProvisioned bool) error {
	query := `
		UPDATE provision
		SET is_provisioned = $1
		WHERE object_id = $2
	`
	_, err := r.db.Exec(query, isProvisioned, id)
	return err
}

// Delete removes a provision
func (r *ProvisionRepository) Delete(id int) error {
	query := "DELETE FROM provision WHERE object_id = $1"
	_, err := r.db.Exec(query, id)
	return err
} 