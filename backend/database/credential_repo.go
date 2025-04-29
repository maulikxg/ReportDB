package database

import (
	"v1/backend/models"
)

// CredentialRepository handles database operations for credential profiles
type CredentialRepository struct {
	db *Database
}

// NewCredentialRepository creates a new credential repository
func NewCredentialRepository(db *Database) *CredentialRepository {
	return &CredentialRepository{db: db}
}

// Create inserts a new credential profile
func (r *CredentialRepository) Create(c *models.CredentialProfile) (int, error) {

	var id int

	query := `
		INSERT INTO credential_profile (username, password, port, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING credential_id
	`

	err := r.db.QueryRow(query, c.Username, c.Password, c.Port, c.CreatedAt, c.UpdatedAt).Scan(&id)

	return id, err
}

// GetByID retrieves a credential profile by ID
func (r *CredentialRepository) GetByID(id int) (*models.CredentialProfile, error) {

	query := `
		SELECT credential_id, username, password, port, created_at, updated_at
		FROM credential_profile
		WHERE credential_id = $1
	`

	c := &models.CredentialProfile{}

	err := r.db.QueryRow(query, id).Scan(

		&c.CredentialID, &c.Username, &c.Password, &c.Port, &c.CreatedAt, &c.UpdatedAt,
	)

	return c, err
}

// GetAll retrieves all credential profiles
func (r *CredentialRepository) GetAll() ([]*models.CredentialProfile, error) {
	query := `
		SELECT credential_id, username, password, port, created_at, updated_at
		FROM credential_profile
		ORDER BY credential_id
	`
	rows, err := r.db.Query(query)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	credentials := make([]*models.CredentialProfile, 0)

	for rows.Next() {

		c := &models.CredentialProfile{}

		if err := rows.Scan(&c.CredentialID, &c.Username, &c.Password, &c.Port, &c.CreatedAt, &c.UpdatedAt); err != nil {

			return nil, err

		}

		credentials = append(credentials, c)
	}

	return credentials, rows.Err()
}

// Update updates a credential profile
func (r *CredentialRepository) Update(c *models.CredentialProfile) error {

	query := `
		UPDATE credential_profile
		SET username = $1, password = $2, port = $3, updated_at = $4
		WHERE credential_id = $5
	`

	_, err := r.db.Exec(query, c.Username, c.Password, c.Port, c.UpdatedAt, c.CredentialID)
	return err
}

// Delete removes a credential profile
func (r *CredentialRepository) Delete(id int) error {

	query := "DELETE FROM credential_profile WHERE credential_id = $1"

	_, err := r.db.Exec(query, id)

	return err
}
