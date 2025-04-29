package database

import (
	"database/sql"
	"log"

	_ "github.com/lib/pq"
)

// Database represents a database connection
type Database struct {
	*sql.DB
}

// New creates a new database connection
func New(connectionString string) (*Database, error) {

	db, err := sql.Open("postgres", connectionString)

	if err != nil {
		return nil, err
	}

	// Check if the connection is working
	if err = db.Ping(); err != nil {
		return nil, err
	}

	return &Database{db}, nil
}

// InitializeTables creates all necessary tables if they don't exist
func (db *Database) InitializeTables() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS credential_profile (
			credential_id SERIAL PRIMARY KEY,
			username VARCHAR NOT NULL,
			password VARCHAR NOT NULL,
			port INTEGER NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS discovery_profile (
			discovery_id SERIAL PRIMARY KEY,
			credential_id JSONB NOT NULL,
			ip VARCHAR,
			ip_range VARCHAR,
			discovery_status VARCHAR NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS provision (
			object_id SERIAL PRIMARY KEY,
			ip VARCHAR NOT NULL,
			credential_id INTEGER NOT NULL,
			discovery_id INTEGER NOT NULL,
			is_provisioned BOOLEAN DEFAULT false,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (credential_id) REFERENCES credential_profile(credential_id),
			FOREIGN KEY (discovery_id) REFERENCES discovery_profile(discovery_id)
		)`,
	}

	for _, query := range queries {

		_, err := db.Exec(query)

		if err != nil {

			log.Printf("Error executing query: %s", query)

			return err

		}

	}

	log.Println("All tables initialized successfully")

	return nil
}
