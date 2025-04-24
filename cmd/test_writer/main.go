package main

import (
	"log"
	"packx/client"
	"packx/models"
	"time"
)

func main() {
	log.Println("Starting test writer...")

	// Create writer client
	cli, err := client.NewWriterClient()
	if err != nil {
		log.Fatalf("Failed to create writer client: %v", err)
	}
	defer cli.Close()

	// Generate test data
	now := time.Now()
	metrics := []models.Metric{
		{
			ObjectID:   1,
			CounterId:  1,
			Value:     float64(100),
			Timestamp: uint32(now.Unix()),
		},
		{
			ObjectID:   1,
			CounterId:  1,
			Value:     float64(200),
			Timestamp: uint32(now.Add(-1 * time.Minute).Unix()),
		},
		{
			ObjectID:   2,
			CounterId:  1,
			Value:     float64(300),
			Timestamp: uint32(now.Unix()),
		},
		{
			ObjectID:   2,
			CounterId:  1,
			Value:     float64(400),
			Timestamp: uint32(now.Add(-1 * time.Minute).Unix()),
		},
	}

	// Write test data
	for _, metric := range metrics {
		log.Printf("Writing metric: %+v", metric)
		if err := cli.Write(metric); err != nil {
			log.Printf("Error writing metric: %v", err)
			continue
		}
		log.Printf("Successfully wrote metric")
		time.Sleep(100 * time.Millisecond) // Small delay between writes
	}

	log.Println("Test writer finished")
} 