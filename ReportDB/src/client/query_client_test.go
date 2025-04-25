package client

import (
	"packx/models"
	"testing"
	"time"
)

func TestQueryClient(t *testing.T) {
	// Create client
	client, err := NewQueryClient()
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create test queries
	testCases := []struct {
		name        string
		query       models.Query
		shouldError bool
		timeout     time.Duration
	}{
		{
			name: "Basic Query",
			query: models.Query{
				QueryID:     1,
				From:        uint32(time.Now().Add(-24 * time.Hour).Unix()),
				To:          uint32(time.Now().Unix()),
				ObjectIDs:   []uint32{1},
				CounterId:   100,
				Aggregation: "avg",
			},
			shouldError: false,
			timeout:     5 * time.Second,
		},
		{
			name: "Multiple Objects Query",
			query: models.Query{
				QueryID:     2,
				From:        uint32(time.Now().Add(-24 * time.Hour).Unix()),
				To:          uint32(time.Now().Unix()),
				ObjectIDs:   []uint32{1, 2, 3},
				CounterId:   100,
				Aggregation: "sum",
			},
			shouldError: false,
			timeout:     5 * time.Second,
		},
		{
			name: "No Aggregation Query",
			query: models.Query{
				QueryID:   3,
				From:      uint32(time.Now().Add(-1 * time.Hour).Unix()),
				To:        uint32(time.Now().Unix()),
				ObjectIDs: []uint32{1},
				CounterId: 100,
			},
			shouldError: false,
			timeout:     5 * time.Second,
		},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create channel for test completion
			done := make(chan struct{})

			go func() {
				response, err := client.SendQuery(tc.query)
				if tc.shouldError && err == nil {
					t.Error("Expected error but got none")
				} else if !tc.shouldError && err != nil {
					t.Errorf("Unexpected error: %v", err)
				}

				if err == nil {
					// Verify response
					if response.QueryID != tc.query.QueryID {
						t.Errorf("Expected QueryID %d, got %d", tc.query.QueryID, response.QueryID)
					}

					// Verify data for each object ID
					for _, objectID := range tc.query.ObjectIDs {
						if dataPoints, ok := response.Data[objectID]; !ok {
							t.Errorf("No data found for ObjectID %d", objectID)
						} else {
							t.Logf("Received %d data points for ObjectID %d", len(dataPoints), objectID)
						}
					}
				}
				close(done)
			}()

			// Wait for test completion or timeout
			select {
			case <-done:
				// Test completed successfully
			case <-time.After(tc.timeout):
				t.Errorf("Test timed out after %v", tc.timeout)
			}
		})

		// Add delay between tests to avoid overwhelming the server
		time.Sleep(100 * time.Millisecond)
	}
} 