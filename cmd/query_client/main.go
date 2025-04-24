package main

import (
	"fmt"
	"log"
	"packx/client"
	"packx/models"
	"time"
)

func main() {
	log.Println("Starting query client CLI with DEBUG mode...")

	cli, err := client.NewQueryClient()
	if err != nil {
		log.Fatalf("Failed to create query client: %v", err)
	}
	defer cli.Close()

	// Test different time ranges
	testQueries := []struct {
		name        string
		fromTime    uint32
		toTime      uint32
		objectIDs   []uint32
		counterID   uint16
		aggregation string
	}{
		{
			name:        "Last Written Data Range",
			fromTime:    uint32(time.Now().Add(-5 * time.Minute).Unix()),
			toTime:      uint32(time.Now().Unix()),
			objectIDs:   []uint32{1, 2},
			counterID:   1,
			aggregation: "",
		},
		{
			name:        "Last Written Data Range with Aggregation",
			fromTime:    uint32(time.Now().Add(-5 * time.Minute).Unix()),
			toTime:      uint32(time.Now().Unix()),
			objectIDs:   []uint32{1, 2},
			counterID:   1,
			aggregation: "avg",
		},
	}

	for _, test := range testQueries {
		fmt.Printf("\n\nExecuting Test: %s\n", test.name)
		fmt.Printf("====================================\n")

		query := models.Query{
			QueryID:     uint64(time.Now().UnixNano()),
			From:        test.fromTime,
			To:          test.toTime,
			ObjectIDs:   test.objectIDs,
			CounterId:   test.counterID,
			Aggregation: test.aggregation,
		}

		log.Printf("Sending query: %+v", query)
		fmt.Printf("\nQUERY TIME RANGE: %s to %s\n",
			time.Unix(int64(query.From), 0).Format("2006-01-02 15:04:05"),
			time.Unix(int64(query.To), 0).Format("2006-01-02 15:04:05"))

		response, err := cli.SendQuery(query)
		if err != nil {
			log.Printf("Error sending query or receiving response: %v", err)
			continue
		}

		// Print response summary
		fmt.Printf("\nResponse Summary:\n")
		fmt.Printf("----------------\n")
		fmt.Printf("Query ID: %d\n", response.QueryID)
		fmt.Printf("Total objects in response: %d\n", len(response.Data))

		// Print detailed data points
		fmt.Printf("\nDetailed Data Points:\n")
		fmt.Printf("-------------------\n")
		for objID, dataPoints := range response.Data {
			fmt.Printf("\nObject ID: %d (Total points: %d)\n", objID, len(dataPoints))

			if len(dataPoints) == 0 {
				fmt.Println("  NO DATA POINTS FOUND")
				continue
			}

			// Print first and last few points
			fmt.Println("\nFirst 5 points:")
			printDataPoints(dataPoints[:min(5, len(dataPoints))])

			if len(dataPoints) > 10 {
				fmt.Printf("\n... %d points in between ...\n", len(dataPoints)-10)
			}

			if len(dataPoints) > 5 {
				fmt.Println("\nLast 5 points:")
				printDataPoints(dataPoints[max(0, len(dataPoints)-5):])
			}

			// Print time range statistics
			if len(dataPoints) > 0 {
				firstTime := time.Unix(int64(dataPoints[0].Timestamp), 0)
				lastTime := time.Unix(int64(dataPoints[len(dataPoints)-1].Timestamp), 0)
				duration := lastTime.Sub(firstTime)
				fmt.Printf("\nTime Range Statistics:\n")
				fmt.Printf("  Start: %s\n", firstTime.Format("2006-01-02 15:04:05"))
				fmt.Printf("  End: %s\n", lastTime.Format("2006-01-02 15:04:05"))
				fmt.Printf("  Duration: %s\n", duration)
				fmt.Printf("  Average points per minute: %.2f\n", float64(len(dataPoints))/duration.Minutes())
			}
		}

		// Add a delay between queries
		time.Sleep(2 * time.Second)
	}

	log.Println("\nQuery testing finished.")
}

func printDataPoints(points []models.DataPoint) {
	for _, dp := range points {
		timeStr := time.Unix(int64(dp.Timestamp), 0).Format("2006-01-02 15:04:05")
		switch v := dp.Value.(type) {
		case float64:
			fmt.Printf("  %s | Value: %.6f\n", timeStr, v)
		default:
			fmt.Printf("  %s | Value: %v (Type: %T)\n", timeStr, v, v)
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
