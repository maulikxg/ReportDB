package main

import (
	"fmt"
	"log"
	"packx/client"
	"packx/models"
	"time"
)

func main() {
	log.Println("Starting query client....")

	cli, err := client.NewQueryClient()

	if err != nil {
		log.Fatalf("Failed to create query client: %v", err)
	}

	defer cli.Close()

	// GET CURRENT SERVER TIME
	currentTime := uint32(time.Now().Unix())

	fiveMinutesAgo := uint32(time.Now().Add(-5 * time.Minute).Unix())

	log.Printf("Current time: %d (%s)", currentTime, time.Unix(int64(currentTime), 0))

	log.Printf("Five minutes ago: %d (%s)", fiveMinutesAgo, time.Unix(int64(fiveMinutesAgo), 0))

	query := models.Query{
		QueryID:     uint64(time.Now().UnixNano()),
		From:        fiveMinutesAgo,
		To:          currentTime,
		ObjectIDs:   []uint32{0, 1},
		CounterId:   1,
		Aggregation: "", // Request raw data points (no aggregation)
	}

	log.Printf("Sending RAW DATA query: %+v", query)

	fmt.Printf("\nQUERY TIME RANGE: %s to %s\n",
		time.Unix(int64(query.From), 0).Format("15:04:05"),
		time.Unix(int64(query.To), 0).Format("15:04:05"))

	response, err := cli.SendQuery(query)

	if err != nil {

		log.Printf("Error sending query or receiving response: %v", err)

		time.Sleep(2 * time.Second)

		return

	}

	// Detailed response inspection
	log.Printf("Successfully received response:")

	log.Printf("  Query ID: %d", response.QueryID)

	log.Printf("  Total objects in response: %d", len(response.Data))

	fmt.Println("\nDETAILED RESPONSE DATA:")

	fmt.Println("=======================")

	for objID, dataPoints := range response.Data {

		fmt.Printf("\nObject ID: %d\n", objID)

		fmt.Printf("Points count: %d\n", len(dataPoints))

		if len(dataPoints) == 0 {

			fmt.Println("  NO DATA POINTS FOUND")

			continue

		}

		fmt.Println("\nTimestamp              | Unix Time | Value              | Value Type")

		fmt.Println("----------------------|-----------|--------------------|-----------")

		for _, dp := range dataPoints {

			// Convert timestamp to readable format
			timeStr := time.Unix(int64(dp.Timestamp), 0).Format("2006-01-02 15:04:05")

			var valueType string

			var valueStr string

			switch v := dp.Value.(type) {

			case float64:

				valueType = "float64"

				valueStr = fmt.Sprintf("%.6f", v)

			case float32:

				valueType = "float32"

				valueStr = fmt.Sprintf("%.6f", v)

			case int:

				valueType = "int"

				valueStr = fmt.Sprintf("%d", v)

			case int64:

				valueType = "int64"

				valueStr = fmt.Sprintf("%d", v)

			case string:

				valueType = "string"

				valueStr = v

			default:

				valueType = fmt.Sprintf("%T", v)

				valueStr = fmt.Sprintf("%v", v)

			}

			fmt.Printf("%-22s | %-9d | %-18s | %s\n",
				timeStr, dp.Timestamp, valueStr, valueType)

		}
	}

	// Request the same data with aggregation to compare

	aggregationQuery := models.Query{

		QueryID: uint64(time.Now().UnixNano()) + 1,

		From: fiveMinutesAgo,

		To: currentTime,

		ObjectIDs: []uint32{0, 1},

		CounterId: 1,

		Aggregation: "avg", // Try average aggregation

	}

	log.Printf("\nSending AGGREGATION query: %+v", aggregationQuery)

	aggResponse, err := cli.SendQuery(aggregationQuery)

	if err != nil {

		log.Printf("Error sending aggregation query: %v", err)

	} else {

		fmt.Println("\nAGGREGATION RESPONSE:")

		fmt.Println("====================")

		for objID, dataPoints := range aggResponse.Data {

			fmt.Printf("\nObject ID: %d (Points: %d)\n", objID, len(dataPoints))

			for _, dp := range dataPoints {

				timeStr := time.Unix(int64(dp.Timestamp), 0).Format("2006-01-02 15:04:05")

				switch v := dp.Value.(type) {

				case float64:
					fmt.Printf("  Avg value: %.6f (Time: %s)\n", v, timeStr)

				default:
					fmt.Printf("  Value: %v (Type: %T, Time: %s)\n", v, v, timeStr)

				}

			}

			fmt.Println("====================")

		}

	}

	log.Println("Query testing finished. Closing client shortly...")

	time.Sleep(1 * time.Second)
}
