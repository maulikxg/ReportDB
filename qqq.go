package v1

import (
	"fmt"
	"log"
	"packx/client"
	"packx/models"
	"time"
)

package main

import (
"encoding/json"
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

	//fiveMinutesAgo := uint32(time.Now().Add(-8 * 24 * time.Hour).Unix())

	log.Printf("Current time: %d (%s)", currentTime, time.Unix(int64(currentTime), 0))

	log.Printf("Five minutes ago: %d (%s)", fiveMinutesAgo, time.Unix(int64(fiveMinutesAgo), 0))

	query := models.Query{
		QueryID:     uint64(time.Now().UnixNano()),
		From:        fiveMinutesAgo,
		To:          currentTime,
		ObjectIDs:   []uint32{0, 1, 2},
		CounterId:   2,
		Aggregation: "", // Request raw data points (no aggregation)
	}

	log.Printf("Sending RAW DATA query: %+v", query)

	fmt.Printf("\nQUERY TIME RANGE: %s to %s\n",
		time.Unix(int64(query.From), 0).Format("15:04:05"),
		time.Unix(int64(query.To), 0).Format("15:04:05"))

	// **** Start Timing ****
	startTimeRawQuery := time.Now()
	// **********************

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

	// **** Stop Timing and Log Duration ****
	durationRawQuery := time.Since(startTimeRawQuery)

	log.Printf("Raw data query execution time: %v", durationRawQuery) // Log the duration
	// **************************************

	//time.Sleep(5 * time.Second)

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

	histogramQuery := models.Query{

		QueryID: uint64(time.Now().UnixNano()) + 2,

		From: fiveMinutesAgo,

		To: currentTime,

		ObjectIDs: []uint32{0, 1},

		CounterId: 1,

		Aggregation: "histogram",

		Interval: 10, // 10-second buckets
	}

	log.Printf("\nSending HISTOGRAM query: %+v", histogramQuery)

	histResponse, err := cli.SendQuery(histogramQuery)

	if err != nil {

		log.Printf("Error sending histogram query: %v", err)

	} else {

		fmt.Println("\nHISTOGRAM RESPONSE:")

		fmt.Println("===================")

		for objID, dataPoints := range histResponse.Data {

			fmt.Printf("\nObject ID: %d (Bucket count: %d)\n", objID, len(dataPoints))

			if len(dataPoints) == 0 {

				fmt.Println("  NO HISTOGRAM DATA FOUND")

				continue

			}

			fmt.Println("\nBucket Start Time      | Unix Time | Count")

			fmt.Println("----------------------|-----------|-------")

			for _, dp := range dataPoints {

				timeStr := time.Unix(int64(dp.Timestamp), 0).Format("2006-01-02 15:04:05")

				var countValue int

				switch v := dp.Value.(type) {

				case int:

					countValue = v

				case float64:

					countValue = int(v)

				default:

					countValue = 0

					fmt.Printf("%-22s | %-9d | %d (original type: %T)\n",
						timeStr, dp.Timestamp, countValue, dp.Value)

					continue

				}

				fmt.Printf("%-22s | %-9d | %d\n", timeStr, dp.Timestamp, countValue)
			}

			fmt.Println("===================")
		}
	}

	gaugeQuery := models.Query{

		QueryID:     uint64(time.Now().UnixNano()) + 3,
		From:        fiveMinutesAgo,
		To:          currentTime,
		ObjectIDs:   []uint32{0, 1},
		CounterId:   1,
		Aggregation: "gauge",
		Interval:    30, // 30-second intervals
	}

	log.Printf("\nSending GAUGE query: %+v", gaugeQuery)

	gaugeResponse, err := cli.SendQuery(gaugeQuery)

	if err != nil {
		log.Printf("Error sending gauge query: %v", err)
	} else {
		fmt.Println("\nGAUGE RESPONSE:")
		fmt.Println("===============")

		for objID, dataPoints := range gaugeResponse.Data {
			fmt.Printf("\nObject ID: %d (Points: %d)\n", objID, len(dataPoints))

			if len(dataPoints) == 0 {
				fmt.Println("  NO GAUGE DATA FOUND")
				continue
			}

			fmt.Println("\nInterval Start Time    | Unix Time | Value              | Value Type")
			fmt.Println("----------------------|-----------|--------------------|-----------")

			for _, dp := range dataPoints {
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

			fmt.Println("===============")
		}
	}

	// Test Grid Query with GroupByObjects
	gridQuery := models.Query{
		QueryID:        uint64(time.Now().UnixNano()) + 4,
		From:           fiveMinutesAgo,
		To:             currentTime,
		ObjectIDs:      []uint32{0, 1, 2},
		CounterId:      1,
		GroupByObjects: true,
		Aggregation:    "AVG",
	}

	log.Printf("\nSending GRID query with GroupByObjects: %+v", gridQuery)

	gridResponse, err := cli.SendQuery(gridQuery)

	if err != nil {
		log.Printf("Error sending grid query: %v", err)
	} else {
		fmt.Println("\nGRID QUERY RESPONSE:")
		fmt.Println("====================")

		for objID, dataPoints := range gridResponse.Data {
			fmt.Printf("\nObject ID: %d (Points: %d)\n", objID, len(dataPoints))

			if len(dataPoints) == 0 {
				fmt.Println("  NO GRID DATA FOUND")
				continue
			}

			for _, dp := range dataPoints {
				timeStr := time.Unix(int64(dp.Timestamp), 0).Format("2006-01-02 15:04:05")
				fmt.Printf("  Timestamp: %s, Value: %v\n", timeStr, dp.Value)
			}
			fmt.Println("--------------------")
		}
	}

	// Test ALL DEVICES query - new feature
	// Use a smaller time range for ALL DEVICES query to reduce processing time
	twoMinutesAgo := uint32(time.Now().Add(-2 * time.Minute).Unix())

	allDevicesQuery := models.Query{
		QueryID:        uint64(time.Now().UnixNano()) + 5,
		From:           twoMinutesAgo, // Use smaller time range to reduce processing time
		To:             currentTime,
		ObjectIDs:      []uint32{}, // Empty array means all devices
		CounterId:      1,
		GroupByObjects: true, // Group results by object ID
		Aggregation:    "avg",
	}

	log.Printf("\nSending ALL DEVICES query for counter %d: %+v", allDevicesQuery.CounterId, allDevicesQuery)
	fmt.Printf("\nQUERY ALL DEVICES - TIME RANGE: %s to %s\n",
		time.Unix(int64(allDevicesQuery.From), 0).Format("15:04:05"),
		time.Unix(int64(allDevicesQuery.To), 0).Format("15:04:05"))

	// **** Start Timing ****
	startTimeAllDevices := time.Now()
	// **********************

	// Use longer timeout (60 seconds) for all-devices query since it can be more intensive
	allDevicesResponse, err := SendQueryWithTimeout(cli, allDevicesQuery, 60*time.Second)

	if err != nil {
		log.Printf("Error sending ALL DEVICES query: %v", err)
	} else {
		// **** Stop Timing and Log Duration ****
		durationAllDevices := time.Since(startTimeAllDevices)
		log.Printf("ALL DEVICES query execution time: %v", durationAllDevices)
		// **************************************

		fmt.Println("\nALL DEVICES QUERY RESPONSE:")
		fmt.Println("===========================")
		fmt.Printf("Found data for %d devices\n", len(allDevicesResponse.Data))

		// Print summary info
		fmt.Println("\nSUMMARY BY DEVICE:")
		fmt.Println("-----------------")
		for objID, dataPoints := range allDevicesResponse.Data {
			fmt.Printf("Device %d: %d data points\n", objID, len(dataPoints))
		}

		// Print detailed info for each device
		fmt.Println("\nDETAILED DEVICE DATA:")
		fmt.Println("--------------------")
		for objID, dataPoints := range allDevicesResponse.Data {
			fmt.Printf("\nDevice ID: %d\n", objID)
			fmt.Printf("Points count: %d\n", len(dataPoints))

			if len(dataPoints) == 0 {
				fmt.Println("  NO DATA POINTS FOUND")
				continue
			}

			// Print first and last data point for each device
			if len(dataPoints) > 0 {
				firstDP := dataPoints[0]
				firstTimeStr := time.Unix(int64(firstDP.Timestamp), 0).Format("2006-01-02 15:04:05")
				fmt.Printf("  First data point: Time=%s, Value=%v\n",
					firstTimeStr, firstDP.Value)

				if len(dataPoints) > 1 {
					lastDP := dataPoints[len(dataPoints)-1]
					lastTimeStr := time.Unix(int64(lastDP.Timestamp), 0).Format("2006-01-02 15:04:05")
					fmt.Printf("  Last data point: Time=%s, Value=%v\n",
						lastTimeStr, lastDP.Value)
				}
			}

			fmt.Println("===========================")
		}
	}

	log.Println("Query testing finished. Closing client shortly...")

	time.Sleep(1 * time.Second)
}

// SendQueryWithTimeout sends a query to the server and waits for a response
// with a custom timeout (instead of the default 30 seconds)
func SendQueryWithTimeout(cli *client.QueryClient, query models.Query, timeout time.Duration) (*models.QueryResponse, error) {
	queryBytes, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %v", err)
	}

	log.Printf("Sending query to server with %s timeout: %+v", timeout, query)

	_, err = cli.GetSendSocket().SendBytes(queryBytes, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to send query: %v", err)
	}

	log.Printf("Query sent successfully (ID: %d)", query.QueryID)

	// Wait for response with the specified timeout
	log.Printf("Waiting for response to query ID: %d (timeout: %s)", query.QueryID, timeout)

	select {
	case response := <-cli.GetResponseChannel():
		if response.QueryID == query.QueryID {
			log.Printf("Received matching response for query ID: %d", query.QueryID)
			return &response, nil
		}
		return nil, fmt.Errorf("received response for different query (expected: %d, got: %d)",
			query.QueryID, response.QueryID)
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout waiting for response to query ID: %d (after %s)",
			query.QueryID, timeout)
	}
}

so now i want the add this  special query case in which if not if not any devices givenin the query just counter and timestamps of from to to given in that case get the data of that time stamps range of all the devices so how to implemt this optimizally