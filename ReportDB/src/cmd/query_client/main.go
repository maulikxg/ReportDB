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

	//fiveMinutesAgo := uint32(time.Now().Add(-8 * 24 * time.Hour).Unix())

	log.Printf("Current time: %d (%s)", currentTime, time.Unix(int64(currentTime), 0))

	log.Printf("Five minutes ago: %d (%s)", fiveMinutesAgo, time.Unix(int64(fiveMinutesAgo), 0))

	query := models.Query{
		QueryID:        uint64(time.Now().UnixNano()),
		From:           fiveMinutesAgo,
		To:             currentTime,
		ObjectIDs:      []uint32{0, 1, 2, 3},
		CounterId:      1,
		Aggregation:    "", // Request raw data points (no aggregation)
		Interval:       0,
		GroupByObjects: false,
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
	time.Sleep(500 * time.Millisecond) // Add delay between queries

	aggregationQuery := models.Query{

		QueryID: uint64(time.Now().UnixNano()) + 1,

		From: fiveMinutesAgo,

		To: currentTime,

		ObjectIDs: []uint32{0, 1, 2, 3},

		CounterId: 1,

		Aggregation: "avg", // Try average aggregation

		Interval: 0,

		GroupByObjects: false,
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

	// Add a small delay before the next query
	time.Sleep(500 * time.Millisecond)

	histogramQuery := models.Query{

		QueryID: uint64(time.Now().UnixNano()) + 2,

		From: fiveMinutesAgo,

		To: currentTime,

		ObjectIDs: []uint32{0, 1, 2, 3},

		CounterId: 1,

		Aggregation: "histogram",

		Interval: 10, // 10-second buckets

		GroupByObjects: false,
	}

	log.Printf("\nSending HISTOGRAM query: %+v", histogramQuery)

	// **** Start Timing ****
	startTimeHistogramQuery := time.Now()
	// **********************

	histResponse, err := cli.SendQuery(histogramQuery)

	// **** Stop Timing and Log Duration ****
	durationHistogramQuery := time.Since(startTimeHistogramQuery)
	log.Printf("Histogram data query execution time: %v", durationHistogramQuery)
	// **************************************

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

	// Add a small delay before the next query
	time.Sleep(500 * time.Millisecond)

	gaugeQuery := models.Query{

		QueryID:        uint64(time.Now().UnixNano()) + 3,
		From:           fiveMinutesAgo,
		To:             currentTime,
		ObjectIDs:      []uint32{0, 1},
		CounterId:      1,
		Aggregation:    "",
		Interval:       0,
		GroupByObjects: false,
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

	// Add a small delay before the grid query
	time.Sleep(500 * time.Millisecond)

	// Test Grid Query with GroupByObjects
	gridQuery := models.Query{
		QueryID:        uint64(time.Now().UnixNano()) + 4,
		From:           fiveMinutesAgo,
		To:             currentTime,
		ObjectIDs:      []uint32{0, 1, 2, 3},
		CounterId:      1,
		GroupByObjects: true,
		Aggregation:    "",
		//Aggregation:    "avg",
	}

	log.Printf("\nSending GRID query with GroupByObjects: %+v", gridQuery)

	// **** Start Timing ****
	startTimeGridQuery := time.Now()
	// **********************

	gridResponse, err := cli.SendQuery(gridQuery)

	// **** Stop Timing and Log Duration ****
	durationGridQuery := time.Since(startTimeGridQuery)
	log.Printf("Grid query execution time: %v", durationGridQuery)
	// **************************************

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

	// Add a small delay before the ALL DEVICES query
	time.Sleep(500 * time.Millisecond)

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
		Aggregation:    "",
	}

	log.Printf("\nSending ALL DEVICES query for counter %d: %+v", allDevicesQuery.CounterId, allDevicesQuery)
	fmt.Printf("\nQUERY ALL DEVICES - TIME RANGE: %s to %s\n",
		time.Unix(int64(allDevicesQuery.From), 0).Format("15:04:05"),
		time.Unix(int64(allDevicesQuery.To), 0).Format("15:04:05"))

	// **** Start Timing ****
	startTimeAllDevices := time.Now()
	// **********************

	// Use longer timeout (60 seconds) for all-devices query since it can be more intensive
	allDevicesResponse, err := cli.SendQuery(allDevicesQuery)

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

	// Now test with a longer time range to test performance with more data
	oneHourAgo := uint32(time.Now().Add(-1 * time.Hour).Unix())

	allDevicesLongQuery := models.Query{
		QueryID:        uint64(time.Now().UnixNano()) + 6,
		From:           oneHourAgo, // One hour of data
		To:             currentTime,
		ObjectIDs:      []uint32{}, // Empty array means all devices
		CounterId:      1,
		GroupByObjects: true,
		Aggregation:    "", // Try a different aggregation function
		Interval:       0,  // 1-minute buckets for histogram-style aggregation
	}

	log.Printf("\nSending LONG-RANGE ALL DEVICES query: %+v", allDevicesLongQuery)
	fmt.Printf("\nLONG RANGE QUERY - TIME RANGE: %s to %s\n",
		time.Unix(int64(allDevicesLongQuery.From), 0).Format("15:04:05"),
		time.Unix(int64(allDevicesLongQuery.To), 0).Format("15:04:05"))

	// **** Start Timing ****
	startTimeLongRange := time.Now()
	// **********************

	// Use longer timeout (120 seconds) for long range query
	longRangeResponse, err := cli.SendQuery(allDevicesLongQuery)

	if err != nil {
		log.Printf("Error sending LONG-RANGE ALL DEVICES query: %v", err)
	} else {
		// **** Stop Timing and Log Duration ****
		durationLongRange := time.Since(startTimeLongRange)
		log.Printf("LONG-RANGE ALL DEVICES query execution time: %v", durationLongRange)
		// **************************************

		fmt.Println("\nLONG-RANGE ALL DEVICES QUERY RESPONSE:")
		fmt.Println("======================================")
		fmt.Printf("Found data for %d devices\n", len(longRangeResponse.Data))

		// Print summary info
		fmt.Println("\nSUMMARY BY DEVICE:")
		fmt.Println("-----------------")
		for objID, dataPoints := range longRangeResponse.Data {
			fmt.Printf("Device %d: %d data points\n", objID, len(dataPoints))
		}

		// Print only a summary to avoid overwhelming output
		totalDataPoints := 0
		for _, dataPoints := range longRangeResponse.Data {
			totalDataPoints += len(dataPoints)
		}
		fmt.Printf("\nTotal data points across all devices: %d\n", totalDataPoints)
		fmt.Printf("Average data points per device: %.2f\n",
			float64(totalDataPoints)/float64(len(longRangeResponse.Data)))
		fmt.Println("======================================")
	}

	log.Println("Query testing finished. Closing client shortly...")

	time.Sleep(1 * time.Second)
}
