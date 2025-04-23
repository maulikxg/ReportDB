package reader

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"packx/models"
	"packx/storageEngine"
	"packx/utils"
	"path/filepath"
	"sync"
	"time"
)

func Reader(queryReceiveCh <-chan models.Query, queryResultCh chan<- models.QueryResponse, shutDownWg *sync.WaitGroup) {

	defer shutDownWg.Done()

	storage, err := storageEngine.NewStorageEngine()

	if err != nil {

		log.Printf("Failed to create storage engine: %v", err)

		return
	}

	for query := range queryReceiveCh {

		log.Printf("Reader processing query: %+v", query)

		// Initialize response
		response := models.QueryResponse{

			QueryID: query.QueryID,

			Data: make(map[uint32][]models.DataPoint),
		}

		// For each object ID in the query
		for _, objectID := range query.ObjectIDs {

			log.Printf("Processing ObjectID: %d", objectID)

			// Calculate the storage path based on the time range
			fromTime := time.Unix(int64(query.From), 0)

			toTime := time.Unix(int64(query.To), 0)

			var allDataPoints []models.DataPoint

			// Iterate through each day in the range
			for d := fromTime; !d.After(toTime); d = d.AddDate(0, 0, 1) {

				dateStr := d.Format("2006/01/02")

				counterPath := filepath.Join(

					utils.GetStoragePath(),

					dateStr,

					fmt.Sprintf("counter_%d", query.CounterId),
				)

				log.Printf("Checking path: %s", counterPath)

				// Set storage path for this read
				if err := storage.SetStoragePath(counterPath); err != nil {

					log.Printf("Error setting storage path for ObjectId %d: %v", objectID, err)

					continue

				}

				// Read data from storage
				rawData, err := storage.Get(int(objectID))

				if err != nil {

					log.Printf("Error reading data for ObjectId %d: %v", objectID, err)

					continue
				}

				log.Printf("Found %d data blocks for ObjectID %d", len(rawData), objectID)

				// Process each data block
				for _, block := range rawData {

					if len(block) < 12 { // 4 bytes timestamp + 8 bytes value

						log.Printf("Skipping short block, length: %d", len(block))

						continue
					}

					// Extract timestamp and value
					timestamp := binary.LittleEndian.Uint32(block[:4])

					valueBytes := block[4:12] // 8 bytes for float64

					value := math.Float64frombits(binary.LittleEndian.Uint64(valueBytes))

					// Only include data points within the time range
					if timestamp >= query.From && timestamp <= query.To {

						dataPoint := models.DataPoint{

							Timestamp: timestamp,

							Value: value,
						}

						log.Printf("Adding data point: timestamp=%d, value=%v", timestamp, value)

						allDataPoints = append(allDataPoints, dataPoint)

					}
				}
			}

			log.Printf("Found total %d data points for ObjectID %d", len(allDataPoints), objectID)

			// Apply aggregation if specified
			if query.Aggregation != "" && len(allDataPoints) > 0 {

				aggregatedPoints := aggregateDataPoints(allDataPoints, query.Aggregation)

				log.Printf("Aggregated %d points to %d points using %s", len(allDataPoints), len(aggregatedPoints), query.Aggregation)

				response.Data[objectID] = aggregatedPoints

			} else {

				response.Data[objectID] = allDataPoints

			}
		}

		// Send response
		log.Printf("Sending response for QueryID %d with %d objects", query.QueryID, len(response.Data))

		queryResultCh <- response

	}
}

func aggregateDataPoints(points []models.DataPoint, aggregation string) []models.DataPoint {

	if len(points) == 0 {

		return points

	}

	// Use the latest timestamp for the aggregated result
	timestamp := points[len(points)-1].Timestamp

	switch aggregation {

	case "avg":

		sum := 0.0

		count := 0

		for _, p := range points {

			if val, ok := p.Value.(float64); ok {

				sum += val

				count++
			}
		}

		if count > 0 {

			return []models.DataPoint{{

				Timestamp: timestamp,

				Value: sum / float64(count),
			}}
		}

	case "sum":

		sum := 0.0

		for _, p := range points {

			if val, ok := p.Value.(float64); ok {

				sum += val

			}
		}

		return []models.DataPoint{{

			Timestamp: timestamp,

			Value: sum,
		}}

	case "max":

		max := math.Inf(-1)

		for _, p := range points {

			if val, ok := p.Value.(float64); ok {

				max = math.Max(max, val)

			}
		}
		return []models.DataPoint{{

			Timestamp: timestamp,

			Value: max,
		}}

	case "min":

		min := math.Inf(1)

		for _, p := range points {

			if val, ok := p.Value.(float64); ok {

				min = math.Min(min, val)

			}
		}
		return []models.DataPoint{{

			Timestamp: timestamp,

			Value: min}}
	}

	return points
}
