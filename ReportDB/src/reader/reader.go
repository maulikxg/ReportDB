package reader

import (
	"fmt"
	"log"
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

		// Process each ObjectID in the query
		for _, objectID := range query.ObjectIDs {

			log.Printf("Processing ObjectID: %d", objectID)

			// Calculate time range for the query
			fromTime := time.Unix(int64(query.From), 0)

			toTime := time.Unix(int64(query.To), 0)

			var allDataPoints []models.DataPoint

			// Iterate through each day in the time range
			for d := fromTime; !d.After(toTime); d = d.AddDate(0, 0, 1) {

				dateStr := d.Format("2006/01/02")

				counterPath := filepath.Join(
					utils.GetStoragePath(),
					dateStr,
					fmt.Sprintf("counter_%d", query.CounterId),
				)

				// Set storage path for this read operation
				if err := storage.SetStoragePath(counterPath); err != nil {

					log.Printf("Error setting storage path for date %s: %v", dateStr, err)

					continue

				}

				// Process data for this object on this day
				dataPoints, err := readDataForObject(storage, int(objectID), query.From, query.To, query.CounterId)

				if err != nil {

					log.Printf("Error reading data for ObjectID %d on %s: %v", objectID, dateStr, err)

					continue

				}

				allDataPoints = append(allDataPoints, dataPoints...)

				log.Printf("Found %d data points for ObjectID %d on %s", len(dataPoints), objectID, dateStr)

			}

			log.Printf("Found total %d data points for ObjectID %d", len(allDataPoints), objectID)

			// Apply aggregation if specified
			if query.Aggregation == "histogram" {

				// Generate histogram with 10-second buckets
				histogramPoints := generateHistogram(allDataPoints, 10) // 10-second buckets

				log.Printf("Generated histogram with %d buckets for ObjectID %d", len(histogramPoints), objectID)

				response.Data[objectID] = histogramPoints

			} else if query.Aggregation != "" && len(allDataPoints) > 0 {

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

// reads data for a object ID from storage ;)
func readDataForObject(storage *storageEngine.StorageEngine, objectID int, fromTime uint32, toTime uint32, counterID uint16) ([]models.DataPoint, error) {

	var dataPoints []models.DataPoint

	rawDataBlocks, err := storage.Get(objectID)

	if err != nil {

		return nil, fmt.Errorf("failed to get data blocks: %v", err)

	}

	if len(rawDataBlocks) == 0 {

		return dataPoints, nil // No data for this object ID

	}

	expectedType, err := utils.GetCounterType(counterID)

	if err != nil {

		return nil, fmt.Errorf("failed to get counter type: %v", err)

	}

	for _, blockData := range rawDataBlocks {

		// Process this block only if it contains data

		if len(blockData) > 0 {

			// Deserialize data points from this block
			points, err := deserializeDataBlock(blockData, fromTime, toTime, expectedType)

			if err != nil {

				log.Printf("Error deserializing block for ObjectID %d: %v", objectID, err)

				continue

			}

			dataPoints = append(dataPoints, points...)
		}
	}

	return dataPoints, nil
}
