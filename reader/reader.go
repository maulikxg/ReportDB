package reader

import (
	"fmt"
	"log"
	. "packx/models"
	"packx/storageEngine"
	"packx/utils"
	"path/filepath"
	"sync"
	"time"
)

// processQuery handles a single query with parallel processing for multiple objects
func processQuery(query Query) QueryResponse {
	response := QueryResponse{
		QueryID: query.QueryID,
		Data:    make(map[uint32][]DataPoint),
	}

	storage, err := storageEngine.NewStorageEngine()
	if err != nil {
		log.Printf("Failed to create storage engine: %v", err)
		return response
	}

	var wg sync.WaitGroup
	dataMutex := sync.RWMutex{}

	// Process objects in parallel
	for _, objectID := range query.ObjectIDs {
		wg.Add(1)
		go func(objID uint32) {
			defer wg.Done()

			dataPoints, err := processObjectData(storage, objID, query)
			if err != nil {
				log.Printf("Error processing object %d: %v", objID, err)
				return
			}

			// If no aggregation is specified, return all datapoints
			var processedPoints []DataPoint
			if query.Aggregation == "" {
				processedPoints = dataPoints
			} else {
				processedPoints = aggregateData(dataPoints, query)
			}

			if len(processedPoints) > 0 {
				dataMutex.Lock()
				response.Data[objID] = processedPoints
				dataMutex.Unlock()
			}
		}(objectID)
	}

	wg.Wait()
	return response
}

// processObjectData handles data processing for a single object
func processObjectData(storage *storageEngine.StorageEngine, objectID uint32, query Query) ([]DataPoint, error) {
	var allDataPoints []DataPoint
	fromTime := time.Unix(int64(query.From), 0)
	toTime := time.Unix(int64(query.To), 0)

	var dayWg sync.WaitGroup
	var dataMutex sync.RWMutex

	// Process each day in parallel
	for d := fromTime; !d.After(toTime); d = d.AddDate(0, 0, 1) {
		dayWg.Add(1)
		go func(date time.Time) {
			defer dayWg.Done()

			dateStr := date.Format("2006/01/02")
			counterPath := filepath.Join(
				utils.GetStoragePath(),
				dateStr,
				fmt.Sprintf("counter_%d", query.CounterId),
			)

			if err := storage.SetStoragePath(counterPath); err != nil {
				log.Printf("Error setting storage path for date %s: %v", dateStr, err)
				return
			}

			dataPoints, err := readDataForObject(storage, int(objectID), query.From, query.To, query.CounterId)
			if err != nil {
				log.Printf("Error reading data for ObjectID %d on %s: %v", objectID, dateStr, err)
				return
			}

			if len(dataPoints) > 0 {
				dataMutex.Lock()
				allDataPoints = append(allDataPoints, dataPoints...)
				dataMutex.Unlock()
			}
		}(d)
	}

	dayWg.Wait()
	return allDataPoints, nil
}

// aggregateData applies the specified aggregation to the data points
func aggregateData(points []DataPoint, query Query) []DataPoint {
	if len(points) == 0 {
		return nil
	}

	switch query.Aggregation {
	case "histogram":
		interval := query.Interval
		if interval == 0 {
			interval = DefaultHistogramInterval
		}
		return generateHistogram(points, int(interval))
	case "gauge":
		return generateGauge(points, int(query.Interval))
	default:
		result, _ := ResultParser(map[uint32][]DataPoint{0: points}, query)
		if processed, ok := result.([]DataPoint); ok {
			return processed
		}
		return points
	}
}

// readDataForObject reads data for a specific object from storage
func readDataForObject(storage *storageEngine.StorageEngine, objectID int, fromTime uint32, toTime uint32, counterID uint16) ([]DataPoint, error) {
	var dataPoints []DataPoint

	rawDataBlocks, err := storage.Get(objectID)
	if err != nil {
		return nil, fmt.Errorf("failed to get data blocks: %v", err)
	}

	if len(rawDataBlocks) == 0 {
		return dataPoints, nil
	}

	expectedType, err := utils.GetCounterType(counterID)
	if err != nil {
		return nil, fmt.Errorf("failed to get counter type: %v", err)
	}

	var blockWg sync.WaitGroup
	var dataMutex sync.RWMutex

	// Process blocks in parallel
	for _, blockData := range rawDataBlocks {
		if len(blockData) == 0 {
			continue
		}

		blockWg.Add(1)
		go func(data []byte) {
			defer blockWg.Done()

			points, err := deserializeDataBlock(data, fromTime, toTime, expectedType)
			if err != nil {
				log.Printf("Error deserializing block: %v", err)
				return
			}

			if len(points) > 0 {
				dataMutex.Lock()
				dataPoints = append(dataPoints, points...)
				dataMutex.Unlock()
			}
		}(blockData)
	}

	blockWg.Wait()
	return dataPoints, nil
}
