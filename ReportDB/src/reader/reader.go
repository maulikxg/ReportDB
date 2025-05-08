package reader

import (
	"fmt"
	"log"
	"math"
	"os"
	"packx/models"
	"packx/storageEngine"
	"packx/utils"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	DefaultHistogramInterval = 10 // Default 10-second interval for histograms
)

// processQuery handles a single query with parallel processing for multiple objects
func processQuery(query models.Query) models.QueryResponse {

	response := models.QueryResponse{

		QueryID: query.QueryID,

		Data: make(map[uint32][]models.DataPoint),
	}

	storage, err := storageEngine.NewStorageEngine()

	if err != nil {

		log.Printf("Failed to create storage engine: %v", err)

		return response

	}

	objectIDs := getObjectIDs(query, storage)

	// Process based on query type
	return processQueryByType(query, objectIDs, storage)

}

func processQueryByType(query models.Query, objectIDs []uint32, storage *storageEngine.StorageEngine) models.QueryResponse {

	if query.Aggregation == "" {

		// Raw data query
		return processRawDataQuery(query, objectIDs, storage)
	}

	// standard aggregation query
	switch query.Aggregation {

	case "avg", "sum", "min", "max":

		return processAggregationQuery(query, objectIDs, storage)

	}

	if query.Interval > 0 {

		if query.GroupByObjects {

			return processGroupedHistogramQuery(query, objectIDs, storage)

		}

		return processHistogramQuery(query, objectIDs, storage)
	}

	if query.GroupByObjects {

		return processGridQuery(query, objectIDs, storage)

	}

	return processGaugeQuery(query, objectIDs, storage)
}

func processRawDataQuery(query models.Query, objectIDs []uint32, storage *storageEngine.StorageEngine) models.QueryResponse {

	response := models.QueryResponse{

		QueryID: query.QueryID,

		Data: make(map[uint32][]models.DataPoint),
	}

	var wg sync.WaitGroup

	dataMutex := sync.RWMutex{}

	sem := make(chan struct{}, getMaxConcurrent(len(objectIDs)))

	for _, objectID := range objectIDs {

		wg.Add(1)

		sem <- struct{}{}

		go func(objID uint32) {

			defer func() {

				<-sem

				wg.Done()

			}()

			dataPoints, err := processObjectData(storage, objID, query)

			if err != nil {

				log.Printf("Error processing object %d: %v", objID, err)

				return

			}

			if len(dataPoints) > 0 {

				dataMutex.Lock()

				response.Data[objID] = deduplicateDataPoints(dataPoints)

				dataMutex.Unlock()

			}

		}(objectID)
	}

	wg.Wait()

	return response
}

func processHistogramQuery(query models.Query, objectIDs []uint32, storage *storageEngine.StorageEngine) models.QueryResponse {

	response := models.QueryResponse{

		QueryID: query.QueryID,

		Data: make(map[uint32][]models.DataPoint),
	}

	var wg sync.WaitGroup

	dataMutex := sync.RWMutex{}

	sem := make(chan struct{}, getMaxConcurrent(len(objectIDs)))

	for _, objectID := range objectIDs {

		wg.Add(1)

		sem <- struct{}{}

		go func(objID uint32) {

			defer func() {

				<-sem

				wg.Done()

			}()

			dataPoints, err := processObjectData(storage, objID, query)

			if err != nil {

				log.Printf("Error processing object %d: %v", objID, err)

				return

			}

			if len(dataPoints) > 0 {

				histogramPoints := generateHistogram(deduplicateDataPoints(dataPoints), int(query.Interval))

				if len(histogramPoints) > 0 {

					dataMutex.Lock()

					response.Data[objID] = histogramPoints

					dataMutex.Unlock()
				}

			}

		}(objectID)
	}

	wg.Wait()

	return response
}

func processGroupedHistogramQuery(query models.Query, objectIDs []uint32, storage *storageEngine.StorageEngine) models.QueryResponse {

	// First collect all data points
	allPoints := make([]models.DataPoint, 0)

	var wg sync.WaitGroup

	var dataMutex sync.RWMutex

	sem := make(chan struct{}, getMaxConcurrent(len(objectIDs)))

	for _, objectID := range objectIDs {

		wg.Add(1)

		sem <- struct{}{}

		go func(objID uint32) {

			defer func() {

				<-sem

				wg.Done()

			}()

			dataPoints, err := processObjectData(storage, objID, query)

			if err != nil {

				log.Printf("Error processing object %d: %v", objID, err)

				return

			}

			if len(dataPoints) > 0 {

				dataMutex.Lock()

				allPoints = append(allPoints, dataPoints...)

				dataMutex.Unlock()

			}

		}(objectID)

	}

	wg.Wait()

	histogramPoints := generateHistogram(deduplicateDataPoints(allPoints), int(query.Interval))

	return models.QueryResponse{

		QueryID: query.QueryID,

		Data: map[uint32][]models.DataPoint{

			0: histogramPoints,
		},
	}

}

func processGridQuery(query models.Query, objectIDs []uint32, storage *storageEngine.StorageEngine) models.QueryResponse {

	response := models.QueryResponse{

		QueryID: query.QueryID,

		Data: make(map[uint32][]models.DataPoint),
	}

	var wg sync.WaitGroup

	dataMutex := sync.RWMutex{}

	sem := make(chan struct{}, getMaxConcurrent(len(objectIDs)))

	for _, objectID := range objectIDs {

		wg.Add(1)

		sem <- struct{}{}

		go func(objID uint32) {

			defer func() {

				<-sem

				wg.Done()

			}()

			dataPoints, err := processObjectData(storage, objID, query)

			if err != nil {

				log.Printf("Error processing object %d: %v", objID, err)

				return

			}

			if len(dataPoints) > 0 {

				aggregatedPoints := aggregateDataPoints(deduplicateDataPoints(dataPoints), query.Aggregation)

				if len(aggregatedPoints) > 0 {

					dataMutex.Lock()

					response.Data[objID] = aggregatedPoints

					dataMutex.Unlock()

				}

			}

		}(objectID)

	}

	wg.Wait()

	return response
}

func processGaugeQuery(query models.Query, objectIDs []uint32, storage *storageEngine.StorageEngine) models.QueryResponse {

	response := models.QueryResponse{

		QueryID: query.QueryID,

		Data: make(map[uint32][]models.DataPoint),
	}

	var wg sync.WaitGroup

	dataMutex := sync.RWMutex{}

	sem := make(chan struct{}, getMaxConcurrent(len(objectIDs)))

	for _, objectID := range objectIDs {

		wg.Add(1)

		sem <- struct{}{}

		go func(objID uint32) {

			defer func() {

				<-sem

				wg.Done()

			}()

			dataPoints, err := processObjectData(storage, objID, query)

			if err != nil {

				log.Printf("Error processing object %d: %v", objID, err)

				return

			}

			if len(dataPoints) > 0 {

				gaugePoints := generateGauge(deduplicateDataPoints(dataPoints), 30) // Default 30-second intervals

				if len(gaugePoints) > 0 {

					dataMutex.Lock()

					response.Data[objID] = gaugePoints

					dataMutex.Unlock()

				}

			}

		}(objectID)

	}

	wg.Wait()

	return response
}

func processAggregationQuery(query models.Query, objectIDs []uint32, storage *storageEngine.StorageEngine) models.QueryResponse {

	response := models.QueryResponse{

		QueryID: query.QueryID,

		Data: make(map[uint32][]models.DataPoint),
	}

	var wg sync.WaitGroup

	dataMutex := sync.RWMutex{}

	sem := make(chan struct{}, getMaxConcurrent(len(objectIDs)))

	for _, objectID := range objectIDs {

		wg.Add(1)

		sem <- struct{}{}

		go func(objID uint32) {

			defer func() {

				<-sem

				wg.Done()

			}()

			dataPoints, err := processObjectData(storage, objID, query)

			if err != nil {

				log.Printf("Error processing object %d: %v", objID, err)

				return

			}

			if len(dataPoints) > 0 {

				aggregatedPoints := aggregateDataPoints(deduplicateDataPoints(dataPoints), query.Aggregation)

				if len(aggregatedPoints) > 0 {

					dataMutex.Lock()

					response.Data[objID] = aggregatedPoints

					dataMutex.Unlock()

				}

			}

		}(objectID)

	}

	wg.Wait()

	return response
}

func getMaxConcurrent(numObjects int) int {

	if numObjects > 100 {

		return 100

	}

	return 200
}

func getObjectIDs(query models.Query, storage *storageEngine.StorageEngine) []uint32 {

	if len(query.ObjectIDs) > 0 {

		return query.ObjectIDs

	}

	// Get all device IDs
	deviceIDsMap := make(map[uint32]bool)

	fromTime := time.Unix(int64(query.From), 0)

	toTime := time.Unix(int64(query.To), 0)

	for day := fromTime; !day.After(toTime); day = day.AddDate(0, 0, 1) {

		dateStr := day.Format("2006/01/02")

		counterPath := filepath.Join(
			utils.GetStoragePath(),
			dateStr,
			fmt.Sprintf("counter_%d", query.CounterId),
		)

		if _, err := os.Stat(counterPath); os.IsNotExist(err) {

			continue

		}

		if err := storage.SetStoragePath(counterPath); err != nil {

			log.Printf("Error setting storage path for date %s: %v", dateStr, err)

			continue

		}

		dayDeviceIDs, err := storage.GetAllDeviceIDs()

		if err != nil {

			log.Printf("Error getting device IDs for date %s: %v", dateStr, err)

			continue

		}

		for _, id := range dayDeviceIDs {

			deviceIDsMap[id] = true

		}

	}

	objectIDs := make([]uint32, 0, len(deviceIDsMap))

	for id := range deviceIDsMap {

		objectIDs = append(objectIDs, id)

	}

	return objectIDs

}

func deduplicateDataPoints(points []models.DataPoint) []models.DataPoint {

	if len(points) == 0 {
		return points
	}

	// Sort by timestamp
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})

	// Group points by timestamp
	timestampMap := make(map[uint32][]models.DataPoint)

	for _, point := range points {

		timestampMap[point.Timestamp] = append(timestampMap[point.Timestamp], point)

	}

	// Process each group to pick the best value
	var deduplicated []models.DataPoint

	for timestamp, timePoints := range timestampMap {

		if len(timePoints) == 1 {

			deduplicated = append(deduplicated, timePoints[0])

			continue

		}

		bestPoint := findBestDataPoint(timePoints)

		deduplicated = append(deduplicated, models.DataPoint{

			Timestamp: timestamp,

			Value: bestPoint.Value,
		})

	}

	sort.Slice(deduplicated, func(i, j int) bool {
		return deduplicated[i].Timestamp < deduplicated[j].Timestamp
	})

	return deduplicated
}

func findBestDataPoint(points []models.DataPoint) models.DataPoint {

	if len(points) == 1 {
		return points[0]
	}

	// Prefer values that are not extremely large or small (likely invalid/pointer values)

	var validPoints []models.DataPoint

	for _, point := range points {

		if isReasonableValue(point.Value) {

			validPoints = append(validPoints, point)

		}
	}

	// If we found valid points, return the first one
	if len(validPoints) > 0 {
		return validPoints[0]
	}

	// If all values seem invalid, create a zero value as fallback
	return models.DataPoint{

		Timestamp: points[0].Timestamp,

		Value: 0.0, // Use 0.0 instead of garbage data

	}
}

func isReasonableValue(value interface{}) bool {

	switch v := value.(type) {

	case float64:
		// Check if it's too large (likely a memory address or invalid float)
		if math.Abs(v) > 1e10 {
			return false
		}

		// Check if it's too small (likely an uninitialized value)
		if math.Abs(v) < 1e-300 {
			return false
		}

		return true

	case int64:

		// Check if it's too large (likely a memory address)
		if math.Abs(float64(v)) > 1e10 {
			return false
		}
		return true

	default:

		return true // Assume other types are valid
	}
}

// processObjectData handles data processing for a single object
func processObjectData(storage *storageEngine.StorageEngine, objectID uint32, query models.Query) ([]models.DataPoint, error) {

	var allDataPoints []models.DataPoint

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
func aggregateData(points []models.DataPoint, query models.Query) []models.DataPoint {

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

		return aggregateDataPoints(points, query.Aggregation)

	}
}

// readDataForObject reads data for a specific object from storage
func readDataForObject(storage *storageEngine.StorageEngine, objectID int, fromTime uint32, toTime uint32, counterID uint16) ([]models.DataPoint, error) {

	// Try to get from cache first
	cache, err := GetCache()
	if err != nil {

		log.Printf("Failed to initialize cache: %v", err)

	} else {

		if dataPoints, found := cache.Get(uint32(objectID), counterID, fromTime, toTime); found {

			return dataPoints, nil

		}

	}

	var dataPoints []models.DataPoint

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

	// Store in cache if we have data and cache is available
	if cache != nil && len(dataPoints) > 0 {

		cache.Set(uint32(objectID), counterID, fromTime, toTime, dataPoints)

	}

	return dataPoints, nil
}

// generateGauge creates gauge data points at specified intervals
func generateGauge(points []models.DataPoint, intervalSeconds int) []models.DataPoint {

	if len(points) == 0 {
		return nil
	}

	// Filter out invalid points first

	var validPoints []models.DataPoint

	for _, point := range points {

		if isReasonableValue(point.Value) {

			validPoints = append(validPoints, point)

		}
	}

	// Default to 30 seconds if interval is not specified
	if intervalSeconds <= 0 {

		intervalSeconds = 30

	}

	// Sort points by timestamp
	sort.Slice(validPoints, func(i, j int) bool {
		return validPoints[i].Timestamp < validPoints[j].Timestamp
	})

	// Find min and max timestamps
	minTime := validPoints[0].Timestamp

	maxTime := validPoints[len(validPoints)-1].Timestamp

	// Align to interval boundaries
	startTime := (minTime / uint32(intervalSeconds)) * uint32(intervalSeconds)

	endTime := ((maxTime / uint32(intervalSeconds)) + 1) * uint32(intervalSeconds)

	var result []models.DataPoint

	// For each interval
	for ts := startTime; ts < endTime; ts += uint32(intervalSeconds) {

		// Find latest value before this interval end
		var latestPoint *models.DataPoint

		for i := range validPoints {

			if validPoints[i].Timestamp <= ts && (latestPoint == nil || validPoints[i].Timestamp > latestPoint.Timestamp) {

				latestPoint = &validPoints[i]

			}

		}

		if latestPoint != nil {

			result = append(result, models.DataPoint{

				Timestamp: ts,

				Value: latestPoint.Value,
			})

		}

	}

	return result
}
