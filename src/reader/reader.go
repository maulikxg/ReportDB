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
			Data:    make(map[uint32][]models.DataPoint),
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

// readDataForObject reads data for a specific object ID from storage
func readDataForObject(storage *storageEngine.StorageEngine, objectID int, fromTime uint32, toTime uint32, counterID uint16) ([]models.DataPoint, error) {
	var dataPoints []models.DataPoint
	
	// Get blocks of data for this object ID
	rawDataBlocks, err := storage.Get(objectID)
	if err != nil {
		return nil, fmt.Errorf("failed to get data blocks: %v", err)
	}
	
	if len(rawDataBlocks) == 0 {
		return dataPoints, nil // No data for this object ID
	}
	
	// Expected data type for this counter
	expectedType, err := utils.GetCounterType(counterID)
	if err != nil {
		return nil, fmt.Errorf("failed to get counter type: %v", err)
	}
	
	// Process each block of data
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

// deserializeDataBlock extracts data points from a block of data
func deserializeDataBlock(blockData []byte, fromTime uint32, toTime uint32, dataType byte) ([]models.DataPoint, error) {
	var dataPoints []models.DataPoint
	
	// Process data starting from offset 0 (header is not included in the data)
	offset := 0
	
	// Continue until we reach the end of the data block
	for offset < len(blockData) {
		// Need at least 4 bytes for timestamp
		if offset+4 > len(blockData) {
			break
		}
		
		// Read timestamp (first 4 bytes of each record)
		timestamp := binary.LittleEndian.Uint32(blockData[offset:offset+4])
		offset += 4
		
		// Skip records outside the requested time range
		if timestamp < fromTime || timestamp > toTime {
			// Still need to advance the offset based on data type
			switch dataType {
			case utils.TypeInt:
				offset += 8 // int64 size
			case utils.TypeFloat:
				offset += 8 // float64 size
			case utils.TypeString:
				// String format: 4 bytes length + string data
				if offset+4 > len(blockData) {
					return dataPoints, fmt.Errorf("invalid string format: insufficient data for length")
				}
				strLen := binary.LittleEndian.Uint32(blockData[offset:offset+4])
				offset += 4 + int(strLen)
			default:
				return dataPoints, fmt.Errorf("unknown data type: %d", dataType)
			}
			continue
		}
		
		// Read the actual value based on data type
		var value interface{}
		var valueErr error
		
		switch dataType {
		case utils.TypeInt:
			value, offset, valueErr = readIntValue(blockData, offset)
		case utils.TypeFloat:
			value, offset, valueErr = readFloatValue(blockData, offset)
		case utils.TypeString:
			value, offset, valueErr = readStringValue(blockData, offset)
		default:
			return dataPoints, fmt.Errorf("unknown data type: %d", dataType)
		}
		
		if valueErr != nil {
			return dataPoints, valueErr
		}
		
		// Add the data point
		dataPoints = append(dataPoints, models.DataPoint{
			Timestamp: timestamp,
			Value:     value,
		})
	}
	
	return dataPoints, nil
}

// Helper functions to read different data types

func readIntValue(data []byte, offset int) (interface{}, int, error) {
	if offset+8 > len(data) {
		return nil, offset, fmt.Errorf("invalid int format: insufficient data")
	}
	
	value := int64(binary.LittleEndian.Uint64(data[offset:offset+8]))
	return value, offset + 8, nil
}

func readFloatValue(data []byte, offset int) (interface{}, int, error) {
	if offset+8 > len(data) {
		return nil, offset, fmt.Errorf("invalid float format: insufficient data")
	}
	
	value := math.Float64frombits(binary.LittleEndian.Uint64(data[offset:offset+8]))
	return value, offset + 8, nil
}

func readStringValue(data []byte, offset int) (interface{}, int, error) {
	// String format: 4 bytes length + string data
	if offset+4 > len(data) {
		return nil, offset, fmt.Errorf("invalid string format: insufficient data for length")
	}
	
	strLen := binary.LittleEndian.Uint32(data[offset:offset+4])
	offset += 4
	
	if offset+int(strLen) > len(data) {
		return nil, offset, fmt.Errorf("invalid string format: string length %d exceeds available data", strLen)
	}
	
	value := string(data[offset:offset+int(strLen)])
	return value, offset + int(strLen), nil
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
			} else if intVal, ok := p.Value.(int64); ok {
				sum += float64(intVal)
				count++
			}
		}
		if count > 0 {
			return []models.DataPoint{{
				Timestamp: timestamp,
				Value:     sum / float64(count),
			}}
		}

	case "sum":
		sum := 0.0
		for _, p := range points {
			if val, ok := p.Value.(float64); ok {
				sum += val
			} else if intVal, ok := p.Value.(int64); ok {
				sum += float64(intVal)
			}
		}
		return []models.DataPoint{{
			Timestamp: timestamp,
			Value:     sum,
		}}

	case "max":
		max := math.Inf(-1)
		for _, p := range points {
			var val float64
			if floatVal, ok := p.Value.(float64); ok {
				val = floatVal
			} else if intVal, ok := p.Value.(int64); ok {
				val = float64(intVal)
			} else {
				continue
			}
			max = math.Max(max, val)
		}
		return []models.DataPoint{{
			Timestamp: timestamp,
			Value:     max,
		}}

	case "min":
		min := math.Inf(1)
		for _, p := range points {
			var val float64
			if floatVal, ok := p.Value.(float64); ok {
				val = floatVal
			} else if intVal, ok := p.Value.(int64); ok {
				val = float64(intVal)
			} else {
				continue
			}
			min = math.Min(min, val)
		}
		return []models.DataPoint{{
			Timestamp: timestamp,
			Value:     min,
		}}
	}

	return points
}
