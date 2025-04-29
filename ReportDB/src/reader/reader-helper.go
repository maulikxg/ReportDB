package reader

import (
	"encoding/binary"
	"fmt"
	"math"
	"packx/models"
	"packx/utils"
)

func generateHistogram(dataPoints []models.DataPoint, bucketSizeSeconds int) []models.DataPoint {

	if len(dataPoints) == 0 {
		return []models.DataPoint{}
	}

	// map to store bucket counts
	buckets := make(map[uint32]int)

	minTime := dataPoints[0].Timestamp

	maxTime := dataPoints[0].Timestamp

	for _, dp := range dataPoints {

		if dp.Timestamp < minTime {

			minTime = dp.Timestamp

		}

		if dp.Timestamp > maxTime {

			maxTime = dp.Timestamp

		}

	}

	bucketSize := uint32(bucketSizeSeconds)

	// Normalize min time to bucket boundary
	minBucketTime := minTime - (minTime % bucketSize)

	// Create empty buckets for the entire range
	for t := minBucketTime; t <= maxTime; t += bucketSize {

		buckets[t] = 0

	}

	for _, dp := range dataPoints {

		bucketTime := dp.Timestamp - (dp.Timestamp % bucketSize)

		buckets[bucketTime]++

	}

	// Convert buckets to data points
	result := make([]models.DataPoint, 0, len(buckets))

	for bucketTime, count := range buckets {

		result = append(result, models.DataPoint{

			Timestamp: bucketTime,

			Value: count,
		})

	}

	//result  Sort  by timestamp
	for i := 0; i < len(result); i++ {

		for j := i + 1; j < len(result); j++ {

			if result[i].Timestamp > result[j].Timestamp {

				result[i], result[j] = result[j], result[i]

			}
		}
	}

	return result
}

func deserializeDataBlock(blockData []byte, fromTime uint32, toTime uint32, dataType byte) ([]models.DataPoint, error) {

	var dataPoints []models.DataPoint

	// Process data starting from offset 0 (header is not included in the data)
	offset := 0

	for offset < len(blockData) {

		if offset+4 > len(blockData) {
			break
		}

		timestamp := binary.LittleEndian.Uint32(blockData[offset : offset+4])

		offset += 4

		if timestamp < fromTime || timestamp > toTime {

			switch dataType {

			case utils.TypeInt:

				offset += 8

			case utils.TypeFloat:

				offset += 8

			case utils.TypeString:

				if offset+4 > len(blockData) {

					return dataPoints, fmt.Errorf("invalid string format: insufficient data for length")

				}

				strLen := binary.LittleEndian.Uint32(blockData[offset : offset+4])

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

		dataPoints = append(dataPoints, models.DataPoint{

			Timestamp: timestamp,
			Value:     value,
		})
	}

	return dataPoints, nil
}

func readIntValue(data []byte, offset int) (interface{}, int, error) {

	if offset+8 > len(data) {

		return nil, offset, fmt.Errorf("invalid int format: insufficient data")

	}

	value := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))

	return value, offset + 8, nil
}

func readFloatValue(data []byte, offset int) (interface{}, int, error) {

	if offset+8 > len(data) {

		return nil, offset, fmt.Errorf("invalid float format: insufficient data")

	}

	value := math.Float64frombits(binary.LittleEndian.Uint64(data[offset : offset+8]))

	return value, offset + 8, nil

}

func readStringValue(data []byte, offset int) (interface{}, int, error) {

	// String format: 4 bytes length + string data
	if offset+4 > len(data) {

		return nil, offset, fmt.Errorf("invalid string format: insufficient data for length")

	}

	strLen := binary.LittleEndian.Uint32(data[offset : offset+4])

	offset += 4

	if offset+int(strLen) > len(data) {

		return nil, offset, fmt.Errorf("invalid string format: string length %d exceeds available data", strLen)

	}

	value := string(data[offset : offset+int(strLen)])

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

				Value: sum / float64(count),
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

			Value: sum,
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

			Value: max,
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

			Value: min,
		}}

	}

	return points
}

// generateGauge processes data points to create gauge visualization data
// It returns the latest value for each interval (default: 30 seconds)
func generateGauge(dataPoints []models.DataPoint, intervalSeconds int) []models.DataPoint {

	if len(dataPoints) == 0 {
		return []models.DataPoint{}
	}

	// If no interval specified, default to 30 seconds
	if intervalSeconds <= 0 {

		intervalSeconds = 30

	}

	// map to store latest values in each interval
	gaugePoints := make(map[uint32]models.DataPoint)

	// Determine min and max times
	minTime := dataPoints[0].Timestamp

	maxTime := dataPoints[0].Timestamp

	for _, dp := range dataPoints {

		if dp.Timestamp < minTime {

			minTime = dp.Timestamp

		}

		if dp.Timestamp > maxTime {

			maxTime = dp.Timestamp

		}
	}

	intervalSize := uint32(intervalSeconds)

	for _, dp := range dataPoints {

		intervalStart := dp.Timestamp - (dp.Timestamp % intervalSize)

		existingPoint, exists := gaugePoints[intervalStart]

		if !exists || existingPoint.Timestamp < dp.Timestamp {

			gaugePoints[intervalStart] = dp

		}

	}

	// Convert to slice
	result := make([]models.DataPoint, 0, len(gaugePoints))

	for _, point := range gaugePoints {

		result = append(result, point)

	}

	// Sort by timestamp
	for i := 0; i < len(result); i++ {

		for j := i + 1; j < len(result); j++ {

			if result[i].Timestamp > result[j].Timestamp {

				result[i], result[j] = result[j], result[i]

			}
		}
	}

	return result
}
