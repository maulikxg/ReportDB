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

	// Find min and max timestamps in one pass
	minTime, maxTime := dataPoints[0].Timestamp, dataPoints[0].Timestamp

	for _, dp := range dataPoints {

		if dp.Timestamp < minTime {

			minTime = dp.Timestamp

		}
		if dp.Timestamp > maxTime {

			maxTime = dp.Timestamp

		}
	}

	bucketSize := uint32(bucketSizeSeconds)

	minBucketTime := minTime - (minTime % bucketSize)

	numBuckets := (maxTime-minBucketTime)/bucketSize + 1

	bucketCounts := make([]int, numBuckets)

	// Pre-calculate bucket indices
	for _, dp := range dataPoints {

		bucketIndex := (dp.Timestamp - minBucketTime) / bucketSize

		bucketCounts[bucketIndex]++

	}

	// Pre-allocate result slice
	result := make([]models.DataPoint, numBuckets)

	for i := range bucketCounts {

		result[i] = models.DataPoint{

			Timestamp: minBucketTime + uint32(i)*bucketSize,

			Value: bucketCounts[i],
		}
	}

	return result
}

func deserializeDataBlock(blockData []byte, fromTime uint32, toTime uint32, dataType byte) ([]models.DataPoint, error) {

	if len(blockData) < 4 {

		return nil, fmt.Errorf("insufficient data: block size < 4 bytes")

	}

	// Pre-allocate dataPoints with estimated capacity
	estimatedPoints := len(blockData) / 12 // Rough estimate based on minimum point size

	dataPoints := make([]models.DataPoint, 0, estimatedPoints)

	var err error

	offset := 0

	for offset < len(blockData)-4 {

		timestamp := binary.LittleEndian.Uint32(blockData[offset:])

		offset += 4

		// Skip type marker byte
		offset++

		if timestamp < fromTime || timestamp > toTime {

			// Skip data based on type without reading
			switch dataType {

			case utils.TypeInt, utils.TypeFloat:

				offset += 8

			case utils.TypeString:

				if offset+4 > len(blockData) {

					continue
				}

				strLen := binary.LittleEndian.Uint32(blockData[offset:])

				offset += 4 + int(strLen)

			default:

				return nil, fmt.Errorf("unknown data type: %d", dataType)

			}

			continue
		}

		var value interface{}

		switch dataType {

		case utils.TypeInt:

			if value, offset, err = readIntValue(blockData, offset); err != nil {

				return dataPoints, err

			}

		case utils.TypeFloat:

			if value, offset, err = readFloatValue(blockData, offset); err != nil {

				return dataPoints, err

			}

		case utils.TypeString:

			if value, offset, err = readStringValue(blockData, offset); err != nil {

				return dataPoints, err

			}

		default:

			return nil, fmt.Errorf("unknown data type: %d", dataType)

		}

		dataPoints = append(dataPoints, models.DataPoint{

			Timestamp: timestamp,

			Value: value,
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

	result := make([]models.DataPoint, 1)

	timestamp := points[len(points)-1].Timestamp

	values := make([]float64, 0, len(points))

	for _, p := range points {

		if val, ok := p.Value.(float64); ok {

			values = append(values, val)

		} else if intVal, ok := p.Value.(int64); ok {

			values = append(values, float64(intVal))

		}

	}

	if len(values) == 0 {

		return points

	}

	switch aggregation {

	case "avg":
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		result[0] = models.DataPoint{
			Timestamp: timestamp,
			Value:     sum / float64(len(values)),
		}

	case "sum":
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		result[0] = models.DataPoint{
			Timestamp: timestamp,
			Value:     sum,
		}

	case "max":
		max := values[0]
		for _, v := range values[1:] {
			if v > max {
				max = v
			}
		}
		result[0] = models.DataPoint{
			Timestamp: timestamp,
			Value:     max,
		}

	case "min":
		min := values[0]
		for _, v := range values[1:] {
			if v < min {
				min = v
			}
		}
		result[0] = models.DataPoint{
			Timestamp: timestamp,
			Value:     min,
		}

	default:
		return points
	}

	return result
}

//func generateGauge(dataPoints []models.DataPoint, intervalSeconds int) []models.DataPoint {
//	// ... existing code until sorting ...
//
//	// Convert to slice and sort efficiently
//	result := make([]models.DataPoint, 0, len(gaugePoints))
//	for _, point := range gaugePoints {
//		result = append(result, point)
//	}
//
//	sort.Slice(result, func(i, j int) bool {
//		return result[i].Timestamp < result[j].Timestamp
//	})
//
//	return result
//}
