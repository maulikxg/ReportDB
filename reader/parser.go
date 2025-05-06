package reader

import (
	"fmt"
	"math"
	. "packx/models"
	. "packx/utils"
	"sort"
)

const (
	DefaultHistogramInterval = 10 // Default 10-second interval for histograms
)

func ResultParser(results map[uint32][]DataPoint, query Query) (interface{}, error) {

	if len(results) == 0 {

		return nil, fmt.Errorf("no data available for processing")

	}

	dataType, err := GetCounterType(query.CounterId)

	if err != nil {

		return nil, fmt.Errorf("reader.fetchData error : %v", err)
	}

	if dataType == byte(TypeString) {

		return results, nil
	}

	// Handle histogram aggregation with zero interval
	if query.Aggregation == "histogram" {
		if query.Interval == 0 {
			query.Interval = DefaultHistogramInterval
		}
		return HistogramQuery(results, query)
	}

	if query.Interval == 0 {

		if query.GroupByObjects {

			return GridQuery(results, query)
		}

		return GaugeQuery(results, query)
	}

	return HistogramQuery(results, query)

}

func GaugeQuery(data map[uint32][]DataPoint, query Query) (interface{}, error) {

	values := make([]interface{}, 0)

	for _, points := range data {

		values = append(values, getValues(points)...)

	}

	return aggregateValues(values, query.Aggregation), nil

}

func HistogramQuery(results map[uint32][]DataPoint, query Query) (interface{}, error) {

	interval := uint32(query.Interval)

	bucketed := bucketData(results, interval, query.From, query.To)

	if query.GroupByObjects {

		return bucketed, nil
	}

	return mergeAllObjects(bucketed), nil
}

func GridQuery(results map[uint32][]DataPoint, query Query) (interface{}, error) {

	grid := make(map[uint32]interface{})

	for objID, points := range results {

		values := getValues(points)

		grid[objID] = aggregateValues(values, query.Aggregation)
	}

	return grid, nil
}

func bucketData(data map[uint32][]DataPoint, interval uint32, from uint32, to uint32) map[uint32][]DataPoint {

	bucketed := make(map[uint32][]DataPoint)

	for objID, points := range data {

		bucketed[objID] = createBuckets(points, interval, from, to)
	}

	return bucketed
}

func createBuckets(points []DataPoint, interval uint32, from uint32, to uint32) []DataPoint {

	sort.Slice(points, func(i, j int) bool {

		return points[i].Timestamp < points[j].Timestamp
	})

	bucketMap := make(map[uint32][]interface{}) // map[timestamp]->[points]

	for _, point := range points {

		if point.Timestamp < from || point.Timestamp > to {

			continue
		}

		bucket := point.Timestamp - (point.Timestamp % interval)

		bucketMap[bucket] = append(bucketMap[bucket], point.Value)
	}

	var bucketed []DataPoint

	for time := from - (from % interval); time <= to; time += interval {

		values := bucketMap[time]

		var aggregated interface{}

		if len(values) > 0 {

			aggregated = aggregateValues(values, "AVG")

		} else {

			aggregated = 0
		}

		bucketed = append(bucketed, DataPoint{

			Timestamp: time,

			Value: aggregated,
		})

	}

	return bucketed
}

func mergeAllObjects(data map[uint32][]DataPoint) []DataPoint {

	var allPoints []DataPoint

	for _, points := range data {

		allPoints = append(allPoints, points...)
	}

	sort.Slice(allPoints, func(i, j int) bool {

		return allPoints[i].Timestamp < allPoints[j].Timestamp
	})

	var merged []DataPoint

	var currentTime uint32

	var valuesAtSameTime []interface{}

	for _, point := range allPoints {

		if point.Timestamp != currentTime && len(valuesAtSameTime) > 0 {

			merged = append(merged, DataPoint{

				Timestamp: currentTime,

				Value: aggregateValues(valuesAtSameTime, "AVG"),
			})

			valuesAtSameTime = nil
		}

		currentTime = point.Timestamp

		valuesAtSameTime = append(valuesAtSameTime, point.Value)

	}

	if len(valuesAtSameTime) > 0 {

		merged = append(merged, DataPoint{

			Timestamp: currentTime,

			Value: aggregateValues(valuesAtSameTime, "AVG"),
		})

	}

	return merged
}

func getValues(points []DataPoint) []interface{} {

	var values []interface{}

	for _, point := range points {

		switch v := point.Value.(type) {

		case []interface{}:

			values = append(values, v...)

		default:

			values = append(values, v)
		}
	}

	return values
}

func aggregateValues(values []interface{}, aggType string) interface{} {

	if len(values) == 0 {

		return nil
	}

	switch aggType {

	case "AVG":

		return getAverage(values)

	case "MIN":

		return getMin(values)

	case "MAX":

		return getMax(values)

	case "SUM":

		return getSum(values)
	}

	if len(values) == 1 {

		return values[0]
	}

	return values
}

func getAverage(values []interface{}) float64 {

	sum := 0.0

	count := 0

	for _, v := range values {

		if f, ok := convertToFloat64(v); ok {

			sum += f

			count++
		}
	}

	if count == 0 {

		return 0
	}

	return sum / float64(count)
}

func getMin(values []interface{}) float64 {

	minVal := math.MaxFloat64

	for _, v := range values {

		if f, ok := convertToFloat64(v); ok && f < minVal {

			minVal = f
		}
	}

	if minVal == math.MaxFloat64 {

		return 0
	}

	return minVal
}

func getMax(values []interface{}) float64 {

	maxVal := -math.MaxFloat64

	for _, v := range values {

		if f, ok := convertToFloat64(v); ok && f > maxVal {

			maxVal = f
		}
	}

	if maxVal == -math.MaxFloat64 {

		return 0
	}
	return maxVal
}

func getSum(values []interface{}) float64 {

	sum := 0.0

	for _, v := range values {

		if f, ok := convertToFloat64(v); ok {

			sum += f

		}
	}

	return sum
}

func convertToFloat64(v interface{}) (float64, bool) {

	switch n := v.(type) {

	case float64:

		return n, true

	case uint64:

		return float64(n), true

	default:

		return 0, false
	}
}
