package writer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"packx/models"
	"packx/storageEngine"
	"packx/utils"

	"strconv"
	"sync"
)

// writer function to process batched data
func writer(writersChannel <-chan WriteObjectWiseBatch, storageEn *storageEngine.StorageEngine, writerWaitGroup *sync.WaitGroup) {
	defer writerWaitGroup.Done()

	// Storage path is now managed solely by the storageEngine instance based on config.
	// basePath := utils.BaseDirProvider()

	for dataBatch := range writersChannel {
		log.Printf("Writer received batch for ObjectId: %d, CounterId: %d, Count: %d\n", dataBatch.ObjectId, dataBatch.CounterId, len(dataBatch.Values))

		for _, dp := range dataBatch.Values {
			metric := models.Metric{ // Use value, not pointer, if Put expects value
				ObjectID:  dataBatch.ObjectId,
				CounterId: dataBatch.CounterId,
				Timestamp: dp.Timestamp,
				Value:     dp.Value,
			}

			// REMOVED: Path construction logic - StorageEngine handles partitioning internally
			// timestamp := time.Unix(int64(metric.Timestamp), 0)
			// dateStr := timestamp.Format("2006/01/02")
			// counterPath := filepath.Join(
			// 	basePath,
			// 	"storage", // This was incorrect
			// 	dateStr,
			// 	fmt.Sprintf("counter_%d", metric.CounterId),
			// )

			// REMOVED: Repeated SetStoragePath calls - Engine uses its configured base path
			// if err := storageEn.SetStoragePath(counterPath); err != nil {
			// 	log.Printf("Error setting storage path for ObjectId %d: %v", dataBatch.ObjectId, err)
			// 	continue
			// }

			// Serialize the metric data
			data, err := serializeMetric(metric)
			if err != nil {
				log.Printf("Error serializing metric for ObjectId %d: %v", dataBatch.ObjectId, err)
				continue
			}

			// Write to storage engine using ObjectID as the key for partitioning
			// Also pass CounterId for directory structure
			if err := storageEn.Put(int(metric.ObjectID), metric.CounterId, data); err != nil {
				log.Printf("Storage engine error for ObjectId %d, CounterId %d: %v", metric.ObjectID, metric.CounterId, err)
				continue
			}

			log.Printf("Successfully stored metric for ObjectId: %d, Timestamp: %d, Value: %v\n", dataBatch.ObjectId, dp.Timestamp, dp.Value)
		}

		log.Printf("Writer finished processing batch for ObjectId: %d\n", dataBatch.ObjectId)
	}

	log.Println("Writer exiting.")
}

func serializeMetric(metric models.Metric) ([]byte, error) {
	// Validate and potentially convert the metric value type first
	if err := ValidateMetricValueType(&metric); err != nil {
		return nil, fmt.Errorf("metric validation failed: %w", err)
	}

	buf := new(bytes.Buffer)

	// Write fixed-size fields individually
	if err := binary.Write(buf, binary.LittleEndian, metric.Timestamp); err != nil {
		return nil, fmt.Errorf("failed to write Timestamp: %w", err)
	}
	// We don't write ObjectID and CounterId here as they are part of the storage key/path
	// log.Printf("Serialized Timestamp: %d", metric.Timestamp) // Debug log

	// Write the value based on its (validated) type
	switch v := metric.Value.(type) {
	case int64:
		// log.Printf("Serializing Value (int64): %d", v) // Debug log
		if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
			return nil, fmt.Errorf("failed to write int64 value: %w", err)
		}
	case float64:
		// log.Printf("Serializing Value (float64): %f", v) // Debug log
		if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
			return nil, fmt.Errorf("failed to write float64 value: %w", err)
		}
	case string:
		// log.Printf("Serializing Value (string): %s", v) // Debug log
		// Write string length (as uint32)
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(v))); err != nil {
			return nil, fmt.Errorf("failed to write string length: %w", err)
		}
		// Write string data
		if _, err := buf.WriteString(v); err != nil {
			return nil, fmt.Errorf("failed to write string value: %w", err)
		}
	default:
		// This should not happen if ValidateMetricValueType worked correctly
		return nil, fmt.Errorf("unsupported validated value type during serialization: %T", v)
	}

	// log.Printf("Serialization successful. Bytes: %x", buf.Bytes()) // Debug log
	return buf.Bytes(), nil
}

func ValidateMetricValueType(metric *models.Metric) error {

	expectedType, err := utils.GetCounterType(metric.CounterId)

	if err != nil {

		return err

	}

	switch expectedType {

	case utils.TypeInt:

		switch v := metric.Value.(type) {

		case float64:

			metric.Value = int64(v)

		case string:

			parsed, err := strconv.ParseInt(v, 10, 64)

			if err != nil {

				return fmt.Errorf("invalid int value for counter %d: %v", metric.CounterId, err)

			}

			metric.Value = parsed

		case int, int32, int64:

		default:

			return fmt.Errorf("type mismatch for counter %d: expected int, got %T", metric.CounterId, metric.Value)

		}

	case utils.TypeFloat:

		switch v := metric.Value.(type) {

		case float64:

		case string:

			parsed, err := strconv.ParseFloat(v, 64)

			if err != nil {

				return fmt.Errorf("invalid float value for counter %d: %v", metric.CounterId, err)

			}

			metric.Value = parsed

		case int, int32, int64:

			metric.Value = float64(convertToInt64(v))

		default:

			return fmt.Errorf("type mismatch for counter %d: expected float, got %T", metric.CounterId, metric.Value)

		}

	case utils.TypeString:

		switch v := metric.Value.(type) {

		case string:

		case float64, int, int32, int64:

			metric.Value = fmt.Sprintf("%v", v)

		default:

			return fmt.Errorf("type mismatch for counter %d: expected string, got %T", metric.CounterId, metric.Value)

		}

	default:

		return fmt.Errorf("unknown expected type for counter %d", metric.CounterId)

	}

	return nil

}

func convertToInt64(v interface{}) int64 {

	switch n := v.(type) {

	case int:

		return int64(n)

	case int32:

		return int64(n)

	case int64:

		return n

	default:

		return 0

	}
}

//for dataBatch := range writersChannel {
//
//	log.Printf("Writer received batch for ObjectId: %d, Count: %d\n", dataBatch.ObjectId, len(dataBatch.Values))
//
//	// Simulate data processing/writing to storage
//	for _, dp := range dataBatch.Values {
//
//		log.Printf("  Processing ObjectId: %d, Timestamp: %d, Value: %f\n", dataBatch.ObjectId, dp.Timestamp, dp.Value)
//
//	}
//
//	log.Printf("Writer finished processing batch for ObjectId: %d\n", dataBatch.ObjectId)
//}
//
//log.Println("Writer exiting.")
