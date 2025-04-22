package writer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"packx/models"
	"packx/storageEngine"
	"packx/utils"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// writer function to process batched data
func writer(writersChannel <-chan WriteObjectWiseBatch, storageEn *storageEngine.StorageEngine, writerWaitGroup *sync.WaitGroup) {

	defer writerWaitGroup.Done()

	basePath := utils.BaseDirProvider()

	for dataBatch := range writersChannel {

		log.Printf("Writer received batch for ObjectId: %d, CounterId: %d, Count: %d\\n", dataBatch.ObjectId, dataBatch.CounterId, len(dataBatch.Values))

		for _, dp := range dataBatch.Values {

			metric := &models.Metric{

				ObjectID: dataBatch.ObjectId,

				CounterId: dataBatch.CounterId,

				Timestamp: dp.Timestamp,

				Value: dp.Value,
			}

			// Create storage path based on timestamp
			timestamp := time.Unix(int64(metric.Timestamp), 0)

			dateStr := timestamp.Format("2006/01/02")

			counterPath := filepath.Join(
				basePath,
				"storage",
				dateStr,
				fmt.Sprintf("counter_%d", metric.CounterId),
			)

			// Set storage path for this write
			if err := storageEn.SetStoragePath(counterPath); err != nil {

				log.Printf("Error setting storage path for ObjectId %d: %v", dataBatch.ObjectId, err)

				continue
			}

			// Serialize the metric data
			data, err := serializeMetric(*metric)

			if err != nil {

				log.Printf("Error serializing metric for ObjectId %d: %v", dataBatch.ObjectId, err)

				continue

			}

			// Write to storage engine
			if err := storageEn.Put(int(metric.ObjectID), data); err != nil {

				log.Printf("Storage engine error for ObjectId %d: %v", dataBatch.ObjectId, err)

				continue
			}

			log.Printf("Successfully stored metric for ObjectId: %d, Timestamp: %d, Value: %v\\n", dataBatch.ObjectId, dp.Timestamp, dp.Value)

		}

		log.Printf("Writer finished processing batch for ObjectId: %d\\n", dataBatch.ObjectId)
	}

	log.Println("Writer exiting.")

}

func serializeMetric(metric models.Metric) ([]byte, error) {

	// Validate the metric value type
	
	if err := ValidateMetricValueType(&metric); err != nil {

		return nil, err
	}

	buf := new(bytes.Buffer)

	// Write timestamp first (this is important for the storage engine)
	if err := binary.Write(buf, binary.LittleEndian, metric); err != nil {

		return nil, fmt.Errorf("failed to write Timestamp: %v", err)

	}

	// Write the value based on its type
	switch v := metric.Value.(type) {

	case int:

		if err := binary.Write(buf, binary.LittleEndian, int64(v)); err != nil {

			return nil, fmt.Errorf("failed to write int value: %v", err)

		}

	case int32:

		if err := binary.Write(buf, binary.LittleEndian, int64(v)); err != nil {

			return nil, fmt.Errorf("failed to write int32 value: %v", err)

		}
	case int64:

		if err := binary.Write(buf, binary.LittleEndian, v); err != nil {

			return nil, fmt.Errorf("failed to write int64 value: %v", err)

		}

	case float32:

		if err := binary.Write(buf, binary.LittleEndian, float64(v)); err != nil {

			return nil, fmt.Errorf("failed to write float32 value: %v", err)

		}

	case float64:

		if err := binary.Write(buf, binary.LittleEndian, v); err != nil {

			return nil, fmt.Errorf("failed to write float64 value: %v", err)

		}

	case string:

		// Write string length
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(v))); err != nil {

			return nil, fmt.Errorf("failed to write string length: %v", err)

		}

		// Write string data
		if _, err := buf.WriteString(v); err != nil {
			return nil, fmt.Errorf("failed to write string value: %v", err)
		}

	default:

		return nil, fmt.Errorf("unsupported value type: %T", v)

	}

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
