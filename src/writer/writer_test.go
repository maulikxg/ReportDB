package writer

import (
	"fmt"
	"os"
	"packx/models"
	"packx/storageEngine"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const (
	testWriters = 4
	testMaxBatchSize = 1000
	testBufferSize = testWriters * testMaxBatchSize
)

// Simple mock storage for testing
type MockStorage struct {
	mu       sync.Mutex
	basePath string
	written  map[string][]models.DataPoint // key: "objectID_counterID", value: data points
}

func NewMockStorage() *MockStorage {
	return &MockStorage{
		written: make(map[string][]models.DataPoint),
	}
}

func (m *MockStorage) Put(metric models.Metric) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%d_%d", metric.ObjectID, metric.CounterId)
	dp := models.DataPoint{
		Timestamp: metric.Timestamp,
		Value:     metric.Value,
	}
	m.written[key] = append(m.written[key], dp)
	return nil
}

func (m *MockStorage) Get(query models.Query) ([]models.Metric, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var metrics []models.Metric
	for _, objectID := range query.ObjectIDs {
		key := fmt.Sprintf("%d_%d", objectID, query.CounterId)
		dataPoints := m.written[key]

		for _, dp := range dataPoints {
			metrics = append(metrics, models.Metric{
				ObjectID:   objectID,
				CounterId: query.CounterId,
				Timestamp: dp.Timestamp,
				Value:     dp.Value,
			})
		}
	}

	return metrics, nil
}

// WriteHandler implementation for testing
type WriteHandler struct {
	storage    storageEngine.Storage
	batchSize  int
	buffer     *BufferBatch
	writerChan chan WriteObjectWiseBatch
	wg         sync.WaitGroup
	shutdown   chan bool
}

func NewWriteHandler(storage storageEngine.Storage) *WriteHandler {
	handler := &WriteHandler{
		storage:    storage,
		batchSize:  testMaxBatchSize,
		writerChan: make(chan WriteObjectWiseBatch, testWriters),
		buffer:     NewBufferBatch(),
		shutdown:   make(chan bool),
	}

	handler.wg.Add(1)
	go handler.processWrites()

	return handler
}

func (h *WriteHandler) processWrites() {
	defer h.wg.Done()
	
	for {
		select {
		case batch, ok := <-h.writerChan:
			if !ok {
				return
			}
			for _, dp := range batch.Values {
				metric := models.Metric{
					ObjectID:   batch.ObjectId,
					CounterId: batch.CounterId,
					Timestamp: dp.Timestamp,
					Value:     dp.Value,
				}
				if err := h.storage.Put(metric); err != nil {
					// In a real implementation, we'd handle this error better
					continue
				}
			}
		case <-h.shutdown:
			return
		}
	}
}

func (h *WriteHandler) SetBatchSize(size int) {
	h.batchSize = size
}

func (h *WriteHandler) AddData(objectID uint32, counterID uint16, dp models.DataPoint) error {
	h.buffer.AddData(objectID, counterID, dp)
	return nil
}

func (h *WriteHandler) Flush() {
	h.buffer.Flush(h.writerChan)
	// Wait a bit to ensure data is processed
	time.Sleep(10 * time.Millisecond)
}

func (h *WriteHandler) Close() {
	close(h.shutdown)
	close(h.writerChan)
	h.wg.Wait()
}

func TestWriteHandler(t *testing.T) {
	// Create mock storage
	mockStorage := NewMockStorage()

	// Create WriteHandler
	handler := NewWriteHandler(mockStorage)
	defer handler.Close()

	// Test data
	testData := []struct {
		objectID  uint32
		counterID uint16
		values    []float64
	}{
		{
			objectID:  1,
			counterID: 100,
			values:    []float64{1.1, 2.2, 3.3},
		},
		{
			objectID:  2,
			counterID: 200,
			values:    []float64{4.4, 5.5},
		},
	}

	// Add data through WriteHandler
	for _, td := range testData {
		for _, val := range td.values {
			err := handler.AddData(td.objectID, td.counterID, models.DataPoint{
				Timestamp: uint32(time.Now().Unix()),
				Value:     val,
			})
			if err != nil {
				t.Errorf("Failed to add data: %v", err)
			}
		}
	}

	// Flush the data
	handler.Flush()

	// Verify data was written correctly
	for _, td := range testData {
		query := models.Query{
			ObjectIDs:  []uint32{td.objectID},
			CounterId: td.counterID,
		}

		metrics, err := mockStorage.Get(query)
		if err != nil {
			t.Errorf("Failed to get metrics: %v", err)
			continue
		}

		// Check number of values written
		if len(metrics) != len(td.values) {
			t.Errorf("Expected %d values for object %d, counter %d, got %d",
				len(td.values), td.objectID, td.counterID, len(metrics))
			continue
		}

		// Check values
		valueMap := make(map[float64]bool)
		for _, metric := range metrics {
			valueMap[metric.Value.(float64)] = true
		}

		for _, expectedValue := range td.values {
			if !valueMap[expectedValue] {
				t.Errorf("Value %f not found for object %d, counter %d",
					expectedValue, td.objectID, td.counterID)
			}
		}
	}
}

func TestBufferBatch(t *testing.T) {
	bb := NewBufferBatch()

	// Test data
	testData := []struct {
		objectID  uint32
		counterID uint16
		value     float64
	}{
		{1, 100, 1.1},
		{1, 100, 2.2},
		{2, 200, 3.3},
	}

	// Add data to buffer
	for _, td := range testData {
		bb.AddData(td.objectID, td.counterID, models.DataPoint{
			Timestamp: uint32(time.Now().Unix()),
			Value:     td.value,
		})
	}

	// Verify data in buffer
	for _, td := range testData {
		data := bb.GetDataP(td.objectID, td.counterID)
		if data == nil {
			t.Errorf("No data found for object %d, counter %d", td.objectID, td.counterID)
			continue
		}

		found := false
		for _, dp := range data {
			if dp.Value.(float64) == td.value {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Value %f not found for object %d, counter %d",
				td.value, td.objectID, td.counterID)
		}
	}
}

func TestWriteHandlerIntegration(t *testing.T) {
	// Create temporary directory for test
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("writer_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create mock storage
	mockStorage := NewMockStorage()

	// Create WriteHandler with small batch size for testing
	handler := NewWriteHandler(mockStorage)
	defer handler.Close()
	handler.SetBatchSize(2) // Small batch size to force multiple flushes

	// Test data with multiple objects and counters
	testCases := []struct {
		objectID  uint32
		counterID uint16
		values    []float64
	}{
		{1, 100, []float64{1.1, 1.2, 1.3}},
		{1, 200, []float64{2.1, 2.2}},
		{2, 100, []float64{3.1, 3.2, 3.3}},
	}

	// Add data through WriteHandler
	for _, tc := range testCases {
		for _, value := range tc.values {
			err := handler.AddData(tc.objectID, tc.counterID, models.DataPoint{
				Timestamp: uint32(time.Now().Unix()),
				Value:     value,
			})
			if err != nil {
				t.Errorf("Failed to add data: %v", err)
			}
		}
	}

	// Force flush
	handler.Flush()

	// Verify all data was written correctly
	for _, tc := range testCases {
		query := models.Query{
			ObjectIDs:  []uint32{tc.objectID},
			CounterId: tc.counterID,
		}

		metrics, err := mockStorage.Get(query)
		if err != nil {
			t.Errorf("Failed to get metrics: %v", err)
			continue
		}

		// Check if all values were written
		if len(metrics) != len(tc.values) {
			t.Errorf("Expected %d values for object %d, counter %d, got %d",
				len(tc.values), tc.objectID, tc.counterID, len(metrics))
			continue
		}

		// Verify each value was written
		valueMap := make(map[float64]bool)
		for _, metric := range metrics {
			valueMap[metric.Value.(float64)] = true
		}

		for _, expectedValue := range tc.values {
			if !valueMap[expectedValue] {
				t.Errorf("Value %f not found for object %d, counter %d",
					expectedValue, tc.objectID, tc.counterID)
			}
		}
	}
}

func TestShowStorageData(t *testing.T) {
	// Create mock storage
	mockStorage := NewMockStorage()

	// Create WriteHandler
	handler := NewWriteHandler(mockStorage)
	defer handler.Close()

	// Test data with different object IDs and counter IDs
	testData := []struct {
		objectID  uint32
		counterID uint16
		value     float64
		timestamp uint32
	}{
		{1, 100, 1.1, uint32(time.Now().Unix())},
		{1, 100, 2.2, uint32(time.Now().Unix())},
		{1, 200, 3.3, uint32(time.Now().Unix())},
		{2, 100, 4.4, uint32(time.Now().Unix())},
	}

	t.Log("\nWriting test data to storage:")
	for _, td := range testData {
		t.Logf("Writing: ObjectID=%d, CounterID=%d, Value=%f, Timestamp=%d",
			td.objectID, td.counterID, td.value, td.timestamp)
		
		err := handler.AddData(td.objectID, td.counterID, models.DataPoint{
			Timestamp: td.timestamp,
			Value:     td.value,
		})
		if err != nil {
			t.Errorf("Failed to add data: %v", err)
		}
	}

	// Flush to ensure data is written
	handler.Flush()

	// Read and verify data for each object/counter combination
	t.Log("\nReading data from storage:")
	
	// Get unique object/counter combinations
	combinations := make(map[string]struct{})
	for _, td := range testData {
		key := fmt.Sprintf("%d_%d", td.objectID, td.counterID)
		combinations[key] = struct{}{}
	}

	// Check each combination
	for key := range combinations {
		var objectID uint32
		var counterID uint16
		fmt.Sscanf(key, "%d_%d", &objectID, &counterID)

		query := models.Query{
			ObjectIDs:  []uint32{objectID},
			CounterId: counterID,
		}

		metrics, err := mockStorage.Get(query)
		if err != nil {
			t.Errorf("Failed to get metrics: %v", err)
			continue
		}

		t.Logf("\nData for ObjectID=%d, CounterID=%d:", objectID, counterID)
		for _, metric := range metrics {
			t.Logf("  Timestamp=%d, Value=%f", metric.Timestamp, metric.Value.(float64))
		}
	}
} 