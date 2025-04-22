package writer

import (
	"packx/models"
	"packx/storageEngine"
	"packx/utils"
	"sync"
	"time"
	"log"
	"fmt"
)

var (
	FlushDuration = time.Second * 2
	maxBatchSize = 1000 // maximum number of the Dataponts store per batch
)

// Initialize these in StartWriteHandler instead of package level
func getBufferSize() int {
	writers := utils.GetWriters()
	return writers * maxBatchSize
}

type WriteObjectWiseBatch struct {
	ObjectId uint32 `json:"object_id"`

	CounterId uint16 `json:"counter_id"`

	Values []models.DataPoint `json:"values"`
}

type BufferBatch struct {
	buffer map[uint32]map[uint16][]models.DataPoint /// objectId -> CounterId -> []DataPoints

	flushTicker *time.Ticker

	BuffEmpty bool

	BuffLock sync.RWMutex
}

func NewBufferBatch() *BufferBatch {

	pool := make(map[uint32]map[uint16][]models.DataPoint)

	flushTicker := time.NewTicker(FlushDuration)

	return &BufferBatch{

		buffer: pool,

		flushTicker: flushTicker,

		BuffEmpty: true,
	}
}

func (b *BufferBatch) AddData(ObjectId uint32, CounterId uint16, Value models.DataPoint) {
	b.BuffLock.Lock()
	defer b.BuffLock.Unlock()
	log.Printf("[BufferBatch] Adding data: ObjectId=%d, CounterId=%d, Timestamp=%d", ObjectId, CounterId, Value.Timestamp)

	// Initialize maps if they don't exist
	if _, exists := b.buffer[ObjectId]; !exists {
		b.buffer[ObjectId] = make(map[uint16][]models.DataPoint)
	}
	if _, exists := b.buffer[ObjectId][CounterId]; !exists {
		b.buffer[ObjectId][CounterId] = make([]models.DataPoint, 0)
	}

	b.buffer[ObjectId][CounterId] = append(b.buffer[ObjectId][CounterId], Value)
	b.BuffEmpty = false
}

// GetDataP returns all DataPoints for a given ObjectId and CounterId
func (b *BufferBatch) GetDataP(ObjectId uint32, CounterId uint16) []models.DataPoint {

	b.BuffLock.Lock()
	
	defer b.BuffLock.Unlock()

	// Check if ObjectId exists
	if counterMap, exists := b.buffer[ObjectId]; exists {

		// Check if CounterId exists
		if dataPoints, exists := counterMap[CounterId]; exists {

			// Return copy of slice to prevent external modifications
			result := make([]models.DataPoint, len(dataPoints))

			copy(result, dataPoints)

			return result

		}
	}
	return nil // Return nil if no data found
}

func (b *BufferBatch) Flush(dataChannel chan<- WriteObjectWiseBatch) {
	b.BuffLock.Lock()
	defer b.BuffLock.Unlock()
	log.Printf("[BufferBatch] Flushing buffer. Object count: %d", len(b.buffer))

	for objId, countermap := range b.buffer {
		log.Printf("[BufferBatch] Flushing ObjectId: %d, Counter count: %d", objId, len(countermap))
		for counterId, dataPoints := range countermap {
			if len(dataPoints) > 0 {
				log.Printf("[BufferBatch] Sending batch: ObjectId=%d, CounterId=%d, Point count: %d", objId, counterId, len(dataPoints))
				batch := WriteObjectWiseBatch{
					ObjectId: objId,
					CounterId: counterId,
					Values: dataPoints,
				}
				dataChannel <- batch
			}
		}
		delete(b.buffer, objId) // removing that objectid datapoints batch.
	}

	b.BuffEmpty = true
	log.Println("[BufferBatch] Flush complete.")
}

func batchBufferFlushRoutine(batchBuffer *BufferBatch, writersChannel chan<- WriteObjectWiseBatch, flushRoutineShutdown chan bool) {
	log.Println("[FlushRoutine] Starting batch buffer flush routine.")
	for {
		select {
		case <-flushRoutineShutdown:
			log.Println("[FlushRoutine] Received shutdown signal.")
			// Flush present entries and exit
			if !batchBuffer.BuffEmpty {
				log.Println("[FlushRoutine] Performing final flush before exit.")
				batchBuffer.Flush(writersChannel)
			}
			batchBuffer.flushTicker.Stop()
			flushRoutineShutdown <- true
			log.Println("[FlushRoutine] Exiting.")
			return
		case <-batchBuffer.flushTicker.C:
			if !batchBuffer.BuffEmpty {
				log.Println("[FlushRoutine] Flush triggered by timer.")
				batchBuffer.Flush(writersChannel)
			}
		}
	}
}

func StartWriteHandler(shutdownWaitGroup *sync.WaitGroup, dataWriteChannel <-chan []models.Metric) error {
	log.Println("[StartWriteHandler] Initializing...")
	defer shutdownWaitGroup.Done()

	writersChannel := make(chan WriteObjectWiseBatch, getBufferSize())
	flushRoutineShutdown := make(chan bool)
	var writersWaitGroup sync.WaitGroup

	writersCount := utils.GetWriters()
	log.Printf("[StartWriteHandler] Configured writers: %d", writersCount)
	writersWaitGroup.Add(writersCount)

	// Get storage path from config
	storagePath := utils.GetStoragePath() // Assuming this function exists or adding it
	if storagePath == "" {
		log.Printf("[StartWriteHandler] CRITICAL: Storage path not found in config.")
		return fmt.Errorf("storage path not configured")
	}

	storageEn, err := storageEngine.NewStorageEngine(storagePath)
	if err != nil {
		log.Printf("[StartWriteHandler] CRITICAL: Failed to initialize StorageEngine with path %s: %v", storagePath, err)
		return err
	}

	for i := 0; i < writersCount; i++ {
		log.Printf("[StartWriteHandler] Starting writer goroutine %d...", i)
		go writer(writersChannel, storageEn, &writersWaitGroup)
	}

	BufferBatch := NewBufferBatch()
	go batchBufferFlushRoutine(BufferBatch, writersChannel, flushRoutineShutdown)

	log.Println("[StartWriteHandler] Waiting for data on dataWriteChannel...")
	for polledData := range dataWriteChannel {
		log.Printf("[StartWriteHandler] Received polled data batch of size: %d", len(polledData))
		for _, dataPoint := range polledData {
			// log.Printf("[StartWriteHandler] Processing data point: %+v", dataPoint) // Optional: Verbose logging
			BufferBatch.AddData(
				dataPoint.ObjectID,
				dataPoint.CounterId,
				models.DataPoint{
					Timestamp: dataPoint.Timestamp,
					Value: dataPoint.Value,
				},
			)
		}
	}

	log.Println("[StartWriteHandler] dataWriteChannel closed. Shutting down...")
	// Channel Closed, Shutting down writer
	flushRoutineShutdown <- true

	// Wait for final flush
	log.Println("[StartWriteHandler] Waiting for final flush...")
	<-flushRoutineShutdown
	log.Println("[StartWriteHandler] Final flush complete.")

	// Close writers channel
	log.Println("[StartWriteHandler] Closing writers channel...")
	close(writersChannel)

	log.Println("[StartWriteHandler] Waiting for writer goroutines to finish...")
	writersWaitGroup.Wait()
	log.Println("[StartWriteHandler] Writer goroutines finished. Exiting.")

	return nil
}
