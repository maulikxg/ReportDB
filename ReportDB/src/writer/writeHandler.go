package writer

import (
	"packx/models"
	"packx/storageEngine"
	"packx/utils"
	"sync"
	"time"
)

var (
	FlushDuration = time.Second * 2

	maxBatchSize = 1000 // maximum number of the Dataponts store per batch
)

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

	for objId, countermap := range b.buffer {

		for counterId, dataPoints := range countermap {

			if len(dataPoints) > 0 {

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

}

func batchBufferFlushRoutine(batchBuffer *BufferBatch, writersChannel chan<- WriteObjectWiseBatch, flushRoutineShutdown chan bool) {

	for {

		select {

		case <-flushRoutineShutdown:

			// Flush present entries and exit
			if !batchBuffer.BuffEmpty {

				batchBuffer.Flush(writersChannel)

			}

			batchBuffer.flushTicker.Stop()

			flushRoutineShutdown <- true

			return

		case <-batchBuffer.flushTicker.C:

			if !batchBuffer.BuffEmpty {

				batchBuffer.Flush(writersChannel)

			}

		}
	}
}

func StartWriteHandler(shutdownWaitGroup *sync.WaitGroup, dataWriteChannel <-chan []models.Metric) error {

	defer shutdownWaitGroup.Done()

	writersChannel := make(chan WriteObjectWiseBatch, getBufferSize())

	flushRoutineShutdown := make(chan bool)

	var writersWaitGroup sync.WaitGroup

	writersWaitGroup.Add(getBufferSize())

	storageEn, _ := storageEngine.NewStorageEngine()

	for i := 0; i < getBufferSize(); i++ {

		go writer(writersChannel, storageEn, &writersWaitGroup)

	}

	BufferBatch := NewBufferBatch()

	go batchBufferFlushRoutine(BufferBatch, writersChannel, flushRoutineShutdown)

	for polledData := range dataWriteChannel {

		for _, dataPoint := range polledData {

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

	// Channel Closed, Shutting down writer
	flushRoutineShutdown <- true

	// Wait for final flush
	<-flushRoutineShutdown

	// Close writers channel
	close(writersChannel)

	writersWaitGroup.Wait()

	return nil

}
