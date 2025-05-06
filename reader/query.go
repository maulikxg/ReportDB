package reader

import (
	. "packx/models"
	"packx/utils"
	"sync"
)

const (
	workerPoolSize = 5 // Number of workers per reader
)

type QueryEngine struct {
	readers     []*Reader
	workerPool  chan struct{}
	queryWg     sync.WaitGroup
	shutdownWg  *sync.WaitGroup
	resultMutex sync.RWMutex
}

func NewQueryEngine(shutdownWg *sync.WaitGroup) *QueryEngine {
	numReaders := utils.GetReaders()
	readers := make([]*Reader, numReaders)
	
	for i := 0; i < numReaders; i++ {
		readers[i] = &Reader{
			id:           uint8(i),
			queryChannel: make(chan Query, utils.GetBufferredChanSize()),
		}
	}

	return &QueryEngine{
		readers:    readers,
		workerPool: make(chan struct{}, workerPoolSize*numReaders),
		shutdownWg: shutdownWg,
	}
}

func InitQueryEngine(queryReceiveCh <-chan Query, queryResultCh chan<- QueryResponse, shutDownWg *sync.WaitGroup) {
	engine := NewQueryEngine(shutDownWg)
	defer shutDownWg.Done()

	// Start the query distributor
	go func() {
		for query := range queryReceiveCh {
			numReaders := uint8(utils.GetReaders())
			index := uint8(query.QueryID % uint64(numReaders))
			if index >= numReaders || index < 0 {
				continue
			}
			engine.readers[index].queryChannel <- query
		}
		// Close all reader channels when input channel is closed
		for _, r := range engine.readers {
			close(r.queryChannel)
		}
	}()

	// Start readers
	for _, r := range engine.readers {
		engine.shutdownWg.Add(1)
		go ProcessQueries(r, queryResultCh, engine)
	}

	engine.shutdownWg.Wait()
	close(queryResultCh)
}

func ProcessQueries(reader *Reader, resultCh chan<- QueryResponse, engine *QueryEngine) {
	defer engine.shutdownWg.Done()

	for query := range reader.queryChannel {
		engine.queryWg.Add(1)
		// Acquire worker from pool
		engine.workerPool <- struct{}{}

		go func(q Query) {
			defer func() {
				<-engine.workerPool // Release worker back to pool
				engine.queryWg.Done()
			}()

			response := processQuery(q)
			
			engine.resultMutex.Lock()
			resultCh <- response
			engine.resultMutex.Unlock()
		}(query)
	}

	engine.queryWg.Wait()
}
