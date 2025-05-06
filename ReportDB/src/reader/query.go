package reader

import (
	"log"
	"packx/models"
	"packx/utils"
	"sync"
)

const (
	workerPoolSize = 5 // Number of workers per reader

)

type QueryEngine struct {
	readers []*Reader

	workerPool chan struct{}

	queryWg sync.WaitGroup

	shutdownWg *sync.WaitGroup

	resultMutex sync.RWMutex
}

type Reader struct {
	id uint8

	queryChannel chan models.Query
}

func NewQueryEngine(shutdownWg *sync.WaitGroup) *QueryEngine {

	numReaders := utils.GetReaders()

	readers := make([]*Reader, numReaders)

	for i := 0; i < numReaders; i++ {

		readers[i] = &Reader{

			id: uint8(i),

			queryChannel: make(chan models.Query, utils.GetBufferredChanSize()),
		}
	}

	return &QueryEngine{

		readers: readers,

		workerPool: make(chan struct{}, workerPoolSize*numReaders),

		shutdownWg: shutdownWg,
	}
}

func InitQueryEngine(queryReceiveCh <-chan models.Query, queryResultCh chan<- models.QueryResponse, shutDownWg *sync.WaitGroup) {

	log.Println("Starting query engine...")

	engine := NewQueryEngine(shutDownWg)

	defer shutDownWg.Done()

	// query distributor
	go func() {

		log.Println("Query distributor started")

		for query := range queryReceiveCh {

			log.Printf("Query engine received query: %+v", query)

			numReaders := uint8(utils.GetReaders())

			index := uint8(query.QueryID % uint64(numReaders))

			if index >= numReaders || index < 0 {

				log.Printf("Query index out of range: %d", index)

				continue

			}

			log.Printf("Distributing query %d to reader %d", query.QueryID, index)

			engine.readers[index].queryChannel <- query

		}

		// Closeing all reader channels when input channel is closed
		log.Println("Query receive channel closed, closing all reader channels")

		for _, r := range engine.readers {

			close(r.queryChannel)

		}

	}()

	// Start readers
	for i, r := range engine.readers {

		engine.shutdownWg.Add(1)

		go ProcessQueries(r, queryResultCh, engine, i)

	}

	engine.shutdownWg.Wait()

	log.Println("Query engine shutting down")

	close(queryResultCh)

}

func ProcessQueries(reader *Reader, resultCh chan<- models.QueryResponse, engine *QueryEngine, readerID int) {

	defer engine.shutdownWg.Done()

	log.Printf("Reader %d started", readerID)

	for query := range reader.queryChannel {

		log.Printf("Reader %d processing query: %+v", readerID, query)

		engine.queryWg.Add(1)

		// Acquire worker from pool
		engine.workerPool <- struct{}{}

		go func(q models.Query) {

			defer func() {

				<-engine.workerPool // Release worker back to pool

				engine.queryWg.Done()

			}()

			log.Printf("Worker processing query ID: %d", q.QueryID)

			response := processQuery(q)

			log.Printf("Query %d processed with %d results", q.QueryID, len(response.Data))

			engine.resultMutex.Lock()

			resultCh <- response

			engine.resultMutex.Unlock()

			log.Printf("Response for query %d sent to result channel", q.QueryID)

		}(query)

	}

	log.Printf("Reader %d channel closed, waiting for pending queries", readerID)

	engine.queryWg.Wait()

	log.Printf("Reader %d stopped", readerID)

}
