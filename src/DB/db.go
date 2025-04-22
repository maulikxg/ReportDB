package DB

import (
	"log"

	"packx/models"

	"packx/writer"
	"sync"
	"time"
)

func InitDB(dataWriteCh <-chan []models.Metric, queryReceiveCh <-chan models.Query, queryResponseCh chan<- models.QueryResponse, globalShutDownWg *sync.WaitGroup) {
	defer globalShutDownWg.Done()
	log.Println("Initializing DB components...")

	var dbInternalWg sync.WaitGroup
	dbInternalWg.Add(2)

	go func() {
		err := writer.StartWriteHandler(&dbInternalWg, dataWriteCh)
		if err != nil {
			log.Printf("CRITICAL: Writer Handler failed during initialization or runtime: %v", err)
		} else {
			log.Println("Writer Handler exited normally.")
		}
	}()

	go func() {
		defer dbInternalWg.Done()
		log.Println("Starting Query Engine... (placeholder)")
		<-time.After(1 * time.Second)
		log.Println("Query Engine stopped. (placeholder)")
	}()

	log.Println("DB Initialized. Waiting for DB components to stop...")
	dbInternalWg.Wait()
	log.Println("DB components shut down.")
}
