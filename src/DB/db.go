package DB

import (
	"log"
	"os"
	"packx/models"
	"packx/utils"
	"packx/writer"
	"sync"
)

func InitDB(dataWriteCh <-chan []models.Metric, queryReceiveCh <-chan models.Query, queryResponseCh chan<- models.QueryResponse, globalShutDownWg *sync.WaitGroup) {

	defer globalShutDownWg.Done()

	log.Println("Initializing DB components...")

	storagePath := utils.BaseDirProvider() + "/storage"

	if err := os.MkdirAll(storagePath, 0755); err != nil {

		log.Printf("CRITICAL: Error creating storage directory %s: %v. DB initialization failed.", storagePath, err)

		return

	}

	log.Printf("Storage directory checked/created: %s", storagePath)

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

		<-make(chan struct{})

		log.Println("Query Engine stopped. (placeholder)")

	}()

	log.Println("DB Initialized. Waiting for Writer Handler and Query Engine to stop...")

	dbInternalWg.Wait()

	log.Println("DB components (Writer, Query) shut down.")
}
