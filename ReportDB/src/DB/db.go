package DB

import (
	"go.uber.org/zap"
	"log"
	"os"
	"packx/models"
	"packx/reader"
	. "packx/utils"
	"packx/writer"
	"sync"
)

func InitDB(dataWriteCh <-chan []models.Metric, queryReceiveCh <-chan models.Query, queryResponseCh chan<- models.QueryResponse, globalShutDownWg *sync.WaitGroup) {

	defer globalShutDownWg.Done()

	defer Logger.Info("DB components shutting down.")

	log.Println("Initializing DB components...")

	storagePath := BaseDirProvider() + "/storage"

	if err := os.MkdirAll(storagePath, 0755); err != nil {

		//log.Printf("CRITICAL: Error creating storage directory %s: %v. DB initialization failed.", storagePath, err)
		//
		//return

		Logger.Error("Error creating storage directory", zap.String("path", storagePath), zap.Error(err))

	}

	//log.Printf("Storage directory checked/created: %s", storagePath)

	Logger.Info("Storage directory checked/created", zap.String("path", storagePath))

	var dbInternalWg sync.WaitGroup

	dbInternalWg.Add(2)

	go func() {

		err := writer.StartWriteHandler(&dbInternalWg, dataWriteCh)

		if err != nil {

			//log.Printf("CRITICAL: Writer Handler failed during initialization or runtime: %v", err)

			Logger.Info("CRITICAL: Writer Handler failed during initialization or runtime:", zap.Error(err))

		} else {

			//log.Println("Writer Handler exited normally.")

			Logger.Info("Writer Handler exited normally.")

		}

	}()

	// Initialize the query engine
	go reader.InitQueryEngine(queryReceiveCh, queryResponseCh, &dbInternalWg)

	//log.Println("DB Initialized. Waiting for Writer Handler and Query Engine to stop...")

	Logger.Info("DB Initialized. Waiting for Writer Handler and Query Engine to stop...")

	dbInternalWg.Wait()

	//log.Println("DB components (Writer, Query) shut down.")
}
