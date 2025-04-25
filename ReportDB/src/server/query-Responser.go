package server

import (
	"encoding/json"
	zmq "github.com/pebbe/zmq4"
	"log"
	"packx/models"
	"sync"
	"time"
)

func InitQueryResponser(queryResultChannel <-chan models.QueryResponse, globalShutdownWaitGroup *sync.WaitGroup) {

	defer globalShutdownWaitGroup.Done()

	log.Println("Initializing query responser...")

	context, err := zmq.NewContext()

	if err != nil {

		log.Printf("Error initializing query result publisher context: %v", err)

		return

	}

	defer context.Term()

	socket, err := context.NewSocket(zmq.PUSH)

	if err != nil {

		log.Printf("Error initializing query result publisher socket: %v", err)

		return
	}

	defer socket.Close()

	err = socket.Bind("tcp://*:8009")

	if err != nil {

		log.Printf("Error binding query result publisher socket: %v", err)

		return

	}

	log.Println("Query responser started on tcp://*:8009")

	// Channel to signal shutdown
	shutdown := make(chan struct{})

	go func() {

		for {

			select {

			case <-shutdown:

				log.Println("Query responser shutting down")

				return

			case result, ok := <-queryResultChannel:

				if !ok {

					log.Println("Query result channel closed")

					return

				}

				log.Printf("Preparing to send response for QueryID: %d with %d objects",
					result.QueryID, len(result.Data))

				resultBytes, err := json.Marshal(result)

				if err != nil {

					log.Printf("Error marshalling query result: %v", err)

					continue

				}

				var sendErr error

				for retries := 0; retries < 3; retries++ {

					if retries > 0 {

						log.Printf("Retrying send for QueryID %d (attempt %d)", result.QueryID, retries+1)

					}

					_, sendErr = socket.SendBytes(resultBytes, zmq.DONTWAIT)

					if sendErr == nil {

						log.Printf("Successfully sent response for QueryID: %d", result.QueryID)

						break

					}

					if retries < 2 {

						time.Sleep(100 * time.Millisecond)

					}
				}

				if sendErr != nil {

					log.Printf("Failed to send response for QueryID %d after retries: %v",
						result.QueryID, sendErr)

				}

			}

		}

	}()

	// Wait for shutdown signal
	<-shutdown

	log.Println("Query responser cleanup complete")
}
