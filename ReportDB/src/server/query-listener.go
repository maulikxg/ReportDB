package server

import (
	"encoding/json"
	"errors"
	zmq "github.com/pebbe/zmq4"
	"log"
	"packx/models"
	"sync"
	"time"
)

func InitQueryListener(queryReceiveChannel chan<- models.Query, globalShutdownWaitGroup *sync.WaitGroup) {

	defer globalShutdownWaitGroup.Done()

	context, err := zmq.NewContext()

	if err != nil {

		log.Printf("Error initializing query listener context: %v", err)

		return

	}

	socket, err := context.NewSocket(zmq.PULL)
	if err != nil {
		log.Printf("Error initializing query listener socket: %v", err)
		context.Term()
		return
	}
	defer socket.Close()

	err = socket.Bind("tcp://*:8008")

	if err != nil {

		log.Printf("Error binding query listener socket: %v", err)

		return

	}

	log.Println("Query listener started on tcp://*:8008")

	// Channel to signal shutdown
	shutdown := make(chan struct{})

	go func() {

		defer close(queryReceiveChannel)

		for {

			select {

			case <-shutdown:

				log.Println("Query listener shutting down")

				return

			default:

				queryBytes, err := socket.RecvBytes(zmq.DONTWAIT)

				if err != nil {

					if zmq.AsErrno(err) == zmq.Errno(11) { // EAGAIN

						// No message available, sleep briefly and continue

						time.Sleep(100 * time.Millisecond)

						continue

					}

					if errors.Is(zmq.AsErrno(err), zmq.ETERM) {

						log.Println("ZMQ context terminated, closing query listener")

						return

					}

					log.Printf("Error receiving query: %v", err)

					continue
				}

				var query models.Query

				if err = json.Unmarshal(queryBytes, &query); err != nil {

					log.Printf("Error unmarshalling query: %v", err)

					continue

				}

				log.Printf("Received query: %+v", query)

				queryReceiveChannel <- query

			}

		}

	}()

	// Wait for shutdown signal
	<-shutdown

	// Clean up
	if err := socket.Close(); err != nil {
		log.Printf("Error closing query listener socket: %v", err)
	}

	if err := context.Term(); err != nil {
		log.Printf("Error terminating query listener context: %v", err)
	}
}
