package main

import (
	"fmt"
	"log"
	. "packx/DB"
	. "packx/models"
	"packx/polling"
	"packx/server"

	//	. "packx/server"
	. "packx/utils"
	"sync"
	"time"
)

var pollData chan Metric

var wg sync.WaitGroup

func main() {
	fmt.Println("Hello world ")

	err := LoadConfig() // loading all the configurations

	if err != nil {

		log.Println("Error loading config:", err)

		return

	}

	pollData = make(chan Metric, GetBufferredChanSize()) // for the Polling data making channel

	var globalShutDownWg sync.WaitGroup

	dataWriteCh := make(chan []Metric, GetBufferredChanSize())

	queryReceiveCh := make(chan Query, GetBufferredChanSize())

	queryResponseCh := make(chan QueryResponse, GetBufferredChanSize())

	globalShutDownWg.Add(3)

	// Start the DB components
	go InitDB(dataWriteCh, queryReceiveCh, queryResponseCh, &globalShutDownWg)

	// Start the poll listener to forward data to writer
	go func() {
		log.Println("[PollListener] Starting...")
		defer globalShutDownWg.Done()
		defer close(dataWriteCh)

		// Configure batching parameters
		maxBatchSize := 100             // Send batch when it reaches this size
		maxBatchWait := 1 * time.Second // Or send after this duration

		batch := make([]Metric, 0, maxBatchSize)
		ticker := time.NewTicker(maxBatchWait)
		defer ticker.Stop()

		for {
			select {
			case metric, ok := <-pollData:
				if !ok { // pollData channel closed
					log.Println("[PollListener] pollData channel closed. Sending final batch...")
					if len(batch) > 0 {
						log.Printf("[PollListener] Sending final batch of size %d", len(batch))
						dataWriteCh <- batch
					}
					log.Println("[PollListener] Exiting.")
					return
				}

				// Add metric to batch
				batch = append(batch, metric)
				log.Printf("[PollListener] Added metric to batch. Current size: %d", len(batch))

				// Send if batch is full
				if len(batch) >= maxBatchSize {
					log.Printf("[PollListener] Batch full. Sending batch of size %d", len(batch))
					dataWriteCh <- batch
					batch = make([]Metric, 0, maxBatchSize) // Reset batch
				}

			case <-ticker.C: // Timer ticked
				if len(batch) > 0 {
					log.Printf("[PollListener] Timer ticked. Sending batch of size %d", len(batch))
					dataWriteCh <- batch
					batch = make([]Metric, 0, maxBatchSize) // Reset batch
				}
			}
		}
	}()

	// Start the server and polling
	go server.ServerPull(pollData)

	go polling.PollCPUData(pollData)

	// Start other components
	go func() {
		defer globalShutDownWg.Done()
		// InitQueryListener placeholder
		<-make(chan struct{})
	}()

	go func() {
		defer globalShutDownWg.Done()
		// InitQueryResponser placeholder
		<-make(chan struct{})
	}()

	globalShutDownWg.Wait()
	
	select {}
}
