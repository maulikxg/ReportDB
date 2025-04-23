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
	dataWriteCh := make(chan []Metric, GetBufferredChanSize())
	queryReceiveCh := make(chan Query, GetBufferredChanSize())
	queryResponseCh := make(chan QueryResponse, GetBufferredChanSize())

	var globalShutDownWg sync.WaitGroup
	globalShutDownWg.Add(4)

	// Start the pull server
	go server.PullServer(pollData)

	// Start polling
	go polling.PollData(wg)

	// Forward data from pollData to dataWriteCh
	go func() {
		defer globalShutDownWg.Done()
		buffer := make([]Metric, 0, 10) // Buffer to accumulate metrics
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case metric, ok := <-pollData:
				if !ok {
					// Channel closed, flush remaining buffer
					if len(buffer) > 0 {
						dataWriteCh <- buffer
					}
					return
				}
				buffer = append(buffer, metric)
				if len(buffer) >= 10 { // Flush when buffer is full
					dataWriteCh <- buffer
					buffer = make([]Metric, 0, 10)
				}
			case <-ticker.C:
				// Flush buffer periodically even if not full
				if len(buffer) > 0 {
					dataWriteCh <- buffer
					buffer = make([]Metric, 0, 10)
				}
			}
		}
	}()

	go InitDB(dataWriteCh, queryReceiveCh, queryResponseCh, &globalShutDownWg)

	//go InitPollListener(dataWriteCh, &globalShutDownWg)

	//go InitQueryListener(queryReceiveCh, &globalShutDownWg)

	//go InitQueryResponser(queryResponseCh, &globalShutDownWg)

	// Wait for all goroutines to finish
	globalShutDownWg.Wait()
	
	select {}
}
