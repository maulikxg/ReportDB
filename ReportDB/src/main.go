package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	. "packx/DB"
	. "packx/models"
	. "packx/server"

	//	. "packx/server"
	. "packx/utils"
	"sync"
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

	//query := Query{
	//	QueryID: 1,
	//
	//	//From: uint32(time.Now().Add(-5 * time.Second).Unix()),
	//	//
	//	//To: uint32(time.Now().Unix()),
	//
	//	From: 1745400861,
	//
	//	To: 1745400889,
	//
	//	ObjectIDs: []uint32{1},
	//
	//	CounterId: 1,
	//
	//	Aggregation: "avg",
	//}

	pollData = make(chan Metric, GetBufferredChanSize()) // for the Polling data making channel

	dataWriteCh := make(chan []Metric, GetBufferredChanSize())

	queryReceiveCh := make(chan Query, GetBufferredChanSize())

	queryResponseCh := make(chan QueryResponse, GetBufferredChanSize())

	var globalShutDownWg sync.WaitGroup

	globalShutDownWg.Add(4)

	// Start the pull server
	go PullServer(pollData)

	// Start polling
	//go PollData(&wg)

	// Forward data from pollData to dataWriteCh
	//go func() {
	//
	//	defer globalShutDownWg.Done()
	//
	//	buffer := make([]Metric, 0, 10) // Buffer to accumulate metrics
	//
	//	ticker := time.NewTicker(1 * time.Second)
	//
	//	defer ticker.Stop()
	//
	//	for {
	//
	//		select {
	//
	//		case metric, ok := <-pollData:
	//
	//			if !ok {
	//
	//				// Channel closed, flush remaining buffer
	//				if len(buffer) > 0 {
	//
	//					dataWriteCh <- buffer
	//
	//				}
	//
	//				return
	//			}
	//
	//			buffer = append(buffer, metric)
	//
	//			if len(buffer) >= 10 { // Flush when buffer is full
	//
	//				dataWriteCh <- buffer
	//
	//				buffer = make([]Metric, 0, 10)
	//
	//			}
	//
	//		case <-ticker.C:
	//
	//			// Flush buffer periodically even if not full
	//			if len(buffer) > 0 {
	//
	//				dataWriteCh <- buffer
	//
	//				buffer = make([]Metric, 0, 10)
	//
	//			}
	//		}
	//	}
	//}()

	go InitDB(dataWriteCh, queryReceiveCh, queryResponseCh, &globalShutDownWg)

	go InitPollListener(dataWriteCh, &globalShutDownWg)

	go InitQueryListener(queryReceiveCh, &globalShutDownWg)

	go InitQueryResponser(queryResponseCh, &globalShutDownWg)
	go InitProfiling()
	//queryReceiveCh <- query

	// Wait for all goroutines to finish
	globalShutDownWg.Wait()

	select {}
}

func InitProfiling() {

	err := http.ListenAndServe("localhost:1234", nil)

	if err != nil {

		log.Println("Error starting profiling:", err)

	}
}
