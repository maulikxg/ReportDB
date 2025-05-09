package main

import (
	"fmt"
	"log"
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

		log.Fatal("Error loading config:", err)

		return

	}

	err = InitLogger() // loading all Logger configurations

	if err != nil {

		log.Fatal("Error loading logger:", err)

		return

	}

	pollData = make(chan Metric, GetBufferredChanSize()) // for the Polling data making channel

	dataWriteCh := make(chan []Metric, GetBufferredChanSize())

	queryReceiveCh := make(chan Query, GetBufferredChanSize())

	queryResponseCh := make(chan QueryResponse, GetBufferredChanSize())

	var globalShutDownWg sync.WaitGroup

	globalShutDownWg.Add(4)

	go PullServer(pollData)

	go InitDB(dataWriteCh, queryReceiveCh, queryResponseCh, &globalShutDownWg)

	go InitPollListener(dataWriteCh, &globalShutDownWg)

	go InitQueryListener(queryReceiveCh, &globalShutDownWg)

	go InitQueryResponser(queryResponseCh, &globalShutDownWg)

	go InitProfiling()

	globalShutDownWg.Wait()

	select {}
}
