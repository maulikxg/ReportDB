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

	go server.ServerPull(pollData) // Start the pull server

	go polling.PollData(wg)

	var globalShutDownWg sync.WaitGroup

	dataWriteCh := make(chan []Metric, GetBufferredChanSize())

	queryReceiveCh := make(chan Query, GetBufferredChanSize())

	queryResponseCh := make(chan QueryResponse, GetBufferredChanSize())

	globalShutDownWg.Add(4)

	go InitDB(dataWriteCh, queryReceiveCh, queryResponseCh, &globalShutDownWg)

	//go InitPollListener(dataWriteCh, &globalShutDownWg)

	//go InitQueryListener(queryReceiveCh, &globalShutDownWg)

	//go InitQueryResponser(queryResponseCh, &globalShutDownWg)

	close(dataWriteCh)

	globalShutDownWg.Wait()

	select {}

}
