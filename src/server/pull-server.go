package server

import (
	"encoding/json"
	zmq "github.com/pebbe/zmq4"
	"log"
	"packx/models"
	//"packx/storageEngine"
)

//func InitPollListener()

func ServerPull(pollData chan<- models.Metric) {

	context, err := zmq.NewContext()

	if err != nil {

		log.Fatal("Fatal error in the Context Loading: ", err)

	}

	defer context.Term()

	socket, err := context.NewSocket(zmq.PULL)

	if err != nil {

		log.Fatal("Fatal error in the Socket Loading: ", err)

	}

	defer socket.Close()

	if err := socket.Bind("tcp://*:5555"); err != nil {

		log.Fatal("Failed to bind PULL socket:", err)
	}

	log.Println("PULL Server started on tcp://*:5555")

	for {

		msgBytes, err := socket.RecvBytes(0)

		if err != nil {

			log.Println("Error receiving message:", err)

			continue
		}

		var metric models.Metric

		if err := json.Unmarshal(msgBytes, &metric); err != nil {

			log.Println("Error unmarshaling message:", err)

			continue

		}

		// Send to channel for processing
		select {

		case pollData <- metric:

			log.Printf("Received and queued metric: DeviceID=%d, CounterID=%d, Value=%v",
				metric.ObjectID, metric.CounterId, metric.Value)

		default:
			log.Printf("Channel full, dropping metric: DeviceID=%d", metric.ObjectID)
		}

	}

}
