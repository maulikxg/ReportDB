package server

import (
	"encoding/json"
	zmq "github.com/pebbe/zmq4"
	"log"
	"packx/models"
	"sync"
	"time"
	//"packx/storageEngine"
)

func InitPollListener(dataWriteCh chan<- []models.Metric, globalShutdownWaitGroup *sync.WaitGroup) {

	defer globalShutdownWaitGroup.Done()

	log.Println("Poll listener started - monitoring for metrics from external pollers")

	buffer := make([]models.Metric, 0, 10)

	ticker := time.NewTicker(1 * time.Second)

	defer ticker.Stop()

	externalPollerCh := make(chan models.Metric, 100)

	go func() {

		pullContext, err := zmq.NewContext()

		if err != nil {

			log.Printf("Error creating ZMQ context for external poller listener: %v", err)

			return

		}

		defer pullContext.Term()

		pullSocket, err := pullContext.NewSocket(zmq.PULL)

		if err != nil {

			log.Printf("Error creating ZMQ socket for external poller listener: %v", err)

			return

		}

		defer pullSocket.Close()

		if err := pullSocket.Bind("tcp://*:5556"); err != nil {

			log.Printf("Error binding ZMQ socket for external poller listener: %v", err)

			return

		}

		log.Println("External poller listener started on tcp://*:5556")

		for {

			msgBytes, err := pullSocket.RecvBytes(0)

			if err != nil {

				log.Printf("Error receiving message from external poller: %v", err)

				continue

			}

			var metric models.Metric

			if err := json.Unmarshal(msgBytes, &metric); err != nil {

				log.Printf("Error unmarshaling message from external poller: %v", err)

				continue

			}

			log.Printf("Received external poller metric: DeviceID=%d, CounterID=%d, Value=%v",
				metric.ObjectID, metric.CounterId, metric.Value)

			select {

			case externalPollerCh <- metric:

			default:
				log.Printf("External poller channel full, dropping metric: DeviceID=%d", metric.ObjectID)
			}

		}

	}()

	for {

		select {

		case metric := <-externalPollerCh:

			buffer = append(buffer, metric)

			// If buffer is full, send to writer
			if len(buffer) >= 10 {

				metricsCopy := make([]models.Metric, len(buffer))

				copy(metricsCopy, buffer)

				dataWriteCh <- metricsCopy

				log.Printf("Poll listener sent %d metrics to writer (buffer full)", len(buffer))

				// Clear buffer
				buffer = make([]models.Metric, 0, 10)
			}

		case <-ticker.C:
			// Periodic flush if buffer has any items

			if len(buffer) > 0 {

				metricsCopy := make([]models.Metric, len(buffer))

				copy(metricsCopy, buffer)

				dataWriteCh <- metricsCopy

				log.Printf("Poll listener sent %d metrics to writer (periodic flush)", len(buffer))

				// Clear buffer
				buffer = make([]models.Metric, 0, 10)
			}
		}
	}
}

func PullServer(pollData chan<- models.Metric) {

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

		log.Println("Received metric:", metric)

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
