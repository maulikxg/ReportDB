package polling

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"log"
	"math/rand"
	"packx/models"

	//. "packx/storageEngine"
	"sync"
	"time"
)

type data struct {
	DeviceID  int
	CounterID int
	Value     interface{}
	Timestamp int64
}

var (
	pushSocket *zmq.Socket
	zmqContext *zmq.Context
)

func initZmq() error {

	var err error

	zmqContext, err = zmq.NewContext()

	if err != nil {

		return err

	}

	pushSocket, err = zmqContext.NewSocket(zmq.PUSH)

	if err != nil {

		return err

	}

	// Connect to the PULL server
	if err := pushSocket.Connect("tcp://localhost:5555"); err != nil {

		return err

	}

	log.Println("PUSH Client connected to tcp://localhost:5555")

	return nil

}

func PollData(wg sync.WaitGroup) {
	defer wg.Done()

	// Create channel for metrics
	pollData := make(chan models.Metric, 100)

	// Start CPU polling
	go PollCPUData(pollData)

	// Your existing push socket logic remains unchanged
	for metric := range pollData {
		// This will use your existing push socket mechanism
		// to send the metrics to your storage
		log.Printf("Sending metric: DeviceID=%d, CPU=%.2f%%",
			metric.ObjectID, metric.Value)
	}
}

func generateMetric(deviceID, counterID int) data {

	metric := data{

		DeviceID: deviceID,

		CounterID: counterID,

		Timestamp: time.Now().Unix(),
	}

	switch counterID {

	case 1:

		metric.Value = int64(42 + deviceID)

	case 2: // TypeFloat (float64)

		metric.Value = 3.14 * float64(deviceID+1)

	case 3: // TypeString (string)

		metric.Value = fmt.Sprintf("Device-%d-ABC", deviceID)

	default:

		log.Printf("Unhandled counter ID: %d", counterID)

		metric.Value = nil

	}

	return metric
}

func GenerateRandomString(length int) string {

	const characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	buff := make([]byte, length)

	for i := range buff {

		buff[i] = characters[rand.Intn(len(characters))]

	}

	return string(buff)
}

//	func generateMetric(deviceID, counterID int) data {
//		metric := data{
//			DeviceID:  deviceID,
//			CounterID: counterID,
//			Timestamp: time.Now().Unix(),
//		}
//
//		counterType, err := GetCounterType(counterID)
//		if err != nil {
//			log.Printf("Error getting counter value: %v", err)
//			metric.Value = nil
//			return metric
//		}
//
//		// Generate value based on counter type
//		switch counterType {
//		case TypeInt:
//			metric.Value = rand.Int63() // int64 value (matches expected type for CounterID 1)
//		case TypeFloat:
//			metric.Value = rand.Float64() * 100 // float64 value (matches expected type for CounterID 2)
//		case TypeString:
//			metric.Value = GenerateRandomString(10) // string value (matches expected type for CounterID 3)
//		default:
//			log.Printf("Unhandled counter type: %d", counterType)
//			metric.Value = nil
//		}
//
//		return metric
//	}
