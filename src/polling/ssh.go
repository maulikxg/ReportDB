package polling

import (
	"encoding/json"
	zmq "github.com/pebbe/zmq4"
	"log"
	"packx/models"

	//. "packx/storageEngine"
	"sync"
)

type data struct {
	ObjectID uint32 `json:"Object_id"`

	CounterId uint16 `json:"counter_id"`

	Value interface{} `json:"value"`

	Timestamp uint32 `json:"timestamp"`
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

	// Initialize ZMQ
	if err := initZmq(); err != nil {
		log.Printf("Failed to initialize ZMQ: %v", err)
		return
	}
	defer pushSocket.Close()
	defer zmqContext.Term()

	// Create channel for metrics
	pollData := make(chan models.Metric, 100)

	// Start CPU polling
	go PollCPUData(pollData)

	// Send metrics through ZMQ socket
	for metric := range pollData {
		// Convert metric to data format
		metricData := data{
			ObjectID:  metric.ObjectID,
			CounterId: metric.CounterId,
			Value:     metric.Value,
			Timestamp: metric.Timestamp,
		}

		// Marshal to JSON for sending
		jsonData, err := json.Marshal(metricData)
		if err != nil {
			log.Printf("Error marshaling metric: %v", err)
			continue
		}

		// Send through ZMQ socket
		_, err = pushSocket.SendBytes(jsonData, 0)
		if err != nil {
			log.Printf("Error sending metric through ZMQ: %v", err)
			continue
		}

		log.Printf("Sent metric through ZMQ: counterID=%d , DeviceID=%d, CPU=%.2f%%",
			metric.CounterId, metric.ObjectID, metric.Value)
	}
}

//func generateMetric(deviceID, counterID int) data {
//
//	metric := data{
//
//		DeviceID: deviceID,
//
//		CounterID: counterID,
//
//		Timestamp: time.Now().Unix(),
//	}
//
//	switch counterID {
//
//	case 1:
//
//		metric.Value = int64(42 + deviceID)
//
//	case 2: // TypeFloat (float64)
//
//		metric.Value = 3.14 * float64(deviceID+1)
//
//	case 3: // TypeString (string)
//
//		metric.Value = fmt.Sprintf("Device-%d-ABC", deviceID)
//
//	default:
//
//		log.Printf("Unhandled counter ID: %d", counterID)
//
//		metric.Value = nil
//
//	}
//
//	return metric
//}

//func GenerateRandomString(length int) string {
//
//	const characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
//
//	buff := make([]byte, length)
//
//	for i := range buff {
//
//		buff[i] = characters[rand.Intn(len(characters))]
//
//	}
//
//	return string(buff)
//}

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
