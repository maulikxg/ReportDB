package utils

import (
	"encoding/json"
	"fmt"
	"log"
	"packxpoller/collector"
	"time"

	zmq "github.com/pebbe/zmq4"
)

// Metric structure for sending to ReportDB
type Metric struct {
	ObjectID  uint32      `json:"Object_id"`
	CounterId uint16      `json:"counter_id"`
	Value     interface{} `json:"value"`
	Timestamp uint32      `json:"timestamp"`
}

var (
	pushSocket *zmq.Socket
	zmqContext *zmq.Context
)

func InitZMQSender() error {

	var err error

	zmqContext, err = zmq.NewContext()

	if err != nil {
		return fmt.Errorf("failed to create ZMQ context: %v", err)
	}

	pushSocket, err = zmqContext.NewSocket(zmq.PUSH)

	if err != nil {

		zmqContext.Term()

		return fmt.Errorf("failed to create ZMQ PUSH socket: %v", err)

	}

	// Connect to Backend's metrics receiver PULL server
	if err := pushSocket.Connect("tcp://localhost:5558"); err != nil {

		pushSocket.Close()

		zmqContext.Term()

		return fmt.Errorf("failed to connect to backend metrics receiver: %v", err)

	}

	log.Println("ZMQ sender initialized and connected to backend metrics receiver at tcp://localhost:5558")

	return nil
}

func CloseZMQSender() {

	if pushSocket != nil {

		pushSocket.Close()

	}

	if zmqContext != nil {

		zmqContext.Term()

	}
}

func SendMetrics(metrics *collector.Metrics, deviceIndex int) error {

	// Ensure we have a valid objectID
	objectID := uint32(deviceIndex)
	
	// Log the device ID we're using
	log.Printf("Using ObjectID %d for device %s", objectID, metrics.DeviceID)

	// Send CPU metrics (CounterId 1 expects float)
	cpuMetric := Metric{
		ObjectID:  objectID,
		CounterId: 1, // Using 1 for CPU metrics (expects float)
		Value:     float64(metrics.CPU.Usage), // Explicitly cast to float64 to ensure correct type
		Timestamp: uint32(time.Now().Unix()),
	}

	if err := sendMetric(cpuMetric); err != nil {
		return fmt.Errorf("failed to send CPU metric: %v", err)
	}

	// Send memory metrics (CounterId 2 expects int)
	memMetric := Metric{
		ObjectID:  objectID, 
		CounterId: 2,       // Using 2 for memory metrics (expects int)
		Value:     int64(metrics.Memory.Used), // Cast uint64 to int64 to match expected type
		Timestamp: uint32(time.Now().Unix()),
	}

	if err := sendMetric(memMetric); err != nil {
		return fmt.Errorf("failed to send memory metric: %v", err)
	}

	return nil
}

func sendMetric(metric Metric) error {

	jsonData, err := json.Marshal(metric)

	if err != nil {
		return fmt.Errorf("failed to marshal metric: %v", err)
	}

	_, err = pushSocket.SendBytes(jsonData, 0)
	
	if err != nil {
		return fmt.Errorf("failed to send metric through ZMQ: %v", err)
	}

	log.Printf("Sent metric to ReportDB: deviceID=%d, counterID=%d, value=%v",
		metric.ObjectID, metric.CounterId, metric.Value)

	return nil
}
