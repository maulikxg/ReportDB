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

	// Connect to ReportDB's PULL server
	if err := pushSocket.Connect("tcp://localhost:5556"); err != nil {

		pushSocket.Close()

		zmqContext.Term()

		return fmt.Errorf("failed to connect to ReportDB: %v", err)

	}

	log.Println("ZMQ sender initialized and connected to ReportDB at tcp://localhost:5556")

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

	// Send CPU metrics
	cpuMetric := Metric{

		ObjectID:  uint32(deviceIndex),
		CounterId: 1, // Using 1 for CPU metrics
		Value:     metrics.CPU.Usage,
		Timestamp: uint32(time.Now().Unix()),
	}

	if err := sendMetric(cpuMetric); err != nil {
		return fmt.Errorf("failed to send CPU metric: %v", err)
	}

	// Send memory metrics
	memMetric := Metric{
		ObjectID:  uint32(deviceIndex), // Using device index as ObjectID
		CounterId: 2,                   // Using 2 for memory metrics
		Value:     int64(metrics.Memory.Used),
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
