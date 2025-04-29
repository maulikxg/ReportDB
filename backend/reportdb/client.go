package reportdb

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/pebbe/zmq4"
)

// DataPoint represents a single point of time series data
type DataPoint struct {
	Timestamp uint32 `json:"timestamp"`

	Value interface{} `json:"value"`
}

// Metric represents a metric data point to be stored
type Metric struct {
	ObjectID uint32 `json:"Object_id"`

	CounterId uint16 `json:"counter_id"`

	Value interface{} `json:"value"`

	Timestamp uint32 `json:"timestamp"`
}

// Query represents a query for metrics data
type Query struct {
	QueryID uint64 `json:"query_id"`

	From uint32 `json:"from"`

	To uint32 `json:"to"`

	ObjectIDs []uint32 `json:"Object_id"`

	CounterId uint16 `json:"counter_id"`

	Aggregation string `json:"aggregation"`
}

// QueryResponse represents a response to a metrics query
type QueryResponse struct {
	QueryID uint64 `json:"query_id"`

	Data map[uint32][]DataPoint `json:"data"`
}

// Client represents a client for querying the ReportDB
type Client struct {
	context *zmq4.Context

	sendSocket *zmq4.Socket

	recvSocket *zmq4.Socket

	metricsSocket *zmq4.Socket

	responses chan QueryResponse

	done chan struct{}

	queryID uint64
}

// New creates a new ReportDB client
func New() (*Client, error) {

	log.Println("Initializing ReportDB client...")

	context, err := zmq4.NewContext()

	if err != nil {
		return nil, fmt.Errorf("failed to create ZMQ context: %v", err)
	}

	// Socket for sending queries
	sendSocket, err := context.NewSocket(zmq4.PUSH)

	if err != nil {

		context.Term()

		return nil, fmt.Errorf("failed to create send socket: %v", err)

	}

	log.Println("Connecting to query server on tcp://localhost:8008...")

	if err := sendSocket.Connect("tcp://localhost:8008"); err != nil {

		sendSocket.Close()

		context.Term()

		return nil, fmt.Errorf("failed to connect send socket: %v", err)

	}

	// Socket for receiving responses
	recvSocket, err := context.NewSocket(zmq4.PULL)

	if err != nil {

		sendSocket.Close()

		context.Term()

		return nil, fmt.Errorf("failed to create receive socket: %v", err)

	}

	log.Println("Connecting to response server on tcp://localhost:8009...")

	if err := recvSocket.Connect("tcp://localhost:8009"); err != nil {

		recvSocket.Close()

		sendSocket.Close()

		context.Term()

		return nil, fmt.Errorf("failed to connect receive socket: %v", err)

	}

	// Socket for sending metrics to ReportDB
	metricsSocket, err := context.NewSocket(zmq4.PUSH)

	if err != nil {

		recvSocket.Close()

		sendSocket.Close()

		context.Term()

		return nil, fmt.Errorf("failed to create metrics socket: %v", err)
	}

	log.Println("Connecting to ReportDB external poller listener on tcp://localhost:5556...")

	if err := metricsSocket.Connect("tcp://localhost:5556"); err != nil {

		metricsSocket.Close()

		recvSocket.Close()

		sendSocket.Close()

		context.Term()

		return nil, fmt.Errorf("failed to connect metrics socket: %v", err)

	}

	log.Println("ReportDB client initialized successfully")

	client := &Client{

		context: context,

		sendSocket: sendSocket,

		recvSocket: recvSocket,

		metricsSocket: metricsSocket,

		responses: make(chan QueryResponse, 10),

		done: make(chan struct{}),

		queryID: 1,
	}

	// Start response receiver
	go client.receiveResponses()

	return client, nil
}

// SendMetric sends a metric to ReportDB
func (c *Client) SendMetric(metric Metric) error {

	if metric.Timestamp == 0 {

		metric.Timestamp = uint32(time.Now().Unix())

	}

	reportDBMetric := struct {
		ObjectID uint32 `json:"Object_id"`

		CounterId uint16 `json:"counter_id"`

		Value interface{} `json:"value"`

		Timestamp uint32 `json:"timestamp"`
	}{

		ObjectID: metric.ObjectID,

		CounterId: metric.CounterId,

		Value: metric.Value,

		Timestamp: metric.Timestamp,
	}

	metricBytes, err := json.Marshal(reportDBMetric)

	if err != nil {

		return fmt.Errorf("failed to marshal metric: %v", err)

	}

	// Send to ReportDB
	log.Printf("Sending metric to ReportDB: ObjectID=%d, CounterID=%d, Value=%v",
		metric.ObjectID, metric.CounterId, metric.Value)

	_, err = c.metricsSocket.SendBytes(metricBytes, 0)

	if err != nil {

		return fmt.Errorf("failed to send metric to ReportDB: %v", err)

	}

	return nil
}

// GetLatestMetrics gets the latest metrics for a given object and counter
func (c *Client) GetLatestMetrics(objectID uint32, counterID uint16) ([]DataPoint, error) {

	now := uint32(time.Now().Unix())

	oneHourAgo := uint32(time.Now().Add(-1 * time.Hour).Unix())

	// Create and send query
	queryID := c.nextQueryID()

	query := Query{

		QueryID: queryID,

		From: oneHourAgo,

		To: now,

		ObjectIDs: []uint32{objectID},

		CounterId: counterID,

		Aggregation: "avg",
	}

	resp, err := c.SendQuery(query)

	if err != nil {

		return nil, err

	}

	if data, ok := resp.Data[objectID]; ok {

		return data, nil

	}

	return []DataPoint{}, nil
}

// GetMetricsRange gets metrics for a given object, counter, and time range
func (c *Client) GetMetricsRange(objectID uint32, counterID uint16, from, to time.Time) ([]DataPoint, error) {

	// Convert times to Unix time
	fromUnix := uint32(from.Unix())

	toUnix := uint32(to.Unix())

	// Create and send query
	queryID := c.nextQueryID()

	query := Query{

		QueryID: queryID,

		From: fromUnix,

		To: toUnix,

		ObjectIDs: []uint32{objectID},

		CounterId: counterID,

		Aggregation: "avg",
	}

	resp, err := c.SendQuery(query)

	if err != nil {

		return nil, err

	}

	if data, ok := resp.Data[objectID]; ok {

		return data, nil

	}

	return []DataPoint{}, nil
}

// SendQuery sends a query to the ReportDB
func (c *Client) SendQuery(query Query) (*QueryResponse, error) {

	queryBytes, err := json.Marshal(query)

	if err != nil {

		return nil, fmt.Errorf("failed to marshal query: %v", err)

	}

	log.Printf("Sending query to ReportDB: %+v", query)

	_, err = c.sendSocket.SendBytes(queryBytes, 0)

	if err != nil {

		return nil, fmt.Errorf("failed to send query: %v", err)

	}

	log.Printf("Query sent successfully (ID: %d)", query.QueryID)

	// Wait for response with timeout
	log.Printf("Waiting for response to query ID: %d", query.QueryID)

	select {

	case response := <-c.responses:

		if response.QueryID == query.QueryID {

			log.Printf("Received matching response for query ID: %d", query.QueryID)

			return &response, nil

		}

		return nil, fmt.Errorf("received response for different query (expected: %d, got: %d)",
			query.QueryID, response.QueryID)

	case <-time.After(10 * time.Second):

		return nil, fmt.Errorf("timeout waiting for response to query ID: %d", query.QueryID)

	}
}

// receiveResponses receives responses from the ReportDB
func (c *Client) receiveResponses() {

	log.Println("Starting response receiver...")

	defer log.Println("Response receiver stopped")

	for {

		select {

		case <-c.done:

			return

		default:

			// Try to receive with timeout
			responseBytes, err := c.recvSocket.RecvBytes(zmq4.DONTWAIT)

			if err != nil {

				if err == zmq4.ErrorSocketClosed {

					log.Println("Response socket closed")

					return
				}

				if zmq4.AsErrno(err) == zmq4.Errno(11) { // EAGAIN

					time.Sleep(100 * time.Millisecond)

					continue
				}

				log.Printf("Error receiving response: %v", err)

				continue

			}

			var response QueryResponse

			if err := json.Unmarshal(responseBytes, &response); err != nil {

				log.Printf("Error unmarshalling response: %v", err)

				continue

			}

			log.Printf("Response received for QueryID: %d with %d objects",
				response.QueryID, len(response.Data))

			// Send to the responses channel with timeout
			select {

			case c.responses <- response:

			case <-time.After(5 * time.Second):

				log.Printf("Timeout sending response for QueryID %d to channel", response.QueryID)

			}

		}

	}

}

func (c *Client) nextQueryID() uint64 {

	c.queryID++

	return c.queryID

}

func (c *Client) Close() error {

	log.Println("Closing ReportDB client...")

	close(c.done)

	if err := c.sendSocket.Close(); err != nil {

		log.Printf("Error closing send socket: %v", err)

	}

	if err := c.recvSocket.Close(); err != nil {

		log.Printf("Error closing receive socket: %v", err)

	}

	if err := c.metricsSocket.Close(); err != nil {

		log.Printf("Error closing metrics socket: %v", err)

	}

	if err := c.context.Term(); err != nil {

		return fmt.Errorf("failed to terminate context: %v", err)
		
	}

	close(c.responses)

	log.Println("ReportDB client closed successfully")

	return nil

}
