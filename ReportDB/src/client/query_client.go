package client

import (
	"encoding/json"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"log"
	"packx/models"
	"time"
)

// QueryClient represents a client that can send queries and receive results
type QueryClient struct {
	context    *zmq.Context
	sendSocket *zmq.Socket
	recvSocket *zmq.Socket
	responses  chan models.QueryResponse
	done       chan struct{}
}

// NewQueryClient creates a new query client
func NewQueryClient() (*QueryClient, error) {

	log.Println("Initializing query client...")

	context, err := zmq.NewContext()

	if err != nil {

		return nil, fmt.Errorf("failed to create ZMQ context: %v", err)

	}

	// Socket for sending queries
	sendSocket, err := context.NewSocket(zmq.PUSH)

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
	recvSocket, err := context.NewSocket(zmq.PULL)

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

	log.Println("Query client initialized successfully")

	client := &QueryClient{
		context:    context,
		sendSocket: sendSocket,
		recvSocket: recvSocket,
		responses:  make(chan models.QueryResponse),
		done:       make(chan struct{}),
	}

	// Start response receiver
	go client.receiveResponses()

	return client, nil
}

// SendQuery sends a query to the server and waits for response
func (c *QueryClient) SendQuery(query models.Query) (*models.QueryResponse, error) {
	queryBytes, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %v", err)
	}

	log.Printf("Sending query to server: %+v", query)
	_, err = c.sendSocket.SendBytes(queryBytes, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to send query: %v", err)
	}

	log.Printf("Query sent successfully (ID: %d)", query.QueryID)

	// Wait for response with timeout
	log.Printf("Waiting for response to query ID: %d", query.QueryID)

	// Store for out-of-order responses
	pendingResponses := make(map[uint64]models.QueryResponse)
	
	startTime := time.Now()
	timeout := 10 * time.Second
	
	for {
		// Check if we've timed out
		if time.Since(startTime) > timeout {
			return nil, fmt.Errorf("timeout waiting for response to query ID: %d", query.QueryID)
		}
		
		select {
		case response := <-c.responses:
			// Check if this is the response we're waiting for
			if response.QueryID == query.QueryID {
				log.Printf("Received matching response for query ID: %d", query.QueryID)
				return &response, nil
			}
			
			// Store this response for future queries that might be waiting for it
			log.Printf("Received out-of-order response for query ID: %d (expected: %d), storing for later", 
				response.QueryID, query.QueryID)
			pendingResponses[response.QueryID] = response
			
		case <-time.After(100 * time.Millisecond):
			// Check stored responses to see if our response arrived out of order
			if storedResponse, ok := pendingResponses[query.QueryID]; ok {
				log.Printf("Found matching response in pending responses for query ID: %d", query.QueryID)
				delete(pendingResponses, query.QueryID)
				return &storedResponse, nil
			}
			// Continue waiting
		}
	}
}

func (c *QueryClient) receiveResponses() {
	log.Println("Starting response receiver...")
	defer log.Println("Response receiver stopped")

	for {
		select {
		case <-c.done:
			return
		default:
			// Try to receive with timeout
			responseBytes, err := c.recvSocket.RecvBytes(zmq.DONTWAIT)
			if err != nil {
				if err == zmq.ErrorSocketClosed {
					log.Println("Response socket closed")
					return
				}
				if zmq.AsErrno(err) == zmq.Errno(11) { // EAGAIN
					// No message available, sleep briefly
					time.Sleep(100 * time.Millisecond)
					continue
				}
				log.Printf("Error receiving response: %v", err)
				continue
			}

			var response models.QueryResponse
			if err := json.Unmarshal(responseBytes, &response); err != nil {
				log.Printf("Error unmarshalling response: %v", err)
				continue
			}

			log.Printf("[Receiver] Raw response received and unmarshalled for QueryID: %d", response.QueryID)
			log.Printf("[Receiver] Response for query ID: %d contains data for %d object(s)",
				response.QueryID, len(response.Data))
				
			// Ensure we always have a valid map even if empty
			if response.Data == nil {
				response.Data = make(map[uint32][]models.DataPoint)
				log.Printf("[Receiver] Initialized empty Data map for QueryID: %d", response.QueryID)
			}

			trySendResponse(c.responses, response)
		}
	}
}

// Helper function to safely send to the responses channel
func trySendResponse(ch chan<- models.QueryResponse, resp models.QueryResponse) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Receiver] Failed to send response to channel (likely closed): %v", r)
		}
	}()

	// Try to send with a longer timeout since we now store responses
	select {
	case ch <- resp:
		log.Printf("[Receiver] Response for QueryID %d sent to waiting SendQuery (if any)", resp.QueryID)
	case <-time.After(5 * time.Second): // Increased from 1 second
		log.Printf("[Receiver] Timeout sending response for QueryID %d to channel. No SendQuery waiting?", resp.QueryID)
	}
}

// Close closes the client connection
func (c *QueryClient) Close() error {

	log.Println("Closing query client...")

	close(c.done)

	if err := c.sendSocket.Close(); err != nil {

		log.Printf("Error closing send socket: %v", err)

	}

	if err := c.recvSocket.Close(); err != nil {

		log.Printf("Error closing receive socket: %v", err)

	}

	if err := c.context.Term(); err != nil {

		return fmt.Errorf("failed to terminate context: %v", err)

	}

	close(c.responses)

	log.Println("Query client closed successfully")

	return nil
}

// Example usage:
//func ExampleUsage() {
//	client, err := NewQueryClient()
//	if err != nil {
//		log.Fatalf("Failed to create client: %v", err)
//	}
//	defer client.Close()
//
//	// Example query
//	query := models.Query{
//		QueryID:     1,
//		From:        uint32(1640995200), // 2022-01-01 00:00:00
//		To:          uint32(1641081600), // 2022-01-02 00:00:00
//		ObjectIDs:   []uint32{1, 2},
//		CounterId:   100,
//		Aggregation: "avg",
//	}
//
//	response, err := client.SendQuery(query)
//	if err != nil {
//		log.Printf("Failed to get query response: %v", err)
//		return
//	}
//
//	log.Printf("Received response: %+v", response)
//}
