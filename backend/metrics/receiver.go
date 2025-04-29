package metrics

import (
	"encoding/json"
	"fmt"
	"github.com/pebbe/zmq4"
	"log"
	"time"
	"v1/backend/reportdb"
)

// PollerMetric represents a metric as sent by the poller
type PollerMetric struct {
	ObjectID uint32 `json:"Object_id"`

	CounterId uint16 `json:"counter_id"`

	Value interface{} `json:"value"`

	Timestamp uint32 `json:"timestamp"`
}

// DeviceMetrics represents metrics data from a device
type DeviceMetrics struct {
	ObjectID uint32 `json:"object_id"`

	Metrics map[string]interface{} `json:"metrics"`

	Timestamp uint32 `json:"timestamp"`
}

// Receiver handles incoming metrics from pollers
type Receiver struct {
	socket *zmq4.Socket

	context *zmq4.Context

	endpoint string

	metricChan chan DeviceMetrics

	done chan struct{}
}

// NewReceiver creates a new metrics receiver
func NewReceiver(endpoint string) (*Receiver, error) {

	log.Println("Initializing metrics receiver...")

	context, err := zmq4.NewContext()

	if err != nil {
		return nil, fmt.Errorf("failed to create ZMQ context: %v", err)
	}

	socket, err := context.NewSocket(zmq4.PULL)

	if err != nil {

		context.Term()

		return nil, fmt.Errorf("failed to create socket: %v", err)

	}

	log.Printf("Binding metrics receiver to %s...", endpoint)

	if err := socket.Bind(endpoint); err != nil {

		socket.Close()

		context.Term()

		return nil, fmt.Errorf("failed to bind socket to %s: %v", endpoint, err)

	}

	log.Println("Metrics receiver initialized successfully")

	r := &Receiver{

		socket: socket,

		context: context,

		endpoint: endpoint,

		metricChan: make(chan DeviceMetrics, 100),

		done: make(chan struct{}),
	}

	// Start the receiver goroutine
	go r.receiveMetrics()

	return r, nil
}

// receiveMetrics continuously receives metrics from devices
func (r *Receiver) receiveMetrics() {

	log.Println("Starting metrics receiver...")

	defer log.Println("Metrics receiver stopped")

	// Counter type mapping
	counterTypeMap := map[uint16]string{

		1: "cpu",
		2: "memory",
		3: "disk",
	}

	// Cache for collecting metrics by device
	metricsByDevice := make(map[uint32]map[string]interface{})

	timestampsByDevice := make(map[uint32]uint32)

	for {

		select {

		case <-r.done:

			return

		default:

			data, err := r.socket.RecvBytes(zmq4.DONTWAIT)

			if err != nil {

				if err == zmq4.ErrorSocketClosed {

					log.Println("Metrics socket closed")

					return

				}

				if zmq4.AsErrno(err) == zmq4.Errno(11) { // EAGAIN

					time.Sleep(100 * time.Millisecond)

					for objectID, metrics := range metricsByDevice {

						if len(metrics) > 0 {

							deviceMetrics := DeviceMetrics{

								ObjectID: objectID,

								Metrics: metrics,

								Timestamp: timestampsByDevice[objectID],
							}

							log.Printf("Processing cached metrics from object %d with %d metrics",
								objectID, len(metrics))

							// Forward to the metric channel
							r.forwardMetrics(deviceMetrics)

							// Reset cache for this device
							delete(metricsByDevice, objectID)

							delete(timestampsByDevice, objectID)
						}
					}

					continue

				}

				log.Printf("Error receiving metrics: %v", err)

				continue

			}

			var pollerMetric PollerMetric

			if err := json.Unmarshal(data, &pollerMetric); err != nil {

				log.Printf("Error unmarshalling poller metric: %v", err)

				continue

			}

			log.Printf("Received poller metric: ObjectID=%d, CounterID=%d, Value=%v",
				pollerMetric.ObjectID, pollerMetric.CounterId, pollerMetric.Value)

			// Get the metric type name from the counter ID
			metricType, ok := counterTypeMap[pollerMetric.CounterId]

			if !ok {

				log.Printf("Unknown counter ID: %d, skipping", pollerMetric.CounterId)

				continue
			}

			// Initialize metric map for this device if needed
			if _, exists := metricsByDevice[pollerMetric.ObjectID]; !exists {

				metricsByDevice[pollerMetric.ObjectID] = make(map[string]interface{})

			}

			// Add metric to the map
			metricsByDevice[pollerMetric.ObjectID][metricType] = pollerMetric.Value

			timestampsByDevice[pollerMetric.ObjectID] = pollerMetric.Timestamp

			// If we have both CPU and memory metrics, forward them
			if len(metricsByDevice[pollerMetric.ObjectID]) >= 2 {

				deviceMetrics := DeviceMetrics{

					ObjectID: pollerMetric.ObjectID,

					Metrics: metricsByDevice[pollerMetric.ObjectID],

					Timestamp: pollerMetric.Timestamp,
				}

				log.Printf("Received complete metrics from object %d with %d metrics",
					deviceMetrics.ObjectID, len(deviceMetrics.Metrics))

				r.forwardMetrics(deviceMetrics)

				// Reset cache for this device
				delete(metricsByDevice, pollerMetric.ObjectID)

				delete(timestampsByDevice, pollerMetric.ObjectID)
			}
		}
	}
}

// forwardMetrics sends the metrics to the metric channel
func (r *Receiver) forwardMetrics(metrics DeviceMetrics) {

	select {

	case r.metricChan <- metrics:
		log.Printf("Forwarded metrics for object %d to processing", metrics.ObjectID)

	case <-time.After(2 * time.Second):

		log.Printf("Timeout forwarding metrics for object %d, channel might be full", metrics.ObjectID)

	}
}

// ProcessMetrics process metrics and forward them to ReportDB
func (r *Receiver) ProcessMetrics(client *reportdb.Client) {

	go func() {
		// Counter ID mappings for different metrics
		counterMappings := map[string]uint16{
			"cpu":    1,
			"memory": 2,
			"disk":   3,
		}

		for {

			select {

			case <-r.done:

				return

			case deviceMetrics := <-r.metricChan:

				log.Printf("Processing metrics for object %d", deviceMetrics.ObjectID)

				if len(deviceMetrics.Metrics) == 0 {

					log.Printf("No metrics available for object %d, skipping", deviceMetrics.ObjectID)

					continue

				}

				// Process each metric type (cpu, memory, disk)
				for metricName, value := range deviceMetrics.Metrics {

					// Get counter ID for this metric type
					counterID, ok := counterMappings[metricName]

					if !ok {

						log.Printf("Unknown metric type: %s, skipping", metricName)

						continue
					}

					metric := reportdb.Metric{

						ObjectID: deviceMetrics.ObjectID,

						CounterId: counterID,

						Value: value,

						Timestamp: deviceMetrics.Timestamp,
					}

					log.Printf("Sending metric to ReportDB: obj=%d, counter=%d, value=%v, time=%d",
						metric.ObjectID, metric.CounterId, metric.Value, metric.Timestamp)

					// Send to ReportDB
					if err := client.SendMetric(metric); err != nil {

						log.Printf("Error sending metric to ReportDB: %v", err)

					} else {

						log.Printf("Successfully sent metric to ReportDB: obj=%d, counter=%d",
							metric.ObjectID, metric.CounterId)

					}
				}
			}
		}
	}()
}

// GetMetricsChannel returns the channel for received metrics
func (r *Receiver) GetMetricsChannel() <-chan DeviceMetrics {

	return r.metricChan

}

// Close closes the metrics receiver
func (r *Receiver) Close() error {

	log.Println("Closing metrics receiver...")

	close(r.done)

	if err := r.socket.Close(); err != nil {
		log.Printf("Error closing socket: %v", err)
	}

	if err := r.context.Term(); err != nil {
		return fmt.Errorf("failed to terminate context: %v", err)
	}

	log.Println("Metrics receiver closed successfully")

	return nil
}
