package utils

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/pebbe/zmq4"
	"v1/backend/models"
)

// ProvisionNotifier sends provision notifications to the poller service
type ProvisionNotifier struct {
	zmqSocket *zmq4.Socket

	zmqEndpoint string

	isConnected bool
}

// NewProvisionNotifier creates a new provision notifier
func NewProvisionNotifier(zmqEndpoint string) (*ProvisionNotifier, error) {

	socket, err := zmq4.NewSocket(zmq4.PUSH)

	if err != nil {
		return nil, fmt.Errorf("failed to create ZMQ socket: %w", err)
	}

	if err := socket.Connect(zmqEndpoint); err != nil {

		socket.Close()

		return nil, fmt.Errorf("failed to connect to ZMQ endpoint %s: %w", zmqEndpoint, err)

	}

	log.Printf("Connected to ZMQ endpoint: %s", zmqEndpoint)

	return &ProvisionNotifier{

		zmqSocket: socket,

		zmqEndpoint: zmqEndpoint,

		isConnected: true,
	}, nil

}

// Close closes the ZMQ socket
func (p *ProvisionNotifier) Close() {

	if p.zmqSocket != nil && p.isConnected {

		p.zmqSocket.Close()

	}
}

type ProvisionMessage struct {
	ObjectID int `json:"object_id"`

	IP string `json:"ip"`

	CredentialID int `json:"credential_id"`

	Username string `json:"username"`

	Password string `json:"password"`

	Port int `json:"port"`
}

// Notify sends a provision notification to the poller service
func (p *ProvisionNotifier) Notify(provision *models.Provision) error {

	// If not connected, just log and return
	if !p.isConnected || p.zmqSocket == nil {

		log.Printf("ZMQ not connected, skipping notification for IP %s", provision.IP)

		return nil

	}

	msg := ProvisionMessage{

		ObjectID: provision.ObjectID,

		IP: provision.IP,

		CredentialID: provision.CredentialID,
	}

	jsonData, err := json.Marshal(msg)

	if err != nil {
		return fmt.Errorf("failed to marshal provision message: %w", err)
	}

	_, err = p.zmqSocket.Send(string(jsonData), 0)

	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	log.Printf("Sent provision notification for IP %s", provision.IP)

	return nil
}
