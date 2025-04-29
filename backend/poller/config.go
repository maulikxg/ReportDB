package poller

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/pebbe/zmq4"
	"v1/backend/models"
)

type Device struct {
	ObjectID int `json:"object_id"`

	Host string `json:"host"`

	Port int `json:"port"`

	Username string `json:"username"`

	Password string `json:"password"`
}

type ConfigMessage struct {
	Action string `json:"action"` // "add", "remove", "update"

	Devices []Device `json:"devices"`
}

type Configurer struct {
	socket *zmq4.Socket

	context *zmq4.Context

	endpoint string

	done chan struct{}
}

func NewConfigurer(endpoint string) (*Configurer, error) {

	log.Println("Initializing poller configurer...")

	context, err := zmq4.NewContext()

	if err != nil {

		return nil, fmt.Errorf("failed to create ZMQ context: %v", err)

	}

	socket, err := context.NewSocket(zmq4.PUSH)

	if err != nil {

		context.Term()

		return nil, fmt.Errorf("failed to create socket: %v", err)

	}

	log.Printf("Connecting to poller config endpoint at %s...", endpoint)

	if err := socket.Connect(endpoint); err != nil {

		socket.Close()

		context.Term()

		return nil, fmt.Errorf("failed to connect to %s: %v", endpoint, err)

	}

	log.Println("Poller configurer initialized successfully")

	return &Configurer{

		socket: socket,

		context: context,

		endpoint: endpoint,

		done: make(chan struct{}),
	}, nil
}

// AddDevice adds a device to the poller's configuration
func (c *Configurer) AddDevice(provision *models.Provision, credential *models.CredentialProfile) error {

	device := Device{

		ObjectID: provision.ObjectID,

		Host: provision.IP,

		Port: credential.Port,

		Username: credential.Username,

		Password: credential.Password,
	}

	message := ConfigMessage{

		Action:  "add",
		Devices: []Device{device},
	}

	return c.sendConfig(message)
}

// UpdateDevice updates a device in the poller's configuration
func (c *Configurer) UpdateDevice(provision *models.Provision, credential *models.CredentialProfile) error {

	device := Device{

		ObjectID: provision.ObjectID,

		Host: provision.IP,

		Port: credential.Port,

		Username: credential.Username,

		Password: credential.Password,
	}

	message := ConfigMessage{

		Action: "update",

		Devices: []Device{device},
	}

	return c.sendConfig(message)
}

// RemoveDevice removes a device from the poller's configuration
func (c *Configurer) RemoveDevice(objectID int) error {

	device := Device{

		ObjectID: objectID,
	}

	message := ConfigMessage{

		Action: "remove",

		Devices: []Device{device},
	}

	return c.sendConfig(message)
}

// sendConfig sends a configuration message to the poller
func (c *Configurer) sendConfig(message ConfigMessage) error {

	data, err := json.Marshal(message)

	if err != nil {

		return fmt.Errorf("failed to marshal config message: %v", err)

	}

	log.Printf("Sending poller config: %s for %d device(s)", message.Action, len(message.Devices))

	_, err = c.socket.SendBytes(data, 0)

	if err != nil {

		return fmt.Errorf("failed to send config message: %v", err)

	}

	log.Printf("Poller config sent successfully")

	return nil

}

// Close closes the configurer
func (c *Configurer) Close() error {

	log.Println("Closing poller configurer...")

	close(c.done)

	if err := c.socket.Close(); err != nil {

		log.Printf("Error closing socket: %v", err)

	}

	if err := c.context.Term(); err != nil {

		return fmt.Errorf("failed to terminate context: %v", err)

	}

	log.Println("Poller configurer closed successfully")
	
	return nil
}
