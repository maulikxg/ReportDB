package utils

import (
	"encoding/json"
	"fmt"
	"github.com/pebbe/zmq4"
	"github.com/spf13/viper"
	"log"
	"packxpoller/config"
	"sync"
)

// ConfigMessage represents a configuration message from the backend
type ConfigMessage struct {
	Action string `json:"action"` // "add", "remove", "update"

	Devices []PollerDevice `json:"devices"`
}

// PollerDevice represents a device configuration from the backend
type PollerDevice struct {
	ObjectID int    `json:"object_id"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
}

var (
	configSocket  *zmq4.Socket
	configContext *zmq4.Context
	configMutex   sync.Mutex
)

// InitConfigReceiver initializes the ZMQ receiver for configuration updates
func InitConfigReceiver() error {
	var err error

	configContext, err = zmq4.NewContext()

	if err != nil {
		return fmt.Errorf("failed to create ZMQ context: %v", err)
	}

	configSocket, err = configContext.NewSocket(zmq4.PULL)

	if err != nil {

		configContext.Term()

		return fmt.Errorf("failed to create ZMQ PULL socket: %v", err)

	}

	// Bind to the configuration endpoint
	if err := configSocket.Bind("tcp://*:5557"); err != nil {

		configSocket.Close()

		configContext.Term()

		return fmt.Errorf("failed to bind to configuration endpoint: %v", err)

	}

	log.Println("Config receiver initialized and listening on tcp://*:5557")

	return nil
}

// CloseConfigReceiver closes the config receiver
func CloseConfigReceiver() {

	if configSocket != nil {

		configSocket.Close()

	}

	if configContext != nil {

		configContext.Term()

	}

}

// StartConfigReceiver starts a goroutine to receive configuration updates
func StartConfigReceiver() {

	go func() {

		for {

			msg, err := configSocket.RecvBytes(0)

			if err != nil {

				log.Printf("Error receiving config message: %v", err)

				continue

			}

			log.Printf("Received configuration update")

			// Parse message
			var configMsg ConfigMessage

			if err := json.Unmarshal(msg, &configMsg); err != nil {

				log.Printf("Error parsing config message: %v", err)

				continue

			}

			if err := updateConfig(configMsg); err != nil {

				log.Printf("Error updating config: %v", err)

				continue

			}

		}

	}()
}

func updateConfig(configMsg ConfigMessage) error {

	configMutex.Lock()

	defer configMutex.Unlock()

	if err := viper.ReadInConfig(); err != nil {
		return fmt.Errorf("error reading config file: %v", err)
	}

	var currentDevices []config.Device

	if err := viper.UnmarshalKey("devices", &currentDevices); err != nil {

		return fmt.Errorf("error parsing devices: %v", err)

	}

	// Process the message
	switch configMsg.Action {

	case "add", "update":
		for _, device := range configMsg.Devices {

			newDevice := config.Device{
				Host:     device.Host,
				Port:     device.Port,
				Username: device.Username,
				Password: device.Password,
			}

			// Check if device already exists
			found := false

			for i, d := range currentDevices {

				if d.Host == device.Host {

					// Update existing device
					currentDevices[i] = newDevice
					found = true
					break

				}
			}

			if !found {

				currentDevices = append(currentDevices, newDevice)

			}

			log.Printf("Added/updated device: %s (ObjectID: %d)", device.Host, device.ObjectID)
		}

	case "remove":

		for _, device := range configMsg.Devices {

			// Remove device
			for i, d := range currentDevices {

				if d.Host == device.Host {

					// Remove device at index i
					currentDevices = append(currentDevices[:i], currentDevices[i+1:]...)

					log.Printf("Removed device: %s (ObjectID: %d)", device.Host, device.ObjectID)

					break

				}
			}
		}
	}

	// Update config file
	viper.Set("devices", currentDevices)

	if err := viper.WriteConfig(); err != nil {
		return fmt.Errorf("error writing config file: %v", err)
	}

	log.Printf("Configuration updated successfully")

	return nil
}
