package utils

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// configuration structure

type Config struct {
	Writers                 int    `json:"writers"`
	Readers                 int    `json:"readers"`
	NumOfPartitions         int    `json:"num_of_partitions"`
	BlockSize               int    `json:"block_size"`
	MaxDevices              int    `json:"max_devices"`
	IntialMmap              int    `json:"initial_mmap"`
	MaxBlocksPerDevice      int    `json:"max_blocks_per_device"`
	BuffredChanSize         int    `json:"buffred_chan_size"`
	StoragePath             string `json:"storage_path"`
	IsProductionEnvironment bool   `json:"is_production_environment"`
}

// Counter Config

type CounterConfig struct {
	Name string `json:"name"`

	Type string `json:"type"`
}

const (
	TypeInt    = 1
	TypeFloat  = 2
	TypeString = 3
)

// config instance
var (
	config *Config // for load all the config vars

	counters map[int]*CounterConfig

	configOnce sync.Once
)

// Loading all the config fils

func LoadConfig() error {

	var loadErr error

	configOnce.Do(func() {

		err := loadConfig()

		if err != nil {

			log.Printf("Error loading config: %v", err)

			loadErr = err

			return

		}

		err = loadCounterConfig()

		if err != nil {

			log.Printf("Error loading counter config: %v", err)

			loadErr = err

			return

		}

	})

	return loadErr
}

func loadConfig() error {

	// Get the absolute path to config directory
	configPath := filepath.Join("/home/maulikpuri/Desktop/v1/ReportDB/config", "config.json")

	data, err := os.ReadFile(configPath)

	if err != nil {

		log.Printf("Error loading config.json from %s: %v", configPath, err)

		return err

	}

	config = &Config{}

	err = json.Unmarshal(data, config)

	if err != nil {

		log.Printf("Error unmarshaling config file: %v", err)

		return err

	}

	// Create storage directory if it doesn't exist
	err = os.MkdirAll(config.StoragePath, 0755)

	if err != nil {

		log.Printf("Error creating storage directory %s: %v", config.StoragePath, err)

		return err

	}

	return nil
}

func loadCounterConfig() error {

	// Get the absolute path to config directory
	countersPath := filepath.Join("/home/maulikpuri/Desktop/v1/ReportDB/config", "counters.json")

	data, err := os.ReadFile(countersPath)

	if err != nil {

		log.Printf("Error loading counters.json from %s: %v", countersPath, err)

		return err

	}

	strCounters := make(map[string]*CounterConfig)

	err = json.Unmarshal(data, &strCounters)

	if err != nil {

		log.Printf("Error unmarshaling counters.json: %v", err)

		return err

	}

	counters = make(map[int]*CounterConfig)

	for i, v := range strCounters {

		id, err := strconv.Atoi(i)

		if err != nil {

			log.Printf("Failed to convert counter config key to int: %v", err)

			return err

		}

		counters[id] = v
	}

	return nil
}

func BaseDirProvider() string {

	path, err := os.Getwd()

	if err != nil {

		log.Println("Error getting current working directory:", err)

		return "."
	}

	return path
}

func GetWriters() int {

	return config.Writers

}

func GetReaders() int {

	return config.Readers

}

func GetNumOfPartitions() int {

	return config.NumOfPartitions

}

func GetBlockSize() int {

	return config.BlockSize

}

func GetMaxDevices() int {

	return config.MaxDevices

}

func GetMaxBlocksPerDevice() int {

	return config.MaxBlocksPerDevice

}

func GetInitialMmap() int {

	return config.IntialMmap
}

func GetBufferredChanSize() int {

	return config.BuffredChanSize

}

func GetCounterType(counterID uint16) (byte, error) {

	switch counterID {

	case 1:
		return TypeFloat, nil

	case 2:
		return TypeInt, nil

	case 3:
		return TypeString, nil

	default:
		return 0, fmt.Errorf("unknown counter ID: %d", counterID)
	}

}

// Add this function to get storage path
func GetStoragePath() string {
	return config.StoragePath
}

func isProductionEnvironment() bool {

	return config.IsProductionEnvironment

}
