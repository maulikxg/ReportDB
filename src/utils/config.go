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
	Writers            int    `json:"writers"`
	Readers           int    `json:"readers"`
	NumOfPartitions   int    `json:"num_of_partitions"`
	BlockSize         int    `json:"block_size"`
	MaxDevices        int    `json:"max_devices"`
	IntialMmap        int    `json:"initial_mmap"`
	MaxBlocksPerDevice int   `json:"max_blocks_per_device"`
	BuffredChanSize   int    `json:"buffred_chan_size"`
	StoragePath       string `json:"storage_path"`
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
	configPath := filepath.Join("/home/maulikpuri/Desktop/v1/config", "config.json")

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
	countersPath := filepath.Join("/home/maulikpuri/Desktop/v1/config", "counters.json")

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
	if config == nil {
		log.Println("Warning: Config not loaded, returning default BufferredChanSize 0")
		return 0 // Or a sensible default
	}
	return config.BuffredChanSize
}

func GetStoragePath() string {
	if config == nil {
		log.Println("Warning: Config not loaded, returning empty storage path")
		return ""
	}
	return config.StoragePath
}

func GetCounterType(counterID uint16) (byte, error) {
	if config == nil {
		return 0, fmt.Errorf("config not loaded, cannot determine counter type")
	}
	if counters == nil {
		return 0, fmt.Errorf("counter config not loaded, cannot determine counter type")
	}
	switch counterID {
	case 1:
		return TypeInt, nil
	case 2:
		return TypeFloat, nil
	case 3:
		return TypeString, nil
	default:
		// Check dynamic counters from config
		if _, ok := counters[int(counterID)]; ok {
			// Assuming type mapping needs refinement based on counter.json content
			// For now, just return a default or handle based on counter.Type
			log.Printf("Warning: Using default type mapping for dynamically loaded counter ID %d", counterID)
			// Placeholder: return TypeInt for now, adjust as needed
			return TypeInt, nil
		}
		return 0, fmt.Errorf("unknown counter ID: %d", counterID)
	}
}
