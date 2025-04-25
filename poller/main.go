package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"packxpoller/collector"
	"packxpoller/config"
	. "packxpoller/utils"
	"time"
)

func pollDevice(device config.Device, cfg *config.Config) {

	log.Printf("Attempting to connect to %s...", device.Host)

	client, err := CreateSSHClient(device)

	if err != nil {

		log.Printf("Failed to connect to %s: %v", device.Host, err)

		return

	}
	defer client.Close()

	log.Printf("Successfully connected to %s", device.Host)

	metrics, err := collector.CollectMetrics(client, device.Host)

	if err != nil {

		log.Printf("Failed to collect metrics from %s: %v", device.Host, err)

		return

	}

	LogMetrics(metrics)

	// Send metrics to backend
	jsonData, err := json.Marshal(metrics)

	if err != nil {

		log.Printf("Failed to marshal metrics for %s: %v", device.Host, err)

		return

	}

	resp, err := http.Post(cfg.BackendURL+"/api/v1/metrics", "application/json", bytes.NewBuffer(jsonData))

	if err != nil {

		log.Printf("Failed to send metrics for %s: %v", device.Host, err)

		return

	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {

		log.Printf("Backend returned non-200 status for %s: %d", device.Host, resp.StatusCode)

		return

	}

	log.Printf("Successfully sent metrics for %s to backend", device.Host)
}

func main() {

	cfg, err := InitConfig()

	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	ticker := time.NewTicker(time.Duration(cfg.PollInterval) * time.Second)

	defer ticker.Stop()

	for {

		for _, device := range cfg.Devices {

			go pollDevice(device, cfg)

		}

		<-ticker.C
	}
}
