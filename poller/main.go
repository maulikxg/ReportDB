package main

import (
	"log"
	"packxpoller/collector"
	"packxpoller/config"
	. "packxpoller/utils"
	"time"
)

func pollDevice(device config.Device, cfg *config.Config, deviceIndex int) {

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

	// Send metrics to ReportDB via ZMQ
	if err := SendMetrics(metrics, deviceIndex); err != nil {

		log.Printf("Failed to send metrics to ReportDB: %v", err)

	} else {

		log.Printf("Successfully sent metrics for %s to ReportDB", device.Host)

	}
}

func main() {

	cfg, err := InitConfig()

	if err != nil {

		log.Fatalf("Failed to load config: %v", err)

	}

	if err := InitZMQSender(); err != nil {

		log.Fatalf("Failed to initialize ZMQ sender: %v", err)

	}

	defer CloseZMQSender()

	log.Println("Starting poller with ZMQ connection to ReportDB...")

	ticker := time.NewTicker(time.Duration(cfg.PollInterval) * time.Second)

	defer ticker.Stop()

	for {

		for i, device := range cfg.Devices {

			go pollDevice(device, cfg, i)

		}

		<-ticker.C
	}
}
