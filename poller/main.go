package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/spf13/viper"
	"golang.org/x/crypto/ssh"
	"io/ioutil"
	"log"
	"net/http"
	"time"
	"github.com/your-username/v1/poller/config"
	"github.com/your-username/v1/poller/collector"
)

func initConfig() (*config.Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config")

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("error reading config file: %v", err)
	}

	var cfg config.Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unable to decode config: %v", err)
	}

	return &cfg, nil
}

func createSSHClient(device config.Device) (*ssh.Client, error) {
	var authMethods []ssh.AuthMethod

	if device.Password != "" {
		authMethods = append(authMethods, ssh.Password(device.Password))
	}

	if device.SSHKey != "" {
		key, err := ioutil.ReadFile(device.SSHKey)
		if err == nil {
			signer, err := ssh.ParsePrivateKey(key)
			if err == nil {
				authMethods = append(authMethods, ssh.PublicKeys(signer))
			}
		}
	}

	config := &ssh.ClientConfig{
		User:            device.Username,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         time.Second * 10,
	}

	addr := fmt.Sprintf("%s:%d", device.Host, device.Port)
	return ssh.Dial("tcp", addr, config)
}

func logMetrics(metrics *collector.Metrics) {
	log.Printf("=== Metrics for %s at %s ===", metrics.DeviceID, metrics.Timestamp.Format(time.RFC3339))
	
	// CPU Metrics
	log.Printf("CPU: Usage=%.2f%%, Load1=%.2f, Load5=%.2f, Load15=%.2f",
		metrics.CPU.Usage,
		metrics.CPU.LoadAvg1,
		metrics.CPU.LoadAvg5,
		metrics.CPU.LoadAvg15)

	// Memory Metrics
	log.Printf("Memory: Total=%d MB, Used=%d MB, Free=%d MB, Usage=%.2f%%",
		metrics.Memory.Total/1024/1024,
		metrics.Memory.Used/1024/1024,
		metrics.Memory.Free/1024/1024,
		metrics.Memory.UsagePerc)

	// Disk Metrics
	log.Println("Disk Usage:")
	for _, disk := range metrics.Disk {
		log.Printf("  Mount=%s, Total=%d GB, Used=%d GB, Free=%d GB, Usage=%.2f%%",
			disk.Mount,
			disk.Total/1024/1024/1024,
			disk.Used/1024/1024/1024,
			disk.Free/1024/1024/1024,
			disk.UsagePerc)
	}

	// Network Metrics
	log.Println("Network Stats:")
	for _, net := range metrics.Network {
		log.Printf("  Interface=%s, RX=%d MB, TX=%d MB, RX_Packets=%d, TX_Packets=%d",
			net.Interface,
			net.RxBytes/1024/1024,
			net.TxBytes/1024/1024,
			net.RxPackets,
			net.TxPackets)
	}

	// Process Metrics
	log.Println("Top Processes:")
	for _, proc := range metrics.Processes {
		log.Printf("  PID=%d, Name=%s, CPU=%.2f%%, Memory=%.2f%%, Command=%s",
			proc.PID,
			proc.Name,
			proc.CPU,
			proc.Memory,
			proc.Command)
	}
	log.Println("=== End of Metrics ===")
}

func pollDevice(device config.Device, cfg *config.Config) {
	log.Printf("Attempting to connect to %s...", device.Host)
	client, err := createSSHClient(device)
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
	
	// Log detailed metrics
	logMetrics(metrics)
	log.Printf("Successfully collected metrics from %s", device.Host)

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
	cfg, err := initConfig()
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