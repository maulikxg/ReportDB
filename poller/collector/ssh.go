package collector

import (
	"fmt"
	"golang.org/x/crypto/ssh"
	"strconv"
	"strings"
	"time"
)

type Metrics struct {
	Timestamp time.Time `json:"timestamp"`

	DeviceID string `json:"device_id"`

	CPU CPUMetrics `json:"cpu"`

	Memory MemoryMetrics `json:"memory"`
}

type CPUMetrics struct {
	Usage float64 `json:"usage"`
}

type MemoryMetrics struct {
	Used uint64 `json:"used"`
}

func CollectMetrics(client *ssh.Client, deviceID string) (*Metrics, error) {
	metrics := &Metrics{
		Timestamp: time.Now(),
		DeviceID:  deviceID,
	}

	// Get CPU usage from top
	cpuCmd := `top -bn1 | awk 'NR==3 {print $2}'`
	cpuOut, err := runCommand(client, cpuCmd)
	if err == nil {
		if cpuUsage, err := strconv.ParseFloat(strings.TrimSpace(cpuOut), 64); err == nil {
			metrics.CPU.Usage = cpuUsage
		} else {
			fmt.Printf("Error parsing CPU usage: %v\n", err)
			metrics.CPU.Usage = 0.0
		}
	} else {
		fmt.Printf("Error running CPU command: %v\n", err)
		metrics.CPU.Usage = 0.0
	}

	// Get memory usage in MB using free command
	memCmd := `free -m | awk 'NR==2 {print $3}'`  // Column 3 is used memory in MB
	memOut, err := runCommand(client, memCmd)
	if err == nil {
		if memUsed, err := strconv.ParseUint(strings.TrimSpace(memOut), 10, 64); err == nil {
			metrics.Memory.Used = memUsed  // This is now in MB
		} else {
			fmt.Printf("Error parsing memory usage: %v\n", err)
			metrics.Memory.Used = 0
		}
	} else {
		fmt.Printf("Error running memory command: %v\n", err)
		metrics.Memory.Used = 0
	}

	return metrics, nil
}

func runCommand(client *ssh.Client, cmd string) (string, error) {

	session, err := client.NewSession()

	if err != nil {
		return "", err
	}

	defer session.Close()

	output, err := session.CombinedOutput(cmd)

	if err != nil {
		return "", err
	}

	return string(output), nil
}
