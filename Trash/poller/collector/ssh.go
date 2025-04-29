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

	// Collect CPU usage
	cpuOut, err := runCommand(client, "mpstat 1 1 | tail -n 1 | awk '{print 100 - $NF}'")

	if err == nil {

		cpuUsage, err := strconv.ParseFloat(strings.TrimSpace(cpuOut), 64)

		if err == nil {

			metrics.CPU.Usage = cpuUsage

		} else {

			fmt.Printf("Error parsing CPU usage: %v\n", err)

		}

	} else {

		fmt.Printf("Error running CPU command: %v\n", err)

	}

	// Collect used memory
	memOut, err := runCommand(client, "free -b | grep Mem | awk '{print $3}'")

	if err == nil {

		usedMem, err := strconv.ParseUint(strings.TrimSpace(memOut), 10, 64)

		if err == nil {

			metrics.Memory.Used = usedMem

		} else {

			fmt.Printf("Error parsing used memory: %v\n", err)

		}

	} else {

		fmt.Printf("Error running memory command: %v\n", err)

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
