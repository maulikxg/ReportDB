package collector

import (
	"golang.org/x/crypto/ssh"
	"time"
	"strings"
	"strconv"
)

type Metrics struct {
	Timestamp time.Time         `json:"timestamp"`
	DeviceID  string           `json:"device_id"`
	CPU       CPUMetrics       `json:"cpu"`
	Memory    MemoryMetrics    `json:"memory"`
	Disk      []DiskMetrics    `json:"disk"`
	Network   []NetworkMetrics `json:"network"`
	Processes []ProcessMetrics `json:"processes"`
}

type CPUMetrics struct {
	Usage     float64 `json:"usage"`
	LoadAvg1  float64 `json:"load_avg_1"`
	LoadAvg5  float64 `json:"load_avg_5"`
	LoadAvg15 float64 `json:"load_avg_15"`
}

type MemoryMetrics struct {
	Total     uint64  `json:"total"`
	Used      uint64  `json:"used"`
	Free      uint64  `json:"free"`
	UsagePerc float64 `json:"usage_perc"`
}

type DiskMetrics struct {
	Mount     string  `json:"mount"`
	Total     uint64  `json:"total"`
	Used      uint64  `json:"used"`
	Free      uint64  `json:"free"`
	UsagePerc float64 `json:"usage_perc"`
}

type NetworkMetrics struct {
	Interface string `json:"interface"`
	RxBytes   uint64 `json:"rx_bytes"`
	TxBytes   uint64 `json:"tx_bytes"`
	RxPackets uint64 `json:"rx_packets"`
	TxPackets uint64 `json:"tx_packets"`
}

type ProcessMetrics struct {
	PID     int     `json:"pid"`
	Name    string  `json:"name"`
	CPU     float64 `json:"cpu"`
	Memory  float64 `json:"memory"`
	Command string  `json:"command"`
}

func CollectMetrics(client *ssh.Client, deviceID string) (*Metrics, error) {
	metrics := &Metrics{
		Timestamp: time.Now(),
		DeviceID:  deviceID,
	}

	// Collect CPU metrics
	out, err := runCommand(client, "cat /proc/loadavg && mpstat 1 1 | tail -n 1")
	if err == nil {
		parts := strings.Fields(out)
		if len(parts) >= 3 {
			metrics.CPU.LoadAvg1, _ = strconv.ParseFloat(parts[0], 64)
			metrics.CPU.LoadAvg5, _ = strconv.ParseFloat(parts[1], 64)
			metrics.CPU.LoadAvg15, _ = strconv.ParseFloat(parts[2], 64)
		}
		// Parse mpstat output for CPU usage
		if len(parts) > 10 {
			idle, _ := strconv.ParseFloat(parts[len(parts)-1], 64)
			metrics.CPU.Usage = 100.0 - idle
		}
	}

	// Collect Memory metrics
	out, err = runCommand(client, "free -b")
	if err == nil {
		lines := strings.Split(out, "\n")
		if len(lines) > 1 {
			parts := strings.Fields(lines[1])
			if len(parts) >= 3 {
				metrics.Memory.Total, _ = strconv.ParseUint(parts[1], 10, 64)
				metrics.Memory.Used, _ = strconv.ParseUint(parts[2], 10, 64)
				metrics.Memory.Free, _ = strconv.ParseUint(parts[3], 10, 64)
				if metrics.Memory.Total > 0 {
					metrics.Memory.UsagePerc = float64(metrics.Memory.Used) / float64(metrics.Memory.Total) * 100
				}
			}
		}
	}

	// Collect Disk metrics
	out, err = runCommand(client, "df -B1")
	if err == nil {
		lines := strings.Split(out, "\n")[1:] // Skip header
		for _, line := range lines {
			parts := strings.Fields(line)
			if len(parts) >= 6 {
				total, _ := strconv.ParseUint(parts[1], 10, 64)
				used, _ := strconv.ParseUint(parts[2], 10, 64)
				free, _ := strconv.ParseUint(parts[3], 10, 64)
				usageStr := strings.TrimRight(parts[4], "%")
				usagePerc, _ := strconv.ParseFloat(usageStr, 64)
				
				metrics.Disk = append(metrics.Disk, DiskMetrics{
					Mount:     parts[5],
					Total:     total,
					Used:      used,
					Free:      free,
					UsagePerc: usagePerc,
				})
			}
		}
	}

	// Collect Network metrics
	out, err = runCommand(client, "cat /proc/net/dev")
	if err == nil {
		lines := strings.Split(out, "\n")[2:] // Skip headers
		for _, line := range lines {
			parts := strings.Fields(line)
			if len(parts) >= 10 {
				iface := strings.TrimRight(parts[0], ":")
				if iface == "lo" {
					continue // Skip loopback
				}
				rx, _ := strconv.ParseUint(parts[1], 10, 64)
				tx, _ := strconv.ParseUint(parts[9], 10, 64)
				rxPkts, _ := strconv.ParseUint(parts[2], 10, 64)
				txPkts, _ := strconv.ParseUint(parts[10], 10, 64)
				
				metrics.Network = append(metrics.Network, NetworkMetrics{
					Interface: iface,
					RxBytes:   rx,
					TxBytes:   tx,
					RxPackets: rxPkts,
					TxPackets: txPkts,
				})
			}
		}
	}

	// Collect Process metrics (top 10 by CPU)
	out, err = runCommand(client, "ps aux --sort=-%cpu | head -11")
	if err == nil {
		lines := strings.Split(out, "\n")[1:] // Skip header
		for _, line := range lines {
			parts := strings.Fields(line)
			if len(parts) >= 11 {
				pid, _ := strconv.Atoi(parts[1])
				cpu, _ := strconv.ParseFloat(parts[2], 64)
				mem, _ := strconv.ParseFloat(parts[3], 64)
				
				metrics.Processes = append(metrics.Processes, ProcessMetrics{
					PID:     pid,
					Name:    parts[10],
					CPU:     cpu,
					Memory:  mem,
					Command: strings.Join(parts[10:], " "),
				})
			}
		}
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