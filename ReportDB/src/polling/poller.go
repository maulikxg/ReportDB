package polling

import (
	"golang.org/x/crypto/ssh"
	"log"
	"math/rand"
	"packx/models"
	"strconv"
	"strings"
	"time"
)

func PollCPUData(pollData chan<- models.Metric) {

	//SSH connection details
	//config := &ssh.ClientConfig{
	//
	//	User: "harekrushn",
	//
	//	Auth: []ssh.AuthMethod{
	//		ssh.Password("Mind@123"),
	//	},
	//
	//	HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	//
	//	Timeout: 10 * time.Second,
	//}

	//config := &ssh.ClientConfig{
	//
	//	User: "maulikpuri",
	//
	//	Auth: []ssh.AuthMethod{
	//		ssh.Password("Mind@123"),
	//	},
	//
	//	HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	//
	//	Timeout: 10 * time.Second,
	//}

	config := &ssh.ClientConfig{

		User: "dhyanesh",

		Auth: []ssh.AuthMethod{
			ssh.Password("Mind@123"),
		},

		HostKeyCallback: ssh.InsecureIgnoreHostKey(),

		Timeout: 10 * time.Second,
	}

	for {

		// Connect to SSH server
		client, err := ssh.Dial("tcp", "10.20.40.103:8008", config)
		//client, err := ssh.Dial("tcp", "192.168.1.10:22", config)

		if err != nil {

			log.Printf("Failed to dial SSH: %v", err)

			time.Sleep(5 * time.Second)

			continue

		}

		// Create session
		session, err := client.NewSession()

		if err != nil {

			log.Printf("Failed to create session: %v", err)

			client.Close()

			time.Sleep(5 * time.Second)

			continue

		}

		// Run top command and get CPU usage
		cmd := "top -bn1 | grep 'Cpu(s)' | awk '{print $2}'"

		output, err := session.CombinedOutput(cmd)

		if err != nil {

			log.Printf("Failed to run command: %v", err)

			session.Close()

			client.Close()

			time.Sleep(5 * time.Second)

			continue

		}

		// Parse CPU percentage
		cpuStr := strings.TrimSpace(string(output))

		cpuValue, err := strconv.ParseFloat(cpuStr, 64)

		if err != nil {

			log.Printf("Failed to parse CPU value: %v", err)

		} else {

			// Create metric
			metric := models.Metric{

				ObjectID: uint32(rand.Intn(2)), // Using fixed device ID for example

				CounterId: 1, // Counter ID for CPU

				Value: cpuValue,

				Timestamp: uint32(time.Now().Unix()),
			}

			// Send to existing push socket mechanism

			pollData <- metric

			log.Printf("Polled CPU usage: %.2f%%", cpuValue)

		}

		session.Close()

		client.Close()

		time.Sleep(5 * time.Second) // Poll every 5 seconds

	}
}
