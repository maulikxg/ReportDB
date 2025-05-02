package utils

import (
	"bufio"
	"fmt"
	"github.com/reiver/go-telnet"
	"golang.org/x/crypto/ssh"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"v1/backend/database"
	"v1/backend/models"
)

// DiscoveryQueue handles queuing and processing of discovery profiles
type DiscoveryQueue struct {
	queue chan *models.DiscoveryProfile

	credRepo *database.CredentialRepository

	discRepo *database.DiscoveryRepository

	provRepo *database.ProvisionRepository

	stopChan chan struct{}

	workerCount int

	workerWaitGroup sync.WaitGroup
}

func NewDiscoveryQueue(

	credRepo *database.CredentialRepository,

	discRepo *database.DiscoveryRepository,

	provRepo *database.ProvisionRepository,

	workerCount int,

) *DiscoveryQueue {

	if workerCount <= 0 {

		workerCount = 5

	}

	queue := &DiscoveryQueue{

		queue: make(chan *models.DiscoveryProfile, 100),

		credRepo: credRepo,

		discRepo: discRepo,

		provRepo: provRepo,

		stopChan: make(chan struct{}),

		workerCount: workerCount,

		workerWaitGroup: sync.WaitGroup{},
	}

	queue.start()

	return queue
}

// start launches worker goroutines to process discoveries
func (q *DiscoveryQueue) start() {

	for i := 0; i < q.workerCount; i++ {

		q.workerWaitGroup.Add(1)

		go q.worker()

	}
}

// Stop stops all worker goroutines
func (q *DiscoveryQueue) Stop() {

	close(q.stopChan)

	q.workerWaitGroup.Wait()

}

// Add adds a discovery to the queue
func (q *DiscoveryQueue) Add(discovery *models.DiscoveryProfile) {
	q.queue <- discovery
}

// worker processes discovery profiles from the queue
func (q *DiscoveryQueue) worker() {

	defer q.workerWaitGroup.Done()

	for {
		select {

		case <-q.stopChan:

			return

		case discovery := <-q.queue:

			// Update status to running
			q.discRepo.UpdateStatus(discovery.DiscoveryID, models.StatusRunning)

			// Process discovery
			err := q.processDiscovery(discovery)

			// Update status based on result
			if err != nil {

				log.Printf("Discovery %d failed: %v", discovery.DiscoveryID, err)
				q.discRepo.UpdateStatus(discovery.DiscoveryID, models.StatusFailed)

			} else {

				q.discRepo.UpdateStatus(discovery.DiscoveryID, models.StatusSuccess)
			}
		}
	}
}

// processDiscovery handles the discovery process
func (q *DiscoveryQueue) processDiscovery(discovery *models.DiscoveryProfile) error {
	var ips []string

	// Determine IPs to scan
	if discovery.IP != "" {
		if ip := net.ParseIP(discovery.IP); ip != nil {
			ips = append(ips, discovery.IP)
		} else {
			return fmt.Errorf("invalid IP address: %s", discovery.IP)
		}
	}

	if discovery.IPRange != "" {
		rangeIPs, err := expandIPRange(discovery.IPRange)
		if err != nil {
			return fmt.Errorf("failed to expand IP range: %w", err)
		}
		ips = append(ips, rangeIPs...)
	}

	if len(ips) == 0 {
		return fmt.Errorf("no valid IPs to scan")
	}

	// Get all credentials
	credentials := make(map[int]*models.CredentialProfile)
	for _, credID := range discovery.CredentialIDs {
		cred, err := q.credRepo.GetByID(credID)
		if err != nil {
			return fmt.Errorf("credential ID %d not found: %w", credID, err)
		}
		credentials[credID] = cred
	}

	// Create a worker pool for concurrent scanning
	type scanResult struct {
		ip        string
		credID    int
		reachable bool
		err       error
	}

	// Use more workers for larger IP ranges
	workers := 20
	if total := len(ips) * len(credentials); total < workers {
		workers = total
	}

	jobs := make(chan struct {
		ip     string
		credID int
		cred   *models.CredentialProfile
	})
	results := make(chan scanResult)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				reachable := isReachable(job.ip, job.cred.Port, job.cred)
				results <- scanResult{
					ip:        job.ip,
					credID:    job.credID,
					reachable: reachable,
				}
			}
		}()
	}

	// Close results when all workers are done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Send jobs
	go func() {
		for _, ip := range ips {
			for credID, cred := range credentials {
				jobs <- struct {
					ip     string
					credID int
					cred   *models.CredentialProfile
				}{ip, credID, cred}
			}
		}
		close(jobs)
	}()

	// Process results and create provisions
	successfulIPs := make(map[string]bool)
	foundDevices := 0
	for result := range results {
		if result.err != nil {
			log.Printf("Error scanning %s: %v", result.ip, result.err)
			continue
		}

		// Skip if we already found a working credential for this IP
		if successfulIPs[result.ip] {
			continue
		}

		if result.reachable {
			successfulIPs[result.ip] = true
			foundDevices++
			provision := models.NewProvision(result.ip, result.credID, discovery.DiscoveryID)
			_, err := q.provRepo.Create(provision)
			if err != nil {
				log.Printf("Failed to create provision for %s: %v", result.ip, err)
			}
		}
	}

	log.Printf("Discovery %d completed: found %d devices out of %d IPs scanned",
		discovery.DiscoveryID, foundDevices, len(ips))

	return nil
}

// expandIPRange expands an IP range in CIDR notation or range format to individual IPs
func expandIPRange(ipRange string) ([]string, error) {

	var ips []string

	// Check if it's CIDR notation
	if strings.Contains(ipRange, "/") {

		_, ipnet, err := net.ParseCIDR(ipRange)

		if err != nil {
			return nil, fmt.Errorf("invalid CIDR format: %w", err)
		}

		// Convert IPNet to a list of IPs
		for ip := ipnet.IP.Mask(ipnet.Mask); ipnet.Contains(ip); incrementIP(ip) {

			ips = append(ips, ip.String())

		}

		return ips, nil
	}

	// Check if it's a range format (e.g., 192.168.1.1-192.168.1.254)
	if strings.Contains(ipRange, "-") {

		parts := strings.Split(ipRange, "-")

		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid IP range format")
		}

		startIP := net.ParseIP(strings.TrimSpace(parts[0]))

		endIP := net.ParseIP(strings.TrimSpace(parts[1]))

		if startIP == nil || endIP == nil {
			return nil, fmt.Errorf("invalid IP addresses in range")
		}

		start := ipToUint32(startIP)

		end := ipToUint32(endIP)

		if end < start {
			return nil, fmt.Errorf("end IP is less than start IP")
		}

		// Generate IPs in range
		for i := start; i <= end; i++ {

			ip := make(net.IP, 4)

			ip[0] = byte(i >> 24)

			ip[1] = byte(i >> 16)

			ip[2] = byte(i >> 8)

			ip[3] = byte(i)

			ips = append(ips, ip.String())

		}

		return ips, nil
	}

	// Single IP
	if ip := net.ParseIP(ipRange); ip != nil {
		return []string{ipRange}, nil
	}

	return nil, fmt.Errorf("invalid IP format")

}

// ipToUint32 converts a net.IP to uint32 for easy manipulation
func ipToUint32(ip net.IP) uint32 {

	ip = ip.To4()

	return uint32(ip[0])<<24 | uint32(ip[1])<<16 | uint32(ip[2])<<8 | uint32(ip[3])

}

// incrementIP increments an IP address by 1
func incrementIP(ip net.IP) {

	for j := len(ip) - 1; j >= 0; j-- {

		ip[j]++

		if ip[j] > 0 {

			break

		}

	}

}

// isReachable checks if a device is reachable and accessible with given credentials
func isReachable(ip string, port int, cred *models.CredentialProfile) bool {

	// First check if port is open
	timeout := 2 * time.Second

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", ip, port), timeout)

	if err != nil {
		return false
	}

	defer conn.Close()

	switch port {

	case 22: // SSH

		config := &ssh.ClientConfig{

			User: cred.Username,

			Auth: []ssh.AuthMethod{

				ssh.Password(cred.Password),
			},

			HostKeyCallback: ssh.InsecureIgnoreHostKey(),

			Timeout: timeout,
		}

		sshClient, err := ssh.Dial("tcp", fmt.Sprintf("%s:%d", ip, port), config)

		if err != nil {
			return false
		}

		defer sshClient.Close()

		return true

	case 23: // Telnet

		telnetClient, err := telnet.DialTo(fmt.Sprintf("%s:%d", ip, port))

		if err != nil {
			return false
		}

		defer telnetClient.Close()

		// Create a channel to signal authentication result
		done := make(chan bool)

		go func() {

			reader := bufio.NewReader(telnetClient)

			writer := bufio.NewWriter(telnetClient)

			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					if err != io.EOF {
						done <- false
					}
					return
				}

				line = strings.ToLower(line)

				if strings.Contains(line, "login") || strings.Contains(line, "username") {

					writer.WriteString(cred.Username + "\n")

					writer.Flush()

				} else if strings.Contains(line, "password") {

					writer.WriteString(cred.Password + "\n")

					writer.Flush()

				} else if strings.Contains(line, "$") || strings.Contains(line, "#") || strings.Contains(line, ">") {

					// Successfully logged in
					done <- true

					return

				}
			}
		}()

		// Wait for authentication result or timeout
		select {

		case success := <-done:

			return success

		case <-time.After(timeout):

			return false

		}

	default:

		return true

	}
}

//// processDiscovery handles the discovery process
//func (q *DiscoveryQueue) processDiscovery(discovery *models.DiscoveryProfile) error {
//
//	var ips []string
//
//	// Determine IPs to scan
//	if discovery.IP != "" {
//
//		ips = append(ips, discovery.IP)
//
//	}
//
//	if discovery.IPRange != "" {
//
//		rangeIPs, err := expandIPRange(discovery.IPRange)
//
//		if err != nil {
//			return fmt.Errorf("failed to expand IP range: %w", err)
//		}
//
//		ips = append(ips, rangeIPs...)
//
//	}
//
//	// Get all credentials
//	credentials := make(map[int]*models.CredentialProfile)
//
//	for _, credID := range discovery.CredentialIDs {
//
//		cred, err := q.credRepo.GetByID(credID)
//
//		if err != nil {
//			return fmt.Errorf("credential ID %d not found: %w", credID, err)
//		}
//
//		credentials[credID] = cred
//	}
//
//	// Try each IP with each credential
//	for _, ip := range ips {
//
//		// For each credential, try to connect
//		for credID, cred := range credentials {
//
//			if isReachable(ip, cred.Port) {
//
//				// Create a provision record for reachable devices
//				provision := models.NewProvision(ip, credID, discovery.DiscoveryID)
//
//				_, err := q.provRepo.Create(provision)
//
//				if err != nil {
//					log.Printf("Failed to create provision for %s: %v", ip, err)
//				}
//
//				// We found one working credential, so we can break
//				break
//			}
//		}
//	}
//
//	return nil
//}
//
//// expandIPRange expands an IP range in CIDR notation to individual IPs
//func expandIPRange(ipRange string) ([]string, error) {
//
//	if !strings.Contains(ipRange, "/") {
//		// Single IP or invalid format
//		return []string{ipRange}, nil
//	}
//
//	// Parse CIDR
//	_, ipNet, err := net.ParseCIDR(ipRange)
//
//	if err != nil {
//		return nil, err
//	}
//
//	// Get all IPs in range
//	var ips []string
//
//	for ip := ipNet.IP.Mask(ipNet.Mask); ipNet.Contains(ip); incrementIP(ip) {
//
//		ips = append(ips, ip.String())
//
//	}
//
//	// Remove network and broadcast addresses for IPv4
//	if len(ips) > 2 && strings.Contains(ipRange, ".") {
//
//		ips = ips[1 : len(ips)-1]
//
//	}
//
//	return ips, nil
//}
//
//// incrementIP increments an IP address by 1
//func incrementIP(ip net.IP) {
//
//	for j := len(ip) - 1; j >= 0; j-- {
//
//		ip[j]++
//
//		if ip[j] > 0 {
//
//			break
//
//		}
//
//	}
//
//}
//
//// isReachable checks if a device is reachable on the specified port
//func isReachable(ip string, port int) bool {
//
//	address := fmt.Sprintf("%s:%d", ip, port)
//
//	conn, err := net.DialTimeout("tcp", address, 3*time.Second)
//
//	if err != nil {
//		return false
//	}
//
//	conn.Close()
//
//	return true
//
//}
