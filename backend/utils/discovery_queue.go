package utils

import (
	"fmt"
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

		ips = append(ips, discovery.IP)

	}

	if discovery.IPRange != "" {

		rangeIPs, err := expandIPRange(discovery.IPRange)

		if err != nil {
			return fmt.Errorf("failed to expand IP range: %w", err)
		}

		ips = append(ips, rangeIPs...)

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

	// Try each IP with each credential
	for _, ip := range ips {

		// For each credential, try to connect
		for credID, cred := range credentials {

			if isReachable(ip, cred.Port) {

				// Create a provision record for reachable devices
				provision := models.NewProvision(ip, credID, discovery.DiscoveryID)

				_, err := q.provRepo.Create(provision)

				if err != nil {
					log.Printf("Failed to create provision for %s: %v", ip, err)
				}

				// We found one working credential, so we can break
				break
			}
		}
	}

	return nil
}

// expandIPRange expands an IP range in CIDR notation to individual IPs
func expandIPRange(ipRange string) ([]string, error) {

	if !strings.Contains(ipRange, "/") {
		// Single IP or invalid format
		return []string{ipRange}, nil
	}

	// Parse CIDR
	_, ipNet, err := net.ParseCIDR(ipRange)

	if err != nil {
		return nil, err
	}

	// Get all IPs in range
	var ips []string

	for ip := ipNet.IP.Mask(ipNet.Mask); ipNet.Contains(ip); incrementIP(ip) {

		ips = append(ips, ip.String())

	}

	// Remove network and broadcast addresses for IPv4
	if len(ips) > 2 && strings.Contains(ipRange, ".") {

		ips = ips[1 : len(ips)-1]

	}

	return ips, nil
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

// isReachable checks if a device is reachable on the specified port
func isReachable(ip string, port int) bool {

	address := fmt.Sprintf("%s:%d", ip, port)

	conn, err := net.DialTimeout("tcp", address, 3*time.Second)

	if err != nil {
		return false
	}

	conn.Close()

	return true

}
