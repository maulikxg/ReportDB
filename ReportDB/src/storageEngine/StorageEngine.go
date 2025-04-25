package storageEngine

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	. "packx/utils"
	"path/filepath"
	"sync"
)

type BlockHeader struct {
	DeviceID        uint32 // 4 bytes - matches ObjectID type
	StartTimestamp  uint32
	EndTimestamp    uint32
	NextBlockOffset int64
	RecordCount     uint32
	DataType        byte // 1 byte - indicates value type
}

type OffsetTableEntry struct {
	Timestamp int64

	Offset uint16 // Offset from the start of the block

	Length uint16 // Length of the record

	Type byte // Type of the record (1=float, 2=string, 3=int)
}

type IndexEntry struct {
	DeviceID int `json:"device_id"`

	BlockOffset int64 `json:"block_offset"`

	CurrentOffset int64 `json:"current_offset"`
}

type BlockManager struct {
	mu sync.Mutex

	nextOffset map[int]int64

	// Tracking the how many bytes are used in each block
	blockUsage map[int]int

	// Tracking current block offset for each device
	currentBlock map[int]int64
}

func newBlockManager() *BlockManager {

	return &BlockManager{

		nextOffset: make(map[int]int64),

		blockUsage: make(map[int]int),

		currentBlock: make(map[int]int64),
	}
}

func (bm *BlockManager) getNextBlockOffset(deviceID int) int64 {

	bm.mu.Lock()

	defer bm.mu.Unlock()

	if offset, exists := bm.nextOffset[deviceID]; exists {

		nextOffset := offset + BlockSize

		bm.nextOffset[deviceID] = nextOffset

		return offset

	}

	// First block for this device - check if we have a persisted offset
	if offset, exists := bm.currentBlock[deviceID]; exists {

		nextOffset := offset + BlockSize

		bm.nextOffset[deviceID] = nextOffset

		return offset
	}

	// Truly first block for this device
	baseOffset := int64(0)

	bm.nextOffset[deviceID] = baseOffset + BlockSize

	return baseOffset

}

type StorageEngine struct {
	partitionLocks [NumPartitions]sync.RWMutex

	indexLocks [NumPartitions]sync.RWMutex

	mmapFiles map[string]*MappedFile

	mmapFilesLock sync.Mutex

	basedir string // base Directory for the strore the all data

	blockManager *BlockManager

	storagePath string

	pathLock sync.RWMutex
}

func NewStorageEngine() (*StorageEngine, error) {

	engine := &StorageEngine{

		mmapFiles: make(map[string]*MappedFile),

		blockManager: newBlockManager(),
	}

	// Initialize block manager with persisted state
	if err := engine.initializeBlockManagerState(); err != nil {

		log.Printf("Warning: Failed to initialize block manager state: %v", err)

	}

	return engine, nil
}

// sets the storage path for the engine
func (bs *StorageEngine) SetStoragePath(path string) error {

	bs.pathLock.Lock()

	defer bs.pathLock.Unlock()

	// Ensure directory exists
	if err := os.MkdirAll(path, 0755); err != nil {

		return fmt.Errorf("failed to create directory structure: %v", err)

	}

	bs.storagePath = path

	return nil
}

func (bs *StorageEngine) Put(key int, data []byte) error {

	basePath := bs.getStoragePath()

	if basePath == "" {
		return fmt.Errorf("storage path not set")
	}

	partition := key % NumPartitions

	partitionPath := filepath.Join(basePath, fmt.Sprintf("partition_%d", partition))

	bs.partitionLocks[partition].Lock()

	defer bs.partitionLocks[partition].Unlock()

	if err := os.MkdirAll(partitionPath, 0755); err != nil {

		log.Printf("failed to create directory structure: %v", err)

		return err

	}

	// Get data file path
	dataFile := filepath.Join(partitionPath, "data.bin")

	// Get or create memory-mapped file
	mmapFile, err := bs.getMappedDataFile(dataFile)

	if err != nil {

		log.Printf("failed to get mapped file: %v", err)

		return err

	}

	// Check if we can use existing block

	var offset int64

	var isNewBlock bool

	if bs.hasSpaceInBlock(key, len(data)) {

		// using the existing block
		currentOffset, _ := bs.blockManager.currentBlock[key]

		offset = currentOffset

		isNewBlock = false

	} else {

		// new block
		offset = bs.blockManager.getNextBlockOffset(key)

		isNewBlock = true

	}

	requiredSize := offset + BlockSize

	if requiredSize > int64(mmapFile.size) {

		newSize := ((requiredSize / BlockSize) + 1) * BlockSize

		if err := mmapFile.grow(int(newSize)); err != nil {
			return fmt.Errorf("failed to extend mapping: %v", err)
		}

	}

	var timestamp uint32

	if len(data) >= 4 {

		timestamp = binary.LittleEndian.Uint32(data[:4])

	}

	if isNewBlock {

		header := bs.initializeBlockHeader(key, determineDataType(data), timestamp)

		headerBytes := encodeBlockHeader(header)

		if _, err := mmapFile.WriteAt(headerBytes, offset); err != nil {

			return fmt.Errorf("failed to write header: %v", err)

		}

		bs.blockManager.mu.Lock()

		bs.blockManager.currentBlock[key] = offset

		bs.blockManager.blockUsage[key] = 0

		bs.blockManager.mu.Unlock()

	} else {

		if err := bs.updateBlockHeader(mmapFile, offset, data); err != nil {

			return fmt.Errorf("failed to update header: %v", err)

		}

	}

	writeOffset := offset + BlockHeaderSize

	if !isNewBlock {

		usage, _ := bs.getBlockUsage(key, offset)

		writeOffset += int64(usage)

	}

	// Write data
	if _, err := mmapFile.WriteAt(data, writeOffset); err != nil {
		return fmt.Errorf("failed to write data: %v", err)
	}

	// Update block usage
	bs.blockManager.mu.Lock()

	bs.blockManager.blockUsage[key] += len(data)

	bs.blockManager.mu.Unlock()

	// Update index
	indexPath := filepath.Join(partitionPath, "index.json")

	if err := bs.updateIndex(indexPath, key, offset); err != nil {

		return fmt.Errorf("failed to update index: %v", err)

	}

	return nil
}

func determineDataType(data []byte) byte {

	// Skip timestamp (first 4 bytes)
	if len(data) < 12 { // Minimum size for data (timestamp + value)
		return TypeInt // Default to int if data is too short
	}

	valueType := data[4]

	switch valueType {

	case TypeFloat:
		return TypeFloat

	case TypeString:
		return TypeString

	default:
		return TypeInt

	}
}

// retrieves data for a any device using the storage path
func (bs *StorageEngine) GetByPath(deviceID int, path string) ([][]byte, error) {

	if err := bs.SetStoragePath(path); err != nil {

		return nil, fmt.Errorf("failed to set storage path: %v", err)

	}

	return bs.Get(deviceID)
}

func (bs *StorageEngine) Get(deviceID int) ([][]byte, error) {

	// Get the current storage path
	basePath := bs.getStoragePath()

	if basePath == "" {

		return nil, fmt.Errorf("storage path not set")

	}

	// Calculate partition
	partition := deviceID % NumPartitions

	// Create the partition path
	partitionPath := filepath.Join(basePath, fmt.Sprintf("partition_%d", partition))

	bs.partitionLocks[partition].RLock()

	defer bs.partitionLocks[partition].RUnlock()

	// Get data file path
	dataFile := filepath.Join(partitionPath, "data.bin")

	// Check if file exists
	if _, err := os.Stat(dataFile); os.IsNotExist(err) {

		return [][]byte{}, nil // Return empty slice if file doesn't exist

	}

	mmapFile, err := bs.getMappedDataFile(dataFile)

	if err != nil {

		return nil, fmt.Errorf("failed to get mapped file: %v", err)

	}

	indexPath := filepath.Join(partitionPath, "index.json")

	index, err := bs.readIndex(indexPath)

	if err != nil {

		return nil, err

	}

	// Find device's data blocks
	var results [][]byte

	for _, entry := range index {

		if entry.DeviceID == deviceID {

			block := make([]byte, BlockSize)

			if _, err := mmapFile.ReadAt(block, entry.BlockOffset); err != nil {

				return nil, fmt.Errorf("failed to read block at offset %d: %v", entry.BlockOffset, err)

			}

			// Skip the header
			data := make([]byte, BlockSize-BlockHeaderSize)

			copy(data, block[BlockHeaderSize:])

			results = append(results, data)

		}

	}

	return results, nil
}

func (bs *StorageEngine) Close() error {

	bs.mmapFilesLock.Lock()

	defer bs.mmapFilesLock.Unlock()

	var errors []error

	for path, mmap := range bs.mmapFiles {

		if err := mmap.syncAndClose(); err != nil {

			errors = append(errors, fmt.Errorf("failed to close file %s: %v", path, err))

		}

		delete(bs.mmapFiles, path)

	}

	if len(errors) > 0 {

		return fmt.Errorf("errors closing files: %v", errors)

	}

	return nil
}

// Add new function to initialize block manager state
func (bs *StorageEngine) initializeBlockManagerState() error {

	basePath := bs.getStoragePath()

	if basePath == "" {
		return nil // No storage path set yet, skip initialization
	}

	// Scan through all partitions
	for partition := 0; partition < NumPartitions; partition++ {

		partitionPath := filepath.Join(basePath, fmt.Sprintf("partition_%d", partition))

		indexPath := filepath.Join(partitionPath, "index.json")

		// Read index file if it exists
		index, err := bs.readIndex(indexPath)

		if err != nil {
			continue // Skip this partition if there's an error
		}

		// Update block manager state from index entries
		for _, entry := range index {

			deviceID := entry.DeviceID

			bs.blockManager.mu.Lock()

			// Update nextOffset if this block offset is higher than what we have
			nextOffset := entry.BlockOffset + BlockSize

			if currentNext, exists := bs.blockManager.nextOffset[deviceID]; !exists || nextOffset > currentNext {

				bs.blockManager.nextOffset[deviceID] = nextOffset

			}

			// Set current block and usage if this is the current block
			if entry.CurrentOffset != 0 {

				bs.blockManager.currentBlock[deviceID] = entry.BlockOffset

				// Read the block header to get current usage
				if usage, err := bs.getBlockUsageFromHeader(entry.BlockOffset, partition); err == nil {

					bs.blockManager.blockUsage[deviceID] = usage

				}

			}

			bs.blockManager.mu.Unlock()
		}
	}
	return nil
}

// Add helper function to get block usage from header
func (bs *StorageEngine) getBlockUsageFromHeader(offset int64, partition int) (int, error) {

	partitionPath := filepath.Join(bs.getStoragePath(), fmt.Sprintf("partition_%d", partition))

	dataFile := filepath.Join(partitionPath, "data.bin")

	mmapFile, err := bs.getMappedDataFile(dataFile)

	if err != nil {
		return 0, err
	}

	headerData := make([]byte, BlockHeaderSize)

	if _, err := mmapFile.ReadAt(headerData, offset); err != nil {
		return 0, err
	}

	header := decodeBlockHeader(headerData)

	return int(header.RecordCount) * 12, nil
}
