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
	DeviceID int32

	StartTimestamp int64

	EndTimestamp int64

	NextBlockOffset int64

	RecordCount int32
}

type OffsetTableEntry struct {
	Timestamp int64 // Timestamp of the record

	Offset uint16 // Offset from the start of the block

	Length uint16 // Length of the record

	Type byte // Type of the record (1=float, 2=string, 3=int)
}

type IndexEntry struct {
	DeviceID int `json:"device_id"`

	Date string `json:"date"`

	BlockOffset int64 `json:"block_offset"`

	CurrentOffset int64 `json:"current_offset"`
}

type BlockManager struct {
	mu sync.Mutex

	nextOffset map[int]int64
}

func newBlockManager() *BlockManager {

	return &BlockManager{

		nextOffset: make(map[int]int64),
	}

}

func (bm *BlockManager) getNextBlockOffset(deviceID int) int64 {

	bm.mu.Lock()

	defer bm.mu.Unlock()

	if offset, exists := bm.nextOffset[deviceID]; exists {

		// Increment by BlockSize for next time
		bm.nextOffset[deviceID] = offset + BlockSize

		return offset

	}

	// First block for this device
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

	return &StorageEngine{

		mmapFiles: make(map[string]*MappedFile),

		blockManager: newBlockManager(),
	}, nil

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

	offset := bs.blockManager.getNextBlockOffset(key)

	requiredSize := offset + BlockSize

	if requiredSize > int64(mmapFile.size) {

		newSize := ((requiredSize / BlockSize) + 1) * BlockSize

		if err := mmapFile.grow(int(newSize)); err != nil {

			return fmt.Errorf("failed to extend mapping: %v", err)

		}

	}

	// Create header
	header := BlockHeader{

		DeviceID: int32(key),

		RecordCount: 1,
	}

	// Extract timestamp if available (first 8 bytes of data)
	if len(data) >= 8 {

		timestamp := int64(binary.LittleEndian.Uint64(data[:8]))

		header.StartTimestamp = timestamp

		header.EndTimestamp = timestamp

	}

	headerBytes := encodeBlockHeader(header)

	// Write header
	if _, err := mmapFile.WriteAt(headerBytes, offset); err != nil {

		return fmt.Errorf("failed to write header: %v", err)

	}

	// Write data
	if _, err := mmapFile.WriteAt(data, offset+BlockHeaderSize); err != nil {

		return fmt.Errorf("failed to write data: %v", err)

	}

	// Update index
	indexPath := filepath.Join(partitionPath, "index.json")

	if err := bs.updateIndex(indexPath, key, offset); err != nil {

		return fmt.Errorf("failed to update index: %v", err)

	}

	return nil
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
