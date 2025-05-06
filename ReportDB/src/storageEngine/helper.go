package storageEngine

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	. "packx/utils"
	"path/filepath"
)

// gets the current storage path
func (bs *StorageEngine) getStoragePath() string {

	bs.pathLock.RLock()

	defer bs.pathLock.RUnlock()

	return bs.storagePath

}

func (bs *StorageEngine) updateIndex(indexPath string, deviceID int, offset int64) error {

	var index []IndexEntry

	// Check if file exists
	if _, err := os.Stat(indexPath); !os.IsNotExist(err) {

		// Read existing index if it exists
		data, err := os.ReadFile(indexPath)

		if err != nil {
			return fmt.Errorf("failed to read index file: %v", err)
		}

		// Skip empty files
		if len(data) > 0 {

			if err := json.Unmarshal(data, &index); err != nil {
				// If cannot parse, start with empty index
				index = []IndexEntry{}
			}

		}

	}

	bs.blockManager.mu.Lock()

	currentOffset, hasCurrent := bs.blockManager.currentBlock[deviceID]

	bs.blockManager.mu.Unlock()

	// Add or update entry
	entry := IndexEntry{

		DeviceID: deviceID,

		BlockOffset: offset,

		CurrentOffset: currentOffset,
	}

	found := false

	for i, e := range index {

		if e.DeviceID == deviceID {

			// Preserve the block offset if it's not a new block allocation
			if hasCurrent && offset == currentOffset {

				entry.BlockOffset = e.BlockOffset

			}

			index[i] = entry

			found = true

			break

		}
	}

	if !found {

		index = append(index, entry)

	}

	// Write updated index
	data, err := json.MarshalIndent(index, "", "    ")

	if err != nil {
		return fmt.Errorf("failed to marshal index: %v", err)
	}

	if err := os.WriteFile(indexPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write index: %v", err)
	}

	return nil
}

func (bs *StorageEngine) readIndex(indexPath string) ([]IndexEntry, error) {

	// Check if file exists
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {

		return []IndexEntry{}, nil

	}

	data, err := os.ReadFile(indexPath)

	if err != nil {

		return nil, fmt.Errorf("failed to read index: %v", err)

	}

	// Skip empty files
	if len(data) == 0 {

		return []IndexEntry{}, nil

	}

	var index []IndexEntry

	if err := json.Unmarshal(data, &index); err != nil {

		return nil, fmt.Errorf("failed to parse index: %v", err)

	}

	return index, nil
}

func extractTimestampFromData(data []byte) (uint32, error) {

	if len(data) == 0 {
		return 0, fmt.Errorf("empty data")
	}

	return binary.LittleEndian.Uint32(data[:4]), nil

}

func getIndexFilePath(baseDir string, partition int) string {

	return filepath.Join(baseDir, fmt.Sprintf("partition_%d", partition), "index.json")

}

func getDataFilePath(baseDir string, partition int) string {

	return filepath.Join(baseDir, fmt.Sprintf("partition_%d", partition), "data.bin")

}

func readIndex(baseDir string, partition int) ([]IndexEntry, error) {

	indexFile := getIndexFilePath(baseDir, partition)

	data, err := os.ReadFile(indexFile)

	if err != nil {

		if os.IsNotExist(err) {
			return []IndexEntry{}, nil
		}

		return nil, fmt.Errorf("failed to read unified index file: %v", err)

	}

	var index []IndexEntry

	if err := json.Unmarshal(data, &index); err != nil {

		return nil, fmt.Errorf("failed to unmarshal unified index file: %v", err)

	}

	return index, nil
}

func writeIndex(baseDir string, partition int, index []IndexEntry) error {

	indexFile := getIndexFilePath(baseDir, partition)

	// Marshal with indentation for readability
	data, err := json.MarshalIndent(index, "", "    ")

	if err != nil {
		return fmt.Errorf("failed to marshal unified index file: %v", err)
	}

	// newline at the end of the file
	data = append(data, '\n')

	if err := os.WriteFile(indexFile, data, 0644); err != nil {

		return fmt.Errorf("failed to write unified index file: %v", err)

	}

	return nil
}

func findDeviceIndex(index []IndexEntry, deviceID int) (int, bool) {

	for i, entry := range index {

		if entry.DeviceID == deviceID {
			return i, true
		}

	}

	return -1, false
}

func getCurrentBlockOffset(index []IndexEntry, deviceID int) (int64, bool) {

	for _, entry := range index {

		if entry.DeviceID == deviceID && entry.CurrentOffset != 0 {
			return entry.CurrentOffset, true
		}

	}

	return 0, false

}

func updateCurrentBlockOffset(index []IndexEntry, deviceID int, offset int64) []IndexEntry {

	for i, entry := range index {

		if entry.DeviceID == deviceID {

			index[i].CurrentOffset = offset

			return index

		}

	}

	return index

}

func (bs *StorageEngine) getMappedDataFile(path string) (*MappedFile, error) {
	bs.mmapFilesLock.Lock()
	defer bs.mmapFilesLock.Unlock()

	if mmap, exists := bs.mmapFiles[path]; exists {
		return mmap, nil
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for data file: %v", err)
	}

	initialSize := BlockSize * 1024 // Initial size for the mmaping the file

	fileInfo, err := os.Stat(path)
	if err == nil && fileInfo.Size() > int64(initialSize) {
		initialSize = int(fileInfo.Size()) + BlockSize*1024
	}

	mmap, err := openMappedFile(path, initialSize)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %v", err)
	}

	bs.mmapFiles[path] = mmap
	return mmap, nil
}

func encodeBlockHeader(header BlockHeader) []byte {
	buf := make([]byte, BlockHeaderSize)
	binary.LittleEndian.PutUint32(buf[0:4], header.DeviceID)
	binary.LittleEndian.PutUint32(buf[4:8], header.StartTimestamp)
	binary.LittleEndian.PutUint32(buf[8:12], header.EndTimestamp)
	binary.LittleEndian.PutUint64(buf[12:20], uint64(header.NextBlockOffset))
	binary.LittleEndian.PutUint32(buf[20:24], header.RecordCount)
	buf[24] = header.DataType
	return buf
}

func decodeBlockHeader(data []byte) BlockHeader {

	return BlockHeader{

		DeviceID: binary.LittleEndian.Uint32(data[0:4]),

		StartTimestamp: binary.LittleEndian.Uint32(data[4:8]),

		EndTimestamp: binary.LittleEndian.Uint32(data[8:12]),

		NextBlockOffset: int64(binary.LittleEndian.Uint64(data[12:20])),

		RecordCount: binary.LittleEndian.Uint32(data[20:24]),

		DataType: data[24],
	}
}

func encodeOffsetTableEntry(entry OffsetTableEntry) []byte {
	buf := make([]byte, OffsetTableEntrySize)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(entry.Timestamp))
	binary.LittleEndian.PutUint16(buf[8:10], entry.Offset)
	binary.LittleEndian.PutUint16(buf[10:12], entry.Length)
	buf[12] = entry.Type
	return buf
}

func decodeOffsetTableEntry(data []byte) OffsetTableEntry {

	return OffsetTableEntry{

		Timestamp: int64(binary.LittleEndian.Uint64(data[0:8])),

		Offset: binary.LittleEndian.Uint16(data[8:10]),

		Length: binary.LittleEndian.Uint16(data[10:12]),

		Type: data[12],
	}
}

//func (bs *BlockStorageEngine) getMappedDataFile(path string) (*MappedFile, error) {
//	bs.mmapFilesLock.Lock()
//	defer bs.mmapFilesLock.Unlock()
//
//	if mmap, exists := bs.mmapFiles[path]; exists && !mmap.isClosed {
//		return mmap, nil
//	}
//
//	// Create directory if needed
//	dir := filepath.Dir(path)
//	if err := os.MkdirAll(dir, 0755); err != nil {
//		return nil, fmt.Errorf("failed to create directory: %v", err)
//	}
//
//	// Initial size calculation
//	initialSize := BlockSize * 16 // Start with space for 16 blocks
//
//	// Open or create file
//	mmap, err := openMappedFile(path, initialSize)
//	if err != nil {
//		return nil, fmt.Errorf("failed to create memory mapping: %v", err)
//	}
//
//	bs.mmapFiles[path] = mmap
//	return mmap, nil
//}

// Update getBlockUsage to use proper BlockManager fields
func (bs *StorageEngine) getBlockUsage(deviceID int, blockOffset int64) (int, error) {
	bs.blockManager.mu.Lock()
	defer bs.blockManager.mu.Unlock()

	if currentOffset, exists := bs.blockManager.currentBlock[deviceID]; exists && blockOffset == currentOffset {
		if usage, exists := bs.blockManager.blockUsage[deviceID]; exists {
			return usage, nil
		}
	}

	return BlockSize - BlockHeaderSize, nil
}

func (bs *StorageEngine) hasSpaceInBlock(deviceID int, dataSize int) bool {
	bs.blockManager.mu.Lock()
	defer bs.blockManager.mu.Unlock()

	if _, exists := bs.blockManager.currentBlock[deviceID]; exists {
		if usage, exists := bs.blockManager.blockUsage[deviceID]; exists {
			return usage+dataSize <= BlockSize-BlockHeaderSize
		}
	}
	return false
}

// Update updateBlockHeader to use proper BlockManager fields
func (bs *StorageEngine) updateBlockHeader(mmapFile *MappedFile, offset int64, newData []byte) error {
	headerData := make([]byte, BlockHeaderSize)
	if _, err := mmapFile.ReadAt(headerData, offset); err != nil {
		return fmt.Errorf("failed to read header: %v", err)
	}

	header := decodeBlockHeader(headerData)
	header.RecordCount++

	// Update timestamps if data contains timestamp
	if len(newData) >= 4 {
		timestamp := binary.LittleEndian.Uint32(newData[:4])
		if timestamp > header.EndTimestamp {
			header.EndTimestamp = timestamp
		}
		if header.StartTimestamp == 0 {
			header.StartTimestamp = timestamp
		}
	}

	headerBytes := encodeBlockHeader(header)
	if _, err := mmapFile.WriteAt(headerBytes, offset); err != nil {
		return fmt.Errorf("failed to update header: %v", err)
	}

	return nil
}

// Update initializeBlockHeader to use proper BlockManager fields
func (bs *StorageEngine) initializeBlockHeader(deviceID int, dataType byte, timestamp uint32) BlockHeader {

	return BlockHeader{

		DeviceID: uint32(deviceID),

		StartTimestamp: timestamp,

		EndTimestamp: timestamp,

		RecordCount: 1,

		DataType: dataType,
	}
}

// GetAllDeviceIDs returns a list of all device IDs found in storage across all partitions
func (bs *StorageEngine) GetAllDeviceIDs() ([]uint32, error) {
	basePath := bs.getStoragePath()
	if basePath == "" {
		return nil, fmt.Errorf("storage path not set")
	}

	deviceIDs := make(map[uint32]bool) // Use map to ensure uniqueness

	// Scan through all partitions
	for partition := 0; partition < NumPartitions; partition++ {
		partitionPath := filepath.Join(basePath, fmt.Sprintf("partition_%d", partition))
		indexPath := filepath.Join(partitionPath, "index.json")

		// Skip if partition directory doesn't exist
		if _, err := os.Stat(partitionPath); os.IsNotExist(err) {
			continue
		}

		// Skip if index file doesn't exist
		if _, err := os.Stat(indexPath); os.IsNotExist(err) {
			continue
		}

		// Read index file
		index, err := bs.readIndex(indexPath)
		if err != nil {
			log.Printf("Error reading index for partition %d: %v", partition, err)
			continue // Skip this partition if there's an error
		}

		// Extract unique device IDs
		for _, entry := range index {
			deviceIDs[uint32(entry.DeviceID)] = true
		}
	}

	// Convert map keys to slice
	result := make([]uint32, 0, len(deviceIDs))
	for id := range deviceIDs {
		result = append(result, id)
	}

	return result, nil
}
