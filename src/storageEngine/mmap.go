package storageEngine

import (
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"sync"
	"syscall"
)

// MappedFile represents a memory-mapped file
type MappedFile struct {
	file          *os.File
	data          []byte
	size          int
	currentOffset int64
	mu            sync.RWMutex
	isClosed      bool
}

// openMappedFile opens a file and maps it into memory
func openMappedFile(path string, initialSize int) (*MappedFile, error) {

	// Open the file with read/write permissions
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {

		return nil, fmt.Errorf("failed to open file: %v", err)
	}

	// Get file stats
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %v", err)
	}

	// Ensure the file is at least initialSize in length
	size := int(info.Size())
	if size < initialSize {
		if err := file.Truncate(int64(initialSize)); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to truncate file: %v", err)
		}
		size = initialSize
	}

	// Memory-map the file
	data, err := syscall.Mmap(
		int(file.Fd()),
		0,
		size,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to mmap file: %v", err)
	}

	return &MappedFile{
		file:          file,
		data:          data,
		size:          size,
		currentOffset: int64(size),
		isClosed:      false,
	}, nil
}

// ReadAt reads data from the mapped file at the specified offset
func (m *MappedFile) ReadAt(b []byte, offset int64) (int, error) {
	m.mu.RLock()

	defer m.mu.RUnlock()

	if m.isClosed {

		return 0, fmt.Errorf("file already closed")

	}

	if offset+int64(len(b)) > int64(m.size) {

		return 0, fmt.Errorf("read would exceed mapped region size")

	}

	copy(b, m.data[offset:offset+int64(len(b))])

	return len(b), nil

}

// WriteAt writes data to the mapped file at the specified offset
func (m *MappedFile) WriteAt(b []byte, offset int64) (int, error) {

	m.mu.Lock()

	defer m.mu.Unlock()

	if m.isClosed {

		return 0, fmt.Errorf("file already closed")

	}

	if offset+int64(len(b)) > int64(m.size) {

		return 0, fmt.Errorf("write would exceed mapped region size")

	}

	copy(m.data[offset:], b)

	// Update current offset if this write extends it
	if offset+int64(len(b)) > m.currentOffset {

		m.currentOffset = offset + int64(len(b))

	}

	return len(b), nil
}

// grow increases the size of the mapped region
func (m *MappedFile) grow(newSize int) error {

	m.mu.Lock()

	defer m.mu.Unlock()

	if m.isClosed {

		return fmt.Errorf("file already closed")

	}

	if newSize <= m.size {

		return nil // Already large enough

	}

	// Unmap current region
	if err := syscall.Munmap(m.data); err != nil {

		return fmt.Errorf("failed to unmap file: %v", err)

	}

	// Extend file size
	if err := m.file.Truncate(int64(newSize)); err != nil {

		return fmt.Errorf("failed to extend file: %v", err)

	}

	// Re-map with new size
	data, err := syscall.Mmap(

		int(m.file.Fd()),

		0,

		newSize,

		syscall.PROT_READ|syscall.PROT_WRITE,

		syscall.MAP_SHARED,
	)

	if err != nil {

		return fmt.Errorf("failed to re-map file: %v", err)

	}

	m.data = data

	m.size = newSize

	return nil

}

// sync flushes changes to disk
func (m *MappedFile) sync() error {

	m.mu.RLock()

	defer m.mu.RUnlock()

	if m.isClosed {

		return fmt.Errorf("file already closed")

	}

	return unix.Msync(m.data, unix.MS_SYNC)

}

// syncAndClose syncs data to disk and closes the file
func (m *MappedFile) syncAndClose() error {

	m.mu.Lock()

	defer m.mu.Unlock()

	if m.isClosed {

		return fmt.Errorf("file already closed")

	}

	// Sync changes to disk
	if err := unix.Msync(m.data, unix.MS_SYNC); err != nil {

		return fmt.Errorf("failed to sync file: %v", err)

	}

	// Unmap memory
	if err := syscall.Munmap(m.data); err != nil {

		return fmt.Errorf("failed to unmap file: %v", err)

	}

	// Close file
	if err := m.file.Close(); err != nil {

		return fmt.Errorf("failed to close file: %v", err)

	}

	m.isClosed = true

	return nil

}

// min returns the minimum of two int64 values
func min(a, b int64) int64 {

	if a < b {

		return a

	}

	return b

}
