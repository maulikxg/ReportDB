package utils

const (

	// BlockSize is the size of each block (4KB)
	BlockSize = 4 * 1024

	// BlockHeaderSize is the size of block header
	BlockHeaderSize = 32

	// OffsetTableEntrySize is the size of each offset table entry
	OffsetTableEntrySize = 16

	// MaxRecordsPerBlock is the maximum number of records per block
	// This is a conservative estimate considering variable-length values
	MaxRecordsPerBlock = 100

	// MaxStringLength is the maximum length of a string value
	// If a string is longer than this, it will be truncated
	MaxStringLength = 1024

	// MinSpaceForOffsetTable ensures we always have space for at least this many entries
	MinSpaceForOffsetTable = 10 * OffsetTableEntrySize

	// NumPartitions is the number of partitions
	NumPartitions = 3

	// NumCounters is the number of counters
	NumCounters = 3

	// StoragePath is the base path for storage
	StoragePath = "storage"

	//// Data type markers
	//TypeFloat  = byte(1)
	//TypeString = byte(2)
	//TypeInt    = byte(3)

)
