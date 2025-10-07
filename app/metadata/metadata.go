package metadata

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// TopicMetadata stores information about a topic
type TopicMetadata struct {
	Name    string
	TopicID [16]byte // UUID
}

// PartitionMetadata stores information about a partition
type PartitionMetadata struct {
	PartitionID int32
	TopicID     [16]byte
	Replicas    []int32
	ISR         []int32
	Leader      int32
	LeaderEpoch int32
}

// ClusterMetadata stores all metadata
type ClusterMetadata struct {
	Topics     map[string]*TopicMetadata         // name -> metadata
	TopicsByID map[[16]byte]*TopicMetadata       // id -> metadata
	Partitions map[[16]byte][]*PartitionMetadata // topicID -> partitions
}

// NewClusterMetadata creates a new ClusterMetadata
func NewClusterMetadata() *ClusterMetadata {
	return &ClusterMetadata{
		Topics:     make(map[string]*TopicMetadata),
		TopicsByID: make(map[[16]byte]*TopicMetadata),
		Partitions: make(map[[16]byte][]*PartitionMetadata),
	}
}

// LoadFromLog reads the cluster metadata log file
func (cm *ClusterMetadata) LoadFromLog(logPath string) error {
	file, err := os.Open(logPath)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}
	defer file.Close()

	batchCount := 0
	for {
		// Read record batch
		batch, err := readRecordBatch(file)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read record batch: %w", err)
		}

		batchCount++
		fmt.Printf("Read batch %d: %d records\n", batchCount, batch.RecordCount)

		// Parse records in the batch
		if err := cm.parseRecords(batch); err != nil {
			fmt.Printf("Error parsing records in batch %d: %v\n", batchCount, err)
			// Continue to next batch instead of failing
			continue
		}
	}

	fmt.Printf("Total batches read: %d\n", batchCount)
	return nil
}

// RecordBatch represents a Kafka record batch
type RecordBatch struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  uint32
	Attributes           int16
	LastOffsetDelta      int32
	BaseTimestamp        int64
	MaxTimestamp         int64
	ProducerID           int64
	ProducerEpoch        int16
	BaseSequence         int32
	RecordCount          int32
	Records              []byte
}

func readRecordBatch(r io.Reader) (*RecordBatch, error) {
	batch := &RecordBatch{}

	// Read base offset (8 bytes)
	if err := binary.Read(r, binary.BigEndian, &batch.BaseOffset); err != nil {
		return nil, err
	}

	// Read batch length (4 bytes)
	if err := binary.Read(r, binary.BigEndian, &batch.BatchLength); err != nil {
		return nil, err
	}

	// Read the rest of the batch header and records
	batchData := make([]byte, batch.BatchLength)
	if _, err := io.ReadFull(r, batchData); err != nil {
		return nil, err
	}

	offset := 0

	// Parse batch header
	batch.PartitionLeaderEpoch = int32(binary.BigEndian.Uint32(batchData[offset : offset+4]))
	offset += 4

	batch.Magic = int8(batchData[offset])
	offset += 1

	batch.CRC = binary.BigEndian.Uint32(batchData[offset : offset+4])
	offset += 4

	batch.Attributes = int16(binary.BigEndian.Uint16(batchData[offset : offset+2]))
	offset += 2

	batch.LastOffsetDelta = int32(binary.BigEndian.Uint32(batchData[offset : offset+4]))
	offset += 4

	batch.BaseTimestamp = int64(binary.BigEndian.Uint64(batchData[offset : offset+8]))
	offset += 8

	batch.MaxTimestamp = int64(binary.BigEndian.Uint64(batchData[offset : offset+8]))
	offset += 8

	batch.ProducerID = int64(binary.BigEndian.Uint64(batchData[offset : offset+8]))
	offset += 8

	batch.ProducerEpoch = int16(binary.BigEndian.Uint16(batchData[offset : offset+2]))
	offset += 2

	batch.BaseSequence = int32(binary.BigEndian.Uint32(batchData[offset : offset+4]))
	offset += 4

	batch.RecordCount = int32(binary.BigEndian.Uint32(batchData[offset : offset+4]))
	offset += 4

	// Store remaining data as records
	batch.Records = batchData[offset:]

	return batch, nil
}

func (cm *ClusterMetadata) parseRecords(batch *RecordBatch) error {
	data := batch.Records
	offset := 0

	fmt.Printf("Parsing batch with %d records, data len=%d\n", batch.RecordCount, len(data))

	for i := int32(0); i < batch.RecordCount; i++ {
		// Read record length (varint)
		recordLen, n := binary.Varint(data[offset:])
		if n <= 0 {
			fmt.Printf("Failed to read record length at offset %d\n", offset)
			return fmt.Errorf("failed to read record length")
		}
		offset += n

		fmt.Printf("Record %d: length=%d, offset=%d\n", i, recordLen, offset)

		if offset+int(recordLen) > len(data) {
			fmt.Printf("Record length %d exceeds remaining data %d\n", recordLen, len(data)-offset)
			return fmt.Errorf("record length exceeds data")
		}

		recordData := data[offset : offset+int(recordLen)]
		offset += int(recordLen)

		// Parse the record
		if err := cm.parseRecord(recordData); err != nil {
			fmt.Printf("Error parsing record %d: %v\n", i, err)
			// Continue on error to be lenient
			continue
		}
	}

	return nil
}

func (cm *ClusterMetadata) parseRecord(data []byte) error {
	offset := 0

	fmt.Printf("Parsing record, data len=%d, hex=%x\n", len(data), data[:min(len(data), 20)])

	// Read attributes (int8)
	// attributes := int8(data[offset])
	offset += 1

	// Read timestamp delta (varint)
	_, n := binary.Varint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read timestamp delta")
	}
	offset += n

	// Read offset delta (varint)
	_, n = binary.Varint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read offset delta")
	}
	offset += n

	// Read key length (varint)
	keyLen, n := binary.Varint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read key length")
	}
	offset += n

	fmt.Printf("Key length: %d, offset now: %d\n", keyLen, offset)

	// Read key (keyLen can be -1 for null)
	var key []byte
	if keyLen == -1 {
		// Null key
		fmt.Printf("Key is null\n")
	} else if keyLen > 0 {
		if offset+int(keyLen) > len(data) {
			return fmt.Errorf("key length exceeds data")
		}
		key = data[offset : offset+int(keyLen)]
		offset += int(keyLen)
		fmt.Printf("Key: %x\n", key)
	}

	// Read value length (varint)
	valueLen, n := binary.Varint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read value length")
	}
	offset += n

	fmt.Printf("Value length: %d\n", valueLen)

	// Read value
	var value []byte
	if valueLen == -1 {
		// Null value
		fmt.Printf("Value is null\n")
	} else if valueLen > 0 {
		if offset+int(valueLen) > len(data) {
			return fmt.Errorf("value length exceeds data")
		}
		value = data[offset : offset+int(valueLen)]
		offset += int(valueLen)
	}

	// Read headers count (varint)
	headersCount, n := binary.Varint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read headers count")
	}
	offset += n

	fmt.Printf("Headers count: %d\n", headersCount)

	// Skip headers for now
	for i := int64(0); i < headersCount; i++ {
		// Read header key length
		headerKeyLen, n := binary.Varint(data[offset:])
		if n <= 0 {
			return fmt.Errorf("failed to read header key length")
		}
		offset += n
		offset += int(headerKeyLen) // Skip header key

		// Read header value length
		headerValueLen, n := binary.Varint(data[offset:])
		if n <= 0 {
			return fmt.Errorf("failed to read header value length")
		}
		offset += n
		offset += int(headerValueLen) // Skip header value
	}

	// Now parse the actual record if we have a key
	if key != nil && len(key) > 0 {
		recordType := parseRecordType(key)
		fmt.Printf("Record type: %d\n", recordType)

		if value != nil && len(value) > 0 {
			// Parse based on record type
			switch recordType {
			case 2: // TopicRecord
				fmt.Printf("Parsing TopicRecord...\n")
				cm.parseTopicRecord(value)
			case 3: // PartitionRecord
				fmt.Printf("Parsing PartitionRecord...\n")
				cm.parsePartitionRecord(value)
			default:
				fmt.Printf("Unknown record type: %d\n", recordType)
			}
		}
	}

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func parseRecordType(key []byte) int8 {
	if len(key) > 0 {
		// First byte is the frame version, second byte is the record type
		if len(key) > 1 {
			return int8(key[1])
		}
	}
	return -1
}

func (cm *ClusterMetadata) parseTopicRecord(data []byte) error {
	offset := 0

	// Read topic name (COMPACT_STRING)
	nameLen, n := readVarint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read name length")
	}
	offset += n
	nameLen-- // Compact string encoding

	if offset+nameLen > len(data) {
		return fmt.Errorf("name length exceeds data")
	}
	name := string(data[offset : offset+nameLen])
	offset += nameLen

	// Read topic ID (UUID - 16 bytes)
	if offset+16 > len(data) {
		return fmt.Errorf("not enough data for topic ID")
	}
	var topicID [16]byte
	copy(topicID[:], data[offset:offset+16])

	// Store the topic metadata
	topic := &TopicMetadata{
		Name:    name,
		TopicID: topicID,
	}
	cm.Topics[name] = topic
	cm.TopicsByID[topicID] = topic

	fmt.Printf("Parsed TopicRecord: name=%s, topicID=%x\n", name, topicID)
	return nil
}

func (cm *ClusterMetadata) parsePartitionRecord(data []byte) error {
	offset := 0

	// Read partition ID (int32)
	if offset+4 > len(data) {
		return fmt.Errorf("not enough data for partition ID")
	}
	partitionID := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// Read topic ID (UUID - 16 bytes)
	if offset+16 > len(data) {
		return fmt.Errorf("not enough data for topic ID")
	}
	var topicID [16]byte
	copy(topicID[:], data[offset:offset+16])
	offset += 16

	// Read replicas (COMPACT_ARRAY)
	replicasLen, n := readVarint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read replicas length")
	}
	offset += n
	replicasLen-- // Compact array encoding

	replicas := make([]int32, replicasLen)
	for i := 0; i < replicasLen; i++ {
		if offset+4 > len(data) {
			return fmt.Errorf("not enough data for replica")
		}
		replicas[i] = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
	}

	// Read ISR (COMPACT_ARRAY)
	isrLen, n := readVarint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read ISR length")
	}
	offset += n
	isrLen-- // Compact array encoding

	isr := make([]int32, isrLen)
	for i := 0; i < isrLen; i++ {
		if offset+4 > len(data) {
			return fmt.Errorf("not enough data for ISR")
		}
		isr[i] = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
	}

	// Skip removing replicas (COMPACT_ARRAY)
	removingLen, n := readVarint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read removing replicas length")
	}
	offset += n
	removingLen-- // Compact array encoding
	offset += removingLen * 4

	// Skip adding replicas (COMPACT_ARRAY)
	addingLen, n := readVarint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read adding replicas length")
	}
	offset += n
	addingLen-- // Compact array encoding
	offset += addingLen * 4

	// Read leader (int32)
	if offset+4 > len(data) {
		return fmt.Errorf("not enough data for leader")
	}
	leader := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// Skip leader recovery state (int8)
	offset += 1

	// Read leader epoch (int32)
	if offset+4 > len(data) {
		return fmt.Errorf("not enough data for leader epoch")
	}
	leaderEpoch := int32(binary.BigEndian.Uint32(data[offset : offset+4]))

	// Store the partition metadata
	partition := &PartitionMetadata{
		PartitionID: partitionID,
		TopicID:     topicID,
		Replicas:    replicas,
		ISR:         isr,
		Leader:      leader,
		LeaderEpoch: leaderEpoch,
	}

	cm.Partitions[topicID] = append(cm.Partitions[topicID], partition)

	return nil
}

func readVarint(data []byte) (int, int) {
	v, n := binary.Varint(data)
	return int(v), n
}
