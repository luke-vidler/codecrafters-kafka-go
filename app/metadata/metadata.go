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

		// Parse records in the batch
		if err := cm.parseRecords(batch); err != nil {
			// Continue to next batch instead of failing
			continue
		}
	}

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

	for i := int32(0); i < batch.RecordCount; i++ {
		// Read record length (varint)
		recordLen, n := binary.Varint(data[offset:])
		if n <= 0 {
			return fmt.Errorf("failed to read record length")
		}
		offset += n

		if offset+int(recordLen) > len(data) {
			return fmt.Errorf("record length exceeds data")
		}

		recordData := data[offset : offset+int(recordLen)]
		offset += int(recordLen)

		// Parse the record
		if err := cm.parseRecord(recordData); err != nil {
			// Continue on error to be lenient
			continue
		}
	}

	return nil
}

func (cm *ClusterMetadata) parseRecord(data []byte) error {
	offset := 0

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

	// Read key (keyLen can be -1 for null)
	if keyLen == -1 {
		// Null key - skip
	} else if keyLen > 0 {
		if offset+int(keyLen) > len(data) {
			return fmt.Errorf("key length exceeds data")
		}
		// Skip key data
		offset += int(keyLen)
	}

	// Read value length (varint)
	valueLen, n := binary.Varint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read value length")
	}
	offset += n

	// Read value
	var value []byte
	if valueLen == -1 {
		// Null value
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

	// Parse the actual record from the value
	// For metadata records, the key is typically null and the record type is in the value
	if value != nil && len(value) > 0 {
		recordType := parseRecordTypeFromValue(value)

		// Parse based on record type
		switch recordType {
		case 2: // TopicRecord
			// Skip frame version and record type bytes
			if len(value) > 2 {
				cm.parseTopicRecord(value[2:])
			}
		case 3: // PartitionRecord
			// Skip frame version and record type bytes
			if len(value) > 2 {
				cm.parsePartitionRecord(value[2:])
			}
		default:
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

func parseRecordTypeFromValue(value []byte) int8 {
	if len(value) >= 2 {
		// First byte is the frame version, second byte is the record type
		return int8(value[1])
	}
	return -1
}

func (cm *ClusterMetadata) parseTopicRecord(data []byte) error {
	offset := 0

	// Skip TAG_BUFFER at the beginning (for flexible versions)
	if offset < len(data) {
		offset++ // Skip TAG_BUFFER byte
	}

	// Read topic name (COMPACT_STRING - uses unsigned varint)
	nameLen, n := readUvarint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read name length")
	}
	offset += n
	nameLen-- // Compact string encoding: length = N + 1

	if offset+nameLen > len(data) {
		return fmt.Errorf("name length %d exceeds data: offset=%d, data len=%d", nameLen, offset, len(data))
	}
	name := string(data[offset : offset+nameLen])
	offset += nameLen

	// Read topic ID (UUID - 16 bytes)
	if offset+16 > len(data) {
		return fmt.Errorf("not enough data for topic ID: need %d, have %d", offset+16, len(data))
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

	return nil
}

func (cm *ClusterMetadata) parsePartitionRecord(data []byte) error {
	offset := 0

	// Skip TAG_BUFFER at the beginning (for flexible versions)
	if offset < len(data) {
		offset++ // Skip TAG_BUFFER byte
	}

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

	// Read replicas (COMPACT_ARRAY - uses unsigned varint)
	replicasLen, n := readUvarint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read replicas length")
	}
	offset += n
	replicasLen-- // Compact array encoding: length = N + 1

	replicas := make([]int32, replicasLen)
	for i := 0; i < replicasLen; i++ {
		if offset+4 > len(data) {
			return fmt.Errorf("not enough data for replica")
		}
		replicas[i] = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
	}

	// Read ISR (COMPACT_ARRAY - uses unsigned varint)
	isrLen, n := readUvarint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read ISR length")
	}
	offset += n
	isrLen-- // Compact array encoding: length = N + 1

	isr := make([]int32, isrLen)
	for i := 0; i < isrLen; i++ {
		if offset+4 > len(data) {
			return fmt.Errorf("not enough data for ISR")
		}
		isr[i] = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
	}

	// Skip removing replicas (COMPACT_ARRAY - uses unsigned varint)
	removingLen, n := readUvarint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read removing replicas length")
	}
	offset += n
	removingLen-- // Compact array encoding: length = N + 1
	offset += removingLen * 4

	// Skip adding replicas (COMPACT_ARRAY - uses unsigned varint)
	addingLen, n := readUvarint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read adding replicas length")
	}
	offset += n
	addingLen-- // Compact array encoding: length = N + 1
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

func readUvarint(data []byte) (int, int) {
	v, n := binary.Uvarint(data)
	return int(v), n
}

// ReadPartitionLog reads the log file for a topic partition and returns the raw bytes
func ReadPartitionLog(topicName string, partitionID int32) ([]byte, error) {
	logPath := fmt.Sprintf("/tmp/kraft-combined-logs/%s-%d/00000000000000000000.log", topicName, partitionID)

	data, err := os.ReadFile(logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read log file: %w", err)
	}

	return data, nil
}

// WriteRecordBatch writes a RecordBatch to the partition log file
// The recordBatch parameter should contain the raw RecordBatch data from the Produce request
func WriteRecordBatch(topicName string, partitionID int32, recordBatch []byte) error {
	logDir := fmt.Sprintf("/tmp/kraft-combined-logs/%s-%d", topicName, partitionID)
	logPath := fmt.Sprintf("%s/00000000000000000000.log", logDir)

	// Create directory if it doesn't exist
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	// Open file in append mode, create if doesn't exist
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}
	defer file.Close()

	// Write the RecordBatch to the log file
	// The recordBatch should already be in the correct format with:
	// - base_offset (8 bytes)
	// - batch_length (4 bytes)
	// - rest of the RecordBatch
	if _, err := file.Write(recordBatch); err != nil {
		return fmt.Errorf("failed to write record batch: %w", err)
	}

	return nil
}

// TopicExists checks if a topic exists in the cluster metadata
func (cm *ClusterMetadata) TopicExists(topicName string) bool {
	_, exists := cm.Topics[topicName]
	return exists
}

// GetTopicID returns the topic ID for a given topic name
func (cm *ClusterMetadata) GetTopicID(topicName string) ([16]byte, bool) {
	topic, exists := cm.Topics[topicName]
	if !exists {
		return [16]byte{}, false
	}
	return topic.TopicID, true
}

// PartitionExists checks if a partition exists for a given topic
func (cm *ClusterMetadata) PartitionExists(topicName string, partitionIndex int32) bool {
	topicID, exists := cm.GetTopicID(topicName)
	if !exists {
		return false
	}

	partitions, exists := cm.Partitions[topicID]
	if !exists {
		return false
	}

	for _, partition := range partitions {
		if partition.PartitionID == partitionIndex {
			return true
		}
	}
	return false
}
