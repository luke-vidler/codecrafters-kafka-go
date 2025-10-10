package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// API Keys
const (
	APIKeyProduce                 = 0
	APIKeyFetch                   = 1
	APIKeyAPIVersions             = 18
	APIKeyDescribeTopicPartitions = 75
)

// Error codes
const (
	ErrorNone                    = 0
	ErrorUnknownTopicOrPartition = 3
	ErrorUnsupportedVersion      = 35
	ErrorUnknownTopicID          = 100
)

// RequestHeader represents a Kafka request header (v2)
type RequestHeader struct {
	MessageSize   int32
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
}

// ReadRequestHeader reads a request header from the connection
func ReadRequestHeader(r io.Reader) (*RequestHeader, []byte, error) {
	// First read the message size
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, sizeBuf); err != nil {
		return nil, nil, fmt.Errorf("failed to read message size: %w", err)
	}
	messageSize := int32(binary.BigEndian.Uint32(sizeBuf))

	// Read the rest of the message
	messageBuf := make([]byte, messageSize)
	if _, err := io.ReadFull(r, messageBuf); err != nil {
		return nil, nil, fmt.Errorf("failed to read message body: %w", err)
	}

	if len(messageBuf) < 8 {
		return nil, nil, fmt.Errorf("message too short: got %d bytes, expected at least 8", len(messageBuf))
	}

	header := &RequestHeader{
		MessageSize:   messageSize,
		APIKey:        int16(binary.BigEndian.Uint16(messageBuf[0:2])),
		APIVersion:    int16(binary.BigEndian.Uint16(messageBuf[2:4])),
		CorrelationID: int32(binary.BigEndian.Uint32(messageBuf[4:8])),
	}

	return header, messageBuf[8:], nil
}

// ResponseHeader represents a Kafka response header (v0)
type ResponseHeader struct {
	MessageSize   int32
	CorrelationID int32
}

// APIVersionsResponse represents the ApiVersions API response
type APIVersionsResponse struct {
	ErrorCode      int16
	APIKeys        []APIVersion
	ThrottleTimeMs int32
}

// APIVersion represents a single API version entry
type APIVersion struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

// Encode encodes the ApiVersions response to bytes
func (r *APIVersionsResponse) Encode() []byte {
	var body []byte

	// error_code (INT16)
	body = append(body, EncodeInt16(r.ErrorCode)...)

	// api_keys array length (COMPACT_ARRAY format: actual length + 1)
	body = append(body, EncodeCompactArrayLength(len(r.APIKeys)))

	// API key entries
	for _, apiKey := range r.APIKeys {
		// api_key (INT16)
		body = append(body, EncodeInt16(apiKey.APIKey)...)

		// min_version (INT16)
		body = append(body, EncodeInt16(apiKey.MinVersion)...)

		// max_version (INT16)
		body = append(body, EncodeInt16(apiKey.MaxVersion)...)

		// TAG_BUFFER (empty)
		body = append(body, EncodeTagBuffer()...)
	}

	// throttle_time_ms (INT32)
	body = append(body, EncodeInt32(r.ThrottleTimeMs)...)

	// TAG_BUFFER (empty)
	body = append(body, EncodeTagBuffer()...)

	return body
}

// WriteResponse writes a response with the given correlation ID and body
func WriteResponse(w io.Writer, correlationID int32, body []byte) error {
	// Calculate total response size (correlation_id + body)
	responseSize := 4 + len(body)

	// Build full response
	response := make([]byte, 4+responseSize)

	// message_size
	binary.BigEndian.PutUint32(response[0:4], uint32(responseSize))

	// correlation_id
	binary.BigEndian.PutUint32(response[4:8], uint32(correlationID))

	// response body
	copy(response[8:], body)

	_, err := w.Write(response)
	return err
}

// WriteFlexibleResponse writes a flexible response with TAG_BUFFER in header
func WriteFlexibleResponse(w io.Writer, correlationID int32, body []byte) error {
	// Calculate total response size (correlation_id + TAG_BUFFER + body)
	responseSize := 4 + 1 + len(body) // correlation_id (4) + TAG_BUFFER (1) + body

	// Build full response
	response := make([]byte, 4+responseSize)

	// message_size
	binary.BigEndian.PutUint32(response[0:4], uint32(responseSize))

	// correlation_id
	binary.BigEndian.PutUint32(response[4:8], uint32(correlationID))

	// TAG_BUFFER (empty)
	response[8] = 0x00

	// response body
	copy(response[9:], body)

	_, err := w.Write(response)
	return err
}

// FetchResponse represents a Fetch API response
type FetchResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	SessionID      int32
	Responses      []FetchTopicResponse
}

// FetchTopicResponse represents a topic in the Fetch response
type FetchTopicResponse struct {
	TopicID    [16]byte // UUID
	Partitions []FetchPartitionResponse
}

// FetchPartitionResponse represents a partition in the Fetch response
type FetchPartitionResponse struct {
	PartitionIndex       int32
	ErrorCode            int16
	HighWatermark        int64
	LastStableOffset     int64
	LogStartOffset       int64
	AbortedTransactions  []AbortedTransaction
	PreferredReadReplica int32
	Records              []byte // COMPACT_RECORDS
}

// AbortedTransaction represents an aborted transaction
type AbortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
}

// FetchRequest represents a Fetch API request
type FetchRequest struct {
	Topics []FetchTopicRequest
}

// FetchTopicRequest represents a topic in the Fetch request
type FetchTopicRequest struct {
	TopicID    [16]byte
	Partitions []FetchPartitionRequest
}

// FetchPartitionRequest represents a partition in the Fetch request
type FetchPartitionRequest struct {
	PartitionIndex int32
	// Other fields are not needed for this stage
}

// ParseFetchRequest parses a Fetch v16 request
func ParseFetchRequest(data []byte) (*FetchRequest, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("request data too short")
	}

	req := &FetchRequest{
		Topics: []FetchTopicRequest{},
	}

	offset := 0

	// Skip client_id (NULLABLE_STRING - 2 byte length)
	if offset+2 > len(data) {
		return nil, fmt.Errorf("not enough data for client_id length")
	}
	clientIDLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if clientIDLen > 0 {
		offset += clientIDLen
	}

	// Skip TAG_BUFFER for request header
	if offset >= len(data) {
		return nil, fmt.Errorf("unexpected end of data reading header tag buffer")
	}
	offset++

	// Skip various Fetch request fields we don't need yet
	// max_wait_ms (INT32)
	offset += 4
	// min_bytes (INT32)
	offset += 4
	// max_bytes (INT32)
	offset += 4
	// isolation_level (INT8)
	offset += 1
	// session_id (INT32)
	offset += 4
	// session_epoch (INT32)
	offset += 4

	// Read topics (COMPACT_ARRAY)
	if offset >= len(data) {
		return nil, fmt.Errorf("unexpected end of data reading topics length")
	}
	topicsLen, n := binary.Uvarint(data[offset:])
	if n <= 0 {
		return nil, fmt.Errorf("failed to read topics length")
	}
	offset += n
	topicsLen-- // Compact array: length = N + 1

	for i := uint64(0); i < topicsLen; i++ {
		// Read topic_id (UUID - 16 bytes)
		if offset+16 > len(data) {
			return nil, fmt.Errorf("not enough data for topic_id")
		}
		var topicID [16]byte
		copy(topicID[:], data[offset:offset+16])
		offset += 16

		// Read partitions (COMPACT_ARRAY)
		if offset >= len(data) {
			return nil, fmt.Errorf("unexpected end of data reading partitions length")
		}
		partitionsLen, n := binary.Uvarint(data[offset:])
		if n <= 0 {
			return nil, fmt.Errorf("failed to read partitions length")
		}
		offset += n
		partitionsLen-- // Compact array: length = N + 1

		var partitions []FetchPartitionRequest
		for j := uint64(0); j < partitionsLen; j++ {
			// Read partition_index (INT32)
			if offset+4 > len(data) {
				return nil, fmt.Errorf("not enough data for partition_index")
			}
			partitionIndex := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4

			// Skip other partition fields we don't need yet
			// current_leader_epoch (INT32)
			offset += 4
			// fetch_offset (INT64)
			offset += 8
			// last_fetched_epoch (INT32)
			offset += 4
			// log_start_offset (INT64)
			offset += 8
			// partition_max_bytes (INT32)
			offset += 4

			// Skip TAG_BUFFER for partition
			if offset >= len(data) {
				return nil, fmt.Errorf("unexpected end of data reading partition tag buffer")
			}
			offset++

			partitions = append(partitions, FetchPartitionRequest{
				PartitionIndex: partitionIndex,
			})
		}

		// Skip TAG_BUFFER for topic
		if offset >= len(data) {
			return nil, fmt.Errorf("unexpected end of data reading topic tag buffer")
		}
		offset++

		req.Topics = append(req.Topics, FetchTopicRequest{
			TopicID:    topicID,
			Partitions: partitions,
		})
	}

	return req, nil
}

// Encode encodes the Fetch response to bytes
func (r *FetchResponse) Encode() []byte {
	var body []byte

	// throttle_time_ms (INT32)
	body = append(body, EncodeInt32(r.ThrottleTimeMs)...)

	// error_code (INT16)
	body = append(body, EncodeInt16(r.ErrorCode)...)

	// session_id (INT32)
	body = append(body, EncodeInt32(r.SessionID)...)

	// responses (COMPACT_ARRAY)
	body = append(body, EncodeCompactArrayLength(len(r.Responses)))

	for _, topicResp := range r.Responses {
		// topic_id (UUID - 16 bytes)
		body = append(body, topicResp.TopicID[:]...)

		// partitions (COMPACT_ARRAY)
		body = append(body, byte(len(topicResp.Partitions)+1))

		for _, partResp := range topicResp.Partitions {
			// partition_index (INT32)
			body = append(body, EncodeInt32(partResp.PartitionIndex)...)

			// error_code (INT16)
			body = append(body, EncodeInt16(partResp.ErrorCode)...)

			// high_watermark (INT64)
			body = append(body, EncodeInt64(partResp.HighWatermark)...)

			// last_stable_offset (INT64)
			body = append(body, EncodeInt64(partResp.LastStableOffset)...)

			// log_start_offset (INT64)
			body = append(body, EncodeInt64(partResp.LogStartOffset)...)

			// aborted_transactions (COMPACT_ARRAY)
			body = append(body, EncodeCompactArrayLength(len(partResp.AbortedTransactions)))

			for _, aborted := range partResp.AbortedTransactions {
				// producer_id (INT64)
				body = append(body, EncodeInt64(aborted.ProducerID)...)

				// first_offset (INT64)
				body = append(body, EncodeInt64(aborted.FirstOffset)...)

				// TAG_BUFFER (empty)
				body = append(body, EncodeTagBuffer()...)
			}

			// preferred_read_replica (INT32)
			body = append(body, EncodeInt32(partResp.PreferredReadReplica)...)

			// records (COMPACT_BYTES)
			body = append(body, EncodeCompactBytes(partResp.Records)...)

			// TAG_BUFFER (empty)
			body = append(body, EncodeTagBuffer()...)
		}

		// TAG_BUFFER (empty) for topic
		body = append(body, EncodeTagBuffer()...)
	}

	// TAG_BUFFER (empty) for response
	body = append(body, EncodeTagBuffer()...)

	return body
}

// DescribeTopicPartitionsRequest represents a DescribeTopicPartitions request
type DescribeTopicPartitionsRequest struct {
	Topics []TopicRequest
}

// TopicRequest represents a topic in the request
type TopicRequest struct {
	Name string
}

// ParseDescribeTopicPartitionsRequest parses a DescribeTopicPartitions v0 request
func ParseDescribeTopicPartitionsRequest(data []byte) (*DescribeTopicPartitionsRequest, error) {
	if len(data) < 3 {
		return nil, fmt.Errorf("request data too short")
	}

	req := &DescribeTopicPartitionsRequest{
		Topics: []TopicRequest{},
	}

	offset := 0

	// Skip client_id (NULLABLE_STRING - not compact format)
	// Read 2-byte length (INT16)
	if offset+2 > len(data) {
		return nil, fmt.Errorf("unexpected end of data reading client_id length")
	}
	clientIDLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	// Skip client_id string content (if clientIDLen is -1, it's null)
	if clientIDLen > 0 {
		offset += clientIDLen
	}

	// Skip TAG_BUFFER for request header (for flexible versions)
	if offset >= len(data) {
		return nil, fmt.Errorf("unexpected end of data reading header tag buffer")
	}
	offset++ // TAG_BUFFER

	// Read topics array (COMPACT_ARRAY)
	if offset >= len(data) {
		return nil, fmt.Errorf("unexpected end of data reading topics array length")
	}
	topicsLen := int(data[offset]) - 1 // Compact array: actual length + 1
	offset++

	for i := 0; i < topicsLen; i++ {
		// Read topic name (COMPACT_STRING)
		if offset >= len(data) {
			return nil, fmt.Errorf("unexpected end of data reading topic name length at offset %d, data len %d", offset, len(data))
		}
		nameLen := int(data[offset]) - 1 // Compact string: actual length + 1
		offset++

		if offset+nameLen > len(data) {
			return nil, fmt.Errorf("unexpected end of data reading topic name: nameLen=%d, offset=%d, data len=%d", nameLen, offset, len(data))
		}
		topicName := string(data[offset : offset+nameLen])
		offset += nameLen

		req.Topics = append(req.Topics, TopicRequest{Name: topicName})

		// Skip TAG_BUFFER for topic
		if offset >= len(data) {
			return nil, fmt.Errorf("unexpected end of data reading topic tag buffer")
		}
		offset++ // TAG_BUFFER
	}

	// Note: ResponsePartitionLimit and Cursor fields are also present but we don't need them
	// for responding with unknown topic error

	return req, nil
}

// DescribeTopicPartitionsResponse represents a DescribeTopicPartitions response
type DescribeTopicPartitionsResponse struct {
	ThrottleTimeMs int32
	Topics         []TopicResponse
	Cursor         int8 // -1 for null
}

// TopicResponse represents a topic in the response
type TopicResponse struct {
	ErrorCode          int16
	Name               string
	TopicID            [16]byte // UUID
	IsInternal         bool
	Partitions         []PartitionResponse
	TopicAuthorizedOps int32
}

// PartitionResponse represents a partition in the response
type PartitionResponse struct {
	ErrorCode       int16
	PartitionIndex  int32
	LeaderID        int32
	LeaderEpoch     int32
	ReplicaNodes    []int32
	IsrNodes        []int32
	EligibleLeaders []int32
	LastKnownELR    []int32
	OfflineReplicas []int32
}

// Encode encodes the DescribeTopicPartitions response to bytes
func (r *DescribeTopicPartitionsResponse) Encode() []byte {
	var body []byte

	// throttle_time_ms (INT32)
	body = append(body, EncodeInt32(r.ThrottleTimeMs)...)

	// topics array (COMPACT_ARRAY)
	body = append(body, EncodeCompactArrayLength(len(r.Topics)))

	for _, topic := range r.Topics {
		// error_code (INT16)
		body = append(body, EncodeInt16(topic.ErrorCode)...)

		// name (COMPACT_STRING)
		body = append(body, byte(len(topic.Name)+1))
		body = append(body, []byte(topic.Name)...)

		// topic_id (UUID - 16 bytes)
		body = append(body, topic.TopicID[:]...)

		// is_internal (BOOLEAN)
		if topic.IsInternal {
			body = append(body, 0x01)
		} else {
			body = append(body, 0x00)
		}

		// partitions (COMPACT_ARRAY)
		body = append(body, EncodeCompactArrayLength(len(topic.Partitions)))

		for _, partition := range topic.Partitions {
			// error_code (INT16)
			body = append(body, EncodeInt16(partition.ErrorCode)...)

			// partition_index (INT32)
			body = append(body, EncodeInt32(partition.PartitionIndex)...)

			// leader_id (INT32)
			body = append(body, EncodeInt32(partition.LeaderID)...)

			// leader_epoch (INT32)
			body = append(body, EncodeInt32(partition.LeaderEpoch)...)

			// replica_nodes (COMPACT_ARRAY)
			body = append(body, EncodeCompactArrayLength(len(partition.ReplicaNodes)))
			for _, node := range partition.ReplicaNodes {
				body = append(body, EncodeInt32(node)...)
			}

			// isr_nodes (COMPACT_ARRAY)
			body = append(body, EncodeCompactArrayLength(len(partition.IsrNodes)))
			for _, node := range partition.IsrNodes {
				body = append(body, EncodeInt32(node)...)
			}

			// eligible_leader_replicas (COMPACT_ARRAY)
			body = append(body, EncodeCompactArrayLength(len(partition.EligibleLeaders)))
			for _, node := range partition.EligibleLeaders {
				body = append(body, EncodeInt32(node)...)
			}

			// last_known_elr (COMPACT_ARRAY)
			body = append(body, EncodeCompactArrayLength(len(partition.LastKnownELR)))
			for _, node := range partition.LastKnownELR {
				body = append(body, EncodeInt32(node)...)
			}

			// offline_replicas (COMPACT_ARRAY)
			body = append(body, EncodeCompactArrayLength(len(partition.OfflineReplicas)))
			for _, node := range partition.OfflineReplicas {
				body = append(body, EncodeInt32(node)...)
			}

			// TAG_BUFFER (empty)
			body = append(body, EncodeTagBuffer()...)
		}

		// topic_authorized_operations (INT32)
		body = append(body, EncodeInt32(topic.TopicAuthorizedOps)...)

		// TAG_BUFFER (empty)
		body = append(body, EncodeTagBuffer()...)
	}

	// next_cursor (INT8 for null, or more complex for actual cursor)
	// For null cursor, use -1
	body = append(body, byte(r.Cursor))

	// TAG_BUFFER (empty)
	body = append(body, EncodeTagBuffer()...)

	return body
}

// ProduceRequest represents a Produce API request
type ProduceRequest struct {
	Topics []ProduceTopicRequest
}

// ProduceTopicRequest represents a topic in the Produce request
type ProduceTopicRequest struct {
	Name       string
	Partitions []ProducePartitionRequest
}

// ProducePartitionRequest represents a partition in the Produce request
type ProducePartitionRequest struct {
	Index   int32
	Records []byte
}

// ParseProduceRequest parses a Produce v11 request
func ParseProduceRequest(data []byte) (*ProduceRequest, error) {
	if len(data) < 3 {
		return nil, fmt.Errorf("request data too short")
	}

	req := &ProduceRequest{
		Topics: []ProduceTopicRequest{},
	}

	offset := 0

	// Skip client_id (NULLABLE_STRING - 2 byte length)
	if offset+2 > len(data) {
		return nil, fmt.Errorf("unexpected end of data reading client_id length at offset %d", offset)
	}
	clientIDLen := int(int16(binary.BigEndian.Uint16(data[offset : offset+2])))
	offset += 2
	if clientIDLen > 0 {
		offset += clientIDLen
	}

	// Skip TAG_BUFFER for request header
	if offset >= len(data) {
		return nil, fmt.Errorf("unexpected end of data reading header tag buffer at offset %d", offset)
	}
	offset++

	// Skip transactional_id (COMPACT_NULLABLE_STRING)
	if offset >= len(data) {
		return nil, fmt.Errorf("unexpected end of data reading transactional_id at offset %d", offset)
	}
	transactionalIDLen := int(data[offset])
	offset++
	if transactionalIDLen > 1 {
		offset += transactionalIDLen - 1
	}

	// Skip acks (INT16)
	if offset+2 > len(data) {
		return nil, fmt.Errorf("unexpected end of data reading acks at offset %d", offset)
	}
	offset += 2

	// Skip timeout_ms (INT32)
	if offset+4 > len(data) {
		return nil, fmt.Errorf("unexpected end of data reading timeout_ms at offset %d", offset)
	}
	offset += 4

	// Read topics array (COMPACT_ARRAY)
	if offset >= len(data) {
		return nil, fmt.Errorf("unexpected end of data reading topics array length at offset %d", offset)
	}
	topicsLen := int(data[offset]) - 1 // Compact array: actual length + 1
	offset++

	for i := 0; i < topicsLen; i++ {
		// Read topic name (COMPACT_STRING)
		if offset >= len(data) {
			return nil, fmt.Errorf("unexpected end of data reading topic name length at offset %d", offset)
		}
		nameLen := int(data[offset]) - 1 // Compact string: actual length + 1
		offset++

		if offset+nameLen > len(data) {
			return nil, fmt.Errorf("unexpected end of data reading topic name at offset %d, nameLen %d, data len %d", offset, nameLen, len(data))
		}
		topicName := string(data[offset : offset+nameLen])
		offset += nameLen

		// Read partitions array (COMPACT_ARRAY)
		if offset >= len(data) {
			return nil, fmt.Errorf("unexpected end of data reading partitions array length at offset %d", offset)
		}
		partitionsLen := int(data[offset]) - 1
		offset++

		var partitions []ProducePartitionRequest
		for j := 0; j < partitionsLen; j++ {
			// Read partition index (INT32)
			if offset+4 > len(data) {
				return nil, fmt.Errorf("unexpected end of data reading partition index at offset %d", offset)
			}
			partitionIndex := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4

			// Read records (COMPACT_NULLABLE_BYTES - uses varint for length)
			if offset >= len(data) {
				return nil, fmt.Errorf("unexpected end of data reading records length at offset %d", offset)
			}

			var records []byte
			recordsLen, n := binary.Uvarint(data[offset:])
			if n <= 0 {
				return nil, fmt.Errorf("failed to read records length at offset %d", offset)
			}
			offset += n

			if recordsLen > 0 {
				recordsLen-- // Compact format: actual length + 1
				if offset+int(recordsLen) > len(data) {
					return nil, fmt.Errorf("unexpected end of data reading records at offset %d", offset)
				}
				records = data[offset : offset+int(recordsLen)]
				offset += int(recordsLen)
			}

			partitions = append(partitions, ProducePartitionRequest{
				Index:   partitionIndex,
				Records: records,
			})

			// Skip TAG_BUFFER for partition
			if offset >= len(data) {
				return nil, fmt.Errorf("unexpected end of data reading partition tag buffer")
			}
			offset++
		}

		req.Topics = append(req.Topics, ProduceTopicRequest{
			Name:       topicName,
			Partitions: partitions,
		})

		// Skip TAG_BUFFER for topic
		if offset >= len(data) {
			return nil, fmt.Errorf("unexpected end of data reading topic tag buffer")
		}
		offset++
	}

	return req, nil
}

// ProduceResponse represents a Produce API response
type ProduceResponse struct {
	Topics         []ProduceTopicResponse
	ThrottleTimeMs int32
}

// ProduceTopicResponse represents a topic in the Produce response
type ProduceTopicResponse struct {
	Name       string
	Partitions []ProducePartitionResponse
}

// ProducePartitionResponse represents a partition in the Produce response
type ProducePartitionResponse struct {
	Index           int32
	ErrorCode       int16
	BaseOffset      int64
	LogAppendTimeMs int64
	LogStartOffset  int64
}

// RecordError represents a record error in the Produce response
type RecordError struct {
	BatchIndex             int32
	BatchIndexErrorMessage string
}

// Encode encodes the Produce response to bytes
func (r *ProduceResponse) Encode() []byte {
	var body []byte

	// topics array (COMPACT_ARRAY)
	body = append(body, EncodeCompactArrayLength(len(r.Topics)))

	for _, topic := range r.Topics {
		// name (COMPACT_STRING)
		body = append(body, byte(len(topic.Name)+1))
		body = append(body, []byte(topic.Name)...)

		// partitions (COMPACT_ARRAY)
		body = append(body, EncodeCompactArrayLength(len(topic.Partitions)))

		for _, partition := range topic.Partitions {
			// index (INT32)
			body = append(body, EncodeInt32(partition.Index)...)

			// error_code (INT16)
			body = append(body, EncodeInt16(partition.ErrorCode)...)

			// base_offset (INT64)
			body = append(body, EncodeInt64(partition.BaseOffset)...)

			// log_append_time_ms (INT64)
			body = append(body, EncodeInt64(partition.LogAppendTimeMs)...)

			// log_start_offset (INT64)
			body = append(body, EncodeInt64(partition.LogStartOffset)...)

			// record_errors (COMPACT_ARRAY) - always empty/null for error responses
			body = append(body, 0x00) // null array

			// error_message (COMPACT_NULLABLE_STRING) - always null for now
			body = append(body, 0x00) // null string

			// TAG_BUFFER (empty)
			body = append(body, EncodeTagBuffer()...)
		}

		// TAG_BUFFER (empty) for topic
		body = append(body, EncodeTagBuffer()...)
	}

	// throttle_time_ms (INT32)
	body = append(body, EncodeInt32(r.ThrottleTimeMs)...)

	// TAG_BUFFER (empty) for response
	body = append(body, EncodeTagBuffer()...)

	return body
}
