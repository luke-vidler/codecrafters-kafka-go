package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// API Keys
const (
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
	errCode := make([]byte, 2)
	binary.BigEndian.PutUint16(errCode, uint16(r.ErrorCode))
	body = append(body, errCode...)

	// api_keys array length (COMPACT_ARRAY format: actual length + 1)
	body = append(body, byte(len(r.APIKeys)+1))

	// API key entries
	for _, apiKey := range r.APIKeys {
		// api_key (INT16)
		key := make([]byte, 2)
		binary.BigEndian.PutUint16(key, uint16(apiKey.APIKey))
		body = append(body, key...)

		// min_version (INT16)
		minVer := make([]byte, 2)
		binary.BigEndian.PutUint16(minVer, uint16(apiKey.MinVersion))
		body = append(body, minVer...)

		// max_version (INT16)
		maxVer := make([]byte, 2)
		binary.BigEndian.PutUint16(maxVer, uint16(apiKey.MaxVersion))
		body = append(body, maxVer...)

		// TAG_BUFFER (empty)
		body = append(body, 0x00)
	}

	// throttle_time_ms (INT32)
	throttle := make([]byte, 4)
	binary.BigEndian.PutUint32(throttle, uint32(r.ThrottleTimeMs))
	body = append(body, throttle...)

	// TAG_BUFFER (empty)
	body = append(body, 0x00)

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
	throttle := make([]byte, 4)
	binary.BigEndian.PutUint32(throttle, uint32(r.ThrottleTimeMs))
	body = append(body, throttle...)

	// error_code (INT16)
	errCode := make([]byte, 2)
	binary.BigEndian.PutUint16(errCode, uint16(r.ErrorCode))
	body = append(body, errCode...)

	// session_id (INT32)
	sessionID := make([]byte, 4)
	binary.BigEndian.PutUint32(sessionID, uint32(r.SessionID))
	body = append(body, sessionID...)

	// responses (COMPACT_ARRAY)
	body = append(body, byte(len(r.Responses)+1))

	for _, topicResp := range r.Responses {
		// topic_id (UUID - 16 bytes)
		body = append(body, topicResp.TopicID[:]...)

		// partitions (COMPACT_ARRAY)
		body = append(body, byte(len(topicResp.Partitions)+1))

		for _, partResp := range topicResp.Partitions {
			// partition_index (INT32)
			partIndex := make([]byte, 4)
			binary.BigEndian.PutUint32(partIndex, uint32(partResp.PartitionIndex))
			body = append(body, partIndex...)

			// error_code (INT16)
			partErrCode := make([]byte, 2)
			binary.BigEndian.PutUint16(partErrCode, uint16(partResp.ErrorCode))
			body = append(body, partErrCode...)

			// high_watermark (INT64)
			highWatermark := make([]byte, 8)
			binary.BigEndian.PutUint64(highWatermark, uint64(partResp.HighWatermark))
			body = append(body, highWatermark...)

			// last_stable_offset (INT64)
			lastStableOffset := make([]byte, 8)
			binary.BigEndian.PutUint64(lastStableOffset, uint64(partResp.LastStableOffset))
			body = append(body, lastStableOffset...)

			// log_start_offset (INT64)
			logStartOffset := make([]byte, 8)
			binary.BigEndian.PutUint64(logStartOffset, uint64(partResp.LogStartOffset))
			body = append(body, logStartOffset...)

			// aborted_transactions (COMPACT_ARRAY)
			body = append(body, byte(len(partResp.AbortedTransactions)+1))

			for _, aborted := range partResp.AbortedTransactions {
				// producer_id (INT64)
				producerID := make([]byte, 8)
				binary.BigEndian.PutUint64(producerID, uint64(aborted.ProducerID))
				body = append(body, producerID...)

				// first_offset (INT64)
				firstOffset := make([]byte, 8)
				binary.BigEndian.PutUint64(firstOffset, uint64(aborted.FirstOffset))
				body = append(body, firstOffset...)

				// TAG_BUFFER (empty)
				body = append(body, 0x00)
			}

			// preferred_read_replica (INT32)
			preferredReplica := make([]byte, 4)
			binary.BigEndian.PutUint32(preferredReplica, uint32(partResp.PreferredReadReplica))
			body = append(body, preferredReplica...)

			// records (COMPACT_BYTES)
			// For COMPACT_BYTES: length is encoded as unsigned varint (length + 1)
			// 0 means null, otherwise length N is encoded as N+1
			if len(partResp.Records) == 0 {
				body = append(body, 0x00) // Null/empty records
			} else {
				// Encode length as unsigned varint (length + 1)
				recordsLen := len(partResp.Records) + 1
				lenBuf := make([]byte, binary.MaxVarintLen64)
				n := binary.PutUvarint(lenBuf, uint64(recordsLen))
				body = append(body, lenBuf[:n]...)

				// Append the actual records
				body = append(body, partResp.Records...)
			}

			// TAG_BUFFER (empty)
			body = append(body, 0x00)
		}

		// TAG_BUFFER (empty) for topic
		body = append(body, 0x00)
	}

	// TAG_BUFFER (empty) for response
	body = append(body, 0x00)

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
	throttle := make([]byte, 4)
	binary.BigEndian.PutUint32(throttle, uint32(r.ThrottleTimeMs))
	body = append(body, throttle...)

	// topics array (COMPACT_ARRAY)
	body = append(body, byte(len(r.Topics)+1))

	for _, topic := range r.Topics {
		// error_code (INT16)
		errCode := make([]byte, 2)
		binary.BigEndian.PutUint16(errCode, uint16(topic.ErrorCode))
		body = append(body, errCode...)

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
		body = append(body, byte(len(topic.Partitions)+1))

		for _, partition := range topic.Partitions {
			// error_code (INT16)
			partErrCode := make([]byte, 2)
			binary.BigEndian.PutUint16(partErrCode, uint16(partition.ErrorCode))
			body = append(body, partErrCode...)

			// partition_index (INT32)
			partIndex := make([]byte, 4)
			binary.BigEndian.PutUint32(partIndex, uint32(partition.PartitionIndex))
			body = append(body, partIndex...)

			// leader_id (INT32)
			leaderID := make([]byte, 4)
			binary.BigEndian.PutUint32(leaderID, uint32(partition.LeaderID))
			body = append(body, leaderID...)

			// leader_epoch (INT32)
			leaderEpoch := make([]byte, 4)
			binary.BigEndian.PutUint32(leaderEpoch, uint32(partition.LeaderEpoch))
			body = append(body, leaderEpoch...)

			// replica_nodes (COMPACT_ARRAY)
			body = append(body, byte(len(partition.ReplicaNodes)+1))
			for _, node := range partition.ReplicaNodes {
				nodeID := make([]byte, 4)
				binary.BigEndian.PutUint32(nodeID, uint32(node))
				body = append(body, nodeID...)
			}

			// isr_nodes (COMPACT_ARRAY)
			body = append(body, byte(len(partition.IsrNodes)+1))
			for _, node := range partition.IsrNodes {
				nodeID := make([]byte, 4)
				binary.BigEndian.PutUint32(nodeID, uint32(node))
				body = append(body, nodeID...)
			}

			// eligible_leader_replicas (COMPACT_ARRAY)
			body = append(body, byte(len(partition.EligibleLeaders)+1))
			for _, node := range partition.EligibleLeaders {
				nodeID := make([]byte, 4)
				binary.BigEndian.PutUint32(nodeID, uint32(node))
				body = append(body, nodeID...)
			}

			// last_known_elr (COMPACT_ARRAY)
			body = append(body, byte(len(partition.LastKnownELR)+1))
			for _, node := range partition.LastKnownELR {
				nodeID := make([]byte, 4)
				binary.BigEndian.PutUint32(nodeID, uint32(node))
				body = append(body, nodeID...)
			}

			// offline_replicas (COMPACT_ARRAY)
			body = append(body, byte(len(partition.OfflineReplicas)+1))
			for _, node := range partition.OfflineReplicas {
				nodeID := make([]byte, 4)
				binary.BigEndian.PutUint32(nodeID, uint32(node))
				body = append(body, nodeID...)
			}

			// TAG_BUFFER (empty)
			body = append(body, 0x00)
		}

		// topic_authorized_operations (INT32)
		authOps := make([]byte, 4)
		binary.BigEndian.PutUint32(authOps, uint32(topic.TopicAuthorizedOps))
		body = append(body, authOps...)

		// TAG_BUFFER (empty)
		body = append(body, 0x00)
	}

	// next_cursor (INT8 for null, or more complex for actual cursor)
	// For null cursor, use -1
	body = append(body, byte(r.Cursor))

	// TAG_BUFFER (empty)
	body = append(body, 0x00)

	return body
}
