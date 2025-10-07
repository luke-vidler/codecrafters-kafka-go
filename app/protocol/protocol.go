package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// API Keys
const (
	APIKeyAPIVersions             = 18
	APIKeyDescribeTopicPartitions = 75
)

// Error codes
const (
	ErrorNone               = 0
	ErrorUnsupportedVersion = 35
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
