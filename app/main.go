package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Read the request
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading from connection: ", err.Error())
		return
	}

	// Parse the request header
	// Request format:
	// - message_size (4 bytes, int32, big-endian)
	// - request_api_key (2 bytes, int16, big-endian)
	// - request_api_version (2 bytes, int16, big-endian)
	// - correlation_id (4 bytes, int32, big-endian)
	// ...

	if n < 12 {
		fmt.Println("Request too short")
		os.Exit(1)
	}

	// Parse request header fields
	apiKey := binary.BigEndian.Uint16(buf[4:6])
	apiVersion := binary.BigEndian.Uint16(buf[6:8])
	correlationID := binary.BigEndian.Uint32(buf[8:12])

	fmt.Printf("API Key: %d, API Version: %d, Correlation ID: %d\n", apiKey, apiVersion, correlationID)

	// Build response based on API key
	var responseBody []byte

	if apiKey == 18 { // ApiVersions request
		// Check if API version is supported
		// ApiVersions supports versions 0-4
		if apiVersion < 0 || apiVersion > 4 {
			// Return UNSUPPORTED_VERSION error (error code 35)
			responseBody = []byte{
				0x00, 0x23, // error_code: 35 (UNSUPPORTED_VERSION)
				0x01,                   // num_api_keys: 0 (COMPACT_ARRAY length, 0 + 1)
				0x00, 0x00, 0x00, 0x00, // throttle_time_ms: 0
				0x00, // TAG_BUFFER: 0 (empty)
			}
		} else {
			// ApiVersions response format (response header v0 + body):
			// - error_code (INT16): 0 for success
			// - num_api_keys (COMPACT_ARRAY length): number of API keys + 1
			// - api_keys: array of supported API keys
			// - throttle_time_ms (INT32): 0
			// - TAG_BUFFER (COMPACT_ARRAY): 0 (empty)

			// Return error_code=0 with empty array
			responseBody = []byte{
				0x00, 0x00, // error_code: 0 (no error)
				0x01,                   // num_api_keys: 0 (COMPACT_ARRAY length, 0 + 1)
				0x00, 0x00, 0x00, 0x00, // throttle_time_ms: 0
				0x00, // TAG_BUFFER: 0 (empty)
			}
		}
	} else {
		// For other APIs, return error
		responseBody = make([]byte, 2)
		binary.BigEndian.PutUint16(responseBody[0:2], 35) // UNSUPPORTED_VERSION
	}

	// Calculate total response size (correlation_id + body)
	responseSize := 4 + len(responseBody)

	// Build full response
	response := make([]byte, 4+responseSize)

	// message_size
	binary.BigEndian.PutUint32(response[0:4], uint32(responseSize))

	// correlation_id
	binary.BigEndian.PutUint32(response[4:8], correlationID)

	// response body
	copy(response[8:], responseBody)

	_, err = conn.Write(response)
	if err != nil {
		fmt.Println("Error writing to connection: ", err.Error())
		os.Exit(1)
	}
}
