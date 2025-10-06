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

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	// Read the request
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading from connection: ", err.Error())
		os.Exit(1)
	}

	// Parse the correlation ID from the request
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

	// Extract correlation_id from offset 8 (after message_size, api_key, api_version)
	correlationID := binary.BigEndian.Uint32(buf[8:12])

	// Send response with the correlation ID from the request
	// Response format:
	// - message_size (4 bytes, int32, big-endian): 0 for now (any value works)
	// - correlation_id (4 bytes, int32, big-endian): from request
	response := make([]byte, 8)

	// message_size: 0 (4 bytes at offset 0)
	binary.BigEndian.PutUint32(response[0:4], 0)

	// correlation_id: from request (4 bytes at offset 4)
	binary.BigEndian.PutUint32(response[4:8], correlationID)

	_, err = conn.Write(response)
	if err != nil {
		fmt.Println("Error writing to connection: ", err.Error())
		os.Exit(1)
	}
}
