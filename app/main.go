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

	// Read the request (we don't need to parse it for this stage)
	buf := make([]byte, 1024)
	_, err = conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading from connection: ", err.Error())
		os.Exit(1)
	}

	// Send response with correlation ID of 7
	// Response format:
	// - message_size (4 bytes, int32, big-endian): 0 for now (any value works)
	// - correlation_id (4 bytes, int32, big-endian): 7
	response := make([]byte, 8)

	// message_size: 0 (4 bytes at offset 0)
	binary.BigEndian.PutUint32(response[0:4], 0)

	// correlation_id: 7 (4 bytes at offset 4)
	binary.BigEndian.PutUint32(response[4:8], 7)

	_, err = conn.Write(response)
	if err != nil {
		fmt.Println("Error writing to connection: ", err.Error())
		os.Exit(1)
	}
}
