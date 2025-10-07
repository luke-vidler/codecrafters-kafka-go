package server

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"

	"github.com/codecrafters-io/kafka-starter-go/app/handlers"
	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

// Server represents the Kafka broker server
type Server struct {
	address string
}

// New creates a new Server instance
func New(address string) *Server {
	return &Server{
		address: address,
	}
}

// Start starts the server and listens for connections
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to bind to %s: %w", s.address, err)
	}
	defer listener.Close()

	log.Printf("Server listening on %s", s.address)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Handle multiple requests on the same connection
	for {
		// Read request header
		header, requestBody, err := protocol.ReadRequestHeader(conn)
		if err != nil {
			if err.Error() != "failed to read message size: EOF" {
				log.Printf("Error reading request header: %v", err)
			}
			return
		}

		log.Printf("API Key: %d, API Version: %d, Correlation ID: %d",
			header.APIKey, header.APIVersion, header.CorrelationID)

		// Route to appropriate handler
		var responseBody []byte

		switch header.APIKey {
		case protocol.APIKeyAPIVersions:
			responseBody = handlers.HandleAPIVersions(header)
		case protocol.APIKeyDescribeTopicPartitions:
			responseBody = handlers.HandleDescribeTopicPartitions(header, requestBody)
		default:
			// Return UNSUPPORTED_VERSION for unknown API keys
			responseBody = make([]byte, 2)
			binary.BigEndian.PutUint16(responseBody, protocol.ErrorUnsupportedVersion)
		}

		// Write response
		if err := protocol.WriteResponse(conn, header.CorrelationID, responseBody); err != nil {
			log.Printf("Error writing response: %v", err)
			return
		}
	}
}
