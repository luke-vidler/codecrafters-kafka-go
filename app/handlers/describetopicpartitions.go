package handlers

import (
	"log"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

// HandleDescribeTopicPartitions handles the DescribeTopicPartitions API request
func HandleDescribeTopicPartitions(header *protocol.RequestHeader, requestBody []byte) []byte {
	log.Printf("Request body length: %d, hex: %x", len(requestBody), requestBody)

	// Parse the request
	req, err := protocol.ParseDescribeTopicPartitionsRequest(requestBody)
	if err != nil {
		log.Printf("Error parsing DescribeTopicPartitions request: %v", err)
		// Return an empty response with error
		response := &protocol.DescribeTopicPartitionsResponse{
			ThrottleTimeMs: 0,
			Topics:         []protocol.TopicResponse{},
			Cursor:         -1,
		}
		return response.Encode()
	}

	// Build response with unknown topic errors
	var topics []protocol.TopicResponse
	for _, topic := range req.Topics {
		// For now, all topics are unknown
		// Create a zero UUID (00000000-0000-0000-0000-000000000000)
		var zeroUUID [16]byte

		topicResp := protocol.TopicResponse{
			ErrorCode:          protocol.ErrorUnknownTopicOrPartition,
			Name:               topic.Name,
			TopicID:            zeroUUID,
			IsInternal:         false,
			Partitions:         []protocol.PartitionResponse{}, // Empty for unknown topic
			TopicAuthorizedOps: -2147483648,                    // Default value
		}
		topics = append(topics, topicResp)
	}

	response := &protocol.DescribeTopicPartitionsResponse{
		ThrottleTimeMs: 0,
		Topics:         topics,
		Cursor:         -1, // Null cursor
	}

	return response.Encode()
}
