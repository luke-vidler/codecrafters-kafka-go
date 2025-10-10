package handlers

import (
	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

// HandleProduce handles the Produce API request
func HandleProduce(header *protocol.RequestHeader, requestData []byte) []byte {
	// Parse the Produce request
	req, err := protocol.ParseProduceRequest(requestData)
	if err != nil {
		// If parsing fails, return an empty error response
		response := &protocol.ProduceResponse{
			Topics:         []protocol.ProduceTopicResponse{},
			ThrottleTimeMs: 0,
		}
		return response.Encode()
	}


	// Build error response for all topics and partitions
	// For this stage, we hardcode UNKNOWN_TOPIC_OR_PARTITION error for all requests
	var topicResponses []protocol.ProduceTopicResponse

	for _, topic := range req.Topics {
		var partitionResponses []protocol.ProducePartitionResponse

		for _, partition := range topic.Partitions {
			partitionResponses = append(partitionResponses, protocol.ProducePartitionResponse{
				Index:           partition.Index,
				ErrorCode:       protocol.ErrorUnknownTopicOrPartition,
				BaseOffset:      -1,
				LogAppendTimeMs: -1,
				LogStartOffset:  -1,
			})
		}

		topicResponses = append(topicResponses, protocol.ProduceTopicResponse{
			Name:       topic.Name,
			Partitions: partitionResponses,
		})
	}

	response := &protocol.ProduceResponse{
		Topics:         topicResponses,
		ThrottleTimeMs: 0,
	}

	return response.Encode()
}
