package handlers

import (
	"github.com/codecrafters-io/kafka-starter-go/app/metadata"
	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

// HandleProduce handles the Produce API request
func HandleProduce(header *protocol.RequestHeader, requestData []byte, clusterMetadata *metadata.ClusterMetadata) []byte {
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

	// Build response for all topics and partitions
	var topicResponses []protocol.ProduceTopicResponse

	for _, topic := range req.Topics {
		var partitionResponses []protocol.ProducePartitionResponse

		// Check if topic exists
		if !clusterMetadata.TopicExists(topic.Name) {
			// Topic doesn't exist - return error for all partitions
			for _, partition := range topic.Partitions {
				partitionResponses = append(partitionResponses, protocol.ProducePartitionResponse{
					Index:           partition.Index,
					ErrorCode:       protocol.ErrorUnknownTopicOrPartition,
					BaseOffset:      -1,
					LogAppendTimeMs: -1,
					LogStartOffset:  -1,
				})
			}
		} else {
			// Topic exists - check each partition
			for _, partition := range topic.Partitions {
				if !clusterMetadata.PartitionExists(topic.Name, partition.Index) {
					// Partition doesn't exist
					partitionResponses = append(partitionResponses, protocol.ProducePartitionResponse{
						Index:           partition.Index,
						ErrorCode:       protocol.ErrorUnknownTopicOrPartition,
						BaseOffset:      -1,
						LogAppendTimeMs: -1,
						LogStartOffset:  -1,
					})
				} else {
					// Both topic and partition exist - return success
					partitionResponses = append(partitionResponses, protocol.ProducePartitionResponse{
						Index:           partition.Index,
						ErrorCode:       protocol.ErrorNone,
						BaseOffset:      0,
						LogAppendTimeMs: -1,
						LogStartOffset:  0,
					})
				}
			}
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
