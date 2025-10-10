package handlers

import (
	"github.com/codecrafters-io/kafka-starter-go/app/metadata"
	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

// createErrorPartitionResponse creates a partition response with an error
func createErrorPartitionResponse(partitionIndex int32) protocol.ProducePartitionResponse {
	return protocol.ProducePartitionResponse{
		Index:           partitionIndex,
		ErrorCode:       protocol.ErrorUnknownTopicOrPartition,
		BaseOffset:      -1,
		LogAppendTimeMs: -1,
		LogStartOffset:  -1,
	}
}

// createSuccessPartitionResponse creates a partition response for a successful produce
func createSuccessPartitionResponse(partitionIndex int32) protocol.ProducePartitionResponse {
	return protocol.ProducePartitionResponse{
		Index:           partitionIndex,
		ErrorCode:       protocol.ErrorNone,
		BaseOffset:      0,
		LogAppendTimeMs: -1,
		LogStartOffset:  0,
	}
}

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
				partitionResponses = append(partitionResponses, createErrorPartitionResponse(partition.Index))
			}
		} else {
			// Topic exists - check each partition
			for _, partition := range topic.Partitions {
				if !clusterMetadata.PartitionExists(topic.Name, partition.Index) {
					// Partition doesn't exist
					partitionResponses = append(partitionResponses, createErrorPartitionResponse(partition.Index))
				} else {
					// Both topic and partition exist - persist records and return success
					// Write the record batch to disk if records are present
					if len(partition.Records) > 0 {
						if err := metadata.WriteRecordBatch(topic.Name, partition.Index, partition.Records); err != nil {
							// If writing fails, return an error
							partitionResponses = append(partitionResponses, createErrorPartitionResponse(partition.Index))
							continue
						}
					}

					partitionResponses = append(partitionResponses, createSuccessPartitionResponse(partition.Index))
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
