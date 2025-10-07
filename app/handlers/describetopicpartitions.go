package handlers

import (
	"log"
	"sort"

	"github.com/codecrafters-io/kafka-starter-go/app/metadata"
	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

// HandleDescribeTopicPartitions handles the DescribeTopicPartitions API request
func HandleDescribeTopicPartitions(header *protocol.RequestHeader, requestBody []byte, clusterMetadata *metadata.ClusterMetadata) []byte {
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

	// Build response for each requested topic
	var topics []protocol.TopicResponse
	for _, topic := range req.Topics {
		// Check if topic exists in metadata
		topicMeta, exists := clusterMetadata.Topics[topic.Name]

		if !exists {
			// Topic not found - return unknown topic error
			var zeroUUID [16]byte
			topicResp := protocol.TopicResponse{
				ErrorCode:          protocol.ErrorUnknownTopicOrPartition,
				Name:               topic.Name,
				TopicID:            zeroUUID,
				IsInternal:         false,
				Partitions:         []protocol.PartitionResponse{},
				TopicAuthorizedOps: -2147483648,
			}
			topics = append(topics, topicResp)
			continue
		}

		// Topic exists - build partition information
		partitions := clusterMetadata.Partitions[topicMeta.TopicID]
		var partitionResponses []protocol.PartitionResponse

		for _, partMeta := range partitions {
			partResp := protocol.PartitionResponse{
				ErrorCode:       protocol.ErrorNone,
				PartitionIndex:  partMeta.PartitionID,
				LeaderID:        partMeta.Leader,
				LeaderEpoch:     partMeta.LeaderEpoch,
				ReplicaNodes:    partMeta.Replicas,
				IsrNodes:        partMeta.ISR,
				EligibleLeaders: []int32{}, // Empty for now
				LastKnownELR:    []int32{}, // Empty for now
				OfflineReplicas: []int32{}, // Empty for now
			}
			partitionResponses = append(partitionResponses, partResp)
		}

		topicResp := protocol.TopicResponse{
			ErrorCode:          protocol.ErrorNone,
			Name:               topicMeta.Name,
			TopicID:            topicMeta.TopicID,
			IsInternal:         false,
			Partitions:         partitionResponses,
			TopicAuthorizedOps: -2147483648,
		}
		topics = append(topics, topicResp)
	}

	// Sort topics alphabetically by name as required by the spec
	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Name < topics[j].Name
	})

	response := &protocol.DescribeTopicPartitionsResponse{
		ThrottleTimeMs: 0,
		Topics:         topics,
		Cursor:         -1, // Null cursor
	}

	return response.Encode()
}
