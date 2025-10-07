package handlers

import (
	"log"

	"github.com/codecrafters-io/kafka-starter-go/app/metadata"
	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

// HandleFetch handles the Fetch API request
func HandleFetch(header *protocol.RequestHeader, requestBody []byte, clusterMetadata *metadata.ClusterMetadata) []byte {
	// Parse the Fetch request
	req, err := protocol.ParseFetchRequest(requestBody)
	if err != nil {
		log.Printf("Error parsing Fetch request: %v", err)
		// Return empty response on parse error
		response := &protocol.FetchResponse{
			ThrottleTimeMs: 0,
			ErrorCode:      protocol.ErrorNone,
			SessionID:      0,
			Responses:      []protocol.FetchTopicResponse{},
		}
		return response.Encode()
	}

	// Build response for each requested topic
	var topicResponses []protocol.FetchTopicResponse
	for _, topicReq := range req.Topics {
		// Check if topic exists in metadata
		topicMeta, exists := clusterMetadata.TopicsByID[topicReq.TopicID]

		var partitionResponses []protocol.FetchPartitionResponse
		if !exists {
			// Topic not found - return UNKNOWN_TOPIC_ID error
			for _, partReq := range topicReq.Partitions {
				partResp := protocol.FetchPartitionResponse{
					PartitionIndex:       partReq.PartitionIndex,
					ErrorCode:            protocol.ErrorUnknownTopicID,
					HighWatermark:        -1,
					LastStableOffset:     -1,
					LogStartOffset:       -1,
					AbortedTransactions:  []protocol.AbortedTransaction{},
					PreferredReadReplica: -1,
					Records:              []byte{},
				}
				partitionResponses = append(partitionResponses, partResp)
			}
		} else {
			// Topic exists - return partition data
			partitions := clusterMetadata.Partitions[topicMeta.TopicID]

			for _, partReq := range topicReq.Partitions {
				// Check if requested partition exists
				var partMeta *metadata.PartitionMetadata
				for _, p := range partitions {
					if p.PartitionID == partReq.PartitionIndex {
						partMeta = p
						break
					}
				}

				if partMeta == nil {
					// Partition not found
					partResp := protocol.FetchPartitionResponse{
						PartitionIndex:       partReq.PartitionIndex,
						ErrorCode:            protocol.ErrorUnknownTopicOrPartition,
						HighWatermark:        -1,
						LastStableOffset:     -1,
						LogStartOffset:       -1,
						AbortedTransactions:  []protocol.AbortedTransaction{},
						PreferredReadReplica: -1,
						Records:              []byte{},
					}
					partitionResponses = append(partitionResponses, partResp)
				} else {
					// Partition exists - return empty records (for now)
					partResp := protocol.FetchPartitionResponse{
						PartitionIndex:       partReq.PartitionIndex,
						ErrorCode:            protocol.ErrorNone,
						HighWatermark:        0, // Empty topic
						LastStableOffset:     0,
						LogStartOffset:       0,
						AbortedTransactions:  []protocol.AbortedTransaction{},
						PreferredReadReplica: -1,
						Records:              []byte{},
					}
					partitionResponses = append(partitionResponses, partResp)
				}
			}
		}

		topicResp := protocol.FetchTopicResponse{
			TopicID:    topicReq.TopicID,
			Partitions: partitionResponses,
		}
		topicResponses = append(topicResponses, topicResp)
	}

	response := &protocol.FetchResponse{
		ThrottleTimeMs: 0,
		ErrorCode:      protocol.ErrorNone,
		SessionID:      0,
		Responses:      topicResponses,
	}

	return response.Encode()
}
