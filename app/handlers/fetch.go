package handlers

import (
	"log"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

// HandleFetch handles the Fetch API request
func HandleFetch(header *protocol.RequestHeader, requestBody []byte) []byte {
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
		// For now, treat all topics as unknown
		// Build partition responses with UNKNOWN_TOPIC_ID error
		var partitionResponses []protocol.FetchPartitionResponse
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
