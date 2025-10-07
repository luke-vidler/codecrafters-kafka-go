package handlers

import (
	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

// HandleFetch handles the Fetch API request
func HandleFetch(header *protocol.RequestHeader, requestBody []byte) []byte {
	// For now, we just return an empty response with no topics
	// We'll parse the request in later stages
	response := &protocol.FetchResponse{
		ThrottleTimeMs: 0,
		ErrorCode:      protocol.ErrorNone,
		SessionID:      0,
		Responses:      []protocol.FetchTopicResponse{},
	}

	return response.Encode()
}
