package handlers

import (
	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

// HandleAPIVersions handles the ApiVersions API request
func HandleAPIVersions(header *protocol.RequestHeader) []byte {
	// Check if API version is supported (versions 0-4)
	if header.APIVersion < 0 || header.APIVersion > 4 {
		// Return UNSUPPORTED_VERSION error
		response := &protocol.APIVersionsResponse{
			ErrorCode:      protocol.ErrorUnsupportedVersion,
			APIKeys:        []protocol.APIVersion{},
			ThrottleTimeMs: 0,
		}
		return response.Encode()
	}

	// Return supported API versions
	response := &protocol.APIVersionsResponse{
		ErrorCode: protocol.ErrorNone,
		APIKeys: []protocol.APIVersion{
			{
				APIKey:     protocol.APIKeyAPIVersions,
				MinVersion: 0,
				MaxVersion: 4,
			},
		},
		ThrottleTimeMs: 0,
	}

	return response.Encode()
}
