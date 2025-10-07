#!/bin/bash
# Kill any existing process on port 9092
lsof -ti:9092 | xargs kill -9 2>/dev/null
sleep 1

# Start server in background
./your_program.sh /tmp/server.properties &
SERVER_PID=$!
sleep 2

# Send ApiVersions request (API key 18, version 4)
# Request format: message_size(4) | api_key(2) | api_version(2) | correlation_id(4) | ...
echo "Sending test request..."
echo -ne '\x00\x00\x00\x0e\x00\x12\x00\x04\x73\x8e\x40\xa6\x00\x00\x00\x00\x00\x00' | nc localhost 9092 | hexdump -C

# Cleanup
kill $SERVER_PID 2>/dev/null
