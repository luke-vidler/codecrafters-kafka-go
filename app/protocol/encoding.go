package protocol

import "encoding/binary"

// EncodeInt16 encodes an int16 value to bytes
func EncodeInt16(value int16) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(value))
	return buf
}

// EncodeInt32 encodes an int32 value to bytes
func EncodeInt32(value int32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(value))
	return buf
}

// EncodeInt64 encodes an int64 value to bytes
func EncodeInt64(value int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(value))
	return buf
}

// EncodeUvarint encodes a uint64 as an unsigned varint
func EncodeUvarint(value uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, value)
	return buf[:n]
}

// EncodeCompactArrayLength encodes the length of a compact array (length + 1)
func EncodeCompactArrayLength(length int) byte {
	return byte(length + 1)
}

// EncodeCompactBytes encodes bytes in COMPACT_BYTES format
// For COMPACT_BYTES: length is encoded as unsigned varint (length + 1)
// 0 means null/empty
func EncodeCompactBytes(data []byte) []byte {
	if len(data) == 0 {
		return []byte{0x00} // Null/empty
	}

	// Encode length as unsigned varint (length + 1)
	recordsLen := len(data) + 1
	result := EncodeUvarint(uint64(recordsLen))

	// Append the actual data
	result = append(result, data...)
	return result
}

// EncodeTagBuffer encodes an empty TAG_BUFFER (for flexible versions)
func EncodeTagBuffer() []byte {
	return []byte{0x00}
}
