package persistence

import "encoding/binary"

const (
	WALMagic   = "DKV1"
	WALVersion = uint32(1)

	MaxPayloadSize = 1024 * 1024
)

func EncodeWALHeader() []byte {
	header := make([]byte, 8)

	copy(header[0:4], WALMagic)

	binary.BigEndian.PutUint32(header[4:8], WALVersion)

	return header
}
