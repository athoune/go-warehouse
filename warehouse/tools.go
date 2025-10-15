package warehouse

import (
	"encoding/base64"
	"encoding/binary"
)

// id is a timestamp, return a base64 string, usable as a file name
func dataName(id int64) string {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(id))
	return base64.StdEncoding.EncodeToString(b)
}
