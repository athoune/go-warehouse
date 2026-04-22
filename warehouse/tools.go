package warehouse

import (
	"encoding/base64"
	"encoding/binary"
)

// dataName converts a tablet identifier (typically a timestamp) into a filesystem-safe string.
// The id is encoded as little-endian uint64 bytes, then base64-encoded for use as a filename.
// This ensures tablet files have valid, unique, and sortable names.
// Parameters:
//   - id: the tablet identifier (usually Unix milliseconds timestamp)
// Returns: base64-encoded string suitable for use as a filename.
func dataName(id int64) string {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(id))
	return base64.StdEncoding.EncodeToString(b)
}
