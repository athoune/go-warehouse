package warehouse

// Document defines the interface for types that can be stored in the warehouse.
// Implementations of this interface can be serialized to binary and indexed by key.
// The interface provides methods for serialization, key generation, and location tracking.
type Document interface {
	// MarshalValue serializes the document's value to binary format for storage.
	// Returns the serialized bytes or an error if serialization fails.
	MarshalValue() ([]byte, error)

	// Key returns the unique identifier for this document.
	// This key is used as the index in BoltDB.
	Key() []byte

	// SetLocation records the location where this document is stored.
	// Called after the document is written to a tablet.
	// Parameters:
	//   - tablet: the tablet identifier (timestamp) where data is stored
	//   - whence: the byte offset within the tablet
	//   - size: the size of the stored data in bytes
	SetLocation(tablet, whence, size int64) error
}
