// Package warehouse provides a storage system for binary data using BoltDB for indexing
// and zstd-compressed files (called tablets) for data storage.
// It supports both read-write and read-only modes, with transactions for data consistency.
package warehouse

import (
	"os"
	"path"

	"github.com/klauspost/compress/zstd"
	bolt "go.etcd.io/bbolt"
)

const BUCKET = "data"

// Warehouse manages a collection of data stored in zstd-compressed files (tablets)
// with a BoltDB index for fast lookups.
// It provides transactional operations for reading and writing data.
type Warehouse struct {
	db       *bolt.DB   // BoltDB database for indexing
	name     string     // Directory path where data is stored
	decoder  *zstd.Decoder  // Shared zstd decoder for decompression
	encoder  *zstd.Encoder  // zstd encoder for compression (nil in read-only mode)
	readonly bool       // Whether this warehouse is in read-only mode
}

// Join represents the location of a data blob within a tablet file.
// It stores the offset, size, and tablet identifier needed to retrieve the data.
type Join struct {
	Start   int64  // Byte offset within the tablet file
	Size    int64  // Size of the data blob in bytes
	Archive int64  // Tablet identifier (timestamp-based)
	Whence  int64  // Future use (e.g., versioning or flags)
}

// New creates a new Warehouse instance in read-write mode at the specified directory.
// The directory will be created if it doesn't exist.
// Returns an error if the database cannot be opened or the zstd encoder/decoder fails to initialize.
func New(name string) (*Warehouse, error) {
	return newWarehouse(name, false)
}

// OpenReadOnly opens an existing Warehouse in read-only mode.
// Useful for accessing data without modifying it or for concurrent access.
// Returns an error if the database cannot be opened or the zstd decoder fails to initialize.
func OpenReadOnly(name string) (*Warehouse, error) {
	return newWarehouse(name, true)
}

// newWarehouse creates a Warehouse with the specified read-only mode.
// Initializes the zstd encoder/decoder and opens the BoltDB database.
func newWarehouse(name string, readonly bool) (*Warehouse, error) {
	var err error
	w := &Warehouse{
		name:     name,
		readonly: readonly,
	}
	if !readonly {
		if err := os.MkdirAll(name, 0750); err != nil {
			return nil, err
		}
		// Initialize zstd encoder for write operations
		w.encoder, err = zstd.NewWriter(nil)
		if err != nil {
			return nil, err
		}
	}
	// Initialize zstd decoder (needed for both read and write modes)
	w.decoder, err = zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}

	// Open BoltDB database for indexing
	w.db, err = bolt.Open(path.Join(w.name, "index.bolt"), 0640, &bolt.Options{
		ReadOnly: readonly,
	})
	if err != nil {
		return nil, err
	}

	return w, nil
}

// Close flushes all pending writes to disk and closes the database connection.
// It syncs the BoltDB database and closes all resources.
// Returns an error if syncing or closing the database fails.
func (w *Warehouse) Close() error {
	err := w.db.Sync()
	if err != nil {
		return err
	}
	err = w.db.Close()
	if err != nil {
		return err
	}
	w.db = nil
	return nil
}
