package warehouse

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/klauspost/compress/zstd"
	bolt "go.etcd.io/bbolt"
)

// readBufferSize defines the buffer size used when reading data from tablets.
// Larger values may improve performance for large reads but consume more memory.
const readBufferSize = 100 * 1000

// Transaction represents an atomic unit of work within a Warehouse.
// It provides methods to read, write, and enumerate data.
// Transactions must be closed using Close() to commit changes or release resources.
type Transaction struct {
	Tx          *bolt.Tx      // Underlying BoltDB transaction
	Bucket      *bolt.Bucket  // The bucket used for storing index entries
	name        string        // Directory path (inherited from Warehouse)
	decoder     *zstd.Decoder // Shared zstd decoder (inherited from Warehouse)
	encoder     *zstd.Encoder // Shared zstd encoder (inherited from Warehouse, nil if read-only)
	writeTablet *tablet       // Current tablet being written to (nil in read-only mode)
	readonly    bool          // Whether this transaction is read-only
}

// Transaction creates a new transaction for this warehouse.
// The transaction mode (read-only or read-write) is determined by the warehouse mode.
// Returns an error if the transaction cannot be started.
func (w *Warehouse) Transaction() (*Transaction, error) {
	var err error
	t := &Transaction{
		name:     w.name,
		decoder:  w.decoder,
		encoder:  w.encoder,
		readonly: w.encoder == nil, // Read-only if no encoder (read-only mode)
	}
	// Begin transaction: writable if encoder is present (read-write mode)
	t.Tx, err = w.db.Begin(w.encoder != nil)
	if err != nil {
		return nil, err
	}
	if w.encoder == nil {
		// Read-only mode: access existing bucket
		t.Bucket = t.Tx.Bucket([]byte(bucket))
	} else {
		// Read-write mode: create bucket if needed
		t.Bucket, err = t.Tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return nil, err
		}
	}

	return t, nil
}

// Close commits (for read-write) or rolls back (for read-only) the transaction
// and releases all resources. For read-write transactions, this also closes
// the current write tablet.
// Returns an error if the operation fails.
func (t *Transaction) Close() error {
	var err error
	if t.readonly {
		// Read-only transactions must be rolled back, not committed
		err = t.Tx.Rollback()
	} else {
		err = t.Tx.Commit()
	}
	if err != nil {
		return err
	}
	if t.writeTablet != nil {
		err = t.writeTablet.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// Get retrieves the raw index entry for the given key.
// Note: This method is currently not implemented.
// Use Read() to get the actual data content.
func (t *Transaction) Get(key []byte) []byte {
	panic("warehouse.Transaction.Get is not implemented, yet") // FIXME
}

// Put stores a value in the warehouse under the specified key.
// The value is written to the current tablet and indexed in BoltDB.
// Returns an error if the transaction is read-only or if writing fails.
func (t *Transaction) Put(key []byte, value []byte) error {
	if t.readonly {
		return errors.New("read only transaction")
	}
	var err error
	// Initialize write tablet on first write
	if t.writeTablet == nil {
		t.writeTablet, err = t.pickTablet(time.Now().UnixMilli())
		if err != nil {
			return err
		}
	}
	// Write value to tablet and get bytes written
	i, err := t.writeTablet.Writer.Write(value)
	if err != nil {
		return err
	}
	// Create location record (Join) for this data
	join := &Location{
		Start:    int64(t.writeTablet.position),
		Size:     int64(i),
		TabletID: t.writeTablet.ID,
	}
	// Serialize the Join structure to binary
	buf := bytes.NewBuffer(nil)
	err = binary.Write(buf, binary.LittleEndian, join)
	if err != nil {
		return err
	}
	t.writeTablet.position += i
	// Store the location in the index
	return t.Bucket.Put(key, buf.Bytes())
}

// Dump outputs all key-value pairs in the index to the provided writer.
// Useful for debugging and inspection. Format: "key => value#length"
func (t *Transaction) Dump(w io.Writer) error {
	return t.Bucket.ForEach(func(k []byte, v []byte) error {
		_, err := fmt.Fprintf(w, "%v => %v#%v\n", string(k), v, len(v))
		return err
	})
}

// Read retrieves the data associated with the given key and writes it to the provided writer.
// It looks up the location in the index, opens the corresponding tablet,
// and reads the data at the stored offset.
// Returns the number of bytes read or an error if the key is not found or reading fails.
func (t *Transaction) Read(key []byte, writer io.Writer) (int64, error) {
	raw := t.Bucket.Get(key)
	if raw == nil {
		return 0, fmt.Errorf("unknown key : %s", string(key))
	}
	// Deserialize the location (Join) from the index
	join := &Location{}
	err := binary.Read(bytes.NewBuffer(raw), binary.LittleEndian, join)
	if err != nil {
		return 0, err
	}
	// Open the tablet containing this data
	tablet, err := t.pickTablet(join.TabletID)
	if err != nil {
		return 0, err
	}
	// Get a reader for the tablet
	archiveReader, err := tablet.Reader()
	if err != nil {
		return 0, err
	}
	// Read the data at the specified location
	return SeekThenReadTo(writer, archiveReader, join.Start, join.Size, readBufferSize)
}

// min returns the smaller of two comparable numeric values.
// Generic function supporting all signed and unsigned integer types.
func min[A int | int32 | int64 | uint | uint32 | uint64](a A, b A) A {
	if a > b {
		return b
	}
	return a
}

// SeekThenReadTo reads exactly 'size' bytes from 'r' starting at 'offset' and writes them to 'w'.
// It reads data in chunks of at most 'chunkSize' bytes to control memory usage.
// Returns the total number of bytes read (which equals 'size' on success) or an error.
// This function is useful for reading specific ranges from seekable compressed files.
func SeekThenReadTo(w io.Writer, r io.ReadSeeker, offset int64, size int64, chunkSize int64) (int64, error) {
	// Seek to the starting position
	_, err := r.Seek(offset, io.SeekStart)
	if err != nil {
		return 0, err
	}

	// Allocate buffer with the smaller of remaining size or chunk size
	buff := make([]byte, min(size, chunkSize))
	todo := size
	for {
		// Determine how much to read in this iteration
		end := min(todo, int64(len(buff)))
		n, err := io.ReadFull(r, buff[:end])
		if err != nil {
			return 0, err
		}
		_, err = w.Write(buff[:end])
		if err != nil {
			return 0, err
		}
		todo -= int64(n)
		if todo == 0 {
			break
		}
	}
	return size, nil

}
