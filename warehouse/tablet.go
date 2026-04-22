package warehouse

import (
	"io"
	"os"
	"path"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
	"github.com/klauspost/compress/zstd"
)

// tablet represents a zstd-compressed file that stores a collection of binary blobs.
// Tablets are the physical storage unit for data in the warehouse.
// They support both reading (via seekable reader) and writing (via concurrent writer).
type tablet struct {
	zstdFile *os.File          // Underlying file handle
	decoder  *zstd.Decoder     // zstd decoder for reading
	reader   io.ReadSeekCloser // Lazy-initialized seekable reader for decompression
	Writer   io.WriteCloser    // Concurrent writer for compression (nil in read-only mode)
	position int               // Current write position within the tablet
	ID       int64             // Tablet identifier (typically a timestamp)
	readonly bool              // Whether this tablet is opened in read-only mode
}

// pickTablet opens or creates a tablet with the specified dataId.
// In read-only mode, it opens an existing tablet for reading.
// In read-write mode, it creates the tablet if it doesn't exist and opens it for writing.
// Returns the tablet or an error if the file cannot be opened.
func (t *Transaction) pickTablet(tabletID int64) (*tablet, error) {
	var err error
	tab := &tablet{
		decoder:  t.decoder,
		readonly: t.readonly,
		ID:       tabletID,
	}
	// Determine file open flags based on mode
	var flag int
	if tab.readonly {
		flag = os.O_RDONLY
	} else {
		flag = os.O_CREATE | os.O_RDWR
	}
	// Open the tablet file
	tab.zstdFile, err = os.OpenFile(
		path.Join(t.name, dataName(tabletID)),
		flag, 0640)
	if err != nil {
		return nil, err
	}
	// Initialize writer for read-write mode
	if !tab.readonly {
		tab.Writer, err = seekable.NewWriter(tab.zstdFile, t.encoder)
		if err != nil {
			return nil, err
		}
	}

	return tab, nil
}

// Reader returns a seekable reader for this tablet.
// The reader is lazily initialized on first call.
// Multiple calls return the same reader instance.
// Returns an error if the seekable reader cannot be created.
func (t *tablet) Reader() (io.ReadSeekCloser, error) {
	var err error
	if t.reader == nil {
		t.reader, err = seekable.NewReader(t.zstdFile, t.decoder)
		if err != nil {
			return nil, err
		}
	}
	return t.reader, nil
}

// Close releases all resources associated with the tablet.
// It closes the reader (if initialized), writer (if not read-only),
// syncs the file to disk, and closes the file handle.
// Returns an error if any of these operations fail.
func (t *tablet) Close() error {
	// Close the lazy-initialized reader if it was used
	if t.reader != nil {
		if err := t.reader.Close(); err != nil {
			return err
		}
	}
	// Close the writer in read-write mode
	if !t.readonly {
		if err := t.Writer.Close(); err != nil {
			return err
		}
	}
	// Ensure all data is written to disk
	if err := t.zstdFile.Sync(); err != nil {
		return err
	}

	return t.zstdFile.Close()
}

// Put is a placeholder method for writing data with explicit key tracking.
// Currently returns default values (not implemented).
// TODO: Implement if needed for specialized storage patterns.
func (t *tablet) Put(key, value []byte) (frame int64, whence int64, err error) {

	return 0, 0, nil
}
