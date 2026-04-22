package warehouse

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"syscall"
	"testing"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	bolt "go.etcd.io/bbolt"
)

// TestWarehouse validates the complete lifecycle of a Warehouse:
// - Creating a new warehouse
// - Writing data via transactions
// - Closing and reopening in read-only mode
// - Reading back the stored data
func TestWarehouse(t *testing.T) {
	dirName, err := os.MkdirTemp(os.TempDir(), "deb-deduplication-")
	assert.NoError(t, err)
	fmt.Println(dirName)
	defer func() {
		_ = os.Remove(dirName)
	}()

	w, err := New(dirName)
	assert.NoError(t, err)
	tx, err := w.Transaction()
	assert.NoError(t, err)
	fixtures := []struct {
		k []byte
		v []byte
	}{
		{[]byte("apple"), []byte{42}},
		{[]byte("orange"), []byte("juice")},
		{[]byte("banana"), []byte("split")},
	}
	for _, fixture := range fixtures {
		err = tx.Put(fixture.k, fixture.v)
		assert.NoError(t, err)
	}
	assert.NoError(t, err)
	err = tx.Close()
	assert.NoError(t, err)
	err = w.Close()
	assert.NoError(t, err)

	dirs, err := os.ReadDir(dirName)
	assert.NoError(t, err)
	for _, dir := range dirs {
		fmt.Println(dir)
	}

	info, err := os.Stat(path.Join(dirName, "index.bolt"))
	assert.NoError(t, err)
	assert.True(t, info.Size() > 0)

	cmd := exec.Command("file", path.Join(dirName, tx.writeTablet.zstdFile.Name()))
	cmd.Stdout = os.Stdout
	err = cmd.Run()
	assert.NoError(t, err)

	info, err = os.Stat(tx.writeTablet.zstdFile.Name())
	assert.NoError(t, err, tx.writeTablet.zstdFile.Name())
	assert.True(t, info.Size() > 0)

	wr, err := OpenReadOnly(dirName)
	assert.NoError(t, err)
	txr, err := wr.Transaction()
	assert.NoError(t, err)
	err = txr.Dump(os.Stdout)
	assert.NoError(t, err)
	buff := &bytes.Buffer{}
	for _, fixture := range fixtures {
		buff.Reset()
		n, err := txr.Read(fixture.k, buff)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(fixture.v)), n)
		assert.Equal(t, string(fixture.v), buff.String())
	}
}

// TestSeekThenReadTo verifies the seek-and-read helper function
// by creating a test file, seeking to an offset, and reading a specific range.
func TestSeekThenReadTo(t *testing.T) {
	dirName, err := os.MkdirTemp(os.TempDir(), "seek-read-to")
	assert.NoError(t, err)
	defer func() {
		_ = os.Remove(dirName)
	}()
	testFile := path.Join(dirName, "test")
	err = os.WriteFile(testFile, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 0640)
	assert.NoError(t, err)
	r, err := os.Open(testFile)
	assert.NoError(t, err)
	w := bytes.NewBuffer(nil)
	n, err := SeekThenReadTo(w, r, 2, 4, 3)
	assert.NoError(t, err)
	assert.Equal(t, int64(4), n)
	assert.Equal(t, []byte{2, 3, 4, 5}, w.Bytes())
}

// TestSeekThenReadToInvalidOffset tests error handling for invalid offsets.
func TestSeekThenReadToInvalidOffset(t *testing.T) {
	dirName, err := os.MkdirTemp(os.TempDir(), "seek-read-invalid")
	assert.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(dirName)
	}()

	testFile := path.Join(dirName, "test")
	err = os.WriteFile(testFile, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 0640)
	assert.NoError(t, err)

	tests := []struct {
		name      string
		offset    int64
		size      int64
		chunkSize int64
		wantErr   bool
	}{
		{
			name:      "negative offset",
			offset:    -1,
			size:      5,
			chunkSize: 3,
			wantErr:   true,
		},
		{
			name:      "offset beyond file",
			offset:    100,
			size:      5,
			chunkSize: 3,
			wantErr:   true, // Seek succeeds but ReadFull fails with EOF
		},
		{
			name:      "size extends beyond file",
			offset:    5,
			size:      100,
			chunkSize: 10,
			wantErr:   true, // ReadFull fails with EOF
		},
		{
			name:      "offset at end of file",
			offset:    10,
			size:      1,
			chunkSize: 1,
			wantErr:   true, // EOF - nothing to read
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := os.Open(testFile)
			assert.NoError(t, err)
			defer func() {
				_ = r.Close()
			}()

			w := bytes.NewBuffer(nil)
			n, err := SeekThenReadTo(w, r, tt.offset, tt.size, tt.chunkSize)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Equal(t, int64(0), n)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.size, n)
			}
		})
	}
}

// TestSeekThenReadToChunkSizes tests various chunk sizes including edge cases.
func TestSeekThenReadToChunkSizes(t *testing.T) {
	dirName, err := os.MkdirTemp(os.TempDir(), "seek-read-chunks")
	assert.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(dirName)
	}()

	// Create file with 100 bytes
	data := bytes.Repeat([]byte("abcdefghij"), 10)
	testFile := path.Join(dirName, "test")
	err = os.WriteFile(testFile, data, 0640)
	assert.NoError(t, err)

	tests := []struct {
		name      string
		offset    int64
		size      int64
		chunkSize int64
		wantData  []byte
	}{
		{
			name:      "chunkSize larger than size",
			offset:    0,
			size:      10,
			chunkSize: 100,
			wantData:  data[:10],
		},
		{
			name:      "chunkSize equals size",
			offset:    10,
			size:      50,
			chunkSize: 50,
			wantData:  data[10:60],
		},
		{
			name:      "small chunkSize - many iterations",
			offset:    0,
			size:      20,
			chunkSize: 3,
			wantData:  data[:20],
		},
		{
			name:      "chunkSize of 1",
			offset:    5,
			size:      5,
			chunkSize: 1,
			wantData:  data[5:10],
		},
		{
			name:      "read from middle",
			offset:    50,
			size:      25,
			chunkSize: 10,
			wantData:  data[50:75],
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := os.Open(testFile)
			assert.NoError(t, err)
			defer func() {
				_ = r.Close()
			}()

			w := bytes.NewBuffer(nil)
			n, err := SeekThenReadTo(w, r, tt.offset, tt.size, tt.chunkSize)

			assert.NoError(t, err)
			assert.Equal(t, tt.size, n)
			assert.Equal(t, tt.wantData, w.Bytes())
		})
	}
}

// TestZstd validates the seekable zstd compression library integration.
// It tests writing compressed data, seeking within it, and reading partial content.
func TestZstd(t *testing.T) {

	dirName, err := os.MkdirTemp(os.TempDir(), "deb-deduplication-zstd-")
	assert.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(dirName)
	}()
	fileName := path.Join(dirName, "test.ztsd")
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0640)
	assert.NoError(t, err)

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	assert.NoError(t, err)
	zstdWriter, err := seekable.NewWriter(f, enc)
	assert.NoError(t, err)
	n, err := zstdWriter.Write([]byte{0, 1, 2, 3})
	assert.NoError(t, err)
	assert.Equal(t, 4, n)
	err = zstdWriter.Close()
	assert.NoError(t, err)

	f, err = os.Open(fileName)
	assert.NoError(t, err)
	dec, err := zstd.NewReader(nil)
	assert.NoError(t, err)
	zstdReader, err := seekable.NewReader(f, dec)
	assert.NoError(t, err)
	buff := make([]byte, 32)
	n, err = zstdReader.Read(buff)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0, 1, 2, 3}, buff[:n])

	_, err = zstdReader.Seek(2, io.SeekStart)
	assert.NoError(t, err)
	buff = make([]byte, 2)
	n, err = zstdReader.Read(buff)
	assert.Equal(t, 2, n)
	assert.NoError(t, err)
	assert.Equal(t, []byte{2, 3}, buff)
}

// TestFlock verifies file locking behavior using syscall.Flock.
// Ensures exclusive locks can be acquired and released properly.
// Note: Tests stdlib locking semantics, not warehouse-specific code.
func TestFlock(t *testing.T) {
	dirName, err := os.MkdirTemp(os.TempDir(), "deb-deduplication-flock-")
	assert.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(dirName)
	}()
	fileName := path.Join(dirName, "lock.me")
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0640)
	assert.NoError(t, err)
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
	assert.NoError(t, err)
	err = f.Close()
	assert.NoError(t, err)

	f2, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0640)
	assert.NoError(t, err)
	err = syscall.Flock(int(f2.Fd()), syscall.LOCK_EX)
	assert.NoError(t, err)
	err = syscall.Flock(int(f2.Fd()), syscall.LOCK_UN)
	assert.NoError(t, err)

	f3, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0640)
	assert.NoError(t, err)
	err = syscall.Flock(int(f3.Fd()), syscall.LOCK_EX)
	assert.NoError(t, err)
}

// TestBbolt validates basic BoltDB operations including opening,
// closing, and reopening databases. Ensures the underlying storage works correctly.
func TestBbolt(t *testing.T) {
	dirName, err := os.MkdirTemp(os.TempDir(), "deb-deduplication-flock-")
	assert.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(dirName)
	}()
	b, err := bolt.Open(path.Join(dirName, "test.bolt"), 0640, nil)
	assert.NoError(t, err)
	err = b.Close()
	assert.NoError(t, err)

	b2, err := bolt.Open(path.Join(dirName, "test.bolt"), 0640, nil)
	assert.NoError(t, err)
	err = b2.Close()
	assert.NoError(t, err)
}

// TestTabletCloseReadOnly verifies that tablet.Close() works correctly
// in read-only mode without initializing the reader (lazy initialization).
func TestTabletCloseReadOnly(t *testing.T) {
	dirName, err := os.MkdirTemp(os.TempDir(), "deb-deduplication-tablet-close-")
	assert.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(dirName)
	}()

	// Create warehouse and write some data
	w, err := New(dirName)
	assert.NoError(t, err)
	tx, err := w.Transaction()
	assert.NoError(t, err)

	err = tx.Put([]byte("key1"), []byte("value1"))
	assert.NoError(t, err)
	err = tx.Close()
	assert.NoError(t, err)
	err = w.Close()
	assert.NoError(t, err)

	// Open in read-only mode and close transaction WITHOUT reading
	// This tests tablet.Close() with reader=nil in read-only mode
	wr, err := OpenReadOnly(dirName)
	assert.NoError(t, err)
	txr, err := wr.Transaction()
	assert.NoError(t, err)

	// Don't read anything - just close (tests reader=nil branch)
	err = txr.Close()
	assert.NoError(t, err)
	err = wr.Close()
	assert.NoError(t, err)
}

// TestTabletCloseWithReader verifies that tablet.Close() properly closes
// the reader when it has been initialized (lazy reader branch).
func TestTabletCloseWithReader(t *testing.T) {
	dirName, err := os.MkdirTemp(os.TempDir(), "deb-deduplication-tablet-reader-")
	assert.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(dirName)
	}()

	// Create warehouse with data
	w, err := New(dirName)
	assert.NoError(t, err)
	tx, err := w.Transaction()
	assert.NoError(t, err)

	err = tx.Put([]byte("key1"), []byte("value1"))
	assert.NoError(t, err)
	err = tx.Close()
	assert.NoError(t, err)
	err = w.Close()
	assert.NoError(t, err)

	// Open read-only and read data (initializes reader), then close
	wr, err := OpenReadOnly(dirName)
	assert.NoError(t, err)
	txr, err := wr.Transaction()
	assert.NoError(t, err)

	// This initializes the reader via lazy loading
	buff := &bytes.Buffer{}
	_, err = txr.Read([]byte("key1"), buff)
	assert.NoError(t, err)

	// Close with reader initialized (tests reader != nil branch)
	err = txr.Close()
	assert.NoError(t, err)
	err = wr.Close()
	assert.NoError(t, err)
}

// TestTabletCloseMultipleTablets tests closing multiple tablets
// created during a single transaction (edge case).
func TestTabletCloseMultipleTablets(t *testing.T) {
	dirName, err := os.MkdirTemp(os.TempDir(), "deb-deduplication-multi-tablet-")
	assert.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(dirName)
	}()

	// Create warehouse
	w, err := New(dirName)
	assert.NoError(t, err)
	tx, err := w.Transaction()
	assert.NoError(t, err)

	// Write some data to ensure tablet is created
	err = tx.Put([]byte("key1"), []byte("value1"))
	assert.NoError(t, err)

	// Close transaction (closes write tablet)
	err = tx.Close()
	assert.NoError(t, err)
	err = w.Close()
	assert.NoError(t, err)

	// Reopen and verify data can be read (tests read tablet close)
	wr, err := OpenReadOnly(dirName)
	assert.NoError(t, err)
	txr, err := wr.Transaction()
	assert.NoError(t, err)

	buff := &bytes.Buffer{}
	_, err = txr.Read([]byte("key1"), buff)
	assert.NoError(t, err)
	assert.Equal(t, "value1", buff.String())

	err = txr.Close()
	assert.NoError(t, err)
	err = wr.Close()
	assert.NoError(t, err)
}

// TestTransactionReadNotFound verifies that Read returns an error
// when attempting to read a key that doesn't exist in the warehouse.
func TestTransactionReadNotFound(t *testing.T) {
	dirName, err := os.MkdirTemp(os.TempDir(), "deb-deduplication-read-")
	assert.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(dirName)
	}()

	// Create warehouse and add some data
	w, err := New(dirName)
	assert.NoError(t, err)
	tx, err := w.Transaction()
	assert.NoError(t, err)

	err = tx.Put([]byte("existing-key"), []byte("some-value"))
	assert.NoError(t, err)
	err = tx.Close()
	assert.NoError(t, err)
	err = w.Close()
	assert.NoError(t, err)

	// Reopen and try to read non-existent key
	wr, err := OpenReadOnly(dirName)
	assert.NoError(t, err)
	txr, err := wr.Transaction()
	assert.NoError(t, err)

	buff := &bytes.Buffer{}
	n, err := txr.Read([]byte("non-existent-key"), buff)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown key")
	assert.Equal(t, int64(0), n)
	assert.Equal(t, 0, buff.Len())

	err = txr.Close()
	assert.NoError(t, err)
	err = wr.Close()
	assert.NoError(t, err)
}

// TestTransactionReadExisting verifies that Read correctly retrieves
// data for existing keys, including edge cases like empty values.
func TestTransactionReadExisting(t *testing.T) {
	dirName, err := os.MkdirTemp(os.TempDir(), "deb-deduplication-read-exist-")
	assert.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(dirName)
	}()

	w, err := New(dirName)
	assert.NoError(t, err)
	tx, err := w.Transaction()
	assert.NoError(t, err)

	// Test with various value sizes
	fixtures := []struct {
		key   string
		value []byte
	}{
		{"small", []byte("x")},
		{"empty", []byte{}},
		{"medium", bytes.Repeat([]byte("abc"), 100)},
		{"binary", []byte{0x00, 0x01, 0xFF, 0xFE}},
	}

	for _, f := range fixtures {
		err = tx.Put([]byte(f.key), f.value)
		assert.NoError(t, err)
	}
	err = tx.Close()
	assert.NoError(t, err)
	err = w.Close()
	assert.NoError(t, err)

	// Read back and verify
	wr, err := OpenReadOnly(dirName)
	assert.NoError(t, err)
	txr, err := wr.Transaction()
	assert.NoError(t, err)

	for _, f := range fixtures {
		buff := &bytes.Buffer{}
		n, err := txr.Read([]byte(f.key), buff)
		assert.NoError(t, err, "Failed to read key: %s", f.key)
		assert.Equal(t, int64(len(f.value)), n, "Wrong size for key: %s", f.key)
		// Handle nil vs empty slice comparison
		if len(f.value) == 0 {
			assert.Equal(t, 0, buff.Len(), "Wrong content for key: %s", f.key)
		} else {
			assert.Equal(t, f.value, buff.Bytes(), "Wrong content for key: %s", f.key)
		}
	}

	err = txr.Close()
	assert.NoError(t, err)
	err = wr.Close()
	assert.NoError(t, err)
}
