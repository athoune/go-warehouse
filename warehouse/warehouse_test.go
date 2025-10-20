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

func TestWarehouse(t *testing.T) {
	dirName, err := os.MkdirTemp(os.TempDir(), "deb-deduplication-")
	assert.NoError(t, err)
	fmt.Println(dirName)
	defer os.Remove(dirName)

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
		err = tx.Set(fixture.k, fixture.v)
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
	txr.Dump(os.Stdout)
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

func TestSeekThenReadTo(t *testing.T) {
	dirName, err := os.MkdirTemp(os.TempDir(), "seek-read-to")
	assert.NoError(t, err)
	defer os.Remove(dirName)
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

func TestZstd(t *testing.T) {

	dirName, err := os.MkdirTemp(os.TempDir(), "deb-deduplication-zstd-")
	assert.NoError(t, err)
	defer os.RemoveAll(dirName)
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

/* Is there any lock drama in the stdlib ? */
func TestFlock(t *testing.T) {
	dirName, err := os.MkdirTemp(os.TempDir(), "deb-deduplication-flock-")
	assert.NoError(t, err)
	defer os.RemoveAll(dirName)
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

func TestBbolt(t *testing.T) {
	dirName, err := os.MkdirTemp(os.TempDir(), "deb-deduplication-flock-")
	assert.NoError(t, err)
	defer os.RemoveAll(dirName)
	b, err := bolt.Open(path.Join(dirName, "test.bolt"), 0640, nil)
	assert.NoError(t, err)
	err = b.Close()
	assert.NoError(t, err)

	b2, err := bolt.Open(path.Join(dirName, "test.bolt"), 0640, nil)
	assert.NoError(t, err)
	err = b2.Close()
	assert.NoError(t, err)
}
