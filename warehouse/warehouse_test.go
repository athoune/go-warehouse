package warehouse

import (
	"bytes"
	"os"
	"path"
	"testing"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
)

func TestWarehouse(t *testing.T) {
	dirName := os.TempDir()
	err := os.MkdirAll(dirName, 0750)
	assert.NoError(t, err)
	defer os.Remove(dirName)
	w, err := New(dirName)
	assert.NoError(t, err)
	tx, err := w.Transaction()
	assert.NoError(t, err)
	err = tx.Append([]byte("apple"), []byte{42})
	assert.NoError(t, err)
	err = tx.Commit()
	assert.NoError(t, err)
	err = w.Close()
	assert.NoError(t, err)

	wr, err := New(dirName)
	assert.NoError(t, err)
	buff := &bytes.Buffer{}
	_, err = wr.Read([]byte("apple"), buff)
	assert.NoError(t, err)
	assert.Equal(t, []byte{42}, buff.Bytes())
}

func TestZstd(t *testing.T) {

	dirName := os.TempDir()
	defer os.RemoveAll(dirName)
	fileName := path.Join(dirName, "test.ztsd")
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0640)
	assert.NoError(t, err)

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	assert.NoError(t, err)
	zstdWriter, err := seekable.NewWriter(f, enc)
	assert.NoError(t, err)
	n, err := zstdWriter.Write([]byte{42})
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
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
	assert.Equal(t, []byte{42}, buff[:n])
}
