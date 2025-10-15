package warehouse

import (
	"bytes"
	"encoding/binary"
	"os"
	"path"
	"time"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
	"github.com/klauspost/compress/zstd"
	bolt "go.etcd.io/bbolt"
)

type Transaction struct {
	tx         *bolt.Tx
	bucket     *bolt.Bucket
	dataId     int64
	zstdFile   *os.File
	zstdWriter seekable.ConcurrentWriter
	poz        int64
}

func (w *Warehouse) Transaction() (*Transaction, error) {
	var err error
	t := &Transaction{}
	t.tx, err = w.db.Begin(true)
	if err != nil {
		return nil, err
	}
	t.bucket, err = t.tx.CreateBucketIfNotExists([]byte("data"))
	if err != nil {
		return nil, err
	}

	t.dataId = time.Now().UnixMilli()

	t.zstdFile, err = os.OpenFile(
		path.Join(w.name, dataName(t.dataId)),
		os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		return nil, err
	}

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}
	t.zstdWriter, err = seekable.NewWriter(t.zstdFile, enc)
	if err != nil {
		return nil, err
	}

	return t, nil
}
func (t *Transaction) Commit() error {
	err := t.zstdWriter.Close()
	if err != nil {
		return err
	}
	err = t.zstdFile.Close()
	if err != nil {
		return err
	}
	return t.tx.Commit()
}

func (t *Transaction) Append(key []byte, value []byte) error {
	i, err := t.zstdWriter.Write(value)
	if err != nil {
		return err
	}
	join := &Join{
		int64(t.poz),
		int64(i),
		t.dataId,
	}
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, join)
	if err != nil {
		return err
	}
	return t.bucket.Put(key, buf.Bytes())
}
