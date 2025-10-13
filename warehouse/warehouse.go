package warehouse

import (
	"encoding/base64"
	"encoding/binary"
	"os"
	"path"
	"time"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
	"github.com/klauspost/compress/zstd"
	bolt "go.etcd.io/bbolt"
)

type Warehouse struct {
	db   *bolt.DB
	name string
}
type Transaction struct {
	tx         *bolt.Tx
	bucket     *bolt.Bucket
	zstdWriter seekable.ConcurrentWriter
	id         string
	poz        int64
}

func New(name string) (*Warehouse, error) {
	err := os.MkdirAll(name, 0750)
	if err != nil {
		return nil, err
	}
	w := &Warehouse{
		name: name,
	}

	w.db, err = bolt.Open(path.Join(name, "index.bolt"), 0640, &bolt.Options{})
	if err != nil {
		return nil, err
	}

	return w, nil
}

func (w Warehouse) Transaction() (*Transaction, error) {
	var err error
	t := &Transaction{}
	t.tx, err = w.db.Begin(true)
	if err != nil {
		return nil, err
	}
	t.bucket, err = t.tx.CreateBucket([]byte("data"))
	if err != nil {
		return nil, err
	}

	now := time.Now()
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(now.UnixMilli()))
	dataName := base64.StdEncoding.EncodeToString(b)
	zstdFile, err := os.OpenFile(path.Join(w.name, dataName), os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		return nil, err
	}

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	defer enc.Close()
	t.zstdWriter, err = seekable.NewWriter(zstdFile, enc)
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
	return t.tx.Commit()
}

func (t *Transaction) Append(key []byte, value []byte) error {
	i, err := t.zstdWriter.Write(value)
	if err != nil {
		return err
	}
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b, uint64(t.poz))
	binary.LittleEndian.PutUint64(b[8:], uint64(len(value)))
	t.bucket.Put(key, b)

	t.poz += int64(i)

	return nil
}
