package warehouse

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path"

	bolt "go.etcd.io/bbolt"
)

const BUCKET = "data"

type Warehouse struct {
	db     *bolt.DB
	name   string
	bucket *bolt.Bucket
	tx     *bolt.Tx
}

type Join struct {
	Start   int64
	Size    int64
	Archive int64
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

	w.tx, err = w.db.Begin(false)
	if err != nil {
		return nil, err
	}

	w.bucket = w.tx.Bucket([]byte(BUCKET))

	return w, nil
}

func (w *Warehouse) Read(key []byte, writer io.Writer) (int64, error) {
	raw := w.bucket.Get(key)
	join := &Join{}
	err := binary.Read(bytes.NewBuffer(raw), binary.LittleEndian, join)
	if err != nil {
		return 0, err
	}
	tablet, err := newTablet(w.name, join.Archive)
	if err != nil {
		return 0, err
	}
	_, err = tablet.zstdReader.Seek(join.Start, io.SeekStart)
	if err != nil {
		return 0, err
	}
	return io.CopyN(writer, tablet.zstdReader, join.Size)
}

/*
	func (w *Warehouse) transactionReader() (*TransactionReader, error) {
		var err error
		t := &TransactionReader{}
		t.tx, err = w.db.Begin(false)
		if err != nil {
			return nil, err
		}
		t.bucket = t.tx.Bucket([]byte(BUCKET))
		if t.bucket == nil {
			return nil, errors.New("unknown bucket name")
		}

		dec, err := zstd.NewReader(nil)
		if err != nil {
			return nil, err
		}
		t.zstdReader, err = seekable.NewReader(t.zstdFile, dec)
		return t, nil
	}
*/

func (w *Warehouse) Close() error {
	return w.db.Close()
}

/*
func (t *Warehouse) _Get(key []byte) ([]byte, error) {
	raw := t.bucket.Get(key)
	if len(raw) == 0 {
		return nil, nil
	}
	join := &Join{}
	err := binary.Read(bytes.NewBuffer(raw), binary.LittleEndian, join)
	if err != nil {
		return nil, err
	}
	_, err = t.zstdReader.Seek(join.Start, io.SeekStart)
	if err != nil {
		return nil, err
	}
	buff := make([]byte, join.End-join.Start)
	_, err = t.zstdReader.Read(buff)
	if err != nil {
		return nil, err
	}
	return buff, nil
}
*/
