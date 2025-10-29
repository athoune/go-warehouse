package warehouse

import (
	"os"
	"path"

	"github.com/klauspost/compress/zstd"
	bolt "go.etcd.io/bbolt"
)

const BUCKET = "data"

type Warehouse struct {
	db       *bolt.DB
	name     string
	decoder  *zstd.Decoder
	encoder  *zstd.Encoder
	readonly bool
}

type Join struct {
	Start   int64
	Size    int64
	Archive int64
	Whence  int64
}

func New(name string) (*Warehouse, error) {
	return newWarehouse(name, false)
}

func OpenReadOnly(name string) (*Warehouse, error) {
	return newWarehouse(name, true)
}

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
		// zstd
		w.encoder, err = zstd.NewWriter(nil)
		if err != nil {
			return nil, err
		}
	}
	w.decoder, err = zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}

	// bbolt
	w.db, err = bolt.Open(path.Join(w.name, "index.bolt"), 0640, &bolt.Options{
		ReadOnly: readonly,
	})
	if err != nil {
		return nil, err
	}

	return w, nil
}

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
