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

const READ_BUFFER_SIZE = 100 * 1000

type Transaction struct {
	Tx          *bolt.Tx
	Bucket      *bolt.Bucket
	name        string
	decoder     *zstd.Decoder
	encoder     *zstd.Encoder
	writeTablet *tablet
	readonly    bool
}

func (w *Warehouse) Transaction() (*Transaction, error) {
	var err error
	t := &Transaction{
		name:    w.name,
		decoder: w.decoder,
		encoder: w.encoder,
	}
	t.Tx, err = w.db.Begin(w.encoder != nil)
	if err != nil {
		return nil, err
	}
	if w.encoder == nil {
		t.Bucket = t.Tx.Bucket([]byte(BUCKET))
	} else {
		t.Bucket, err = t.Tx.CreateBucketIfNotExists([]byte(BUCKET))
		if err != nil {
			return nil, err
		}
	}

	return t, nil
}

func (t *Transaction) Close() error {
	err := t.Tx.Commit()
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

func (t *Transaction) Get(key []byte) []byte {
	panic("warehouse.Transaction.Get is not implemented, yet") // FIXME
}

func (t *Transaction) Put(key []byte, value []byte) error {
	if t.readonly {
		return errors.New("read only transaction")
	}
	var err error
	if t.writeTablet == nil {
		t.writeTablet, err = t.pickTablet(time.Now().UnixMilli())
		if err != nil {
			return err
		}
	}
	i, err := t.writeTablet.Writer.Write(value)
	if err != nil {
		return err
	}
	join := &Join{
		Start:   int64(t.writeTablet.poz),
		Size:    int64(i),
		Archive: t.writeTablet.DataId,
	}
	buf := bytes.NewBuffer(nil)
	err = binary.Write(buf, binary.LittleEndian, join)
	if err != nil {
		return err
	}
	t.writeTablet.poz += i
	return t.Bucket.Put(key, buf.Bytes())
}

func (t *Transaction) Dump(w io.Writer) error {
	return t.Bucket.ForEach(func(k []byte, v []byte) error {
		fmt.Fprintf(w, "%v => %v#%v\n", string(k), v, len(v))
		return nil
	})
}

func (t *Transaction) Read(key []byte, writer io.Writer) (int64, error) {
	raw := t.Bucket.Get(key)
	if raw == nil {
		return 0, fmt.Errorf("unknown key : %s", string(key))
	}
	join := &Join{}
	err := binary.Read(bytes.NewBuffer(raw), binary.LittleEndian, join)
	if err != nil {
		return 0, err
	}
	tablet, err := t.pickTablet(join.Archive)
	if err != nil {
		return 0, err
	}
	archiveReader, err := tablet.Reader()
	if err != nil {
		return 0, err
	}
	return SeekThenReadTo(writer, archiveReader, join.Start, join.Size, READ_BUFFER_SIZE)
}

func min[A int | int32 | int64 | uint | uint32 | uint64](a A, b A) A {
	if a > b {
		return b
	}
	return a
}

func SeekThenReadTo(w io.Writer, r io.ReadSeeker, offset int64, size int64, chunkSize int64) (int64, error) {
	_, err := r.Seek(offset, io.SeekStart)
	if err != nil {
		return 0, err
	}

	buff := make([]byte, min(size, chunkSize))
	todo := size
	for {
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
