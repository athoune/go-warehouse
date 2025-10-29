package warehouse

import (
	"io"
	"os"
	"path"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
	"github.com/klauspost/compress/zstd"
)

/*
zstd archive, for storing a collection of blobs.
*/
type tablet struct {
	zstdFile *os.File
	decoder  *zstd.Decoder
	reader   io.ReadSeekCloser //seekable.Reader
	Writer   io.WriteCloser    //seekable.ConcurrentWriter
	poz      int
	DataId   int64
	readonly bool
}

func (t *Transaction) pickTablet(dataId int64) (*tablet, error) {
	var err error
	tab := &tablet{
		decoder:  t.decoder,
		readonly: t.readonly,
		DataId:   dataId,
	}
	var flag int
	if tab.readonly {
		flag = os.O_RDONLY
	} else {
		flag = os.O_CREATE | os.O_RDWR
	}
	tab.zstdFile, err = os.OpenFile(
		path.Join(t.name, dataName(dataId)),
		flag, 0640)
	if err != nil {
		return nil, err
	}
	if !tab.readonly {
		tab.Writer, err = seekable.NewWriter(tab.zstdFile, t.encoder)
		if err != nil {
			return nil, err
		}
	}

	return tab, nil
}

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

func (t *tablet) Close() error {
	if t.reader != nil { // t.reader is lazy
		if err := t.reader.Close(); err != nil {
			return err
		}
	}
	if !t.readonly {
		if err := t.Writer.Close(); err != nil {
			return err
		}
	}
	if err := t.zstdFile.Sync(); err != nil {
		return err
	}

	return t.zstdFile.Close()
}

func (t *tablet) Put(key, value []byte) (frame int64, whence int64, err error) {

	return 0, 0, nil
}
