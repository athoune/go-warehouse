package warehouse

import (
	"os"
	"path"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
	"github.com/klauspost/compress/zstd"
)

type tablet struct {
	zstdFile   *os.File
	zstdReader seekable.Reader
}

func newTablet(name string, id int64) (*tablet, error) {
	var err error
	t := &tablet{}
	t.zstdFile, err = os.OpenFile(
		path.Join(name, dataName(id)),
		os.O_RDONLY, 0640)
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	t.zstdReader, err = seekable.NewReader(t.zstdFile, dec)
	if err != nil {
		return nil, err
	}
	return t, nil
}
