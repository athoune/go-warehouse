package warehouse

import (
	"os"
	"testing"

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
	tx.Commit()
}
