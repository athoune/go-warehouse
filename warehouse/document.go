package warehouse

type Document interface {
	ValueToBin() ([]byte, error) // value in the index
	Key() []byte                 // key in the index
	SetJoin(tablet, whence, size int64) error
}
