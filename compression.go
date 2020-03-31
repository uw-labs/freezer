package freezer

import (
	"os"
	"time"

	"github.com/uw-labs/straw"
)

type CompressionType int

const (
	CompressionTypeNone   CompressionType = 0
	CompressionTypeSnappy CompressionType = 1
)

func newSnappyStreamStore(store straw.StreamStore) *snappyStreamStore {
	return &snappyStreamStore{store}
}

// noSizeFileInfo wraps another os.FileInfo but will panic if Size() is called.
type noSizeFileInfo struct {
	fi os.FileInfo
}

func (n *noSizeFileInfo) Name() string {
	return n.fi.Name()
}

func (n *noSizeFileInfo) Size() int64 {
	panic("Size() is not implemented here")
}

func (n *noSizeFileInfo) Mode() os.FileMode {
	return n.fi.Mode()
}

func (n *noSizeFileInfo) ModTime() time.Time {
	return n.fi.ModTime()
}

func (n *noSizeFileInfo) IsDir() bool {
	return n.fi.IsDir()
}

func (n *noSizeFileInfo) Sys() interface{} {
	return nil
}
