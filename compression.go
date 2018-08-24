package freezer

import (
	"io"
	"os"
	"time"

	"github.com/golang/snappy"
	"github.com/uw-labs/straw"
)

type CompressionType int

const (
	CompressionTypeNone   CompressionType = 0
	CompressionTypeSnappy CompressionType = 1
)

var _ straw.StreamStore = &snappyStreamStore{}

func newSnappyStreamStore(store straw.StreamStore) *snappyStreamStore {
	return &snappyStreamStore{store}
}

// snappyStreamStore is a straw.StreamStore wrapper that implements transparent snappy compression. Everything is supported, except for calling Size() on the os.FileInfo returned from Stat or Lstat.  Calling Size() like this will panic, but freezer does not need that functionality anyway.
type snappyStreamStore struct {
	store straw.StreamStore
}

func (fs *snappyStreamStore) Lstat(name string) (os.FileInfo, error) {
	fi, err := fs.store.Lstat(name)
	if err != nil {
		return nil, err
	}
	return &noSizeFileInfo{fi}, nil
}

func (fs *snappyStreamStore) Stat(name string) (os.FileInfo, error) {
	fi, err := fs.store.Stat(name)
	if err != nil {
		return nil, err
	}
	return &noSizeFileInfo{fi}, nil
}

func (fs *snappyStreamStore) OpenReadCloser(name string) (straw.StrawReader, error) {
	rc, err := fs.store.OpenReadCloser(name)
	if err != nil {
		return nil, err
	}

	return &snappyReadCloser{snappy.NewReader(rc), rc}, nil
}

func (fs *snappyStreamStore) Mkdir(name string, mode os.FileMode) error {
	return fs.store.Mkdir(name, mode)
}

func (fs *snappyStreamStore) Remove(name string) error {
	return fs.store.Remove(name)
}

func (fs *snappyStreamStore) CreateWriteCloser(name string) (straw.StrawWriter, error) {
	wc, err := fs.store.CreateWriteCloser(name)
	if err != nil {
		return nil, err
	}
	return &snappyWriteCloser{snappy.NewBufferedWriter(wc), wc}, nil
}

func (fs *snappyStreamStore) Readdir(name string) ([]os.FileInfo, error) {
	return fs.store.Readdir(name)
}

type snappyReadCloser struct {
	sr    io.Reader
	inner io.Closer
}

func (src *snappyReadCloser) Read(buf []byte) (int, error) {
	return src.sr.Read(buf)
}

func (src *snappyReadCloser) Close() error {
	return src.inner.Close()
}

type snappyWriteCloser struct {
	swc   io.WriteCloser
	inner io.Closer
}

func (src *snappyWriteCloser) Write(buf []byte) (int, error) {
	return src.swc.Write(buf)
}

func (src *snappyWriteCloser) Close() error {
	if err := src.swc.Close(); err != nil {
		_ = src.inner.Close()
		return err
	}
	return src.inner.Close()
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
