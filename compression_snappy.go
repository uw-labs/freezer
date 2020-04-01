package freezer

import (
	"io"
	"os"

	"github.com/golang/snappy"
	"github.com/uw-labs/straw"
)

var _ straw.StreamStore = &snappyStreamStore{}

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

func (fs *snappyStreamStore) Close() error {
	return fs.store.Close()
}

type snappyReadCloser struct {
	sr    *snappy.Reader
	inner io.Closer
}

func (src *snappyReadCloser) Read(buf []byte) (int, error) {
	return src.sr.Read(buf)
}

func (src *snappyReadCloser) Close() error {
	return src.inner.Close()
}

func (src *snappyReadCloser) Seek(int64, int) (int64, error) {
	panic("freezer: Seek not supported in snappy read closer")
}

func (src *snappyReadCloser) ReadAt([]byte, int64) (int, error) {
	panic("freezer: ReadAt not supported in snappy read closer")
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
