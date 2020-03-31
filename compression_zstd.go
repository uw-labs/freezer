package freezer

import (
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
	"github.com/uw-labs/straw"
)

var _ straw.StreamStore = &zstdStreamStore{}

// zstdStreamStore is a straw.StreamStore wrapper that implements transparent zstd compression. Everything is supported, except for calling Size() on the os.FileInfo returned from Stat or Lstat.  Calling Size() like this will panic, but freezer does not need that functionality anyway.
type zstdStreamStore struct {
	store straw.StreamStore
}

func (fs *zstdStreamStore) Lstat(name string) (os.FileInfo, error) {
	fi, err := fs.store.Lstat(name)
	if err != nil {
		return nil, err
	}
	return &noSizeFileInfo{fi}, nil
}

func (fs *zstdStreamStore) Stat(name string) (os.FileInfo, error) {
	fi, err := fs.store.Stat(name)
	if err != nil {
		return nil, err
	}
	return &noSizeFileInfo{fi}, nil
}

func (fs *zstdStreamStore) OpenReadCloser(name string) (straw.StrawReader, error) {
	rc, err := fs.store.OpenReadCloser(name)
	if err != nil {
		return nil, err
	}

	r, err := zstd.NewReader(rc)
	if err != nil {
		_ = rc.Close()
		return nil, err
	}
	return &zstdReadCloser{r, rc}, nil

}

func (fs *zstdStreamStore) Mkdir(name string, mode os.FileMode) error {
	return fs.store.Mkdir(name, mode)
}

func (fs *zstdStreamStore) Remove(name string) error {
	return fs.store.Remove(name)
}

func (fs *zstdStreamStore) CreateWriteCloser(name string) (straw.StrawWriter, error) {
	wc, err := fs.store.CreateWriteCloser(name)
	if err != nil {
		return nil, err
	}
	w, err := zstd.NewWriter(wc)
	if err != nil {
		return nil, err
	}
	return &zstdWriteCloser{w, wc}, nil
}

func (fs *zstdStreamStore) Readdir(name string) ([]os.FileInfo, error) {
	return fs.store.Readdir(name)
}

func (fs *zstdStreamStore) Close() error {
	return fs.store.Close()
}

type zstdReadCloser struct {
	sr    *zstd.Decoder
	inner io.Closer
}

func (src *zstdReadCloser) Read(buf []byte) (int, error) {
	return src.sr.Read(buf)
}

func (src *zstdReadCloser) Close() error {
	return src.inner.Close()
}

func (src *zstdReadCloser) Seek(int64, int) (int64, error) {
	panic("freezer: Seek not supported in zstd read closer")
}

func (src *zstdReadCloser) ReadAt([]byte, int64) (int, error) {
	panic("freezer: ReadAt not supported in zstd read closer")
}

type zstdWriteCloser struct {
	swc   io.WriteCloser
	inner io.Closer
}

func (src *zstdWriteCloser) Write(buf []byte) (int, error) {
	return src.swc.Write(buf)
}

func (src *zstdWriteCloser) Close() error {
	if err := src.swc.Close(); err != nil {
		_ = src.inner.Close()
		return err
	}
	return src.inner.Close()
}
