package freezer

import (
	"bytes"
	"context"
	"encoding/binary"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uw-labs/straw"
)

var (
	payload = []byte("payload")
	delim   = []byte{0, 0, 0, 0}
)

func TestConsumeMessagesCorruptDataTest(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		name   string
		data   []byte
		errMsg string
	}{
		{
			"Trailing garbage after eof delimiter",
			append(delim, []byte("trailing stuff")...),
			"Was able to read past end marker. This is broken, bailing out.",
		},
		{
			"Incorrect length",
			append(append(length(len(payload)+1), payload...), delim...),
			"Could not read length (unexpected EOF)",
		},
		{
			"Incomplete length",
			[]byte{0, 0},
			"Could not read length (unexpected EOF)",
		},
		{
			"Incomplete payload",
			append(length(len(payload)), []byte("short")...),
			"Could not read payload from /foo/bar/baz/00/00/00/00/00/00/00. Expected len was 7. (unexpected EOF)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ss := newMockStrawStore(test.data)
			source := NewMessageSource(ss, MessageSourceConfig{Path: "/foo/bar/baz"})

			assert.EqualError(source.ConsumeMessages(context.Background(), func([]byte) error { return nil }), test.errMsg)
		})
	}
}

func length(l int) []byte {
	var lenBytes [4]byte
	binary.LittleEndian.PutUint32(lenBytes[:], uint32(l))
	return lenBytes[:]
}

type mockStrawStore struct {
	d []byte
}

func newMockStrawStore(d []byte) mockStrawStore {
	return mockStrawStore{d}
}

func (fs mockStrawStore) OpenReadCloser(name string) (straw.StrawReader, error) {
	return ioutil.NopCloser(bytes.NewReader(fs.d)), nil
}

func (fs mockStrawStore) CreateWriteCloser(name string) (straw.StrawWriter, error) {
	return nil, nil
}
func (fs mockStrawStore) Lstat(path string) (os.FileInfo, error) {
	return nil, nil

}
func (fs mockStrawStore) Stat(path string) (os.FileInfo, error) {
	return nil, nil

}
func (fs mockStrawStore) Readdir(path string) ([]os.FileInfo, error) {
	return nil, nil

}
func (fs mockStrawStore) Mkdir(path string, mode os.FileMode) error {
	return nil

}
func (fs mockStrawStore) Remove(path string) error {
	return nil
}
