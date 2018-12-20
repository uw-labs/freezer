package freezer

import (
	"bytes"
	"context"
	"encoding/binary"
	"io/ioutil"
	"os"
	"testing"
	"time"

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

func TestAwaitInputFile(t *testing.T) {
	assert := assert.New(t)

	ss := straw.NewMemStreamStore()

	source := NewMessageSource(ss, MessageSourceConfig{Path: "/", PollPeriod: 20 * time.Millisecond})

	messages := make(chan []byte, 1)
	consumeErrors := make(chan error, 1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()
	go func() {
		consumeErrors <- source.ConsumeMessages(ctx, func(m []byte) error { messages <- m; return nil })
	}()

	time.Sleep(30 * time.Millisecond)
	sink, err := NewMessageSink(ss, MessageSinkConfig{Path: "/", MaxUnflushedTime: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	assert.NoError(sink.PutMessage([]byte{1, 2, 3, 4, 5}))

	select {
	case <-ctx.Done():
		t.Error("timeout before message")
	case m := <-messages:
		cancel()
		assert.Equal([]byte{1, 2, 3, 4, 5}, m)
		assert.NoError(sink.Close())
	}

	if err := <-consumeErrors; err != nil && err != ctx.Err() {
		t.Errorf("error during consume %v", err)
	}

}

func TestSourceContextCancelDuringRead(t *testing.T) {

	assert := assert.New(t)

	ss := straw.NewMemStreamStore()

	sink, err := NewMessageSink(ss, MessageSinkConfig{Path: "/foo/bar/baz"})
	if err != nil {
		t.Fatal(err)
	}

	assert.NoError(sink.PutMessage([]byte{1}))
	assert.NoError(sink.PutMessage([]byte{2}))
	assert.NoError(sink.Close())

	source := NewMessageSource(ss, MessageSourceConfig{Path: "/foo/bar/baz"})

	messages := make(chan []byte)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	consumeErr := make(chan error, 1)
	defer cancel()
	go func() {
		consumeErr <- source.ConsumeMessages(ctx, func(m []byte) error {
			select {
			case messages <- m:
				return nil
			case <-ctx.Done():
				return nil
			}
		})
	}()

	select {
	case <-ctx.Done():
		t.Error("timeout before message")
	case m := <-messages:
		assert.Equal([]byte{1}, m)
		cancel()
	}

	if err := <-consumeErr; err != nil {
		t.Error(err)
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
