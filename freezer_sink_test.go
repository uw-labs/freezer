package freezer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uw-labs/straw"
)

func TestSinkCreatesNewDir(t *testing.T) {
	assert := assert.New(t)

	ss := straw.NewMemStreamStore()

	sink, err := NewMessageAutoFlushSink(ss, MessageSinkAutoFlushConfig{Path: "/foo/bar/baz"})
	if err != nil {
		t.Fatal(err)
	}
	assert.NoError(sink.Close())

	fi, err := ss.Stat("/foo/bar/baz")
	assert.NoError(err)
	assert.True(fi.IsDir())
	assert.Equal("baz", fi.Name())
}
func TestMaxUnflushedTime(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	ss := straw.NewMemStreamStore()

	sink, err := NewMessageAutoFlushSink(ss, MessageSinkAutoFlushConfig{Path: "/foo/", MaxUnflushedTime: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	assert.NoError(sink.PutMessage([]byte{1}))
	time.Sleep(7 * time.Millisecond)
	assert.NoError(sink.PutMessage([]byte{2}))
	assert.NoError(sink.Close())

	fis, err := ss.Readdir("/foo/00/00/00/00/00/00/")
	require.NoError(err)

	assert.Equal(2, len(fis))
	assert.Equal("00", fis[0].Name())
	assert.Equal("01", fis[1].Name())
}
func TestMaxUnflushedMessages(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	ss := straw.NewMemStreamStore()

	sink, err := NewMessageAutoFlushSink(ss, MessageSinkAutoFlushConfig{Path: "/foo/", MaxUnflushedTime: 5 * time.Second, MaxUnflushedMessages: 1})
	if err != nil {
		t.Fatal(err)
	}

	assert.NoError(sink.PutMessage([]byte{1}))
	assert.NoError(sink.PutMessage([]byte{2}))
	assert.NoError(sink.Close())

	fis, err := ss.Readdir("/foo/00/00/00/00/00/00/")
	require.NoError(err)

	assert.Equal(2, len(fis))
	assert.Equal("00", fis[0].Name())
	assert.Equal("01", fis[1].Name())
}
