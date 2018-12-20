package freezer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uw-labs/straw"
)

func TestSimpleHappyPathRoundTrip(t *testing.T) {
	assert := assert.New(t)

	ss := straw.NewMemStreamStore()

	sink, err := NewMessageSink(ss, MessageSinkConfig{Path: "/foo/bar/baz"})
	if err != nil {
		t.Fatal(err)
	}

	assert.NoError(sink.PutMessage([]byte{1, 2, 3, 4, 5}))
	assert.NoError(sink.Close())

	source := NewMessageSource(ss, MessageSourceConfig{Path: "/foo/bar/baz"})

	messages := make(chan []byte)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()
	go func() {
		source.ConsumeMessages(ctx, func(m []byte) error { messages <- m; return nil })
	}()

	select {
	case <-ctx.Done():
		t.Error("timeout before message")
	case m := <-messages:
		assert.Equal([]byte{1, 2, 3, 4, 5}, m)
	}

}
