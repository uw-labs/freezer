package freezer

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/uw-labs/straw"
)

type ConsumerMessageHandler func([]byte) error

type messageSource struct {
	streamstore straw.StreamStore
	path        string
	pollPeriod  time.Duration
}

type MessageSourceConfig struct {
	Path       string
	PollPeriod time.Duration
}

func NewMessageSource(streamstore straw.StreamStore, config MessageSourceConfig) *messageSource {
	ms := &messageSource{
		streamstore: streamstore,
		path:        config.Path,
		pollPeriod:  config.PollPeriod,
	}
	if ms.pollPeriod == 0 {
		ms.pollPeriod = 5 * time.Second
	}
	return ms
}

func (mq *messageSource) ConsumeMessages(ctx context.Context, handler ConsumerMessageHandler) error {
	for seq := 0; ; seq++ {
		fullname := seqToPath(mq.path, seq)

		var rc io.ReadCloser
		var err error

	waitLoop:
		for {
			rc, err = mq.streamstore.OpenReadCloser(fullname)
			if err == nil {
				break waitLoop
			}
			if !os.IsNotExist(err) {
				return err
			}
			t := time.NewTimer(mq.pollPeriod)
			select {
			case <-ctx.Done():
				t.Stop()
				if ctx.Err() == context.DeadlineExceeded || ctx.Err() == context.Canceled {
					return nil
				}
				return ctx.Err()
			case <-t.C:
			}
		}
	readLoop:
		for {
			lenBytes := []byte{0, 0, 0, 0}
			_, err := io.ReadFull(rc, lenBytes[:])
			if err != nil {
				return fmt.Errorf("Could no read length (%v)", err)
			}

			len := int(binary.LittleEndian.Uint32(lenBytes[:]))
			if len == 0 {
				// next read should be EOF
				buf := []byte{0}
				if _, err := rc.Read(buf); err != io.EOF {
					return fmt.Errorf("Was able to read past end marker. This is broken, bailing out.")
				}
				break readLoop
			}
			buf := make([]byte, len)
			if _, err := io.ReadFull(rc, buf); err != nil {
				return fmt.Errorf("Could no read payload (%v)", err)
			}
			if err := handler(buf); err != nil {
				return err
			}
		}
	}
}
