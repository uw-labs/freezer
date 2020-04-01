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

type MessageSource struct {
	streamstore straw.StreamStore
	path        string
	pollPeriod  time.Duration
}

type MessageSourceConfig struct {
	Path            string
	PollPeriod      time.Duration
	CompressionType CompressionType
}

func NewMessageSource(streamstore straw.StreamStore, config MessageSourceConfig) *MessageSource {

	switch config.CompressionType {
	case CompressionTypeNone:
	case CompressionTypeSnappy:
		streamstore = newSnappyStreamStore(streamstore)
	case CompressionTypeZstd:
		streamstore = newZstdStreamStore(streamstore)
	}

	ms := &MessageSource{
		streamstore: streamstore,
		path:        config.Path,
		pollPeriod:  config.PollPeriod,
	}
	if ms.pollPeriod == 0 {
		ms.pollPeriod = 5 * time.Second
	}
	return ms
}

func (mq *MessageSource) ConsumeMessages(ctx context.Context, handler ConsumerMessageHandler) error {

	var err error
	var rc io.ReadCloser

	defer func() {
		if rc != nil {
			rc.Close()
		}
	}()

	lenBytes := []byte{0, 0, 0, 0}

	for seq := 0; ; seq++ {
		fullname := seqToPath(mq.path, seq)

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
			_, err := io.ReadFull(rc, lenBytes[:])
			if err != nil {
				if err == io.EOF {
					// file is likely still being written to, sleep and retry.
					select {
					case <-ctx.Done():
						if ctx.Err() == context.DeadlineExceeded || ctx.Err() == context.Canceled {
							return nil
						}
						return ctx.Err()
					case <-time.After(mq.pollPeriod):
						continue readLoop
					}
				}
				return fmt.Errorf("Could not read length (%v)", err)
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
				return fmt.Errorf("Could not read payload from %v. Expected len was %d. (%v)", fullname, len, err)
			}
			if err := handler(buf); err != nil {
				return err
			}
		}
		if err := rc.Close(); err != nil {
			return err
		}
	}
}
