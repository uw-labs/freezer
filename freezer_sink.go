package freezer

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/uw-labs/straw"
)

type MessageSink struct {
	streamstore straw.StreamStore
	path        string

	reqs chan *messageReq

	maxUnflushedTime     time.Duration
	maxUnflushedMessages int
	flushReqs            chan flushReq

	exitErr  error
	closeReq chan struct{}
	closed   chan struct{}
}

type MessageSinkConfig struct {
	Path                 string
	MaxUnflushedTime     time.Duration
	MaxUnflushedMessages int
	CompressionType      CompressionType
}

const (
	DefaultMaxUnflushedTime = time.Second * 10
)

func NewMessageSink(streamstore straw.StreamStore, config MessageSinkConfig) (*MessageSink, error) {

	if config.MaxUnflushedTime == 0 {
		config.MaxUnflushedTime = DefaultMaxUnflushedTime
	}

	_, err := streamstore.Stat(config.Path)
	if os.IsNotExist(err) {
		if err := straw.MkdirAll(streamstore, config.Path, 0755); err != nil {
			return nil, err
		}
		_, err = streamstore.Stat(config.Path)
	}
	if err != nil {
		return nil, err
	}

	switch config.CompressionType {
	case CompressionTypeNone:
	case CompressionTypeSnappy:
		streamstore = newSnappyStreamStore(streamstore)
	}

	ms := &MessageSink{
		streamstore: streamstore,
		path:        config.Path,
		reqs:        make(chan *messageReq),

		maxUnflushedTime:     config.MaxUnflushedTime,
		maxUnflushedMessages: config.MaxUnflushedMessages,
		flushReqs:            make(chan flushReq),

		closeReq: make(chan struct{}),
		closed:   make(chan struct{}),
	}

	if ms.maxUnflushedTime == 0 {
		ms.maxUnflushedTime = 60 * time.Second
	}

	nextSeq, err := nextSequence(ms.streamstore, config.Path)
	if err != nil {
		return nil, err
	}

	go ms.run(nextSeq)

	return ms, nil
}

func (mq *MessageSink) run(nextSeq int) {
	mq.exitErr = mq.loop(nextSeq)
	close(mq.closed)
}

func (mq *MessageSink) loop(nextSeq int) error {
	writtenCount := 0
	var t *time.Timer
	var timerC <-chan time.Time

	var wc io.WriteCloser
	var err error

	var flushNeeded bool

	for {
		if t == nil {
			timerC = nil
		} else {
			timerC = t.C
		}
		select {
		case r := <-mq.reqs:
			var lenBytes [4]byte
			binary.LittleEndian.PutUint32(lenBytes[:], uint32(len(r.m)))
			if wc == nil {
				nextFile := seqToPath(mq.path, nextSeq)
				if err := straw.MkdirAll(mq.streamstore, filepath.Dir(nextFile), 0755); err != nil {
					return err
				}
				wc, err = mq.streamstore.CreateWriteCloser(nextFile)
				if err != nil {
					return err
				}
			}
			if _, err := wc.Write(lenBytes[:]); err != nil {
				return err
			}
			if _, err := wc.Write(r.m); err != nil {
				return err
			}
			close(r.writtenOk)
			writtenCount++
			if writtenCount == mq.maxUnflushedMessages {
				flushNeeded = true
			} else if t == nil {
				t = time.NewTimer(mq.maxUnflushedTime)
			}
		case <-timerC:
			t = nil
			flushNeeded = true
		case <-mq.closeReq:
			if wc != nil {
				if _, err := wc.Write([]byte{0, 0, 0, 0}); err != nil {
					return err
				}
				return wc.Close()
			}
			return nil
		case fr := <-mq.flushReqs:
			if wc != nil {
				if _, err := wc.Write([]byte{0, 0, 0, 0}); err != nil {
					return err
				}
				if err := wc.Close(); err != nil {
					return err
				}
				nextSeq++
				wc = nil
				writtenCount = 0
				flushNeeded = false
			}
			close(fr.flushedOk)
		}
		if flushNeeded {
			if wc != nil {
				if _, err := wc.Write([]byte{0, 0, 0, 0}); err != nil {
					return err
				}
				if err := wc.Close(); err != nil {
					return err
				}
				nextSeq++
				wc = nil
				writtenCount = 0
			}
			flushNeeded = false
		}
	}
}

type messageReq struct {
	m         []byte
	writtenOk chan struct{}
}

func (mq *MessageSink) PutMessage(m []byte) error {
	req := &messageReq{m, make(chan struct{})}
	select {
	case mq.reqs <- req:
		select {
		case <-req.writtenOk:
			return nil
		case <-mq.closed:
			return mq.exitErr
		}
	case <-mq.closed:
		return mq.exitErr
	}
}

type flushReq struct {
	flushedOk chan struct{}
}

func (mq *MessageSink) Flush() error {
	req := flushReq{make(chan struct{})}

	select {
	case mq.flushReqs <- req:
		select {
		case <-req.flushedOk:
			return nil
		case <-mq.closed:
			return mq.exitErr
		}
	case <-mq.closed:
		return mq.exitErr
	}
}

func (mq *MessageSink) Close() error {
	select {
	case mq.closeReq <- struct{}{}:
		<-mq.closed
		return mq.exitErr
	case <-mq.closed:
		return errors.New("already closed")
	}
}
