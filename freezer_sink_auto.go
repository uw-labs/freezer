package freezer

import (
	"errors"
	"time"

	"github.com/uw-labs/straw"
)

type MessageSinkAutoFlush struct {
	ms *MessageSink

	maxUnflushedTime     time.Duration
	maxUnflushedMessages int

	reqs chan *messageReqAf

	exitErr  error
	closeReq chan struct{}
	closed   chan struct{}
}

type MessageSinkAutoFlushConfig struct {
	Path                 string
	MaxUnflushedTime     time.Duration
	MaxUnflushedMessages int
	CompressionType      CompressionType
}

const (
	DefaultMaxUnflushedTime = time.Second * 10
)

func NewMessageAutoFlushSink(streamstore straw.StreamStore, config MessageSinkAutoFlushConfig) (*MessageSinkAutoFlush, error) {

	if config.MaxUnflushedTime == 0 {
		config.MaxUnflushedTime = DefaultMaxUnflushedTime
	}

	ms, err := NewMessageSink(streamstore, MessageSinkConfig{
		Path:            config.Path,
		CompressionType: config.CompressionType,
	})
	if err != nil {
		return nil, err
	}

	msa := &MessageSinkAutoFlush{
		ms: ms,

		maxUnflushedTime:     config.MaxUnflushedTime,
		maxUnflushedMessages: config.MaxUnflushedMessages,

		reqs: make(chan *messageReqAf),

		closeReq: make(chan struct{}),
		closed:   make(chan struct{}),
	}

	if msa.maxUnflushedTime == 0 {
		msa.maxUnflushedTime = 60 * time.Second
	}

	go msa.run()

	return msa, nil
}

func (mq *MessageSinkAutoFlush) run() {
	mq.exitErr = mq.loop()
	close(mq.closed)
}

func (mq *MessageSinkAutoFlush) loop() error {
	writtenCount := 0
	var t *time.Timer
	var timerC <-chan time.Time

	var flushNeeded bool

	for {
		if t == nil {
			timerC = nil
		} else {
			timerC = t.C
		}
		select {
		case r := <-mq.reqs:
			err := mq.ms.PutMessage(r.m)
			if err != nil {
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
			if writtenCount != 0 {
				if err := mq.ms.Flush(); err != nil {
					return err
				}
			}
			return nil
		}
		if flushNeeded {
			if err := mq.ms.Flush(); err != nil {
				return err
			}
			writtenCount = 0
			flushNeeded = false
		}
	}
}

type messageReqAf struct {
	m         []byte
	writtenOk chan struct{}
}

func (mq *MessageSinkAutoFlush) PutMessage(m []byte) error {
	req := &messageReqAf{m, make(chan struct{})}
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

func (mq *MessageSinkAutoFlush) Close() error {
	select {
	case mq.closeReq <- struct{}{}:
		<-mq.closed
		return mq.exitErr
	case <-mq.closed:
		return errors.New("already closed")
	}
}
