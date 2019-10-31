package freezer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/uw-labs/straw"
)

func main() {

	produce()

	ss, err := straw.Open("file:///")
	if err != nil {
		log.Fatal(err)
	}
	cons := NewMessageSource(ss, MessageSourceConfig{Path: "/tmp/"})

	// consume messages for 2 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	handler := func(m []byte) error {
		fmt.Printf("message is: %s\n", m)
		return nil
	}

	if err := cons.ConsumeMessages(ctx, handler); err != nil {
		log.Fatal(err)
	}

}

type MyMessage struct {
	CustomerID string
	Message    string
}

func (m MyMessage) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

func produce() {

	ss, err := straw.Open("file:///")
	if err != nil {
		log.Fatal(err)
	}
	sink, err := NewMessageAutoFlushSink(
		ss,
		MessageSinkAutoFlushConfig{Path: "/tmp/"},
	)
	if err != nil {
		log.Fatal(err)
	}

	m, err := MyMessage{
		CustomerID: "customer-01",
		Message:    fmt.Sprintf("hello. it is currently %v", time.Now()),
	}.Marshal()

	if err != nil {
		panic(err)
	}

	sink.PutMessage(m)

	sink.Close()
}
