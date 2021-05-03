package main

import (
	"context"

	"github.com/lovoo/goka/codec"
)

var (
	brokers = []string{"localhost:9092"}
)

func main() {

	ctx := context.Background()

	go runEmitter() // emits one message per second

	// the appender retrieves the current window
	// and appends the latest message to the end
	// then emits
	appender := NewAppender(Topic{
		name:  "example-stream",
		codec: new(codec.String),
	})
	go appender.Run(ctx, brokers)

	// the windowState consumes the window messages
	// emitted by the appender and adds them to the state
	windowState := NewWindowState()
	go windowState.Run(ctx, brokers)

	runView()
}
