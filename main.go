package main

import (
	"context"
	"log"
)

var (
	brokers = []string{"localhost:9092"}
)

func main() {

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	ctx := context.Background()

	go runEmitter() // emits one message per second

	// the appender retrieves the current window
	// and appends the latest message to the end
	// then emits
	appender := NewAppender(Topic{
		name:  "example-stream",
		codec: new(eventCodec),
	})
	go appender.Run(ctx, brokers)

	// the windowState consumes the window messages
	// emitted by the appender and adds them to the state
	windowState := NewWindowState()
	go windowState.Run(ctx, brokers)

	// the featureBuilder consumes the window messages
	// emitted by the appender and creates a new stream
	// of aggregates, one per window
	featureBuilder := NewFeatureBuilder()
	go featureBuilder.Run(ctx, brokers)

	runView()
}
