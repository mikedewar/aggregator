package main

import (
	"context"
	"log"

	"github.com/lovoo/goka"
)

var (
	brokers = []string{"localhost:9092"}
)

func main() {

	tmgr, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), goka.NewTopicManagerConfig())
	if err != nil {
		log.Fatalf("error creating topic manager: %v", err)
	}

	err = tmgr.EnsureStreamExists("example-stream", 10)
	if err != nil {
		log.Fatalf("Error creating sessions: %v", err)
	}

	err = tmgr.EnsureStreamExists("sessions", 10)
	if err != nil {
		log.Fatalf("Error creating sessions: %v", err)
	}
	err = tmgr.EnsureStreamExists("features", 10)
	if err != nil {
		log.Fatalf("Error creating features: %v", err)
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	ctx := context.Background()

	go runEmitter() // emits one message per second

	// // the appender retrieves the current window
	// // and appends the latest message to the end
	// // then emits
	// appender := NewAppender(Topic{
	// 	name:  "example-stream",
	// 	codec: new(eventCodec),
	// })
	// go appender.Run(ctx, brokers)

	// the windowState consumes the window messages
	// emitted by the appender and adds them to the state
	windowState := NewWindowState(Topic{
		name:  "example-stream",
		codec: new(eventCodec),
	})
	go windowState.Run(ctx, brokers)

	// the featureBuilder consumes the window messages
	// emitted by the appender and creates a new stream
	// of aggregates, one per window
	featureBuilder := NewFeatureBuilder()
	go featureBuilder.Run(ctx, brokers)

	runView()
}
