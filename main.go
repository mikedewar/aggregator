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

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	ctx := context.Background()

	go runEmitter() // emits one message per second

	go runWindowBuilder(ctx, brokers)

	runView()
}
