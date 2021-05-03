package main

import (
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

// Emit messages forever every second
func runEmitter() {
	log.Println("running emitter")
	emitter, err := goka.NewEmitter(brokers, "example-stream", new(codec.String))
	if err != nil {
		log.Fatalf("error creating emitter: %v", err)
	}
	defer emitter.Finish()
	for {
		time.Sleep(10 * time.Millisecond)
		key := strconv.Itoa(rand.Intn(1000))
		err = emitter.EmitSync(key, "some-value")
		if err != nil {
			log.Fatalf("error emitting message: %v", err)
		}
	}
}
