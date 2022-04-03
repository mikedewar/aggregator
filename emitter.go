package main

import (
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/lovoo/goka"
)

// Emit messages forever every second
func runEmitter() {
	log.Println("running emitter")
	emitter, err := goka.NewEmitter(brokers, "example-stream", new(eventCodec))
	if err != nil {
		log.Fatalf("error creating emitter: %v", err)
	}
	defer emitter.Finish()
	for {
		time.Sleep(1 * time.Second)
		key := strconv.Itoa(rand.Intn(1000))
		v := Event{
			T:     time.Now(),
			Value: rand.NormFloat64(),
		}
		err = emitter.EmitSync(key, v)
		if err != nil {
			log.Fatalf("error emitting message: %v", err)
		}
	}
}
