package main

import (
	"context"
	"log"
	"time"

	"github.com/lovoo/goka"
)

type KeyedTimer struct {
	key   string
	timer *time.Timer
}

type DelayCache struct {
	cache map[string]*time.Timer
	in    chan KeyedTimer
}

type DelayedEmitter struct {
	g *goka.GroupGraph
}

const min_duration = 10 * time.Second

func NewDelayedEmitter() *DelayedEmitter {
	cache := make(map[string]*time.Timer)
	return &DelayedEmitter{
		goka.DefineGroup("delayedEmitter",
			goka.Input("sessions", new(arrayCodec), NewDelayProcessor(cache)),
			goka.Output("sessions", new(arrayCodec)),
		),
		cache,
	}
}

func (s *DelayedEmitter) Run(ctx context.Context, brokers []string) {
	log.Println("running delayed emitter")
	p, err := goka.NewProcessor(brokers, s.g)
	if err != nil {
		log.Fatal(err)
	}
	err = p.Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("shut down nicely")
}

func NewDelayProcessor(cache map[string]*time.Timer) func(goka.Context, interface{}) {
	return func(ctx goka.Context, msg interface{}) {

		window := msg.([]Event)

		latest_event := window[0]

		delta := time.Since(latest_event.T)

		if delta < min_duration {
			delta = min_duration
		}

		// is there an existing timer?
		timer, ok := cache[ctx.Key()]
		if !ok {
			log.Println("waiting", delta, ctx.Key())
			timer = time.AfterFunc(delta, func() {
				log.Println("waited", delta, ctx.Key())
				ctx.Emit("sessions", ctx.Key(), window)
			})
		} else {
			log.Println("resetting timer for", ctx.Key())
			timer.Reset(delta)
		}
		cache[ctx.Key()] = timer
	}

}
