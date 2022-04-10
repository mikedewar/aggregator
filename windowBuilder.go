package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/lovoo/goka"
)

type WindowState struct {
	g *goka.GroupGraph
}

type Event struct {
	T     time.Time
	Value interface{}
}

type Topic struct {
	name  goka.Stream
	codec goka.Codec
}

func runWindowBuilder(ctx context.Context, brokers []string, done chan bool) {
	g := goka.DefineGroup("window",
		goka.Input("example-stream", new(eventCodec), windowBuilder),
		goka.Persist(new(arrayCodec)),
	)
	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		log.Fatal(err)
	}
	defer close(done)
	err = p.Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("window builder shut down nicely")
}

func windowBuilder(ctx goka.Context, msg interface{}) {
	var window []Event
	var ok bool

	// get the existing window against this key
	t := time.Now()
	windowI := ctx.Value()
	if windowI == nil {
		// make a new window
		window = make([]Event, 0)
	} else {
		window, ok = windowI.([]Event)
		if !ok {
			log.Println(windowI)
			ctx.Fail(fmt.Errorf("didn't receive a window from ctx.Value"))
		}
	}
	log.Println("get", time.Since(t), len(window))

	// assert the msg is an Event
	event, ok := msg.(Event)
	if !ok {
		ctx.Fail(fmt.Errorf("couldn't assert that the received message was of type Event"))
	}

	t = time.Now()
	// insert the new event into the history ensuring that order is correct
	newWindow := append(window, event)

	// emit the new window
	ctx.SetValue(newWindow)
	log.Println("set", time.Since(t), len(newWindow))

}
