package main

import (
	"context"
	"log"

	"github.com/google/btree"
	"github.com/lovoo/goka"
)

type Topic struct {
	name  goka.Stream
	codec goka.Codec
}

type Appender struct {
	g *goka.GroupGraph
}

func NewAppender(in Topic) *Appender {
	return &Appender{
		goka.DefineGroup("appender",
			goka.Input(in.name, in.codec, appenderProcessor),
			goka.Output("sessions", new(btreeCodec)),
			goka.Lookup("windowState-table", new(btreeCodec)),
		),
	}

}

func (s *Appender) Run(ctx context.Context, brokers []string) {
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

func appenderProcessor(ctx goka.Context, msg interface{}) {

	// get the existing window
	windowI := ctx.Lookup("windowState-table", ctx.Key())
	window, ok := windowI.(*btree.BTree)

	// if anything went wrong, let's make a fresh one
	if !ok {
		window = btree.New(2)
	}

	// make sure the msg is the Event that we expect
	event, ok := msg.(Event)
	if !ok {
		log.Fatal("couldn't convert value to event")
	}

	// insert the new event into the history ensuring that order is correct
	window.ReplaceOrInsert(event)

	// emit the new, ordered window
	ctx.Emit("sessions", ctx.Key(), window)

}
