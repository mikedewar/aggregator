package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/btree"
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

func (e Event) Less(than btree.Item) bool {
	x := than.(Event)
	return e.T.After(x.T)
}

type Window struct {
	values btree.BTree
}

func NewWindowState(in Topic) *WindowState {

	return &WindowState{
		goka.DefineGroup("windowState",
			goka.Input(in.name, in.codec, windowStateProcessor),
			goka.Output("sessions", new(btreeCodec)),
			goka.Persist(new(btreeCodec)),
		),
	}
}

func (w *WindowState) Run(ctx context.Context, brokers []string) {
	p, err := goka.NewProcessor(brokers, w.g)
	if err != nil {
		log.Fatal(err)
	}
	err = p.Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("shut down nicely")
}

func windowStateProcessor(ctx goka.Context, msg interface{}) {

	// get the existing window
	windowI := ctx.Value()
	window, ok := windowI.(*btree.BTree)

	// if anything went wrong, let's make a fresh one
	if !ok {
		window = btree.New(2)
	}

	// make sure the msg is the Event that we expect
	event, ok := msg.(Event)
	if !ok {
		// suggesting to use ctx.Fail, because it cleanly shuts down the processor instead of killing the process.
		// Also: technically the type assertion (msg.(Event)) is not necessary.
		// The eventCodec will either return a valid `Event` or it will fail the whole processor upon unmarshalling errors.
		ctx.Fail(fmt.Errorf("couldn't convert value to event"))
	}

	// insert the new event into the history ensuring that order is correct
	window.ReplaceOrInsert(event)

	// emit the new, ordered window
	ctx.SetValue(window)

	ctx.Emit("sessions", ctx.Key(), window)

}
