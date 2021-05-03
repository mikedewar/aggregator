package main

import (
	"context"
	"log"
	"time"

	"github.com/google/btree"
	"github.com/lovoo/goka"
)

type WindowState struct {
	g *goka.GroupGraph
}

type Event struct {
	t     time.Time
	value interface{}
}

func (e *Event) Less(than Event) bool {
	return e.t.After(than.t)
}

type Window struct {
	values btree.BTree
}

func NewWindowState() *WindowState {

	return &WindowState{
		goka.DefineGroup("windowState",
			goka.Input("sessions", new(arrayCodec), windowStateProcessor),
			goka.Persist(new(arrayCodec)),
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
	ctx.SetValue(msg)
}
