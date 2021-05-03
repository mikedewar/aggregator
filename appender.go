package main

import (
	"context"
	"log"

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
			goka.Output("sessions", new(arrayCodec)),
			goka.Lookup("windowState-table", new(arrayCodec)),
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

	historyI := ctx.Lookup("windowState-table", ctx.Key())

	history, ok := historyI.([]int64)

	if !ok {
		if history == nil {
			log.Println("no history for key", ctx.Key())
			history = make([]int64, 0, 1)
			log.Println("empty history", history)
		} else {
			log.Println(historyI)
			log.Fatal("couldn't cast history to []int64")
		}
	}

	history = append(history, 1)

	ctx.Emit("sessions", ctx.Key(), history)

}
