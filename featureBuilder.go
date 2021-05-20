package main

import (
	"context"
	"log"

	"github.com/lovoo/goka"
)

type Feature struct {
	Mean float64
}

type FeatureBuilder struct {
	g *goka.GroupGraph
}

func NewFeatureBuilder() *FeatureBuilder {
	return &FeatureBuilder{
		goka.DefineGroup("builder",
			goka.Input("sessions", new(arrayCodec), builderProcessor),
			goka.Output("features", new(featureCodec))),
	}
}

func (fb *FeatureBuilder) Run(ctx context.Context, brokers []string) {
	p, err := goka.NewProcessor(brokers, fb.g)
	if err != nil {
		log.Fatal(err)
	}
	err = p.Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("shut down nicely")
}

func builderProcessor(ctx goka.Context, msg interface{}) {

	// we know the msg is an array of Event because of the codec
	events := msg.([]Event)

	sum := 0.0
	count := 0

	for _, e := range events {
		vI := e.Value
		v, ok := vI.(float64)
		if !ok {
			continue
		}
		sum += v
		count++
	}

	out := Feature{
		Mean: sum / float64(count),
	}

	ctx.Emit("features", ctx.Key(), out)

}
