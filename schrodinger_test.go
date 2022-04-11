package main

import (
	"context"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lovoo/goka"
	"github.com/stretchr/testify/assert"
)

// assertWindowTimes makes sure that the time stamps  in the expected window
// match the timestamps in the actual window
func assertWindowTimes(t *testing.T, expected, actual []Event) {
	assert.Truef(t, len(expected) == len(actual), "lengths of expected window and actual window don't match")
	for i, _ := range expected {
		assert.True(t, expected[i].T.Equal(actual[i].T))
	}
}

func runAsserter(ctx context.Context, t *testing.T, topic, expectedKey string, expected []Event, finishTest, done chan bool) {

	log.Println("running window building assertion tests. Window size = ", len(expected))

	i := 0
	cb := func(ctx goka.Context, msg interface{}) {

		// if we're running this test over and over there will be loads of junk in
		// the group table; skip everything other than the expeted key
		if ctx.Key() == expectedKey {
			i++
			actual := msg.([]Event)

			// assert that the recorded times match in order as the window increases
			assertWindowTimes(t, expected[:i], actual)

			// once we've gotten to the end of the window, trigger a cancellation via
			// the `finishTest` channel
			if i == len(expected)-1 {
				log.Println("completed window time assertion test")
				finishTest <- true
			}
		}
	}

	g := goka.DefineGroup("asserter",
		goka.Input(goka.Stream(topic), new(arrayCodec), cb),
	)

	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		log.Fatalf("error creating processor: %v", err)
	}
	defer close(done)
	err = p.Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("asserterReader shut down nicely")
}

// TestOrder pushes test data to the "example-stream" topic, runs the
// windowBuilder, which writes to the group topic. We also run an "asserter"
// reader against the group topic to make sure what comes out is what we expected.
func TestOrder(t *testing.T) {

	// cancel lets us cancel the processors, and the two done chans wait for them
	// to complete
	ctx, cancel := context.WithCancel(context.Background())
	windowBuilderDone := make(chan bool)
	asserterDone := make(chan bool)

	// make sure the source stream exists
	tmgr, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), goka.NewTopicManagerConfig())
	err = tmgr.EnsureStreamExists("example-stream", 10)
	if err != nil {
		log.Fatalf("Error creating sessions: %v", err)
	}

	// run the windowBuilder
	log.Println("running windowbuilder")
	go runWindowBuilder(ctx, brokers, windowBuilderDone)

	// create the expected window after N messages
	N := 10
	expected := make([]Event, N)
	key := uuid.New().String()
	for i := 0; i < N; i++ {
		v := Event{
			T:     time.Now(),
			Value: rand.NormFloat64(),
		}
		expected[i] = v
	}

	// run the asserter, which will incrementally assert that the expected window
	// so far matches the window as it's built up by the windowBuilder and
	// emitted into the groupTopic
	finishTest := make(chan bool)
	go runAsserter(ctx, t, "window-table", key, expected, finishTest, asserterDone)

	// now the window builder and the asserter are running goahead and emit the messages one at a time
	emitter, err := goka.NewEmitter(brokers, "example-stream", new(eventCodec))
	if err != nil {
		log.Fatalf("error creating emitter: %v", err)
	}
	defer emitter.Finish()
	for _, v := range expected {
		_, err = emitter.Emit(key, v)
		if err != nil {
			log.Fatalf("error emitting message: %v", err)
		}
	}

	// wait for the asserter to receive all its expected messages
	<-finishTest

	// send cancel to both consumers
	cancel()

	// wait for the consumers to finish before closing
	<-windowBuilderDone
	<-asserterDone
}

func runCompletionTest(ctx context.Context, topic, expectedKey string, windowLen int, finishTest, done chan bool) {

	log.Println("running window building completion test")

	cb := func(ctx goka.Context, msg interface{}) {

		// if we're running this test over and over there will be loads of junk in
		// the group table; skip everything other than the expeted key
		if ctx.Key() == expectedKey {
			actual := msg.([]Event)

			// once we've gotten to the end of the window, trigger a cancellation via
			// the `finishTest` channel
			if len(actual) == windowLen {
				log.Println("completed window time assertion test")
				finishTest <- true
			}
		}
	}

	g := goka.DefineGroup("completion",
		goka.Input(goka.Stream(topic), new(arrayCodec), cb),
	)

	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		log.Fatalf("error creating processor: %v", err)
	}
	defer close(done)
	err = p.Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("asserterReader shut down nicely")
}

func BenchmarkSingleWindow(b *testing.B) {
	// cancel lets us cancel the processors, and the two done chans wait for them
	// to complete
	ctx, cancel := context.WithCancel(context.Background())
	windowBuilderDone := make(chan bool)
	completionDone := make(chan bool)
	finishTest := make(chan bool)

	// make sure the source stream exists
	tmgr, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), goka.NewTopicManagerConfig())
	err = tmgr.EnsureStreamExists("example-stream", 10)
	if err != nil {
		log.Fatalf("Error creating sessions: %v", err)
	}

	// run the windowBuilder
	log.Println("running windowbuilder")
	go runWindowBuilder(ctx, brokers, windowBuilderDone)

	emitter, err := goka.NewEmitter(brokers, "example-stream", new(eventCodec))
	for i := 1; i < b.N; i++ {
		key := uuid.New().String()
		for j := 0; j < j; j++ {
			v := Event{
				T:     time.Now(),
				Value: rand.NormFloat64(),
			}
			_, err = emitter.Emit(key, v)
			if err != nil {
				log.Fatalf("error emitting message: %v", err)
			}
		}
		go runCompletionTest(ctx, "windowTable", key, i, finishTest, completionDone)
		<-finishTest
		cancel()
		<-completionDone
		<-windowBuilderDone
	}
}
