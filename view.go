package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/mux"
	"github.com/lovoo/goka"
)

// filter removes any events from the in array that are older than now-delta.
// The window width is specified as a duration
// The inbound array is assumed to be ordered by desceding time
// A zero delta means no filtering at all and the inbound array will be
// immediately returned unfiltered
func filter(in []Event, delta time.Duration) []Event {
	if delta == 0 {
		// if delta is 0 we're going to return the whole array
		// this is a special case
		return in
	}
	out := []Event{}
	windowStart := time.Now().Add(-delta)
	for i := range in {
		if in[i].T.After(windowStart) {
			out = append(out, in[i])
		} else {
			// we know that the stored array is orderd, so as soon as we
			// reach an event off the end of the window we know we're done
			break
		}
	}
	return out
}

func runView() {
	log.Println("running view")

	view, err := goka.NewView(brokers,
		goka.GroupTable("windowState"),
		new(arrayCodec),
	)
	if err != nil {
		panic(err)
	}

	root := mux.NewRouter()

	// /{key} returns the full history for that key ordered by descending
	// time
	root.HandleFunc("/{key}", func(w http.ResponseWriter, r *http.Request) {

		// get the stored window
		value, err := view.Get(mux.Vars(r)["key"])
		if err != nil {
			log.Fatal(err)
		}

		if value == nil {
			w.Write([]byte("value is nil"))
			return
		}

		// get the query parameters
		params, _ := url.ParseQuery(r.URL.RawQuery)
		width, ok := params["width"]

		// parse the window if it's there
		var delta time.Duration
		if ok {
			delta, err = time.ParseDuration(width[0])
			if err != nil {
				log.Fatal(err)
			}
		}

		// marshal the filtered window
		in := value.([]Event)
		data, err := json.Marshal(filter(in, delta))
		if err != nil {
			log.Fatal(err)
		}

		// write the serialised window to the responseWriter
		w.Write(data)
	})

	// /len/{key} returns the number of events in the window
	root.HandleFunc("/len/{key}", func(w http.ResponseWriter, r *http.Request) {
		value, err := view.Get(mux.Vars(r)["key"])
		if err != nil {
			log.Fatal(err)
		}
		if value == nil {
			w.Write([]byte("value is nil"))
			return
		}
		window := value.([]Event)
		l := len(window)
		data, err := json.Marshal(l)
		if err != nil {
			log.Fatal(err)
		}
		w.Write(data)
	})
	fmt.Println("View opened at http://localhost:9095/")
	go http.ListenAndServe(":9095", root)

	err = view.Run(context.Background())
	log.Println(err)
}
