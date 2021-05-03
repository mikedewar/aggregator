package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/lovoo/goka"
)

func runView() {
	log.Println("running view")

	view, err := goka.NewView(brokers,
		"windowState-table",
		new(arrayCodec),
	)
	if err != nil {
		panic(err)
	}

	root := mux.NewRouter()
	root.HandleFunc("/{key}", func(w http.ResponseWriter, r *http.Request) {
		value, err := view.Get(mux.Vars(r)["key"])
		if err != nil {
			log.Fatal(err)
		}
		data, err := json.Marshal(value)
		if err != nil {
			log.Fatal(err)
		}
		w.Write(data)
	})
	root.HandleFunc("/len/{key}", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		value, err := view.Get(mux.Vars(r)["key"])
		log.Println("Get:", time.Since(start))
		if err != nil {
			log.Fatal(err)
		}
		window := value.([]int64)
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
