package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/examples/3-messaging"
	"github.com/lovoo/goka/examples/3-messaging/collector"
)

func Run(brokers []string, stream goka.Stream) {
	view, err := goka.NewView(brokers, collector.Table, new(collector.MessageListCodec))
	if err != nil {
		panic(err)
	}
	go view.Run(context.Background())

	emitter, err := goka.NewEmitter(brokers, stream, new(messaging.MessageCodec))
	if err != nil {
		panic(err)
	}
	defer emitter.Finish()

	router := mux.NewRouter()
	router.HandleFunc("/{user}/send", send(emitter, stream)).Methods("POST")
	router.HandleFunc("/{user}/feed", feed(view)).Methods("GET")

	log.Printf("Listen port 8080")
	log.Fatal(http.ListenAndServe(":8080", router))
}

func send(emitter *goka.Emitter, stream goka.Stream) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var m messaging.Message

		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			fmt.Fprintf(w, "error: %v", err)
			return
		}

		err = json.Unmarshal(b, &m)
		if err != nil {
			fmt.Fprintf(w, "error: %v", err)
			return
		}

		m.From = mux.Vars(r)["user"]

		if stream == messaging.ReceivedStream {
			err = emitter.EmitSync(m.To, &m)
		} else {
			err = emitter.EmitSync(m.From, &m)
		}
		if err != nil {
			fmt.Fprintf(w, "error: %v", err)
			return
		}
		log.Printf("Sent message:\n %v\n", m)
		fmt.Fprintf(w, "Sent message:\n %v\n", m)
	}
}

func feed(view *goka.View) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		user := mux.Vars(r)["user"]
		val, _ := view.Get(user)
		if val == nil {
			fmt.Fprintf(w, "%s not found!", user)
			return
		}
		messages := val.([]messaging.Message)
		fmt.Fprintf(w, "Latest messages for %s\n", user)
		for i, m := range messages {
			fmt.Fprintf(w, "%d %10s: %v\n", i, m.From, m.Content)
		}
	}
}
