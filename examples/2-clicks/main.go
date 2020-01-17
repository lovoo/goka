package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/kafka"
)

var (
	brokers             = []string{"127.0.0.1:9092"}
	topic   goka.Stream = "user-click"
	group   goka.Group  = "mini-group"
)

// A user is the object that is stored in the processor's group table
type user struct {
	// number of clicks the user has performed.
	Clicks int
}

// This codec allows marshalling (encode) and unmarshalling (decode) the user to and from the
// group table.
type userCodec struct{}

// Encodes a user into []byte
func (jc *userCodec) Encode(value interface{}) ([]byte, error) {
	if _, isUser := value.(*user); !isUser {
		return nil, fmt.Errorf("Codec requires value *user, got %T", value)
	}
	return json.Marshal(value)
}

// Decodes a user from []byte to it's go representation.
func (jc *userCodec) Decode(data []byte) (interface{}, error) {
	var (
		c   user
		err error
	)
	err = json.Unmarshal(data, &c)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshaling user: %v", err)
	}
	return &c, nil
}

func runEmitter() {
	emitter, err := goka.NewEmitter(brokers, topic,
		new(codec.String))
	if err != nil {
		panic(err)
	}
	defer emitter.Finish()

	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()

	var i int
	for range t.C {
		key := fmt.Sprintf("user-%d", i%10)
		value := fmt.Sprintf("%s", time.Now())
		emitter.EmitSync(key, value)
		i++
	}
}

func process(ctx goka.Context, msg interface{}) {
	var u *user
	if val := ctx.Value(); val != nil {
		u = val.(*user)
	} else {
		u = new(user)
	}

	u.Clicks++
	ctx.SetValue(u)
	fmt.Printf("[proc] key: %s clicks: %d, msg: %v\n", ctx.Key(), u.Clicks, msg)
}

func runProcessor() {
	g := goka.DefineGroup(group,
		goka.Input(topic, new(codec.String), process),
		goka.Persist(new(userCodec)),
	)
	tmc := kafka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
	p, err := goka.NewProcessor(brokers,
		g,
		goka.WithTopicManagerBuilder(kafka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
		goka.WithConsumerGroupBuilder(kafka.DefaultConsumerGroupBuilder),
	)
	if err != nil {
		panic(err)
	}

	p.Run(context.Background())
}

func runView() {
	view, err := goka.NewView(brokers,
		goka.GroupTable(group),
		new(userCodec),
	)
	if err != nil {
		panic(err)
	}

	root := mux.NewRouter()
	root.HandleFunc("/{key}", func(w http.ResponseWriter, r *http.Request) {
		value, _ := view.Get(mux.Vars(r)["key"])
		data, _ := json.Marshal(value)
		w.Write(data)
	})
	fmt.Println("View opened at http://localhost:9095/")
	go http.ListenAndServe(":9095", root)

	view.Run(context.Background())
}

func main() {
	go runEmitter()
	go runProcessor()
	runView()
}
