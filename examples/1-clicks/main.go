package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

var (
	brokers             = []string{"127.0.0.1:9092"}
	topic   goka.Stream = "user-click"
	group   goka.Group  = "mini-group"
)

// type clickCodec struct{}
//
// func (jc *clickCodec) Encode(value interface{}) ([]byte, error) {
// 	if _, isClick := value.(*click); !isClick {
// 		return nil, fmt.Errorf("Codec requires value *click, got %T", value)
// 	}
// 	return json.Marshal(value)
// }
//
// func (jc *clickCodec) Decode(data []byte) (interface{}, error) {
// 	var (
// 		c   click
// 		err error
// 	)
// 	err = json.Unmarshal(data, &c)
// 	if err != nil {
// 		return nil, fmt.Errorf("Error unmarshaling click: %v", err)
// 	}
// 	return &c, nil
// }
//
// type click struct {
// }

type userCodec struct{}

func (jc *userCodec) Encode(value interface{}) ([]byte, error) {
	if _, isUser := value.(*user); !isUser {
		return nil, fmt.Errorf("Codec requires value *user, got %T", value)
	}
	return json.Marshal(value)
}

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

type user struct {
	Clicks int
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
}

func runProcessor() {
	g := goka.DefineGroup(group,
		goka.Input(topic, new(codec.String), process),
		goka.Persist(new(userCodec)),
	)
	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		panic(err)
	}

	err = p.Start()
	if err != nil {
		panic(err)
	} else {
		fmt.Println("Processor stopped without errors")
	}
}

func runView() {
	view, err := goka.NewView(brokers,
		goka.GroupTable(group),
		new(userCodec),
	)
	if err != nil {
		panic(err)
	}
	go view.Start()
	defer view.Stop()

	root := mux.NewRouter()
	root.HandleFunc("/{key}", func(w http.ResponseWriter, r *http.Request) {
		value, _ := view.Get(mux.Vars(r)["key"])
		data, _ := json.Marshal(value)
		w.Write(data)
	})
	fmt.Println("View opened at http://localhost:9095/")
	http.ListenAndServe(":9095", root)
}

func main() {
	go runEmitter()
	go runProcessor()
	runView()
}
