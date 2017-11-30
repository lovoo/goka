package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/web/index"
	"github.com/lovoo/goka/web/monitor"
	"github.com/lovoo/goka/web/query"
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
		key := fmt.Sprintf("user-%d", i%20)
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

func runProcessor(monitor *monitor.Server, query *query.Server) {
	g := goka.DefineGroup(group,
		goka.Input(topic, new(codec.String), process),
		goka.Persist(new(userCodec)),
	)
	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		panic(err)
	}

	// attach the processor to the monitor
	monitor.AttachProcessor(p)
	query.AttachSource("user-clicks", p.Get)

	err = p.Start()
	if err != nil {
		panic(err)
	} else {
		fmt.Println("Processor stopped without errors")
	}
}

func runView(root *mux.Router, monitor *monitor.Server) {
	view, err := goka.NewView(brokers,
		goka.GroupTable(group),
		new(userCodec),
	)
	if err != nil {
		panic(err)
	}

	// attach the processor to the monitor
	monitor.AttachView(view)

	go view.Start()
	defer view.Stop()

	root.HandleFunc("/{key}", func(w http.ResponseWriter, r *http.Request) {
		value, _ := view.Get(mux.Vars(r)["key"])
		data, _ := json.Marshal(value)
		w.Write(data)
	})
	fmt.Println("View opened at http://localhost:9095/")
	http.ListenAndServe(":9095", root)
}

func main() {
	root := mux.NewRouter()
	monitorServer := monitor.NewServer("/monitor", root)
	queryServer := query.NewServer("/query", root)
	idxServer := index.NewServer("/", root)
	idxServer.AddComponent(monitorServer, "Monitor")
	idxServer.AddComponent(queryServer, "Query")
	go runEmitter()
	go runProcessor(monitorServer, queryServer)
	runView(root, monitorServer)
}
