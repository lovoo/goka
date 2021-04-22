package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/multierr"
	"github.com/lovoo/goka/web/index"
	"github.com/lovoo/goka/web/monitor"
	"github.com/lovoo/goka/web/query"
)

var (
	brokers               = []string{"localhost:9092"}
	topic     goka.Stream = "user-click"
	group     goka.Group  = "mini-group"
	joinGroup goka.Group  = group + "-join"
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

func runEmitter(ctx context.Context) (rerr error) {
	emitter, err := goka.NewEmitter(brokers, topic,
		new(codec.String))
	if err != nil {
		rerr = err
		return
	}
	defer func() {
		rerr = emitter.Finish()
	}()

	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()

	var i int
	for {
		select {
		case <-t.C:
			key := fmt.Sprintf("user-%d", i%50)
			value := fmt.Sprintf("%s", time.Now())
			emitter.EmitSync(key, value)
			i++
		case <-ctx.Done():
			return
		}
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
func runStatelessProcessor(ctx context.Context, monitor *monitor.Server) error {
	g := goka.DefineGroup(group+"-stateless",
		goka.Input(topic,
			new(codec.String),
			func(ctx goka.Context, msg interface{}) {
				//ignored
			}),
	)
	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		return err
	}

	// attach the processor to the monitor
	monitor.AttachProcessor(p)

	return p.Run(ctx)
}

func runJoinProcessor(ctx context.Context, monitor *monitor.Server) error {
	g := goka.DefineGroup(joinGroup,
		goka.Input(topic,
			new(codec.String),
			func(ctx goka.Context, msg interface{}) {
				var u *user
				if val := ctx.Value(); val != nil {
					u = val.(*user)
				} else {
					u = new(user)
				}

				u.Clicks++
				ctx.SetValue(u)
			}),
		goka.Lookup(goka.GroupTable(group), new(userCodec)),
		goka.Persist(new(userCodec)),
	)
	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		return err
	}

	// attach the processor to the monitor
	monitor.AttachProcessor(p)

	return p.Run(ctx)
}

func runProcessor(ctx context.Context, monitor *monitor.Server, query *query.Server) error {
	g := goka.DefineGroup(group,
		goka.Input(topic, new(codec.String), process),
		goka.Join(goka.GroupTable(joinGroup), new(codec.String)),
		goka.Persist(new(userCodec)),
	)
	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		return err
	}

	// attach the processor to the monitor
	monitor.AttachProcessor(p)
	query.AttachSource("user-clicks", p.Get)

	err = p.Run(ctx)
	if err != nil {
		log.Printf("Error running processor: %v", err)
	}
	return err
}

func runView(errg *multierr.ErrGroup, ctx context.Context, root *mux.Router, monitor *monitor.Server) error {
	view, err := goka.NewView(brokers,
		goka.GroupTable(group),
		new(userCodec),
	)
	if err != nil {
		return err
	}

	// attach the processor to the monitor
	monitor.AttachView(view)

	errg.Go(func() error {
		return view.Run(ctx)
	})

	server := &http.Server{Addr: ":9095", Handler: root}

	errg.Go(func() error {

		root.HandleFunc("/{key}", func(w http.ResponseWriter, r *http.Request) {
			value, _ := view.Get(mux.Vars(r)["key"])
			data, _ := json.Marshal(value)
			w.Write(data)
		})
		fmt.Println("View opened at http://localhost:9095/")
		err := server.ListenAndServe()
		if err != http.ErrServerClosed {
			return err
		}
		return nil
	})
	errg.Go(func() error {
		// wait for outer context to be finished
		<-ctx.Done()
		log.Printf("context cancelled, will shutdown server")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(ctx)
	})
	return nil
}

func pprofInit(root *mux.Router) {
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)

	sub := root.PathPrefix("/debug/pprof").Subrouter()
	sub.HandleFunc("/", pprof.Index)

	sub.HandleFunc("/cmdline", pprof.Cmdline)
	sub.Handle("/heap", pprof.Handler("heap"))
	sub.Handle("/goroutine", pprof.Handler("goroutine"))
	sub.Handle("/block", pprof.Handler("block"))
	sub.Handle("/mutex", pprof.Handler("mutex"))
	sub.HandleFunc("/profile", pprof.Profile)
	sub.HandleFunc("/symbol", pprof.Symbol)
	sub.HandleFunc("/trace", pprof.Trace)
}

func main() {

	cfg := goka.DefaultConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Version = sarama.V2_4_0_0
	goka.ReplaceGlobalConfig(cfg)

	tmgr, _ := goka.NewTopicManager(brokers, goka.DefaultConfig(), goka.NewTopicManagerConfig())
	tmgr.EnsureStreamExists(string(topic), 2)
	tmgr.EnsureTableExists(string(goka.GroupTable(group)), 2)

	root := mux.NewRouter()
	pprofInit(root)
	monitorServer := monitor.NewServer("/monitor", root)
	queryServer := query.NewServer("/query", root)
	idxServer := index.NewServer("/", root)
	idxServer.AddComponent(monitorServer, "Monitor")
	idxServer.AddComponent(queryServer, "Query")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		waiter := make(chan os.Signal, 1)
		signal.Notify(waiter, syscall.SIGINT, syscall.SIGTERM)
		<-waiter
		cancel()
	}()

	errg, ctx := multierr.NewErrGroup(ctx)
	errg.Go(func() error {
		defer log.Printf("emitter done")
		return runEmitter(ctx)
	})
	errg.Go(func() error {
		defer log.Printf("processor done")
		return runProcessor(ctx, monitorServer, queryServer)
	})
	errg.Go(func() error {
		defer log.Printf("stateless processor done")
		return runStatelessProcessor(ctx, monitorServer)
	})
	errg.Go(func() error {
		defer log.Printf("join procdessor done")
		return runJoinProcessor(ctx, monitorServer)
	})
	if err := runView(errg, ctx, root, monitorServer); err != nil {
		log.Printf("Error running view, will shutdown: %v", err)
		cancel()
	}

	if err := errg.Wait().NilOrError(); err != nil {
		log.Fatalf("Error running monitoring example: %v", err)
	} else {
		log.Printf("Example gracefully shutdown")
	}
}
