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
	"github.com/hashicorp/go-multierror"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/multierr"
	"github.com/lovoo/goka/storage"
	"github.com/lovoo/goka/web/index"
	"github.com/lovoo/goka/web/monitor"
	"github.com/lovoo/goka/web/query"
)

var (
	brokers             = []string{"localhost:9092"}
	inputA  goka.Stream = "input-A"
	inputB  goka.Stream = "input-B"
	group   goka.Group  = "multiInput"
)

func randomStorageBuilder(suffix string) storage.Builder {
	return storage.DefaultBuilder(fmt.Sprintf("/tmp/goka-%d/%s", time.Now().Unix(), suffix))
}

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
	emitterA, err := goka.NewEmitter(brokers, inputA, new(codec.String))
	if err != nil {
		return err
	}
	emitterB, err := goka.NewEmitter(brokers, inputB, new(codec.String))
	if err != nil {
		return err
	}

	defer func() {
		rerr = multierror.Append(
			emitterA.Finish(),
			emitterB.Finish(),
		).ErrorOrNil()
	}()

	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()

	var i int
	for {
		select {
		case <-t.C:
			keyA := fmt.Sprintf("user-%d", i%50)
			keyB := fmt.Sprintf("user-%d", i%60)
			value := fmt.Sprintf("%s", time.Now())
			emitterA.EmitSync(keyA, value)
			emitterB.EmitSync(keyB, value)
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
}

func runProcessor(ctx context.Context, monitor *monitor.Server, query *query.Server) error {
	p, err := goka.NewProcessor(brokers, goka.DefineGroup(group,
		goka.Input(inputA, new(codec.String), process),
		goka.Input(inputB, new(codec.String), process),
		goka.Persist(new(userCodec)),
	),
		goka.WithStorageBuilder(randomStorageBuilder("proc")),
	)
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

func runView(ctx context.Context, errg *multierr.ErrGroup, root *mux.Router, monitor *monitor.Server) error {
	view, err := goka.NewView(brokers,
		goka.GroupTable(group),
		new(userCodec),
		goka.WithViewStorageBuilder(randomStorageBuilder("view")),
	)
	if err != nil {
		return err
	}

	// attach the processor to the monitor
	monitor.AttachView(view)

	errg.Go(func() error {
		return view.Run(ctx)
	})

	server := &http.Server{Addr: ":0", Handler: root}
	errg.Go(func() error {

		root.HandleFunc("/{key}", func(w http.ResponseWriter, r *http.Request) {
			value, _ := view.Get(mux.Vars(r)["key"])
			data, _ := json.Marshal(value)
			w.Write(data)
		})
		fmt.Println("View opened at http://localhost:0/")
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
	if err := runView(ctx, errg, root, monitorServer); err != nil {
		log.Printf("Error running view, will shutdown: %v", err)
		cancel()
	}

	if err := errg.Wait().ErrorOrNil(); err != nil {
		log.Fatalf("Error running example: %v", err)
	} else {
		log.Printf("Example gracefully shutdown")
	}
}
