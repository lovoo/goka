# User Clicks

This example is a toy application that counts how often users click on some button. Whenever a user clicks on the button, a message is emitted to a topic “user-clicks”. The message’s key is the user ID and, for the sake of the example, the message’s content is a timestamp, which is irrelevant for the example. We have one table storing a counter for each user. A processor updates the table whenever such a message is delivered. A view is exposed using a web interface to display the current counts of users.

This example shows how to:

* Write a processor that consumes data from kafka, counting clicks for a user
* Write an emitter to push data to kafka
* Writing a view to query the user table

To get an introduction into goka, see this [blog post](http://tech.lovoo.com/2017/05/23/goka).

## How to get it running
```bash
# kafka and zookeeper must be running, as described the examples Readme by running
# make restart in the examples-directory

# run the example
go run main.go
```

This should output something like

```
2017/05/23 15:09:20 Table mini-group-table has 10 partitions
2017/05/23 15:09:20 Processor: started
View opened at http://localhost:9095/
2017/05/23 15:09:20 View: started
2017/05/23 15:09:33 Processor: rebalancing: map[]
2017/05/23 15:09:37 Processor: rebalancing: map[5:-1 6:-1 2:-1 3:-1 4:-1 8:-1 9:-1 0:-1 1:-1 7:-1]
[proc] key: user-0 clicks: 23, msg: 2017-05-23 15:09:04.265935416 +0200 CEST
[proc] key: user-5 clicks: 23, msg: 2017-05-23 15:09:03.757817584 +0200 CEST
[proc] key: user-8 clicks: 23, msg: 2017-05-23 15:09:04.062448921 +0200 CEST
[proc] key: user-7 clicks: 23, msg: 2017-05-23 15:09:03.960607552 +0200 CEST
...
```
Now open the browser and get the number of clicks for user-3: <http://localhost:9095/user-3>

This should return e.g.

```json
{"Clicks":153}
```

### Count User Clicks

The clicks for each user are kept in the group table of the processor.
To process user clicks we create a process()-callback that takes two arguments
(see the code sample below): the callback context ctx and the message’s content
msg. Each key has an associated value in the processor’s group table. In our
example, we store an integer counter representing how often the user has
performed clicks.

```go
1  func process(ctx goka.Context, msg interface{}) {
2   var u *user
3   if val := ctx.Value(); val != nil {
4    u = val.(*user)
5   } else {
6    u = new(user)
7   }
8
9   u.Clicks++
10  ctx.SetValue(u)
11  fmt.Printf("[proc] key: %s clicks: %d, msg: %v", ctx.Key(), u.Clicks, msg)
12 }
```


To retrieve the current value of the user, we try to retrieve the value from the group table that matches the message's key (3) by calling
`ctx.Value()`
If it exists, it should be a `*user`, because that's what we will store there later and what the
codec expects (4).
If it's nil, the user has not been saved yet and we'll create it (6).
Now that we have a user, we simply increment the clicks (9) and
update it in our group table (10) by calling `ctx.SetValue(u)`.
We conclude process() with a print statement showing message’s content, the
current count of the user, and the user ID fetched with ctx.Key().

The context interface never returns errors to the callbacks. Instead, if an error is encountered while executing the context functions, the processor instance is stopped and its Start() method returns an error.


We configure the processor using `goka.DefineGroup`, which we later
pass to `NewProcessor`.

```go
1  g := goka.DefineGroup(group,
2    goka.Input(topic, new(codec.String), process),
3    goka.Persist(new(userCodec)),
4  )
```

* `goka.Input` configures the processor to consume the topic as a stream using the `string`-codec.
The consumer of a topic must use the same codec as the writer, otherwise we'll get unexpected messages or
unmarshalling will simply fail.

* `goka.Persist` makes the processor store its group table persistently using kafka. That means on every
restart (either the same host or somewhere else), the group table will be restored.
This option also makes the processor cache the group table locally using a key-value store.
That avoids holding the full group table in memory and a long-running recovery on every restart.

  To persist the group table, again we need a `Codec` which encodes the user for this case.
  We want to store objects of type `*user`, so we have to implement our own codec. In our example,
  the Codec simply marshals using the default go json-Marshaller.

* In Goka message keys and table keys are always strings.

* `NewProcessor()` takes a slice of Kafka broker addresses and the processor group definition.

For more information on configuring a processor using `DefineGroup`, see the [GoDoc](https://godoc.org/github.com/lovoo/goka#DefineGroup).


### View and Emitter

For this example, we want to have a look on the user counter one by one. To query a user's click
count we use a *View* on the processor's group table and create a simple web endpoint
to query it.
In contrast to a processor, a view always contains all partitions and allows to query
values for any key. Think of it as a lookup table.
The view also requires a codec for the values, as it caches the group table locally on disk like the processor does.

```go
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
```

Finally, an `Emitter` is used to simulate user clicks. This allows us to write objects conveniently to a specific topic. The key for each message is the user ID which is simply generated by the sender loop.

```go
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
```

The `Emitter` is created specifying the topic and a `Codec` that marshals passed messages for us into Kafka.
Here we use a codec provided by goka called `codec.String`, that simply marshals from `string` values.
In our case that's sufficient since a payload of a click is simply a time-string. If we wanted to write more
complex objects (e.g., structs) we would have to implement our own codec, similar to the `userCodec` mentioned below.

### Codec

A codec is an interface that encodes and decodes between an arbitrary value and `[]byte`. This is used to store data in kafka and on disk.
```go
type Codec interface {
	Encode(value interface{}) (data []byte, err error)
	Decode(data []byte) (value interface{}, err error)
}
```
Convenience Codecs for often-used types are provided by goka, like `codec.Int64` and `codec.String`. In most cases however the data is
complex and custom codecs must be provided. In this example we implemented a userCodec that uses the `json`-Marshal/Unmarshal functionality for encoding and decoding.

Note that errors returned by the codec lead to a shutdown of the
processor/view/emitter using it immediately. We chose that fail-early-approach since data corruption would occur if,
for example, a processor accidentally reads and writes using a wrong codec and mixes different codecs in the group table.
If you need to tolerate codec-errors, you'll have to handle them inside the codec and make sure it returns a `nil`-error.

### Partitioning and Concurrency

Messages are partitioned in Kafka using the message key. Within a given partition, messages are processed sequentially. However, different partitions process messages concurrently.

In this example, the user ID is used as the message key, i.e., the messages are partitioned by the user ID.
Therefore, different users are modified concurrently whereas a single user is always modified sequentially.

That's why there is no need to create any locks as long as all modifications are performed using the `context`.
Everything else needs to be protected by locks as usual.


[GoDoc]: https://godoc.org/github.com/lovoo/goka
[examples]: https://github.com/lovoo/goka/tree/master/examples
