# User clicks

Let us create a toy application that counts how often users click on some button. Whenever a user clicks on the button, a message is emitted to a topic “user-clicks”. The message’s key is the user ID and, for the sake of the example, the message’s content is a timestamp, which is irrelevant for the application. In our application, we have one table storing a counter for each user. A processor updates the table whenever such a message is delivered. A view is used to periodically display the current counts of the users.

Updating the table. To process the user-clicks topic, we create a process() callback that takes two arguments (see the code sample below): the callback context ctx and the message’s content msg. Each key has an associated value in the processor’s group table. In our example, we store an integer counter representing how often the user has performed clicks.

```go
func process(ctx goka.Context, msg interface{}) {
	var counter int64
	if val := ctx.Value(); val != nil {
		counter = val.(int64)
	}
	counter++
	ctx.SetValue(counter)
 
	fmt.Println("[proc] key:", ctx.Key(),
		"count:", counter, "msg:", msg)
}
```

To retrieve the current value of counter, we call ctx.Value(). If the result is nil, nothing has been stored so far, otherwise we cast the value to an integer. We then process the message by simply incrementing the counter and saving the result back in the table with ctx.SetValue(). We conclude process() with a print statement showing message’s content, the current count of the user, and the user ID fetched with ctx.Key().

Note that the context is a rich interface, allowing the processor to emit messages into other stream topics using ctx.Emit(), read values from tables of other groups with ctx.Join() and ctx.Lookup(), and more. We look at other context methods later in this document [TODO: do we do that?].

[TODO: needs revise/rework.. bit too much detail]The following snippet shows the code to create the processor. Before creating a processor instance, we define each instance in group to process the stream topic, invoking process() for each message received. The content of the messages is a string, hence, Input() takes a string codec as argument. Persist() defines that the group table contains a 64-bit integer for each user. NewProcessor() takes as arguments a slice Kafka broker addresses and the processor group definition. In Goka message keys and table keys are always strings.

The context interface never returns errors to the callbacks. Instead, if an error is encountered while executing the context functions, the processor instance is stopped and its Start() method returns an error.

```go
func runProcessor() {
	g := goka.DefineGroup(group,
		goka.Input(topic, new(codec.String), process),
		goka.Persist(new(codec.Int64)),
	)
	if p, err := goka.NewProcessor(brokers, g); err != nil {
		panic(err)
	} else if err = p.Start(); err != nil {
		panic(err)
	}
}
```

View and emitter. Besides processing the messages in the user-clicks topic, we would like to use these values for some task. The following code shows how to use a view to print the content of the group table for each user ID in {0, . . . , 9} every 10 seconds. NewView() takes as arguments the brokers slice and a table name – the group table of group – and the codec of the values in the table. Views can be started, stopped, and queried. The Get() method returns the value for a key.

```
func runView() {
	view, err := goka.NewView(brokers,
		goka.GroupTable(group),
		new(codec.Int64),
	)
	if err != nil {
		panic(err)
	}
	go view.Start()
	defer view.Stop()

	t := time.NewTicker(10 * time.Second)
	defer t.Stop()

	for range t.C {
		for i := 0; i < 10; i++ {
			val, _ := view.Get(fmt.Sprintf("%d", i))
			fmt.Printf("[view] %d: %v\n", i, val)
		}
	}
}
```
[TODO: too much detail, the user is not stupid] Finally, an emitter is used to simulate user clicks. Every 5 seconds a counter i is incremented and used to create a user ID (modulo 10). A message is then emitted with content being the string representation of the current time. The emitter is created with the brokers slice, the topic into which it will write (i.e., user-clicks), and the codec used to serialize the content of the messages.

```go
func runEmitter() {
	emitter, err := goka.NewEmitter(brokers, topic,
		new(codec.String))
	if err != nil {
		panic(err)
	}
	defer emitter.Finish()

	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	i := 0
	for range t.C {
		key := fmt.Sprintf("%d", i%10)
		value := fmt.Sprintf("%s", time.Now())
		emitter.EmitSync(key, value)
		i++
	}
}
```
