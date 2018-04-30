# Goka web interface

Observing the progress of recovery and execution of a goka processor is often vital for basic
debugging purposes or performance analyses.

Goka provides a very simple web interface that allows to show statistics about individual partitions including
state, offsets and lag as well as input- and output rate.

For low level inspection of table data, it provides a simple interface to query values from
views or processor tables.

## Monitoring

First, to get a web interface we need a router. If the program using Goka already uses a router, Goka's web
interface can easily be attached to it instead.

So first, initialize the monitoring server using a router with

```go
root := mux.NewRouter()
monitorServer := monitor.NewServer("/monitor", root)
```

Suppose we have a processor *proc*, then we can simply attach it to the monitor
```go
proc := goka.NewProcessor(...)
monitorServer.AttachProcessor(proc)
```

Ok, then let's start the processor and the server with
```go
go proc.Run(context.Background())
http.ListenAndServe(":9095", root)
```

Opening the browser on [localhost:9095/monitor/]() will then show us a list of all attached processors
and views and by selecting one of them we'll get statistics of the processor, similar like this

![Processor View](images/processor-view.png?raw=true "processor view")

## Index

Ok nice, but if you don't have another router with an index and links to the monitor,
it's really annoying to add *monitor/* to your browser url every time you want to open the monitor.
That's why Goka provides a small convenience index server, that the Monitoring and the Query servers can be attached to.

```go
idxServer := index.NewServer("/", root)
idxServer.AddComponent(monitorServer, "Monitor")
```

So next time we open the page at [localhost:9095](), we'll get some nice links to the monitor.

![Index](images/index.png?raw=true "index view")


## Query
As mentioned earlier, we can also add a query server that allows us to request a value by key from
a View or a Processor table, simply by attaching it to a Query server. Also let's add the new server to the
index as well to avoid repeated typing of *query/* in the url.

```go
queryServer := query.NewServer("/query", root)
idxServer.AddComponent(queryServer, "Query")
queryServer.AttachSource("user-clicks", p.Get)
```

Opening a query page gives us a page like this:

![Query](images/query.png?raw=true "query view")

Voila!

## Example

The example in [main.go]() demonstrates the concepts and typical applications of the web interface by
creating an Emitter, multiple Processors and a web interface.

Be sure to have Apache Kafka and Zookeeper running by starting it in the examples-folder.

```console
examples$ make restart
```

Then run the monitoring example with:

```console
examples$ go run monitoring/main.go
```
