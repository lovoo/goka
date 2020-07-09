This document sums up issues for migrating between 0.1.4 to 0.9.x

## restartable/auto reconnect view.Run()

In 0.1.4, if a view was created to be restartable, it returned from `Run` in case of errors, but allowed
to be restarted calling `Run` again. The view was still usable, even if it was not running and receiving updates.

The behavior of that option has changed in 0.9.x in a way that the `view.Run` does the reconnect internally using configurable backoff.
The Option was also renamed (the old version has been kept for compatibility reasons).

```go

// create a view
view014, _ := NewView(..., WithViewRestartable())

// reconnect logic in 0.1.4
go func(){
  for running{
    err:= view014.Run(context.Background())
    // handle error

    // sleep or simple backoff logic
    time.Sleep(time.Second)
  }
}()

// After migration:
// create a view
view09x, _ := NewView(..., WithViewAutoReconnect())
ctx, cancel := context.WithCancel(context.Background())

// no need for reconnect logic, it's handled by the view internally
go func(){
  err:= view09x.Run(ctx)
  // handle shutdown error
}()

// stop view
cancel()

```


## Offset bug in local storage
In 0.1.4 there was a bug that caused the table offset being stored in the local cache always be +1 compared the actual offset stored in kafka.
A second bug kind of evened it out so it never was an issue. 

From 0.9.x, both bugs are fixed. However, if you upgrade goka and restart a processor using the same cache files that were maintained by the old version you'll see a warning like this
```
Error: local offset is higher than partition offset. topic some-topic, partition 0, hwm 1312, local offset 1314. This can have several reasons: 
(1) The kafka topic storing the table is gone --> delete the local cache and restart! 
(2) the processor crashed last time while writing to disk. 
(3) You found a bug!
```
This is because goka sees an offset that it is not expecting. 
You should see this error only once per partition and processor. The offset will be fixed automatically. If it appears on every start or regularily, it might actually a bug or some error and should be further investigated
(or reported to goka :)).