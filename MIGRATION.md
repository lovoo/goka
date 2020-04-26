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
