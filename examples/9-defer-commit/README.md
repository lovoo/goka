# Defer Commit

This example demonstrates the context function `DeferCommit`.

Usually the goka-context takes care of committing the incoming message offset once the callback has returned *and* all the started side-operations like `SetValue`, `Emit` or `Loopback` have returned to make sure that the message is not reconsumed, except in case of failure.

However, there are rare use cases which require more control over the comitting behavior of the context, especially when using external services that implement asynchronous/callback interfaces. 

The following example simulates to forward messages into a different kafka cluster (so `ctx.Emit` won't work, but an extra `goka.Emitter` will do). 

Without `DeferCommit` we would have two options: (a) use `emitter.EmitSync` and wait for the result. This however disables batching - so it comes with performance penalty. (b) use `emitter.Emit` which fails when the returned `Promise` fails asynchronously. This however means that failing to emit a message results in data loss as the message will not be reconsumed.

`DeferCommit` returns a function `func(error)` that passes the responsibility to commit to the caller. Failing to call that function will cause the messages to be reconsumed eventually and most likely to block the processor, as the involved input messages never seem to be processed and the Processor waits indefinitely.
**If `DeferCommit` is called multiple times, all returned functions need to be called individually.**

It should be used like this:
```go
func callback(ctx goka.Context, msg interface{}){
  // ...

  commit := ctx.DeferCommit()

  // in some async-code 
  go func(){
       
    if /* no error */{
    // success
      commit(nil)
    }else{
      // call commit with error. 
      // This will actually not commit the message but shutdown the processor.
      commit(errors.New("some error"))
    }
  }()
}
```

The example uses one emitter that emits the current unix-timestamp into topic `input-topic`. 
The forwarding processor instantiates a new emitter that represents the asynchronous component and emits the message
to a `forward-topic` using `DeferCommit`. The third processor just prints the received messages.

Running the example prints the message every second:
```bash 
go run main.go

# output: 
# 2021/02/17 11:18:50 received message time: 1613557129
# 2021/02/17 11:18:51 received message time: 1613557130
# 2021/02/17 11:18:52 received message time: 1613557131
# 2021/02/17 11:18:53 received message time: 1613557132
``` 
Stopping and restarting the example continues to print the incrementing timestamp.

Now run the example with disabled commit. Note that stopping with `ctrl-c` does not work as the processor hangs at the uncommitted messages. Doing `ctrl-\` does the trick and prints the stack trace.
```bash
go run main.go --no-commit

# output:
# 2021/02/17 11:20:28 received message time: 1613557228
# 2021/02/17 11:20:29 received message time: 1613557229
# 2021/02/17 11:20:30 received message time: 1613557230
# ^C
# <stack trace>
```

Running it again, shows that the messages are reconsumed as the offset was not commited
```bash
go run main.go --no-commit

# output:
# --> old messages are reconsumed
# 2021/02/17 11:21:48 received message time: 1613557228
# 2021/02/17 11:21:48 received message time: 1613557229
# 2021/02/17 11:21:48 received message time: 1613557230
## --> new messages start here
# 2021/02/17 11:21:48 received message time: 1613557306
# 2021/02/17 11:21:48 received message time: 1613557307
# 2021/02/17 11:21:48 received message time: 1613557308
```

