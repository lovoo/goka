## Reconnecting View

This example shows a reconnecting view by observing the state changes.
Run a local Kafka cluster by calling `make start` in folder `examples/`.

Then run this example (`go run 6-reconnecting-views/main.go`).
You should see the view state changes upon starting.

Now kill the kafka cluster `make stop`, you should see some error messages and the view
trying to reconnect using a default backoff