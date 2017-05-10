# Example 1 User Clicks

This very simple example gives an impression on how to

* Write an emitter to push data to kafka
* Write a processor that consumes data from kafka, reducing it to a user state
* Writing a view to query the user state

## How to get it running
```bash
# go to the goka examples directory
cd $GOPATH/src/github.com/lovoo/goka/examples
# make sure kafka/zookkeeper are running
make restart

# run the example
go run 1-clicks/main.go
```
Now open the browser and checkout any user
<http://localhost:9095/user-3>
which should return e.g.

```json
{"Clicks":153}
```

## What is it doing?
As a simple use case we assume users click and for each click an event is pushed to Kafka. To simulate this, we use an
`Emitter` that conveniently allows us to write objects to a specific topic.
As a key for each event we use the User's ID.

A processor consumes those clicks and reduces this by counting the clicks each user has created.

To query a user's click count we use a *View* on the processor's table and create a simple web endpoint
to query it.

## Using Codecs
A codec is an interface to encode and decode a go-object to and from kafka, which uses `[]byte` to transfer data.

For this example, we use two codecs.

// Note that goka is returned error leads to a shutdown of the
// processor/view/emitter using the codec, so if you want to tolerate codec-errors, you'll have to handle
// them here.

1. a string-codec provided by goka, that simply type-casts from `string` to `[]byte`.
1. that is provided by goka for

convenience, and simply encodes/decodes from string by simply type-casting.

The second one wraps the standard `JSON-Marshaller` provided by go and is used within the processor.
