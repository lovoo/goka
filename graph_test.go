package goka

import (
	"reflect"
	"strings"
	"testing"

	"github.com/facebookgo/ensure"
	"github.com/lovoo/goka/codec"
)

var (
	c  = new(codec.String)
	cb = func(ctx Context, msg interface{}) {}
)

func TestGroupGraph_Validate(t *testing.T) {
	g := DefineGroup("group")
	err := g.Validate()
	ensure.StringContains(t, err.Error(), "no input")

	g = DefineGroup("group",
		Input("input-topic", c, cb))
	err = g.Validate()
	ensure.Nil(t, err)

	g = DefineGroup("group",
		Input("input-topic", c, cb),
		Loop(c, cb),
		Loop(c, cb),
	)
	err = g.Validate()
	ensure.StringContains(t, err.Error(), "more than one loop")

	g = DefineGroup("group",
		Input("input-topic", c, cb),
		Persist(c),
		Persist(c),
	)
	err = g.Validate()
	ensure.StringContains(t, err.Error(), "more than one group table")

	g = DefineGroup("group",
		Input(Stream(tableName("group")), c, cb),
		Persist(c),
	)
	err = g.Validate()
	ensure.StringContains(t, err.Error(), "group table")

	g = DefineGroup("group",
		Input(Stream(loopName("group")), c, cb),
		Loop(c, cb),
	)
	err = g.Validate()
	ensure.StringContains(t, err.Error(), "loop stream")

	g = DefineGroup("group",
		Input("input-topic", c, cb),
		Join(Table(loopName("group")), c),
	)
	err = g.Validate()
	ensure.StringContains(t, err.Error(), "loop stream")

	g = DefineGroup("group",
		Input("input-topic", c, cb),
		Output(Stream(loopName("group")), c),
	)
	err = g.Validate()
	ensure.StringContains(t, err.Error(), "loop stream")

	g = DefineGroup("group",
		Input("input-topic", c, cb),
		Lookup(Table(loopName("group")), c),
	)
	err = g.Validate()
	ensure.StringContains(t, err.Error(), "loop stream")

}

func TestGroupGraph_codec(t *testing.T) {
	g := DefineGroup("group",
		Input("input-topic", c, cb),
		Inputs(Streams{"input-topic2", "input-topic3"}, c, cb),
	)

	for _, topic := range []string{"input-topic", "input-topic2", "input-topic3"} {
		codec := g.codec(topic)
		ensure.DeepEqual(t, codec, c)
	}

}

func TestGroupGraph_callback(t *testing.T) {
	g := DefineGroup("group",
		Input("input-topic", c, cb),
		Inputs(Streams{"input-topic2", "input-topic3"}, c, cb),
	)

	for _, topic := range []string{"input-topic", "input-topic2", "input-topic3"} {
		callback := g.callback(topic)
		ensure.True(t, reflect.ValueOf(callback).Pointer() == reflect.ValueOf(cb).Pointer())
	}
}

func TestGroupGraph_getters(t *testing.T) {
	g := DefineGroup("group",
		Input("t1", c, cb),
		Input("t2", c, cb),
		Output("t3", c),
		Output("t4", c),
		Output("t5", c),
		Inputs(Streams{"t6", "t7"}, c, cb),
	)
	ensure.True(t, g.Group() == "group")
	ensure.True(t, len(g.InputStreams()) == 4)
	ensure.True(t, len(g.OutputStreams()) == 3)
	ensure.True(t, g.LoopStream() == nil)

	g = DefineGroup("group",
		Input("t1", c, cb),
		Input("t2", c, cb),
		Output("t3", c),
		Output("t4", c),
		Output("t5", c),
		Loop(c, cb),
	)
	ensure.True(t, len(g.InputStreams()) == 2)
	ensure.True(t, len(g.OutputStreams()) == 3)
	ensure.True(t, g.GroupTable() == nil)
	ensure.DeepEqual(t, g.LoopStream().Topic(), loopName("group"))

	g = DefineGroup("group",
		Input("t1", c, cb),
		Input("t2", c, cb),
		Output("t3", c),
		Output("t4", c),
		Output("t5", c),
		Loop(c, cb),
		Join("a1", c),
		Join("a2", c),
		Join("a3", c),
		Join("a4", c),
		Lookup("b1", c),
		Lookup("b2", c),
		Persist(c),
	)
	ensure.True(t, len(g.InputStreams()) == 2)
	ensure.True(t, len(g.OutputStreams()) == 3)
	ensure.True(t, len(g.JointTables()) == 4)
	ensure.True(t, len(g.LookupTables()) == 2)
	ensure.DeepEqual(t, g.GroupTable().Topic(), tableName("group"))
}

func TestGroupGraph_Inputs(t *testing.T) {

	topics := Inputs(Streams{"a", "b", "c"}, c, cb)
	ensure.DeepEqual(t, topics.Topic(), "a,b,c")
	ensure.True(t, strings.Contains(topics.String(), "a,b,c/*codec.String"))
}
