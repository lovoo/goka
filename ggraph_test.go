package goka

import (
	"reflect"
	"testing"

	"github.com/facebookgo/ensure"
	"github.com/lovoo/goka/codec"
)

var c = new(codec.String)

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
		LoopStream(c, cb),
		LoopStream(c, cb),
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
		Input(GroupTable("group"), c, cb),
		Persist(c),
	)
	err = g.Validate()
	ensure.StringContains(t, err.Error(), "group table")

	g = DefineGroup("group",
		Input(loopName("group"), c, cb),
		LoopStream(c, cb),
	)
	err = g.Validate()
	ensure.StringContains(t, err.Error(), "loop stream")

	g = DefineGroup("group",
		Input("input-topic", c, cb),
		Join(loopName("group"), c),
	)
	err = g.Validate()
	ensure.StringContains(t, err.Error(), "loop stream")

	g = DefineGroup("group",
		Input("input-topic", c, cb),
		Output(loopName("group"), c),
	)
	err = g.Validate()
	ensure.StringContains(t, err.Error(), "loop stream")

	g = DefineGroup("group",
		Input("input-topic", c, cb),
		Lookup(loopName("group"), c),
	)
	err = g.Validate()
	ensure.StringContains(t, err.Error(), "loop stream")

}

func TestGroupGraph_codec(t *testing.T) {
	g := DefineGroup("group",
		Input("input-topic", c, cb))

	codec := g.codec("input-topic")
	ensure.DeepEqual(t, codec, c)
}

func TestGroupGraph_callback(t *testing.T) {
	g := DefineGroup("group",
		Input("input-topic", c, cb))

	callback := g.callback("input-topic")
	ensure.True(t, reflect.ValueOf(callback).Pointer() == reflect.ValueOf(cb).Pointer())
}

func TestGroupGraph_getters(t *testing.T) {
	g := DefineGroup("group",
		Input("t1", c, cb),
		Input("t2", c, cb),
		Output("t3", c),
		Output("t4", c),
		Output("t5", c),
	)
	ensure.True(t, g.Group() == "group")
	ensure.True(t, len(g.InputStreams()) == 2)
	ensure.True(t, len(g.OutputStreams()) == 3)
	ensure.True(t, g.getLoopStream() == nil)

	g = DefineGroup("group",
		Input("t1", c, cb),
		Input("t2", c, cb),
		Output("t3", c),
		Output("t4", c),
		Output("t5", c),
		LoopStream(c, cb),
	)
	ensure.True(t, len(g.InputStreams()) == 3)
	ensure.True(t, len(g.OutputStreams()) == 4)
	ensure.True(t, g.GroupTable() == nil)
	ensure.True(t, g.getLoopStream().Topic() == loopName("group"))

	g = DefineGroup("group",
		Input("t1", c, cb),
		Input("t2", c, cb),
		Output("t3", c),
		Output("t4", c),
		Output("t5", c),
		LoopStream(c, cb),
		Join("a1", c),
		Join("a2", c),
		Join("a3", c),
		Join("a4", c),
		Lookup("b1", c),
		Lookup("b2", c),
		Persist(c),
	)
	ensure.True(t, len(g.InputStreams()) == 3)
	ensure.True(t, len(g.OutputStreams()) == 4)
	ensure.True(t, len(g.JointTables()) == 4)
	ensure.True(t, len(g.LookupTables()) == 2)
	ensure.True(t, g.GroupTable().Topic() == GroupTable("group"))
}
