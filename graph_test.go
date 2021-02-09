package goka

import (
	"reflect"
	"strings"
	"testing"

	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/internal/test"
)

var (
	c  = new(codec.String)
	cb = func(ctx Context, msg interface{}) {}
)

func TestGroupGraph_Validate(t *testing.T) {
	g := DefineGroup("group")
	err := g.Validate()
	test.AssertStringContains(t, err.Error(), "no input")

	g = DefineGroup("group",
		Input("input-topic", c, cb))
	err = g.Validate()
	test.AssertNil(t, err)

	g = DefineGroup("group",
		Input("input-topic", c, cb),
		Loop(c, cb),
		Loop(c, cb),
	)
	err = g.Validate()
	test.AssertStringContains(t, err.Error(), "more than one loop")

	g = DefineGroup("group",
		Input("input-topic", c, cb),
		Persist(c),
		Persist(c),
	)
	err = g.Validate()
	test.AssertStringContains(t, err.Error(), "more than one group table")

	g = DefineGroup("group",
		Input(Stream(tableName("group")), c, cb),
		Persist(c),
	)
	err = g.Validate()
	test.AssertStringContains(t, err.Error(), "group table")

	g = DefineGroup("group",
		Input(Stream(loopName("group")), c, cb),
		Loop(c, cb),
	)
	err = g.Validate()
	test.AssertStringContains(t, err.Error(), "loop stream")

	g = DefineGroup("group",
		Input("input-topic", c, cb),
		Join(Table(loopName("group")), c),
	)
	err = g.Validate()
	test.AssertStringContains(t, err.Error(), "loop stream")

	g = DefineGroup("group",
		Input("input-topic", c, cb),
		Output(Stream(loopName("group")), c),
	)
	err = g.Validate()
	test.AssertStringContains(t, err.Error(), "loop stream")

	g = DefineGroup("group",
		Input("input-topic", c, cb),
		Lookup(Table(loopName("group")), c),
	)
	err = g.Validate()
	test.AssertStringContains(t, err.Error(), "loop stream")
}

func TestGroupGraph_chainEdges(t *testing.T) {
	test.AssertEqual(t, len(chainEdges()), 0)
	test.AssertEqual(t, len(chainEdges(Edges{}, Edges{})), 0)
	test.AssertEqual(t, chainEdges(Edges{Join("a", nil)}, Edges{}), Edges{Join("a", nil)})
	test.AssertEqual(t, chainEdges(Edges{Join("a", nil)}, Edges{Join("a", nil), Join("b", nil)}), Edges{Join("a", nil), Join("a", nil), Join("b", nil)})
}

func TestGroupGraph_codec(t *testing.T) {
	g := DefineGroup("group",
		Input("input-topic", c, cb),
		Inputs(Streams{"input-topic2", "input-topic3"}, c, cb),
	)

	for _, topic := range []string{"input-topic", "input-topic2", "input-topic3"} {
		codec := g.codec(topic)
		test.AssertEqual(t, codec, c)
	}
}

func TestGroupGraph_callback(t *testing.T) {
	g := DefineGroup("group",
		Input("input-topic", c, cb),
		Inputs(Streams{"input-topic2", "input-topic3"}, c, cb),
	)

	for _, topic := range []string{"input-topic", "input-topic2", "input-topic3"} {
		callback := g.callback(topic)
		test.AssertTrue(t, reflect.ValueOf(callback).Pointer() == reflect.ValueOf(cb).Pointer())
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
	test.AssertTrue(t, g.Group() == "group")
	test.AssertTrue(t, len(g.InputStreams()) == 4)
	test.AssertTrue(t, len(g.OutputStreams()) == 3)
	test.AssertTrue(t, g.LoopStream() == nil)

	g = DefineGroup("group",
		Input("t1", c, cb),
		Input("t2", c, cb),
		Output("t3", c),
		Output("t4", c),
		Output("t5", c),
		Loop(c, cb),
	)
	test.AssertTrue(t, len(g.InputStreams()) == 2)
	test.AssertTrue(t, len(g.OutputStreams()) == 3)
	test.AssertTrue(t, g.GroupTable() == nil)
	test.AssertEqual(t, g.LoopStream().Topic(), loopName("group"))

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
	test.AssertTrue(t, len(g.InputStreams()) == 2)
	test.AssertTrue(t, len(g.OutputStreams()) == 3)
	test.AssertTrue(t, len(g.JointTables()) == 4)
	test.AssertTrue(t, len(g.LookupTables()) == 2)
	test.AssertEqual(t, g.GroupTable().Topic(), tableName("group"))
}

func TestGroupGraph_Inputs(t *testing.T) {

	topics := Inputs(Streams{"a", "b", "c"}, c, cb)
	test.AssertEqual(t, topics.Topic(), "a,b,c")
	test.AssertTrue(t, strings.Contains(topics.String(), "a,b,c/*codec.String"))
}

func TestStringsToStreams(t *testing.T) {
	inputTopics := []string{"input1",
		"input2",
	}

	streams := StringsToStreams(inputTopics...)
	test.AssertEqual(t, streams[0], Stream("input1"))
	test.AssertEqual(t, streams[1], Stream("input2"))
}

func ExampleStringsToStreams() {
	inputTopics := []string{"input1",
		"input2",
		"input3",
	}

	// use it, e.g. in the Inputs-Edge in the group graph
	graph := DefineGroup("group",
		Inputs(StringsToStreams(inputTopics...), new(codec.String), func(ctx Context, msg interface{}) {}),
	)
	_ = graph
}
