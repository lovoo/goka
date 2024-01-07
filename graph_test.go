package goka

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/lovoo/goka/codec"
	"github.com/stretchr/testify/require"
)

var (
	c  = convertOrFakeCodec(new(codec.String))
	cb = func(ctx Context, msg interface{}) {}
)

func TestGroupGraph_Validate(t *testing.T) {
	g := DefineGroup("group")
	err := g.Validate()
	require.Contains(t, err.Error(), "no input")

	g = DefineGroup("group",
		Input("input-topic", c, cb))
	err = g.Validate()
	require.NoError(t, err)

	g = DefineGroup("group",
		Input("input-topic", c, cb),
		Loop(c, cb),
		Loop(c, cb),
	)
	err = g.Validate()
	require.Contains(t, err.Error(), "more than one loop")

	g = DefineGroup("group",
		Input("input-topic", c, cb),
		Persist(c),
		Persist(c),
	)
	err = g.Validate()
	require.Contains(t, err.Error(), "more than one group table")

	g = DefineGroup("group",
		Input(Stream(tableName("group")), c, cb),
		Persist(c),
	)
	err = g.Validate()
	require.Contains(t, err.Error(), "group table")

	g = DefineGroup("group",
		Input(Stream(loopName("group")), c, cb),
		Loop(c, cb),
	)
	err = g.Validate()
	require.Contains(t, err.Error(), "loop stream")

	g = DefineGroup("group",
		Input("input-topic", c, cb),
		Join(Table(loopName("group")), c),
	)
	err = g.Validate()
	require.Contains(t, err.Error(), "loop stream")

	g = DefineGroup("group",
		Input("input-topic", c, cb),
		Output(Stream(loopName("group")), c),
	)
	err = g.Validate()
	require.Contains(t, err.Error(), "loop stream")

	g = DefineGroup("group",
		Input("input-topic", c, cb),
		Lookup(Table(loopName("group")), c),
	)
	err = g.Validate()
	require.Contains(t, err.Error(), "loop stream")
}

func TestGroupGraph_chainEdges(t *testing.T) {
	require.Empty(t, chainEdges())
	require.Empty(t, chainEdges(Edges{}, Edges{}))
	require.Equal(t, Edges{Join("a", nil)}, chainEdges(Edges{Join("a", nil)}, Edges{}))
	require.Equal(t, Edges{Join("a", nil), Join("a", nil), Join("b", nil)}, chainEdges(Edges{Join("a", nil)}, Edges{Join("a", nil), Join("b", nil)}))
}

func TestGroupGraph_codec(t *testing.T) {
	g := DefineGroup("group",
		Input("input-topic", c, cb),
		Inputs(Streams{"input-topic2", "input-topic3"}, c, cb),
	)

	for _, topic := range []string{"input-topic", "input-topic2", "input-topic3"} {
		codec := g.codec(topic)
		require.Equal(t, c, codec)
	}
}

func TestGroupGraph_callback(t *testing.T) {
	g := DefineGroup("group",
		Input("input-topic", c, cb),
		Inputs(Streams{"input-topic2", "input-topic3"}, c, cb),
	)

	for _, topic := range []string{"input-topic", "input-topic2", "input-topic3"} {
		callback := g.callback(topic)
		require.True(t, reflect.ValueOf(callback).Pointer() == reflect.ValueOf(cb).Pointer())
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
	require.True(t, g.Group() == "group")
	require.True(t, len(g.InputStreams()) == 4)
	require.True(t, len(g.OutputStreams()) == 3)
	require.True(t, g.LoopStream() == nil)

	g = DefineGroup("group",
		Input("t1", c, cb),
		Input("t2", c, cb),
		Output("t3", c),
		Output("t4", c),
		Output("t5", c),
		Loop(c, cb),
	)
	require.True(t, len(g.InputStreams()) == 2)
	require.True(t, len(g.OutputStreams()) == 3)
	require.True(t, g.GroupTable() == nil)
	require.Equal(t, loopName("group"), g.LoopStream().Topic())

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
	require.True(t, len(g.InputStreams()) == 2)
	require.True(t, len(g.OutputStreams()) == 3)
	require.True(t, len(g.JointTables()) == 4)
	require.True(t, len(g.LookupTables()) == 2)
	require.Equal(t, tableName("group"), g.GroupTable().Topic())
}

func TestGroupGraph_Inputs(t *testing.T) {
	topics := Inputs(Streams{"a", "b", "c"}, c, cb)
	require.Equal(t, "a,b,c", topics.Topic())
	require.True(t, strings.Contains(topics.String(), "a,b,c/*codec.String"))
}

func TestGroupGraph_UpdateSuffixes(t *testing.T) {
	SetLoopSuffix("-loop1")
	SetTableSuffix("-table1")

	g := DefineGroup("group",
		Input("input-topic", c, cb),
		Persist(c),
		Persist(c),
	)

	require.Equal(t, "group-table1", tableName(g.Group()))
	require.Equal(t, "group-loop1", loopName(g.Group()))

	ResetSuffixes()

	require.Equal(t, fmt.Sprintf("group%s", defaultTableSuffix), tableName(g.Group()))
	require.Equal(t, fmt.Sprintf("group%s", defaultLoopSuffix), loopName(g.Group()))
}

func TestStringsToStreams(t *testing.T) {
	inputTopics := []string{
		"input1",
		"input2",
	}

	streams := StringsToStreams(inputTopics...)
	require.Equal(t, Stream("input1"), streams[0])
	require.Equal(t, Stream("input2"), streams[1])
}

func ExampleStringsToStreams() {
	inputTopics := []string{
		"input1",
		"input2",
		"input3",
	}

	// use it, e.g. in the Inputs-Edge in the group graph
	graph := DefineGroup("group",
		Inputs(StringsToStreams(inputTopics...), new(codec.String), func(ctx Context, msg interface{}) {}),
	)
	_ = graph
}
