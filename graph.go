package goka

import (
	"errors"
	"fmt"
	"strings"
)

var (
	tableSuffix = "-table"
	loopSuffix  = "-loop"
)

type Stream string
type Streams []Stream
type Table string
type Group string

type GroupGraph struct {
	group         string
	inputTables   []Edge
	crossTables   []Edge
	inputStreams  []Edge
	outputStreams []Edge
	loopStream    []Edge
	groupTable    []Edge

	codecs    map[string]Codec
	callbacks map[string]ProcessCallback

	joinCheck map[string]bool
}

func (gg *GroupGraph) Group() Group {
	return Group(gg.group)
}

func (gg *GroupGraph) InputStreams() Edges {
	return gg.inputStreams
}

func (gg *GroupGraph) JointTables() Edges {
	return gg.inputTables
}

func (gg *GroupGraph) LookupTables() Edges {
	return gg.crossTables
}

func (gg *GroupGraph) LoopStream() Edge {
	// only 1 loop stream is valid
	if len(gg.loopStream) > 0 {
		return gg.loopStream[0]
	}
	return nil
}

func (gg *GroupGraph) GroupTable() Edge {
	// only 1 group table is valid
	if len(gg.groupTable) > 0 {
		return gg.groupTable[0]
	}
	return nil
}

func (gg *GroupGraph) OutputStreams() Edges {
	return gg.outputStreams
}

// inputs returns all input topics (tables and streams)
func (gg *GroupGraph) inputs() Edges {
	return append(append(gg.inputStreams, gg.inputTables...), gg.crossTables...)
}

// copartitioned returns all copartitioned topics (joint tables and input streams)
func (gg *GroupGraph) copartitioned() Edges {
	return append(gg.inputStreams, gg.inputTables...)
}

func (gg *GroupGraph) codec(topic string) Codec {
	return gg.codecs[topic]
}

func (gg *GroupGraph) callback(topic string) ProcessCallback {
	return gg.callbacks[topic]
}

func (gg *GroupGraph) joint(topic string) bool {
	return gg.joinCheck[topic]
}

func DefineGroup(group Group, edges ...Edge) *GroupGraph {
	gg := GroupGraph{group: string(group),
		codecs:    make(map[string]Codec),
		callbacks: make(map[string]ProcessCallback),
		joinCheck: make(map[string]bool),
	}

	for _, e := range edges {
		switch e := e.(type) {
		case inputStreams:
			for _, input := range e {
				inputStr := input.(*inputStream)
				gg.codecs[input.Topic()] = input.Codec()
				gg.callbacks[input.Topic()] = inputStr.cb
				gg.inputStreams = append(gg.inputStreams, inputStr)
			}
		case *inputStream:
			gg.codecs[e.Topic()] = e.Codec()
			gg.callbacks[e.Topic()] = e.cb
			gg.inputStreams = append(gg.inputStreams, e)
		case *loopStream:
			e.setGroup(group)
			gg.codecs[e.Topic()] = e.Codec()
			gg.callbacks[e.Topic()] = e.cb
			gg.loopStream = append(gg.loopStream, e)
		case *outputStream:
			gg.codecs[e.Topic()] = e.Codec()
			gg.outputStreams = append(gg.outputStreams, e)
		case *inputTable:
			gg.codecs[e.Topic()] = e.Codec()
			gg.inputTables = append(gg.inputTables, e)
			gg.joinCheck[e.Topic()] = true
		case *crossTable:
			gg.codecs[e.Topic()] = e.Codec()
			gg.crossTables = append(gg.crossTables, e)
		case *groupTable:
			e.setGroup(group)
			gg.codecs[e.Topic()] = e.Codec()
			gg.groupTable = append(gg.groupTable, e)
		}
	}
	return &gg
}

func (gg *GroupGraph) Validate() error {
	if len(gg.loopStream) > 1 {
		return errors.New("more than one loop stream in group graph")
	}
	if len(gg.groupTable) > 1 {
		return errors.New("more than one group table in group graph")
	}
	if len(gg.inputStreams) == 0 {
		return errors.New("no input stream in group graph")
	}
	for _, t := range append(gg.outputStreams,
		append(gg.inputStreams, append(gg.inputTables, gg.crossTables...)...)...) {
		if t.Topic() == loopName(gg.Group()) {
			return errors.New("should not directly use loop stream")
		}
		if t.Topic() == tableName(gg.Group()) {
			return errors.New("should not directly use group table")
		}
	}
	return nil
}

type Edge interface {
	String() string
	Topic() string
	Codec() Codec
}

type Edges []Edge

func (e Edges) Topics() []string {
	var t []string
	for _, i := range e {
		t = append(t, i.Topic())
	}
	return t
}

type topicDef struct {
	name  string
	codec Codec
}

func (t *topicDef) Topic() string {
	return t.name
}

func (t *topicDef) String() string {
	return fmt.Sprintf("%s/%T", t.name, t.codec)
}

func (t *topicDef) Codec() Codec {
	return t.codec
}

type inputStream struct {
	*topicDef
	cb ProcessCallback
}

// Stream returns a subscription for a co-partitioned topic. The processor
// subscribing for a stream topic will start reading from the newest offset of
// the partition.
func Input(topic Stream, c Codec, cb ProcessCallback) Edge {
	return &inputStream{&topicDef{string(topic), c}, cb}
}

type inputStreams Edges

func (is inputStreams) String() string {
	if is == nil {
		return "empty input streams"
	}

	return fmt.Sprintf("input streams: %s/%T", is.Topic(), is.Codec())
}

func (is inputStreams) Topic() string {
	if is == nil {
		return ""
	}
	var topics []string

	for _, stream := range is {
		topics = append(topics, stream.Topic())
	}
	return strings.Join(topics, ",")
}

func (is inputStreams) Codec() Codec {
	if is == nil {
		return nil
	}
	return is[0].Codec()
}

// Inputs creates Edges for multiple input streams sharing the same
// codec and callback.
func Inputs(topics Streams, c Codec, cb ProcessCallback) Edge {
	if len(topics) == 0 {
		return nil
	}
	var edges Edges
	for _, topic := range topics {
		edges = append(edges, Input(topic, c, cb))
	}
	return inputStreams(edges)
}

type loopStream inputStream

// Loop defines a consume callback on the loop topic
func Loop(c Codec, cb ProcessCallback) Edge {
	return &loopStream{&topicDef{codec: c}, cb}
}

func (s *loopStream) setGroup(group Group) {
	s.topicDef.name = string(loopName(group))
}

type inputTable struct {
	*topicDef
}

// Table is one or more co-partitioned, log-compacted topic. The processor
// subscribing for a table topic will start reading from the oldest offset
// of the partition.
func Join(topic Table, c Codec) Edge {
	return &inputTable{&topicDef{string(topic), c}}
}

type crossTable struct {
	*topicDef
}

func Lookup(topic Table, c Codec) Edge {
	return &crossTable{&topicDef{string(topic), c}}
}

type groupTable struct {
	*topicDef
}

func Persist(c Codec) Edge {
	return &groupTable{&topicDef{codec: c}}
}

func (t *groupTable) setGroup(group Group) {
	t.topicDef.name = string(GroupTable(group))
}

type outputStream struct {
	*topicDef
}

func Output(topic Stream, c Codec) Edge {
	return &outputStream{&topicDef{string(topic), c}}
}

// GroupTable returns the name of the group table of group.
func GroupTable(group Group) Table {
	return Table(tableName(group))
}

func tableName(group Group) string {
	return string(group) + tableSuffix
}

// loopName returns the name of the loop topic of group.
func loopName(group Group) string {
	return string(group) + loopSuffix
}
