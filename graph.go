package goka

import (
	"errors"
	"fmt"
	"strings"
)

var (
	defaultTableSuffix = "-table"
	defaultLoopSuffix  = "-loop"

	tableSuffix = defaultTableSuffix
	loopSuffix  = defaultLoopSuffix
)

// SetTableSuffix changes `tableSuffix` which is a suffix for table topic.
// Use it to modify table's suffix to otherwise in case you cannot use the default suffix.
func SetTableSuffix(suffix string) {
	tableSuffix = suffix
}

// SetLoopSuffix changes `loopSuffix` which is a suffix for loop topic of group.
// Use it to modify loop topic's suffix to otherwise in case you cannot use the default suffix.
func SetLoopSuffix(suffix string) {
	loopSuffix = suffix
}

// ResetSuffixes reset both `loopSuffix` and `tableSuffix` to their default value.
// This function is helpful when there are multiple testcases, so you can do clean-up any change for suffixes.
func ResetSuffixes() {
	loopSuffix = defaultLoopSuffix
	tableSuffix = defaultTableSuffix
}

// Stream is the name of an event stream topic in Kafka, ie, a topic with
// cleanup.policy=delete
type Stream string

// Streams is a slice of Stream names.
type Streams []Stream

// Table is the name of a table topic in Kafka, ie, a topic with
// cleanup.policy=compact
type Table string

// Group is the name of a consumer group in Kafka and represents a processor
// group in Goka. A processor group may have a group table and a group loopback
// stream. By default, the group table is named <group>-table and the loopback
// stream <group>-loop.
type Group string

// GroupGraph is the specification of a processor group. It contains all input,
// output, and any other topic from which and into which the processor group
// may consume or produce events. Each of these links to Kafka is called Edge.
type GroupGraph struct {
	// the group marks multiple processor instances to be long together
	group string

	// the edges define the group graph
	inputTables   []Edge
	crossTables   []Edge
	inputStreams  []Edge
	outputStreams []Edge
	loopStream    []Edge
	groupTable    []Edge
	visitors      []Edge

	// those fields cache the info from above edges or are used to avoid naming/codec collisions
	codecs    map[string]Codec
	callbacks map[string]ProcessCallback

	outputStreamTopics map[Stream]struct{}

	joinCheck map[string]bool
}

// Group returns the group name.
func (gg *GroupGraph) Group() Group {
	return Group(gg.group)
}

// InputStreams returns all input stream edges of the group.
func (gg *GroupGraph) InputStreams() Edges {
	return gg.inputStreams
}

// JointTables retuns all joint table edges of the group.
func (gg *GroupGraph) JointTables() Edges {
	return gg.inputTables
}

// LookupTables retuns all lookup table edges  of the group.
func (gg *GroupGraph) LookupTables() Edges {
	return gg.crossTables
}

// LoopStream returns the loopback edge of the group.
func (gg *GroupGraph) LoopStream() Edge {
	// only 1 loop stream is valid
	if len(gg.loopStream) > 0 {
		return gg.loopStream[0]
	}
	return nil
}

// GroupTable returns the group table edge of the group.
func (gg *GroupGraph) GroupTable() Edge {
	// only 1 group table is valid
	if len(gg.groupTable) > 0 {
		return gg.groupTable[0]
	}
	return nil
}

// OutputStreams returns the output stream edges of the group.
func (gg *GroupGraph) OutputStreams() Edges {
	return gg.outputStreams
}

// AllEdges returns a list of all edges for the group graph.
// This allows to modify a graph by cloning it's edges into a new one.
//
//  var existing Graph
//  edges := existiting.AllEdges()
//  // modify edges as required
//  // recreate the modifiedg raph
//  newGraph := DefineGroup(existing.Groug(), edges...)
func (gg *GroupGraph) AllEdges() Edges {
	return chainEdges(
		gg.inputTables,
		gg.crossTables,
		gg.inputStreams,
		gg.outputStreams,
		gg.loopStream,
		gg.groupTable,
		gg.visitors)
}

// returns whether the passed topic is a valid group output topic
func (gg *GroupGraph) isOutputTopic(topic Stream) bool {
	_, ok := gg.outputStreamTopics[topic]
	return ok
}

// inputs returns all input topics (tables and streams)
func (gg *GroupGraph) inputs() Edges {
	return chainEdges(gg.inputStreams, gg.inputTables, gg.crossTables)
}

// copartitioned returns all copartitioned topics (joint tables and input streams)
func (gg *GroupGraph) copartitioned() Edges {
	return chainEdges(gg.inputStreams, gg.inputTables)
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

// DefineGroup creates a group graph with a given group name and a list of
// edges.
func DefineGroup(group Group, edges ...Edge) *GroupGraph {
	gg := GroupGraph{group: string(group),
		codecs:             make(map[string]Codec),
		callbacks:          make(map[string]ProcessCallback),
		joinCheck:          make(map[string]bool),
		outputStreamTopics: make(map[Stream]struct{}),
	}

	for _, e := range edges {
		switch e := e.(type) {
		case inputStreams:
			for _, input := range e {
				gg.validateInputTopic(input.Topic())
				inputStr := input.(*inputStream)
				gg.codecs[input.Topic()] = input.Codec()
				gg.callbacks[input.Topic()] = inputStr.cb
				gg.inputStreams = append(gg.inputStreams, inputStr)
			}
		case *inputStream:
			gg.validateInputTopic(e.Topic())
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
			gg.outputStreamTopics[Stream(e.Topic())] = struct{}{}
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
		case *visitor:
			gg.visitors = append(gg.visitors, e)
		}
	}

	return &gg
}

func (gg *GroupGraph) validateInputTopic(topic string) {
	if topic == "" {
		panic("Input topic cannot be empty. This will not work.")
	}

	if _, exists := gg.callbacks[topic]; exists {
		panic(fmt.Errorf("Callback for topic %s already exists. It is illegal to consume a topic twice", topic))
	}
}

// Validate validates the group graph and returns an error if invalid.
// Main validation checks are:
// - at most one loopback stream edge is allowed
// - at most one group table edge is allowed
// - at least one input stream is required
// - table and loopback topics cannot be used in any other edge.
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
	for _, t := range chainEdges(gg.outputStreams, gg.inputStreams, gg.inputTables, gg.crossTables) {
		if t.Topic() == loopName(gg.Group()) {
			return errors.New("should not directly use loop stream")
		}
		if t.Topic() == tableName(gg.Group()) {
			return errors.New("should not directly use group table")
		}
	}
	if len(gg.visitors) > 0 && len(gg.groupTable) == 0 {
		return fmt.Errorf("visitors cannot be used in a stateless processor")
	}
	return nil
}

// Edge represents a topic in Kafka and the corresponding codec to encode and
// decode the messages of that topic.
type Edge interface {
	String() string
	Topic() string
	Codec() Codec
}

// Edges is a slice of edge objects.
type Edges []Edge

// chainEdges chains edges together to avoid error-prone
// append(edges, moreEdges...) constructs in the graph
func chainEdges(edgeList ...Edges) Edges {
	var sum int
	for _, edges := range edgeList {
		sum += len(edges)
	}
	chained := make(Edges, 0, sum)

	for _, edges := range edgeList {
		chained = append(chained, edges...)
	}
	return chained
}

// Topics returns the names of the topics of the edges.
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

// Input represents an edge of an input stream topic. The edge
// specifies the topic name, its codec and the ProcessorCallback used to
// process it. The topic has to be copartitioned with any other input stream of
// the group and with the group table.
// The group starts reading the topic from the newest offset.
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

// Inputs creates edges of multiple input streams sharing the same
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

type visitor struct {
	name string
	cb   ProcessCallback
}

func (m *visitor) Topic() string {
	return m.name
}
func (m *visitor) Codec() Codec {
	return nil
}
func (m *visitor) String() string {
	return fmt.Sprintf("visitor %s", m.name)
}

// Visitor adds a visitor edge to the processor. This allows to iterate over the whole processor state
// while running. Note that this can block rebalance or processor shutdown.
// EXPERIMENTAL! This feature is not fully tested and might trigger unknown bugs. Be careful!
func Visitor(name string, cb ProcessCallback) Edge {
	return &visitor{
		name: name,
		cb:   cb,
	}
}

type loopStream inputStream

// Loop represents the edge of the loopback topic of the group. The edge
// specifies the codec of the messages in the topic and ProcesCallback to
// process the messages of the topic. Context.Loopback() is used to write
// messages into this topic from any callback of the group.
func Loop(c Codec, cb ProcessCallback) Edge {
	return &loopStream{&topicDef{codec: c}, cb}
}

func (s *loopStream) setGroup(group Group) {
	s.topicDef.name = loopName(group)
}

type inputTable struct {
	*topicDef
}

// Join represents an edge of a copartitioned, log-compacted table topic. The
// edge specifies the topic name and the codec of the messages of the topic.
// The group starts reading the topic from the oldest offset.
// The processing of input streams is blocked until all partitions of the table
// are recovered.
func Join(topic Table, c Codec) Edge {
	return &inputTable{&topicDef{string(topic), c}}
}

type crossTable struct {
	*topicDef
}

// Lookup represents an edge of a non-copartitioned, log-compacted table
// topic. The edge specifies the topic name and the codec of the messages of
// the topic.  The group starts reading the topic from the oldest offset.
// The processing of input streams is blocked until the table is fully
// recovered.
func Lookup(topic Table, c Codec) Edge {
	return &crossTable{&topicDef{string(topic), c}}
}

type groupTable struct {
	*topicDef
}

// Persist represents the edge of the group table, which is log-compacted and
// copartitioned with the input streams.
// Without Persist, calls to ctx.Value or ctx.SetValue in the consume callback will
// fail and lead to shutdown of the processor.
//
// This edge specifies the codec of the
// messages in the topic, ie, the codec of the values of the table.
// The processing of input streams is blocked until all partitions of the group
// table are recovered.
//
// The topic name is derived from the group name by appending "-table".
func Persist(c Codec) Edge {
	return &groupTable{&topicDef{codec: c}}
}

func (t *groupTable) setGroup(group Group) {
	t.topicDef.name = string(GroupTable(group))
}

type outputStream struct {
	*topicDef
}

// Output represents an edge of an output stream topic. The edge
// specifies the topic name and the codec of the messages of the topic.
// Context.Emit() only emits messages into Output edges defined in the group
// graph.
// The topic does not have to be copartitioned with the input streams.
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

// StringsToStreams is a simple cast/conversion functions that allows to pass a slice
// of strings as a slice of Stream (Streams)
// Avoids the boilerplate loop over the string array that would be necessary otherwise.
func StringsToStreams(strings ...string) Streams {
	streams := make(Streams, 0, len(strings))

	for _, str := range strings {
		streams = append(streams, Stream(str))
	}
	return streams
}
