package goka

import (
	"log"
	"time"
)

// PartitionStatus is the status of the partition of a table (group table or joined table).
type PartitionStatus int

const (
	// PartitionStopped indicates the partition stopped and should not be used anymore.
	PartitionStopped PartitionStatus = iota
	// PartitionInitializing indicates that the underlying storage is initializing (e.g. opening leveldb files),
	// and has not actually started working yet.
	PartitionInitializing
	// PartitionConnecting indicates the partition trying to (re-)connect to Kafka
	PartitionConnecting
	// PartitionRecovering indicates the partition is recovering and the storage
	// is writing updates in bulk-mode (if the storage implementation supports it).
	PartitionRecovering
	// PartitionPreparing indicates the end of the bulk-mode. Depending on the storage
	// implementation, the Preparing phase may take long because the storage compacts its logs.
	PartitionPreparing
	// PartitionRunning indicates the partition is recovered and processing updates
	// in normal operation.
	PartitionRunning
)

const (
	statsHwmUpdateInterval = 5 * time.Second
	fetchStatsTimeout      = 10 * time.Second
)

// InputStats represents the number of messages and the number of bytes consumed
// from a stream or table topic since the process started.
type InputStats struct {
	Count      uint
	Bytes      int
	OffsetLag  int64
	LastOffset int64
	Delay      time.Duration
}

// OutputStats represents the number of messages and the number of bytes emitted
// into a stream or table since the process started.
type OutputStats struct {
	Count uint
	Bytes int
}

// PartitionProcStats represents metrics and measurements of a partition processor
type PartitionProcStats struct {
	Now time.Time

	TableStats *TableStats

	Joined map[string]*TableStats

	Input  map[string]*InputStats
	Output map[string]*OutputStats
}

// RecoveryStats groups statistics during recovery
type RecoveryStats struct {
	StartTime    time.Time
	RecoveryTime time.Time

	Offset int64 // last offset processed or recovered
	Hwm    int64 // next offset to be written
}

// TableStats represents stats for a table partition
type TableStats struct {
	Stalled bool

	Status PartitionStatus

	RunMode PPRunMode

	Recovery *RecoveryStats

	Input  *InputStats
	Writes *OutputStats
}

func newInputStats() *InputStats {
	return &InputStats{}
}

func newOutputStats() *OutputStats {
	return &OutputStats{}
}

func (is *InputStats) clone() *InputStats {
	return &(*is)
}

func (os *OutputStats) clone() *OutputStats {
	return &(*os)
}

type inputStatsMap map[string]*InputStats
type outputStatsMap map[string]*OutputStats

func (isp inputStatsMap) clone() map[string]*InputStats {
	var c = map[string]*InputStats{}
	if isp == nil {
		return c
	}
	for k, v := range isp {
		c[k] = v.clone()
	}
	return c
}

func (osp outputStatsMap) clone() map[string]*OutputStats {
	var c = map[string]*OutputStats{}
	if osp == nil {
		return c
	}
	for k, v := range osp {
		c[k] = v.clone()
	}
	return c
}

func newRecoveryStats() *RecoveryStats {
	return new(RecoveryStats)
}

func (rs *RecoveryStats) clone() *RecoveryStats {
	var rsCopy = *rs
	return &rsCopy
}

func newPartitionProcStats(inputs []string, outputs []string) *PartitionProcStats {
	procStats := &PartitionProcStats{
		Now: time.Now(),

		Input:  make(map[string]*InputStats),
		Output: make(map[string]*OutputStats),
	}

	for _, input := range inputs {
		procStats.Input[input] = newInputStats()
	}

	for _, output := range outputs {
		procStats.Output[output] = newOutputStats()
	}

	return procStats
}

func newTableStats() *TableStats {
	return &TableStats{
		Input:    newInputStats(),
		Writes:   newOutputStats(),
		Recovery: newRecoveryStats(),
	}
}

func (ts *TableStats) reset() {
	ts.Input = newInputStats()
	ts.Writes = newOutputStats()
}

func (ts *TableStats) clone() *TableStats {
	return &TableStats{
		Input:    ts.Input.clone(),
		Writes:   ts.Writes.clone(),
		Recovery: ts.Recovery.clone(),
		Stalled:  ts.Stalled,
	}
}

func (s *PartitionProcStats) clone() *PartitionProcStats {
	pps := newPartitionProcStats(nil, nil)
	pps.Now = time.Now()
	pps.Joined = make(map[string]*TableStats)
	pps.Input = inputStatsMap(s.Input).clone()
	pps.Output = outputStatsMap(s.Output).clone()

	return pps
}

func (s *PartitionProcStats) trackOutput(topic string, valueLen int) {
	outStats := s.Output[topic]
	if outStats == nil {
		log.Printf("no out stats for topic %s", topic)
		return
	}
	outStats.Count++
	outStats.Bytes += valueLen
}

// ViewStats represents the metrics of all partitions of a view.
type ViewStats struct {
	Partitions map[int32]*TableStats
}

func newViewStats() *ViewStats {
	return &ViewStats{
		Partitions: make(map[int32]*TableStats),
	}
}

// ProcessorStats represents the metrics of all partitions of the processor,
// including its group, joined tables and lookup tables.
type ProcessorStats struct {
	Group  map[int32]*PartitionProcStats
	Lookup map[string]*ViewStats
}

func newProcessorStats(partitions int) *ProcessorStats {
	stats := &ProcessorStats{
		Group:  make(map[int32]*PartitionProcStats, partitions),
		Lookup: make(map[string]*ViewStats, partitions),
	}

	return stats
}
