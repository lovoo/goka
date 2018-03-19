package goka

import "time"

// InputStats represents the number of messages and the number of bytes consumed
// from a stream or table topic since the process started.
type InputStats struct {
	Count uint
	Bytes int
	Delay time.Duration
}

// OutputStats represents the number of messages and the number of bytes emitted
// into a stream or table since the process started.
type OutputStats struct {
	Count uint
	Bytes int
}

// PartitionStatus is the status of the partition of a table (group table or joined table).
type PartitionStatus int

const (
	// PartitionRecovering indicates the partition is recovering and the storage
	// is writing updates in bulk-mode (if the storage implementation supports it).
	PartitionRecovering PartitionStatus = iota
	// PartitionPreparing indicates the end of the bulk-mode. Depending on the storage
	// implementation, the Preparing phase may take long because the storage compacts its logs.
	PartitionPreparing
	// PartitionRunning indicates the partition is recovered and processing updates
	// in normal operation.
	PartitionRunning
)

// PartitionStats represents metrics and measurements of a partition.
type PartitionStats struct {
	Now time.Time

	Table struct {
		Status  PartitionStatus
		Stalled bool

		Offset int64 // last offset processed or recovered
		Hwm    int64 // next offset to be written

		StartTime    time.Time
		RecoveryTime time.Time
	}
	Input  map[string]InputStats
	Output map[string]OutputStats
}

func newPartitionStats() *PartitionStats {
	return &PartitionStats{
		Now:    time.Now(),
		Input:  make(map[string]InputStats),
		Output: make(map[string]OutputStats),
	}
}

func (s *PartitionStats) init(o *PartitionStats, offset, hwm int64) *PartitionStats {
	s.Table.Status = o.Table.Status
	s.Table.Stalled = o.Table.Stalled
	s.Table.StartTime = o.Table.StartTime
	s.Table.RecoveryTime = o.Table.RecoveryTime
	s.Table.Offset = offset
	s.Table.Hwm = hwm
	s.Now = time.Now()
	for k, v := range o.Input {
		s.Input[k] = v
	}
	for k, v := range o.Output {
		s.Output[k] = v
	}
	return s
}

func (s *PartitionStats) reset() {
	s.Input = make(map[string]InputStats)
	s.Output = make(map[string]OutputStats)
}

// ViewStats represents the metrics of all partitions of a view.
type ViewStats struct {
	Partitions map[int32]*PartitionStats
}

func newViewStats() *ViewStats {
	return &ViewStats{
		Partitions: make(map[int32]*PartitionStats),
	}
}

// ProcessorStats represents the metrics of all partitions of the processor,
// including its group, joined tables and lookup tables.
type ProcessorStats struct {
	Group  map[int32]*PartitionStats
	Joined map[int32]map[string]*PartitionStats
	Lookup map[string]*ViewStats
}

func newProcessorStats(partitions int) *ProcessorStats {
	stats := &ProcessorStats{
		Group:  make(map[int32]*PartitionStats),
		Joined: make(map[int32]map[string]*PartitionStats),
		Lookup: make(map[string]*ViewStats),
	}

	for i := int32(0); i < int32(partitions); i++ {
		stats.Joined[i] = make(map[string]*PartitionStats)
	}
	return stats
}
