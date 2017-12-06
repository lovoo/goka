package goka

import "time"

// Input Streams/Table
type InputStats struct {
	Count uint
	Bytes int
	Delay time.Duration
}

// Output Streams/Table
type OutputStats struct {
	Count uint
	Bytes int
}

type PartitionStats struct {
	Now time.Time

	Table struct {
		Recovered bool
		Stalled   bool

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
		Input:  make(map[string]InputStats),
		Output: make(map[string]OutputStats),
	}
}

func (s *PartitionStats) copy(o *PartitionStats) {
	s.Now = o.Now
	s.Table.Recovered = o.Table.Recovered
	s.Table.Stalled = o.Table.Stalled
	s.Table.StartTime = o.Table.StartTime
	s.Table.RecoveryTime = o.Table.RecoveryTime
	for k, v := range o.Input {
		s.Input[k] = v
	}
	for k, v := range o.Output {
		s.Output[k] = v
	}
}

func (s *PartitionStats) reset() {
	s.Input = make(map[string]InputStats)
	s.Output = make(map[string]OutputStats)
}

type ViewStats struct {
	Partitions map[int32]*PartitionStats
}

func newViewStats() *ViewStats {
	return &ViewStats{
		Partitions: make(map[int32]*PartitionStats),
	}
}

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
