package goka

import "time"

// Input Streams/Table
type InputStats struct {
	Count map[string]uint
	Bytes map[string]int
	Delay map[string]time.Duration
}

// Output Streams/Table
type OutputStats struct {
	Count map[string]uint
	Bytes map[string]int
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
	Input  InputStats
	Output OutputStats
}

func newPartitionStats() *PartitionStats {
	return &PartitionStats{
		Input: InputStats{
			Count: make(map[string]uint),
			Bytes: make(map[string]int),
			Delay: make(map[string]time.Duration),
		},
		Output: OutputStats{
			Count: make(map[string]uint),
			Bytes: make(map[string]int),
		},
	}
}

func (s *PartitionStats) copy(o *PartitionStats) {
	s.Now = o.Now
	s.Table.Recovered = o.Table.Recovered
	s.Table.Stalled = o.Table.Stalled
	for k, v := range o.Input.Count {
		s.Input.Count[k] = v
	}
	for k, v := range o.Input.Bytes {
		s.Input.Bytes[k] = v
	}
	for k, v := range o.Input.Delay {
		s.Input.Delay[k] = v
	}
	for k, v := range o.Output.Count {
		s.Output.Count[k] = v
	}
	for k, v := range o.Output.Bytes {
		s.Output.Bytes[k] = v
	}
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
