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

type partitionStats struct {
	Now time.Time

	Table struct {
		Recovered bool
		Stalled   bool

		Offset       int64 // last offset processed or recovered (group table)
		Hwm          int64 // next offset to be written (group table)
		StartTime    time.Time
		RecoveryTime time.Time
	}
	Input  InputStats
	Output OutputStats
}

func newStats() *partitionStats {
	return &partitionStats{
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
