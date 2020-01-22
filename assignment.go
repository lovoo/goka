package goka

import (
	"fmt"
)

// Assignment represents a partition:offset assignment for the current connection
type Assignment map[int32]int64

func (a *Assignment) string() string {
	var am map[int32]int64 = *a
	return fmt.Sprintf("Assignment %v", am)
}
