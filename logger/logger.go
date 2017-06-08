package logger

import "log"

var (
	defaultLogger = &std{}
)

// Logger is the interface Goka and its subpackages use for logging.
type Logger interface {
	// Printf will be used for informational messages. These can be thought of
	// having an 'Info'-level in a structured logger.
	Printf(string, ...interface{})
	// Panicf will be only called an unexpected programming error such as a type
	// assertion which should never fail. Regular errors will be returned out
	// from the library.
	Panicf(string, ...interface{})
}

// std bridges the logger calls to the standard library log.
type std struct{}

func (s *std) Printf(msg string, args ...interface{}) {
	log.Printf(msg, args...)
}

func (s *std) Panicf(msg string, args ...interface{}) {
	log.Panicf(msg, args...)
}

// Default returns the standard library logger
func Default() Logger {
	return defaultLogger
}
