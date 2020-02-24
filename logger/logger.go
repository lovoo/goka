package logger

import (
	"fmt"
	"log"
	"strings"
)

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

	// PrefixedLogger returns a logger that prefixes all messages with passed prefix
	Prefix(string) Logger
}

// std bridges the logger calls to the standard library log.
type std struct {
	prefixPath []string
	prefix     string
}

func (s *std) Printf(msg string, args ...interface{}) {
	log.Printf(fmt.Sprintf("%s%s", s.prefix, msg), args...)
}

func (s *std) Panicf(msg string, args ...interface{}) {
	log.Panicf(fmt.Sprintf("%s%s", s.prefix, msg), args...)
}

func (s *std) Prefix(prefix string) Logger {
	return s.StackPrefix(prefix).(*std)
}

// Default returns the standard library logger
func Default() Logger {
	return defaultLogger
}

func EmptyPrefixer() Prefixer {
	return &std{}
}

type Prefixer interface {
	CurrentPrefix() string
	StackPrefix(prefix string) Prefixer
}

func (s *std) CurrentPrefix() string {
	return s.prefix
}
func (s *std) StackPrefix(prefix string) Prefixer {
	var prefPath []string
	// append existing path
	prefPath = append(prefPath, s.prefixPath...)

	// if new is not empty, append to path
	if prefix != "" {
		prefPath = append(prefPath, prefix)
	}

	// make new prefix
	newPrefix := strings.Join(prefPath, " > ")
	if newPrefix != "" {
		newPrefix = "[" + newPrefix + "] "
	}

	return &std{
		prefixPath: prefPath,
		prefix:     newPrefix,
	}
}
