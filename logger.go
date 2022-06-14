package goka

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
)

var defaultLogger = &std{
	log: log.New(os.Stderr, "", log.LstdFlags),
}

// Logger is the interface Goka and its subpackages use for logging.
type Logger interface {
	// Print will simply print the params
	Print(...interface{})

	// Print will simply print the params
	Println(...interface{})

	// Printf will be used for informational messages. These can be thought of
	// having an 'Info'-level in a structured logger.
	Printf(string, ...interface{})
}

type logger interface {
	Logger
	// Debugf is used for debugging messages, mostly for debugging goka itself.
	// It is turned off unless goka is initialized
	Debugf(string, ...interface{})
	// PrefixedLogger returns a logger that prefixes all messages with passed prefix
	Prefix(string) logger

	CurrentPrefix() string

	StackPrefix(prefix string) logger
}

// std bridges the logger calls to the standard library log.
type std struct {
	log        Logger
	debug      bool
	prefixPath []string
	prefix     string
}

func (s *std) Print(msgs ...interface{}) {
	s.log.Print(msgs...)
}

func (s *std) Println(msgs ...interface{}) {
	s.log.Print(msgs...)
}

func (s *std) Printf(msg string, args ...interface{}) {
	s.log.Printf(fmt.Sprintf("%s%s", s.prefix, msg), args...)
}

func (s *std) Debugf(msg string, args ...interface{}) {
	if s.debug {
		s.log.Printf(fmt.Sprintf("%s%s", s.prefix, msg), args...)
	}
}

func (s *std) Prefix(prefix string) logger {
	return s.StackPrefix(prefix).(*std)
}

// Default returns the standard library logger
func DefaultLogger() Logger {
	return defaultLogger
}

// Debug enables or disables debug logging using the global logger.
// The goka debugging setting is applied to any custom loggers in goka components (Processors, Views, Emitters).
func Debug(gokaDebug, saramaDebug bool) {
	defaultLogger.debug = gokaDebug
	if saramaDebug {
		SetSaramaLogger((&std{log: defaultLogger, debug: true}).Prefix("Sarama"))
	}
}

func SetSaramaLogger(logger Logger) {
	sarama.Logger = logger
}

// newLogger creates a new goka logger
func wrapLogger(l Logger, debug bool) logger {
	return &std{
		log:   l,
		debug: debug,
	}
}

func (s *std) CurrentPrefix() string {
	return s.prefix
}

func (s *std) StackPrefix(prefix string) logger {
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
		log:        s.log,
		prefixPath: prefPath,
		prefix:     newPrefix,
		debug:      s.debug,
	}
}
