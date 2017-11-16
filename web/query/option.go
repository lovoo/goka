package query

import "github.com/lovoo/goka/logger"

// Option is a function that applies a configuration to the server.
type Option func(s *Server)

// WithHumanizer sets the humanizer to use for the queried values. By default,
// the value will be printed out as JSON.
func WithHumanizer(h Humanizer) Option {
	return func(s *Server) {
		s.humanizer = h
	}
}

// WithLogger sets the logger to use. By default, it logs to the standard
// library logger.
func WithLogger(l logger.Logger) Option {
	return func(s *Server) {
		s.log = l
	}
}
