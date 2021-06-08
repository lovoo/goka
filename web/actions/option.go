package actions

import "github.com/lovoo/goka"

// Option is a function that applies a configuration to the server.
type Option func(s *Server)

// WithLogger sets the logger to use. By default, it logs to the standard
// library logger.
func WithLogger(l goka.Logger) Option {
	return func(s *Server) {
		s.log = l
	}
}
