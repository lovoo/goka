package query

// Option is a function that applies a configuration to the server.
type Option func(s *Server)

// WithHumanizer sets the humanizer to use for the queried values. By default,
// the value will be printed out as JSON.
func WithHumanizer(h Humanizer) Option {
	return func(s *Server) {
		s.humanizer = h
	}
}
