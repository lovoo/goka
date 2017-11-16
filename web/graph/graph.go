package graph

import (
	"sync"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/web/templates"

	"net/http"

	"github.com/gorilla/mux"
)

var baseTemplates = append(templates.BaseTemplates)

// Server is the main type used by client sot interact with the monitoring
// functionality of goka.
type Server struct {
	log logger.Logger
	m   sync.RWMutex

	basePath string
	graphs   []*goka.GroupGraph
}

// NewServer creates a new Server
func NewServer(basePath string, router *mux.Router) *Server {
	srv := &Server{
		log:      logger.Default(),
		basePath: basePath,
	}

	sub := router.PathPrefix(basePath).Subrouter()
	sub.HandleFunc("/", srv.index)

	return srv
}

func (s *Server) BasePath() string {
	return s.basePath
}

// AttachView attaches a processor to the monitor.
func (s *Server) Attach(graph *goka.GroupGraph) {
	s.m.Lock()
	defer s.m.Unlock()
	s.graphs = append(s.graphs, graph)
}

// index page: all processors
func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	tmpl, err := templates.LoadTemplates(append(baseTemplates, "web/templates/graph/index.go.html")...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	params := map[string]interface{}{
		"base_path":  s.basePath,
		"page_title": "Topology",
		"graphs":     s.graphs,
	}

	if err := tmpl.Execute(w, params); err != nil {
		s.log.Printf("error rendering index template: %v", err)
	}
}
