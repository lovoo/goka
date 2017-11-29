package monitor

import (
	"encoding/json"
	"errors"
	"strconv"
	"sync"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/web/templates"

	"net/http"

	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"
)

var baseTemplates = append(templates.BaseTemplates, "web/templates/monitor/menu.go.html")

// Server is the main type used by client sot interact with the monitoring
// functionality of goka.
type Server struct {
	log logger.Logger
	m   sync.RWMutex

	basePath   string
	views      []*goka.View
	processors []*goka.Processor
}

// NewServer creates a new Server
func NewServer(basePath string, router *mux.Router, opts ...Option) *Server {
	srv := &Server{
		log:      logger.Default(),
		basePath: basePath,
	}

	for _, opt := range opts {
		opt(srv)
	}

	sub := router.PathPrefix(basePath).Subrouter()
	sub.HandleFunc("/", srv.index)
	sub.HandleFunc("/processor/{processorId}", srv.renderProcessor)
	sub.HandleFunc("/processordata/{processorId}", srv.renderProcessorData)

	return srv
}

func (s *Server) BasePath() string {
	return s.basePath
}

// processorStats represents the details and statistics of a processor.
type processorStats struct {
	ID       string
	ClientID string
	metrics  metrics.Registry
}

func (s *Server) AttachProcessor(processor *goka.Processor) {
	s.m.Lock()
	defer s.m.Unlock()
	s.processors = append(s.processors, processor)
}

// AttachView attaches a processor to the monitor.
func (s *Server) AttachView(view *goka.View) {
	s.m.Lock()
	defer s.m.Unlock()
	s.views = append(s.views, view)
}

// index page: all processors
func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	tmpl, err := templates.LoadTemplates(append(baseTemplates, "web/templates/monitor/index.go.html")...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	params := map[string]interface{}{
		"base_path":  s.basePath,
		"page_title": "Monitor",
		"menu_title": "Index",
		"processors": s.processors,
		"views":      s.views,
	}

	if err := tmpl.Execute(w, params); err != nil {
		s.log.Printf("error rendering index template: %v", err)
	}
}

func (s *Server) renderProcessorData(w http.ResponseWriter, r *http.Request) {
	s.m.RLock()
	defer s.m.RUnlock()

	proc, err := s.getProcessorByIdx(r)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	marshalled, err := json.Marshal(proc.Stats())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(marshalled)
}

// renders the processor page
func (s *Server) renderProcessor(w http.ResponseWriter, r *http.Request) {
	tmpl, err := templates.LoadTemplates(append(baseTemplates, "web/templates/monitor/processor.go.html")...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	proc, err := s.getProcessorByIdx(r)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	params := map[string]interface{}{
		"base_path":     s.basePath,
		"page_title":    proc.Graph().Group(),
		"processorName": proc.Graph().Group(),
		"graph":         proc.Graph(),
		"processors":    s.processors,
		"vars":          mux.Vars(r),
	}

	if err = tmpl.Execute(w, params); err != nil {
		s.log.Printf("error rendering processor view: %v", err)
	}
}

func (s *Server) getProcessorByIdx(r *http.Request) (*goka.Processor, error) {
	vars := mux.Vars(r)
	consID, exists := vars["processorId"]
	if !exists {
		return nil, errors.New("no processorId passed")
	}
	consIDParsed, err := strconv.Atoi(consID)
	if err != nil {
		return nil, errors.New("error parsing processorId from request")
	}

	if consIDParsed < 0 || consIDParsed >= len(s.processors) {
		return nil, errors.New("processor ID out of range")
	}

	return s.processors[consIDParsed], nil
}
