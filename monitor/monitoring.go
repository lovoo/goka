package monitor

import (
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/templates"

	"net/http"

	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"
)

var baseTemplates = append(templates.BaseTemplates, "templates/monitor/menu.go.html")

// Server is the main type used by client sot interact with the monitoring
// functionality of goka.
type Server struct {
	log logger.Logger
	m   sync.RWMutex

	basePath string
	procs    []*processorStats
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

// processorStats represents the details and statistics of a processor.
type processorStats struct {
	ID       string
	ClientID string
	metrics  metrics.Registry
}

// AttachProcessor attaches a processor to the monitor.
func (s *Server) AttachProcessor(clientID string, metrics metrics.Registry) {
	s.attach("processor-"+clientID, metrics)
}

// AttachView attaches a processor to the monitor.
func (s *Server) AttachView(clientID string, metrics metrics.Registry) {
	s.attach("view-"+clientID, metrics)
}

func (s *Server) attach(clientID string, metrics metrics.Registry) {
	s.m.Lock()
	defer s.m.Unlock()
	s.procs = append(s.procs, &processorStats{
		ID:       fmt.Sprintf("%d", len(s.procs)),
		ClientID: clientID,
		metrics:  metrics,
	})
}

// index page: all processors
func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	tmpl, err := templates.LoadTemplates(append(baseTemplates, "templates/monitor/index.go.html")...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	params := map[string]interface{}{
		"base_path":  s.basePath,
		"page_title": "Monitor",
		"menu_title": "Index",
		"processors": s.procs,
	}

	if err := tmpl.Execute(w, params); err != nil {
		s.log.Printf("error rendering index template: %v", err)
	}
}

func (s *Server) renderProcessorData(w http.ResponseWriter, r *http.Request) {
	s.m.RLock()
	defer s.m.RUnlock()

	cons, err := s.getProcessorByID(r)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	metrics.WriteJSONOnce(cons.metrics, w)
}

// renders the processor page
func (s *Server) renderProcessor(w http.ResponseWriter, r *http.Request) {
	tmpl, err := templates.LoadTemplates(append(baseTemplates, "templates/monitor/processor.go.html")...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	cons, err := s.getProcessorByID(r)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	params := map[string]interface{}{
		"base_path":     s.basePath,
		"page_title":    cons.ClientID,
		"menu_title":    "Menu Title",
		"processorName": cons.ClientID,
		"processorID":   cons.ID,
	}

	if err = tmpl.Execute(w, params); err != nil {
		s.log.Printf("error rendering processor view: %v", err)
	}
}

func (s *Server) getProcessorByID(r *http.Request) (*processorStats, error) {
	vars := mux.Vars(r)
	consID, exists := vars["processorId"]
	if !exists {
		return nil, errors.New("no processorId passed")
	}
	consIDParsed, err := strconv.Atoi(consID)
	if err != nil {
		return nil, errors.New("error parsing processorId from request")
	}

	if consIDParsed < 0 || consIDParsed >= len(s.procs) {
		return nil, errors.New("processor ID out of range")
	}

	return s.procs[consIDParsed], nil
}
