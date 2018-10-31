package monitor

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/web/templates"

	"net/http"

	"github.com/gorilla/mux"
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
	sub.HandleFunc("/processor/{idx}", srv.renderProcessor)
	sub.HandleFunc("/view/{idx}", srv.renderView)
	sub.HandleFunc("/data/{type}/{idx}", srv.renderData)

	return srv
}

func (s *Server) BasePath() string {
	return s.basePath
}

// processorStats represents the details and statistics of a processor.
type processorStats struct {
	ID       string
	ClientID string
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

func (s *Server) renderData(w http.ResponseWriter, r *http.Request) {
	s.m.RLock()
	defer s.m.RUnlock()

	var stats interface{}
	vars := mux.Vars(r)
	idx, err := strconv.Atoi(mux.Vars(r)["idx"])
	if err != nil {
		http.NotFound(w, r)
		return
	}
	switch vars["type"] {
	case "processor":

		if idx < 0 || idx > len(s.processors) {
			http.NotFound(w, r)
			return
		}
		stats = s.processors[idx].Stats()
	case "view":
		if idx < 0 || idx > len(s.views) {
			http.NotFound(w, r)
			return
		}
		stats = s.views[idx].Stats()
	default:
		w.Write([]byte("Invalid render type"))
		http.NotFound(w, r)
		return
	}
	marshalled, err := json.Marshal(stats)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(marshalled)
}

// renders the processor page
func (s *Server) renderProcessor(w http.ResponseWriter, r *http.Request) {
	tmpl, err := templates.LoadTemplates(append(baseTemplates, "web/templates/monitor/details.go.html")...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	idx, err := strconv.Atoi(mux.Vars(r)["idx"])
	if err != nil || idx < 0 || idx > len(s.processors) {
		http.NotFound(w, r)
		return
	}
	proc := s.processors[idx]

	params := map[string]interface{}{
		"base_path":  s.basePath,
		"page_title": fmt.Sprintf("Processor details for %s", proc.Graph().Group()),
		"title":      proc.Graph().Group(),
		"processors": s.processors,
		"views":      s.views,
		"vars":       mux.Vars(r),
		"renderType": "processor",
	}

	if err = tmpl.Execute(w, params); err != nil {
		s.log.Printf("error rendering processor details: %v", err)
	}
}

// renders the processor page
func (s *Server) renderView(w http.ResponseWriter, r *http.Request) {
	tmpl, err := templates.LoadTemplates(append(baseTemplates, "web/templates/monitor/details.go.html")...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	idx, err := strconv.Atoi(mux.Vars(r)["idx"])
	if err != nil || idx < 0 || idx > len(s.views) {
		http.NotFound(w, r)
		return
	}
	view := s.views[idx]

	params := map[string]interface{}{
		"base_path":  s.basePath,
		"page_title": fmt.Sprintf("View details for %s", view.Topic()),
		"title":      view.Topic(),
		"processors": s.processors,
		"views":      s.views,
		"vars":       mux.Vars(r),
		"renderType": "view",
	}

	if err = tmpl.Execute(w, params); err != nil {
		s.log.Printf("error rendering view details: %v", err)
	}
}
