package query

import (
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/templates"

	"github.com/gorilla/mux"
)

const jsonIndent = "  "

// Marshaler takes an object in and writes into out it's human readable
// representation.
type Marshaler func(interface{}) (string, error)

// Server is a provides HTTP routes for querying the group table.
type Server struct {
	m sync.RWMutex

	basePath string
	loader   templates.Loader
	sources  map[string]goka.Getter
	marshal  Marshaler
}

// NewServer creates a server with the given options.
func NewServer(basePath string, router *mux.Router, marshal Marshaler) *Server {
	srv := &Server{
		basePath: basePath,
		loader:   &templates.BinLoader{},
		sources:  make(map[string]goka.Getter),
		marshal:  marshal,
	}

	sub := router.PathPrefix(basePath).Subrouter()
	sub.HandleFunc("/", srv.index)
	sub.HandleFunc("/{name}", srv.source)
	sub.HandleFunc("/{name}/{key:.*}", srv.key)

	return srv
}

func (s *Server) getter(name string) goka.Getter {
	s.m.RLock()
	defer s.m.RUnlock()
	return s.sources[name]
}

func (s *Server) exists(name string) bool {
	return s.getter(name) != nil
}

func (s *Server) sourceNames() []string {
	s.m.RLock()
	defer s.m.RUnlock()
	var names []string
	for name := range s.sources {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// AttachSource attaches a new source to the query server.
func (s *Server) AttachSource(name string, getter goka.Getter) error {
	s.m.Lock()
	defer s.m.Unlock()
	if _, exists := s.sources[name]; exists {
		return fmt.Errorf("source with name '%s' is already attached", name)
	}
	s.sources[name] = getter
	return nil
}

func (s *Server) executeQueryTemplate(w http.ResponseWriter, params map[string]interface{}) {
	tmpl, err := templates.LoadTemplates(append(templates.BaseTemplates, "templates/query/index.go.html")...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	params["menu_title"] = "menu title"
	params["base_path"] = s.basePath

	if err := tmpl.Execute(w, params); err != nil {
		log.Printf("error executing query template: %v", err)
	}
}

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	params := map[string]interface{}{
		"page_title": "Overview",
	}

	if names := s.sourceNames(); len(names) > 0 {
		params["selected_source"] = names[0]
		params["sources"] = names
	}

	s.executeQueryTemplate(w, params)
}

func (s *Server) source(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	names := s.sourceNames()

	params := map[string]interface{}{
		"page_title": "Overview",
	}

	// defaults for topics
	if len(names) > 0 {
		params["selected_source"] = names[0]
		params["sources"] = names
	}

	if !s.exists(name) {
		params["warning"] = fmt.Errorf("Source '%s' not found!", name)
		s.executeQueryTemplate(w, params)
		return
	}

	params["selected_source"] = name
	s.executeQueryTemplate(w, params)
}

func (s *Server) key(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	names := s.sourceNames()

	params := map[string]interface{}{
		"page_title": "Overview",
	}

	// defaults for topics
	if len(names) > 0 {
		params["selected_source"] = names[0]
		params["sources"] = names
	}

	if !s.exists(name) {
		params["error"] = fmt.Errorf("Source '%s' not found!", name)
		s.executeQueryTemplate(w, params)
		return
	}
	params["selected_source"] = name

	key := vars["key"]
	value, err := s.getter(name)(strings.TrimSpace(key))
	if err != nil {
		params["error"] = fmt.Errorf("error getting key: %v", err)
		s.executeQueryTemplate(w, params)
		return
	}
	params["key"] = key

	if value != nil {
		data, err := s.marshal(value)
		if err != nil {
			params["error"] = fmt.Errorf("error marshaling value: %v", err)
			s.executeQueryTemplate(w, params)
			return
		}
		params["value"] = data
	} else {
		params["warning"] = fmt.Errorf("Key '%s' not found!", key)
	}

	s.executeQueryTemplate(w, params)
}
