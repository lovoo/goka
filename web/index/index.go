package index

import (
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/web/templates"
)

var baseTemplates = append(templates.BaseTemplates) // "web/templates/monitor/menu.go.html"

type component struct {
	Name     string
	BasePath string
}

type Server struct {
	log logger.Logger
	m   sync.RWMutex

	basePath   string
	components []*component
}

type ComponentPathProvider interface {
	BasePath() string
}

func NewServer(basePath string, router *mux.Router) *Server {
	srv := &Server{
		log:      logger.Default(),
		basePath: basePath,
	}

	sub := router.PathPrefix(basePath).Subrouter()
	sub.HandleFunc("/", srv.index)

	return srv
}

func (s *Server) AddComponent(pathProvider ComponentPathProvider, name string) {
	s.components = append(s.components, &component{
		Name:     name,
		BasePath: pathProvider.BasePath(),
	})
}

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	tmpl, err := templates.LoadTemplates(append(baseTemplates, "web/templates/index/index.go.html")...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	params := map[string]interface{}{
		"basePath":   s.basePath,
		"components": s.components,
	}

	if err := tmpl.Execute(w, params); err != nil {
		s.log.Printf("error rendering index template: %v", err)
	}
}
