package templates

import (
	"embed"
	"fmt"
	"path/filepath"
)

var (
	// DefaultLoader is the default loader to be used with LoadTemplates.
	DefaultLoader = &EmbedLoader{}

	// templateFiles embeds all the files in the templates directory.
	//go:embed *
	templateFiles embed.FS
)

// Loader is an interface for a template loader.
type Loader interface {
	LoadTemplates(filename string) (string, error)
}

// EmbedLoader loads templates from the templates directory.
type EmbedLoader struct{}

// NewEmbedLoader creates a new EmbedLoader.
func NewEmbedLoader() *EmbedLoader {
	return &EmbedLoader{}
}

// LoadTemplates reads and returns the file's content.
func (l *EmbedLoader) LoadTemplates(filename string) (string, error) {
	directory := filepath.Base(filepath.Dir(filename))
	file := filepath.Base(filename)
	fileBytes, err := templateFiles.ReadFile(directory + "/" + file)
	if err != nil {
		return "", fmt.Errorf("error reading and returning contents of embedded file: %v", err)
	}
	return string(fileBytes), nil
}
