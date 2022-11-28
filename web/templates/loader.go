package templates

import (
	"embed"
	"fmt"
	"path/filepath"
)

var (
	// DefaultLoader is the default loader to be used with Get.
	DefaultLoader = &EmbedLoader{}

	// templateFiles embeds all the files in the templates directory.
	//go:embed *
	templateFiles embed.FS
)

// Loader is an interface for a template loader.
type Loader interface {
	Get(filename string) (string, error)
}

// EmbedLoader loads templates from the templates directory.
type EmbedLoader struct{}

// NewEmbedLoader creates a new EmbedLoader.
func NewEmbedLoader() *EmbedLoader {
	return &EmbedLoader{}
}

// Get reads and returns the file's content.
func (l *EmbedLoader) Get(filename string) (string, error) {
	return Get(filename)
}

// Get reads and returns the file's content.
func Get(filename string) (string, error) {
	directory := filepath.Base(filepath.Dir(filename))
	file := filepath.Base(filename)
	fileBytes, err := templateFiles.ReadFile(directory + "/" + file)
	if err != nil {
		return "", fmt.Errorf("error reading and returning contents of embedded file: %v", err)
	}
	return string(fileBytes), nil
}
