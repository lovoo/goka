package templates

import "fmt"

// DefaultLoader is the default loader to be used with Get when it is not called
// on a loader.
var DefaultLoader = &BinLoader{}

// Loader is an interface for a template loader.
type Loader interface {
	Get(filename string) (string, error)
}

// BinLoader is for loading templates from bindata assets.
type BinLoader struct{}

// NewBinLoader creates a new BinLoader.
func NewBinLoader() *BinLoader {
	return &BinLoader{}
}

// Get retrieves a template by filename.
func (l *BinLoader) Get(filename string) (string, error) {
	return Get(filename)
}

// Get retrieves a template by filename.
func Get(filename string) (string, error) {
	data, err := Asset(filename)
	if err != nil {
		return "", fmt.Errorf("error loading bindata template: %v", err)
	}

	return string(data), nil
}
