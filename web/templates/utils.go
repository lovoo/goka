package templates

import (
	"fmt"
	"html/template"
)

// BaseTemplates represents all partial templates used to build the base of the
// site.
var BaseTemplates = []string{
	"web/templates/common/base.go.html",
	"web/templates/common/head.go.html",
	"web/templates/common/menu.go.html",
}

// LoadTemplates loads and associates the given templates.
func LoadTemplates(filenames ...string) (*template.Template, error) {
	files := make(map[string]string)

	for _, name := range filenames {
		content, err := DefaultLoader.Get(name)
		if err != nil {
			return nil, fmt.Errorf("error retrieving %s: %v", name, err)
		}
		files[name] = content
	}

	tmpl := template.New("base")
	for name, content := range files {
		if _, err := tmpl.Parse(content); err != nil {
			return nil, fmt.Errorf("error parsing %s: %v", name, err)
		}
	}

	return tmpl, nil
}
