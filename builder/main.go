package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/token"
	"log"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/packages"
)

// TODO
// validation: only one state
// * check what happens when fields are not exported
// * how are cross-package-generations handled?
// * document if key-types are different

var (
	pkg       = flag.String("package", "", "package to generate topology from")
	typeNames = flag.String("type", "", "comma-separated list of type names; must be set")
	output    = flag.String("output", "", "output file name; default srcdir/<type>_topology.go")
)

// Usage is a replacement usage function for the flags package.
func Usage() {
	fmt.Fprintf(os.Stderr, "Usage ... TODO \n")
	flag.PrintDefaults()
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("goka topology generator: ")
	flag.Usage = Usage
	flag.Parse()

	if len(*typeNames) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	types := strings.Split(*typeNames, ",")

	// We accept either one directory or a list of files. Which do we have?
	args := flag.Args()
	if len(args) == 0 {
		// Default: process whole package in current directory.
		args = []string{"."}
	}
	// Parse the package once.
	var dir string
	g := Generator2{}
	// TODO: what about tags?
	g.parsePackage(args, nil)

	// Run generate for each type.
	for _, typeName := range types {
		g.collect(typeName)

		outputName := *output
		if outputName == "" {
			baseName := fmt.Sprintf("%s_builder.go", typeName)
			outputName = filepath.Join(dir, strings.ToLower(baseName))
		}

		err := g.renderBuilder()
		if err != nil {
			log.Fatalf("error rendering: %v", err)
		}

		err = os.WriteFile(outputName, g.format(), 0o644)
		if err != nil {
			log.Fatalf("writing output: %s", err)
		}
	}
}

// isDirectory reports whether the named file is a directory.
func isDirectory(name string) bool {
	info, err := os.Stat(name)
	if err != nil {
		log.Fatal(err)
	}
	return info.IsDir()
}

type edgeDecl struct {
	name       string
	edgeType   string // define as enum
	typeName   string
	keyType    string
	valueTypes []string
}

type topoDecl struct {
	name  string
	edges []*edgeDecl
}

type Generator2 struct {
	pkg *Package

	buf  bytes.Buffer // Accumulated output.
	topo *topoDecl
}

// parsePackage analyzes the single package constructed from the patterns and tags.
// parsePackage exits if there is an error.
func (g *Generator2) parsePackage(patterns []string, tags []string) error {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedSyntax,
		// TODO: Need to think about constants inf test files. Maybe write type_string_test.go
		// in a separate pass? For later.
		Tests:      false,
		BuildFlags: []string{fmt.Sprintf("-tags=%s", strings.Join(tags, " "))},
	}
	pkgs, err := packages.Load(cfg, patterns...)
	if err != nil {
		return fmt.Errorf("error loading packages %v: %w", patterns, err)
	}
	if len(pkgs) != 1 {
		return fmt.Errorf(" %d packages found. Expected one", len(pkgs))
	}
	pkg := pkgs[0]
	g.pkg = &Package{
		name:  pkg.Name,
		defs:  pkg.TypesInfo.Defs,
		files: make([]*File, 0, len(pkg.Syntax)),
	}

	for _, file := range pkg.Syntax {
		g.pkg.files = append(g.pkg.files, &File{
			file: file,
			pkg:  g.pkg,
		})
	}
	return nil
}

func (g *Generator2) Printf(format string, args ...interface{}) {
	fmt.Fprintf(&g.buf, format, args...)
}

func (g *Generator2) addTopoField(fieldName string, typeName string, keyType string, valueTypes ...string) {
	edge := edgeDecl{
		edgeType:   typeName,
		name:       fieldName,
		typeName:   typeName,
		keyType:    keyType,
		valueTypes: valueTypes,
	}
	g.topo.edges = append(g.topo.edges, &edge)
}

func (g *Generator2) collect(typename string) error {
	g.topo = &topoDecl{
		name: typename,
	}
	visit := func(node ast.Node) bool {
		decl, ok := node.(*ast.GenDecl)
		if !ok || decl.Tok != token.TYPE {
			return true
		}

		for _, spec := range decl.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}

			if typeSpec.Name.String() == typename {

				structSpec, ok := typeSpec.Type.(*ast.StructType)
				if !ok {
					continue
				}

				for _, field := range structSpec.Fields.List {
					switch ex := field.Type.(type) {
					case *ast.IndexListExpr: // generic with multiple

						if len(ex.Indices) < 2 || len(ex.Indices) > 3 {
							log.Printf("unexpected number of type args. Should be 2 or 3 to support key/value and mapped values only")
						}

						fieldType, ok := ex.X.(*ast.Ident)
						if !ok {
							// handle
							log.Printf("unexpected type")
						}

						var values []string
						for _, ind := range ex.Indices {
							values = append(values, findType(ind))
						}

						g.addTopoField(field.Names[0].Name,
							fieldType.Name,
							values[0], values[1:]...)
					}
				}
			}
		}

		return false
	}

	for _, file := range g.pkg.files {
		ast.Inspect(file.file, visit)
	}
	return nil
}

func findType(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.StarExpr:
		return "*" + findType(t.X)
	case *ast.Ident:
		return t.Name
	case *ast.IndexListExpr:
		log.Printf("nested types are not supported")
	}
	return fmt.Sprintf("UNSUPPORTED %#v", expr)
}

// format returns the gofmt-ed contents of the Generator's buffer.
func (g *Generator2) format() []byte {
	src, err := format.Source(g.buf.Bytes())
	if err != nil {
		// Should never happen, but can arise when developing this code.
		// The user can compile the output to see the error.
		log.Printf("warning: internal error: invalid Go generated: %s", err)
		log.Printf("warning: compile the package to analyze the error")
		return g.buf.Bytes()
	}
	return src
}

func (g *Generator2) renderBuilder() error {
	g.Printf(`package goka

// AUTO GENERATED DO NOT MODIFY!
`)

	procBuilderName := fmt.Sprintf("%sProcessorBuilder", g.topo.name)
	ctxName := fmt.Sprintf("%sContext", g.topo.name)
	g.Printf("type %s struct{\n", procBuilderName)
	g.Printf("}\n")

	g.Printf("type %s interface{\n", ctxName)
	for _, edge := range g.topo.edges {
		edgeName := edge.name // cases.Title(language.Und).String(edge.name)
		switch edge.edgeType {
		case "GOutput":
			g.Printf("Emit%s(key %s, msg %s)\n", edgeName, edge.keyType, edge.valueTypes[0])
		case "GLookup":
			g.Printf("Lookup%s(key %s) %s\n", edgeName, edge.keyType, edge.valueTypes[len(edge.valueTypes)-1])
		case "GMappedLookup":
			g.Printf("Lookup%s(key %s) %s\n", edgeName, edge.keyType, edge.valueTypes[len(edge.valueTypes)-1])
		case "GJoin":
			g.Printf("Join%s() %s\n", edgeName, edge.valueTypes[0])
		case "GState":
			g.Printf("SetValue(value %s)\n", edge.valueTypes[0])
			g.Printf("Value()%s\n", edge.valueTypes[0])
		case "GInput":
			// not ctx-relevant
		default:
			log.Printf("warning: unhandled edge name when generating context: %s", edge.name)
		}
	}
	g.Printf("}\n")

	for _, edge := range g.topo.edges {
		edgeName := edge.name // cases.Title(language.Und).String(edge.name)
		if edge.edgeType == "GInput" {
			g.Printf("func (p*%s) Handle%s(handler func(ctx %s,key %s, msg %s)){\n", procBuilderName,
				edgeName,
				ctxName,
				edge.keyType,
				edge.valueTypes[len(edge.valueTypes)-1])
			g.Printf("}\n")
		}
	}
	return nil
}
