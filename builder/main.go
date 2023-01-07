package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/token"
	"go/types"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/tools/go/packages"
)

// TODO
// validation: only one state
// * check what happens when fields are not exported
// * how are cross-package-generations handled?
// * validate if key-types are different
// * validate that we only have one state
// * all input edge's types must be the same (check that it really does)

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

func (e *edgeDecl) ctxTopicFieldName() string {
	return fmt.Sprintf("_%sTopic", e.name)
}

func (e *edgeDecl) builderFieldName() string {
	return fmt.Sprintf("_%s", e.name)
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

// File holds a single parsed file and associated data.
type File struct {
	pkg  *Package  // Package to which this file belongs.
	file *ast.File // Parsed AST.
	// These fields are reset for each type being generated.
	typeName string  // Name of the constant type.
	values   []Value // Accumulator for constant values of that type.

	trimPrefix  string
	lineComment bool
}

type Package struct {
	name  string
	defs  map[*ast.Ident]types.Object
	files []*File
}

// Value represents a declared constant.
type Value struct {
	originalName string // The name of the constant.
	name         string // The name with trimmed prefix.
	// The value is stored as a bit pattern alone. The boolean tells us
	// whether to interpret it as an int64 or a uint64; the only place
	// this matters is when sorting.
	// Much of the time the str field is all we need; it is printed
	// by Value.String.
	value  uint64 // Will be converted to int64 when needed.
	signed bool   // Whether the constant is a signed type.
	str    string // The string representation given by the "go/constant" package.
}

func (v *Value) String() string {
	return v.str
}

// byValue lets us sort the constants into increasing order.
// We take care in the Less method to sort in signed or unsigned order,
// as appropriate.
type byValue []Value

func (b byValue) Len() int      { return len(b) }
func (b byValue) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b byValue) Less(i, j int) bool {
	if b[i].signed {
		return int64(b[i].value) < int64(b[j].value)
	}
	return b[i].value < b[j].value
}

type printer struct {
	buf bytes.Buffer
	gen *Generator2
}

func (p *printer) Printf(format string, args ...interface{}) *printer {
	fmt.Fprintf(&p.buf, format, args...)
	return p
}

func (p *printer) write() {
	p.gen.Printf("%s", p.buf.String())
}

func (g *Generator2) NewPrinter() *printer {
	return &printer{gen: g}
}

func (g *Generator2) Printf(format string, args ...interface{}) {
	fmt.Fprintf(&g.buf, format, args...)
}

func (g *Generator2) PrintPrinters(ps ...*printer) {
	for _, p := range ps {
		p.write()
	}
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

	// render topology context and context-functions in parallel (using individual printers)
	ctxStructP := g.NewPrinter()
	ctxFactoryP := g.NewPrinter()
	ctxFunctionP := g.NewPrinter()
	ctxStructP.Printf("type %s struct{\n", ctxName)
	ctxStructP.Printf("ctx Context // goka context\n")

	ctxFactoryP.Printf("func (b*%s)newContext(ctx Context)*%s{\n", procBuilderName, ctxName)
	ctxFactoryP.Printf("return &%s{\n", ctxName)
	ctxFactoryP.Printf("ctx:ctx,\n")
	for _, edge := range g.topo.edges {
		edgeName := edge.name
		valueType := edge.valueTypes[len(edge.valueTypes)-1]
		switch edge.edgeType {
		case "GOutput":
			ctxStructP.Printf("%s Stream\n", edge.ctxTopicFieldName())
			ctxFactoryP.Printf("%s:Stream(b.%s.topicName()),\n", edge.ctxTopicFieldName(), edge.builderFieldName())

			// emit function
			ctxFunctionP.Printf("func (c*%s)Emit%s(key %s, msg %s){\n", ctxName, edgeName, edge.keyType, valueType)
			ctxFunctionP.Printf("c.ctx.Emit(c.%s, key, msg)\n", edge.ctxTopicFieldName())
			ctxFunctionP.Printf("}\n")

		case "GLookup", "GMappedLookup":
			ctxStructP.Printf("%s Table\n", edge.ctxTopicFieldName())
			ctxFactoryP.Printf("%s:Table(b.%s.topicName()),\n", edge.ctxTopicFieldName(), edge.builderFieldName())

			ctxFunctionP.Printf("func (c*%s)Lookup%s(key %s) %s{\n", ctxName, edgeName, edge.keyType, edge.valueTypes[len(edge.valueTypes)-1])
			ctxFunctionP.Printf("if val := c.ctx.Lookup(c.%s, key); val != nil{\n", edge.ctxTopicFieldName())
			ctxFunctionP.Printf("return val.(%s)\n", valueType)
			ctxFunctionP.Printf("}\n")
			ctxFunctionP.Printf("return nil\n")
			ctxFunctionP.Printf("}\n")
		case "GJoin":
			ctxStructP.Printf("%s Table\n", edge.ctxTopicFieldName())
			ctxFactoryP.Printf("%s:Table(b.%s.topicName()),\n", edge.ctxTopicFieldName(), edge.builderFieldName())

			ctxFunctionP.Printf("func (c*%s)Join%s() %s{\n", ctxName, edgeName, edge.valueTypes[0])
			ctxFunctionP.Printf("if val := c.ctx.Join(c.%s); val != nil{\n", edge.ctxTopicFieldName())
			ctxFunctionP.Printf("return val.(%s)\n", valueType)
			ctxFunctionP.Printf("}\n")
			ctxFunctionP.Printf("return nil\n")
			ctxFunctionP.Printf("}\n")

		case "GState":

			ctxFunctionP.Printf("func (c*%s)SetValue(value %s){\n", ctxName, edge.valueTypes[0])
			ctxFunctionP.Printf("c.ctx.SetValue(value)\n")
			ctxFunctionP.Printf("}\n")

			ctxFunctionP.Printf("func (c*%s)Value()%s{\n", ctxName, edge.valueTypes[0])
			ctxFunctionP.Printf("if val := c.ctx.Value(); val != nil{\n")
			ctxFunctionP.Printf("return val.(%s)\n", valueType)
			ctxFunctionP.Printf("}\n")
			ctxFunctionP.Printf("return nil\n")
			ctxFunctionP.Printf("}\n")
		case "GInput":
			// not ctx-relevant
		default:
			log.Printf("warning: unhandled edge name when generating context: %s", edge.name)
		}
	}
	ctxStructP.Printf("}\n")
	ctxFactoryP.Printf("}\n}\n")

	ctxStructP.write()
	ctxFactoryP.write()
	ctxFunctionP.write()

	// render processor builder struct
	g.Printf("type %s struct{\n", procBuilderName)
	g.Printf("edges []Edge\n")
	for _, edge := range g.topo.edges {
		g.Printf("%s %s[%s,%s]\n", edge.builderFieldName(), edge.edgeType, edge.keyType, strings.Join(edge.valueTypes, ","))
	}
	g.Printf("}\n")

	// builder constructor
	g.Printf("func New%s(", makeExported(procBuilderName))
	for _, edge := range g.topo.edges {
		g.Printf("%s %s[%s,%s],\n", edge.builderFieldName(), edge.edgeType, edge.keyType, strings.Join(edge.valueTypes, ","))
	}
	g.Printf(")*%s{\n", procBuilderName)
	g.Printf("return &%s{", procBuilderName)
	for _, edge := range g.topo.edges {
		g.Printf("%s:%s,\n", edge.builderFieldName(), edge.builderFieldName())
	}
	g.Printf("}\n")
	g.Printf("}\n")

	for _, edge := range g.topo.edges {
		edgeName := edge.name
		if edge.edgeType == "GInput" {
			valueType := edge.valueTypes[len(edge.valueTypes)-1]
			err := g.PrintTemplate(`
			func (p*{{.builder}}) On{{.edgeName}}(handler func(ctx *{{.ctxName}},key {{.keyType}}, msg {{.valueType}})){
			var (
				valCodec = p.{{.edgeField}}.valueCodec()
				keyCodec = p.{{.edgeField}}.keyCodec()
			)
		
			codecBridge := NewCodecBridge(valCodec)
			edge := Input(Stream(p.{{.edgeField}}.topicName()), codecBridge, func(ctx Context, _msg interface{}) {
				
				decKey, err := keyCodec.Decode([]byte(ctx.Key()))
				if err != nil {
					ctx.Fail(err)
				}
				var msg {{.valueType}}
				if _msg != nil {
					msg = _msg.({{.valueType}})
				}

				handler(p.newContext(ctx), decKey, msg)
			})
			p.edges = append(p.edges, edge)
			`, kv("builder", procBuilderName), kv("edgeName", edgeName),
				kv("ctxName", ctxName), kv("keyType", edge.keyType),
				kv("valueType", valueType), kv("edgeField", edge.builderFieldName()))
			if err != nil {
				return err
			}
			g.Printf("}\n")
		}
	}

	g.Printf("func (p*%s)Build(group Group)(*GroupGraph, error){\n", procBuilderName)
	g.Printf("var edges []Edge\n")
	for _, edge := range g.topo.edges {
		switch edge.edgeType {
		case "GState":
			// set up the codec-bridges and add a "Persist"-edge
			// follow the pattern with all other edge types that need edge-initialization
			g.Printf("edges = append(edges, Persist(NewCodecBridge(p.%s.valueCodec())))\n", edge.builderFieldName())

		case "GJoin":
			g.Printf("edges = append(edges, Join(Table(p.%s.topicName()), NewCodecBridge(p.%s.valueCodec())))\n", edge.builderFieldName(), edge.builderFieldName())
		case "GLookup":
			g.Printf("edges = append(edges, Lookup(Table(p.%s.topicName()), NewCodecBridge(p.%s.valueCodec())))\n", edge.builderFieldName(), edge.builderFieldName())
		case "GMappedLookup":
			log.Printf("mapped lookup is not supported yet")
			g.Printf("edges = append(edges, Lookup(Table(p.%s.topicName()), NewCodecBridge(p.%s.valueCodec())))\n", edge.builderFieldName(), edge.builderFieldName())

		case "GOutput":
			g.Printf("edges = append(edges, Output(Stream(p.%s.topicName()), NewCodecBridge(p.%s.valueCodec())))\n", edge.builderFieldName(), edge.builderFieldName())
		}
	}
	g.Printf("\n")
	g.Printf("return DefineGroup(group, append(edges, p.edges...)...), nil\n")

	g.Printf("}\n")

	return nil
}

func makeExported(value string) string {
	return cases.Title(language.Und, cases.NoLower).String(value)
}

type KV struct {
	Key   string
	Value interface{}
}

func kv(key string, value interface{}) *KV {
	return &KV{
		Key:   key,
		Value: value,
	}
}

func (g *Generator2) PrintTemplate(tpl string, values ...*KV) error {
	valueMap := make(map[string]interface{}, len(values))

	for _, value := range values {
		valueMap[value.Key] = value.Value
	}

	templateFuncs := template.FuncMap{}

	content := template.Must(template.New("").Funcs(templateFuncs).Parse(tpl))

	return content.Execute(&g.buf, valueMap)
}
