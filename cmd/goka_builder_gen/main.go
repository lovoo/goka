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
// * all input edge's key types must be the same (does it?)
//
// Next steps
// * topic/codec registry
// * reuse topologies for each other
// * --> use the processor's topology in another processor which joins it etc.

var (
	// pkg       = flag.String("package", "", "package to generate builder from")
	typeNames     = flag.String("type", "", "comma-separated list of type names; must be set")
	output        = flag.String("output", "", "output file name; default srcdir/<type>_topology.go")
	outputPackage = flag.String("output-package", "", "package for the generated code. Defaults to the source package")
	extraImports  = flag.String("extra-imports", "", "comma-separated list of extra imports to add to the generated code")
)

// Usage is a replacement usage function for the flags package.
func Usage() {
	fmt.Fprintf(os.Stderr, "Usage ... TODO \n")
	flag.PrintDefaults()
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("goka builder generator: ")
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
		// reset buffer
		g.buf = bytes.Buffer{}

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

func (e *edgeDecl) edgeTypeName() string {
	return "typed." + e.edgeType
}

func (e *edgeDecl) edgeInitializerMethodName() string {
	switch e.edgeType {
	case "GInput":
		return fmt.Sprintf("Handle%s", e.name)
	case "GState":
		return fmt.Sprintf("WithPersist%s", e.name)
	case "GJoin":
		return fmt.Sprintf("WithJoin%s", e.name)
	case "GLookup":
		return fmt.Sprintf("WithLookup%s", e.name)
	case "GMappedLookup":
		return fmt.Sprintf("WithLookup%s", e.name)
	case "GOutput":
		return fmt.Sprintf("WithOutput%s", e.name)
	default:
		panic(fmt.Sprintf("unhandled edge type %s", e.name))
	}
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
}

type Package struct {
	name  string
	defs  map[*ast.Ident]types.Object
	files []*File
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
				log.Printf("found type: %s", typename)
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

						selector, ok := ex.X.(*ast.SelectorExpr)
						if !ok {
							// TODO handle
							log.Printf("unexpected type on field %#v: %T", field.Names[0], ex.X)
						}
						fieldType := selector.Sel

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
	pkg := g.pkg.name
	if *outputPackage != "" {
		pkg = *outputPackage
	}
	g.Printf(`package %s

// AUTO GENERATED DO NOT MODIFY!

import (
	"fmt"

	"github.com/lovoo/goka/typed"
 	"github.com/lovoo/goka"
)
`, pkg)

	for _, imp := range strings.Split(*extraImports, ",") {
		if imp != "" {
			g.Printf("import \"%s\"\n", imp)
		}
	}

	procBuilderName := fmt.Sprintf("%sProcessorBuilder", g.topo.name)

	ctxName := fmt.Sprintf("%sContext", g.topo.name)

	// render builder context and context-functions in parallel (using individual printers)
	ctxStructP := g.NewPrinter()
	ctxFactoryP := g.NewPrinter()
	ctxFunctionP := g.NewPrinter()
	ctxStructP.Printf("type %s struct{\n", ctxName)
	ctxStructP.Printf("goka.Context // goka context\n")

	ctxFactoryP.Printf("func (b*%s)newContext(ctx goka.Context)*%s{\n", procBuilderName, ctxName)
	ctxFactoryP.Printf("return &%s{\n", ctxName)
	ctxFactoryP.Printf("Context:ctx,\n")
	for _, edge := range g.topo.edges {
		valueType := edge.valueTypes[len(edge.valueTypes)-1]
		switch edge.edgeType {
		case "GOutput":
			ctxStructP.Printf("%s goka.Stream\n", edge.ctxTopicFieldName())

			ctxFactoryP.Printf("%s:goka.Stream(b.%s.TopicName()),\n", edge.ctxTopicFieldName(), edge.builderFieldName())

			ctxStructP.Printf("%sKeyCodec *typed.CodecBridge[%s]\n", edge.ctxTopicFieldName(), edge.keyType)
			ctxFactoryP.Printf("%sKeyCodec: typed.NewCodecBridge(b.%s.KeyCodec()),\n", edge.ctxTopicFieldName(), edge.builderFieldName())

			// emit function
			ctxFunctionP.PrintTemplate(`
			func (c*{{.ctxName}})Emit{{.edgeName}}(key {{.keyType}}, msg {{.valueType}}){
				encodedKey, err := c.{{.ctxTopicField}}KeyCodec.Encode(key)
				if err != nil{
					c.Fail(err)
				}
				c.Emit(c.{{.ctxTopicField}}, string(encodedKey), msg)
			}
			`,
				m{
					"ctxName":       ctxName,
					"edgeName":      edge.name,
					"keyType":       edge.keyType,
					"valueType":     valueType,
					"ctxTopicField": edge.ctxTopicFieldName(),
				})

		case "GLookup", "GMappedLookup":
			ctxStructP.Printf("%s goka.Table\n", edge.ctxTopicFieldName())
			ctxFactoryP.Printf("%s:goka.Table(b.%s.TopicName()),\n", edge.ctxTopicFieldName(), edge.builderFieldName())

			ctxStructP.Printf("%sKeyCodec *typed.CodecBridge[%s]\n", edge.ctxTopicFieldName(), edge.keyType)
			ctxFactoryP.Printf("%sKeyCodec: typed.NewCodecBridge(b.%s.KeyCodec()),\n", edge.ctxTopicFieldName(), edge.builderFieldName())

			ctxFunctionP.PrintTemplate(`
			func (c*{{.ctxName}})Lookup{{.edgeName}}(key {{.keyType}}) {{.valueType}}{
				encodedKey, err := c.{{.ctxTopicField}}KeyCodec.Encode(key)
				if err != nil{
					c.Fail(err)
				}
				if val := c.Lookup(c.{{.ctxTopicField}}, string(encodedKey)); val != nil{
					return val.({{.valueType}})
				}
				return nil
			}
			`,
				m{
					"ctxName":       ctxName,
					"edgeName":      edge.name,
					"keyType":       edge.keyType,
					"valueType":     valueType,
					"ctxTopicField": edge.ctxTopicFieldName(),
				})

		case "GJoin":
			ctxStructP.Printf("%s goka.Table\n", edge.ctxTopicFieldName())
			ctxFactoryP.Printf("%s:goka.Table(b.%s.TopicName()),\n", edge.ctxTopicFieldName(), edge.builderFieldName())

			ctxStructP.Printf("%sKeyCodec *typed.CodecBridge[%s]\n", edge.ctxTopicFieldName(), edge.keyType)
			ctxFactoryP.Printf("%sKeyCodec: typed.NewCodecBridge(b.%s.KeyCodec()),\n", edge.ctxTopicFieldName(), edge.builderFieldName())

			ctxFunctionP.PrintTemplate(`
			func (c*{{.ctxName}})Join{{.edgeName}}() {{.valueType}}{
				if val := c.Context.Join({{.ctxTopicField}}); val != nil{
					return val.({{.valueType}})
				}
				return nil
			}
			`,
				m{
					"ctxName":       ctxName,
					"edgeName":      edge.name,
					"keyType":       edge.keyType,
					"valueType":     valueType,
					"ctxTopicField": edge.ctxTopicFieldName(),
				})

		case "GState":

			ctxFunctionP.PrintTemplate(`
			func (c*{{.ctxName}})SetValue(value {{.valueType}}){
				c.Context.SetValue(value)
			}

			func (c*{{.ctxName}})Value() {{.valueType}}{
				if val := c.Context.Value(); val != nil{
					return val.({{.valueType}})
				}
				return nil
			}
			`,
				m{
					"ctxName":   ctxName,
					"valueType": valueType,
				})

		case "GInput":
			// not ctx-relevant
		default:
			log.Printf("warning: unhandled edge name when generating context: %s (type %s)", edge.name, edge.edgeType)
		}
	}
	ctxStructP.Printf("}\n")
	ctxFactoryP.Printf("}\n}\n")

	ctxStructP.write()
	ctxFactoryP.write()
	ctxFunctionP.write()

	// render processor builder struct
	g.Printf("type %s struct{\n", procBuilderName)
	g.Printf("group goka.Group\n")
	g.Printf("edges []goka.Edge\n")
	for _, edge := range g.topo.edges {
		g.Printf("%s %s[%s,%s]\n", edge.builderFieldName(), edge.edgeTypeName(), edge.keyType, strings.Join(edge.valueTypes, ","))
	}
	g.Printf("}\n")

	// builder constructor
	g.Printf("func New%s(group goka.Group", makeExported(procBuilderName))
	// for _, edge := range g.topo.edges {
	// 	g.Printf("%s %s[%s,%s],\n", edge.builderFieldName(), edge.edgeTypeName(), edge.keyType, strings.Join(edge.valueTypes, ","))
	// }
	g.Printf(")*%s{\n", procBuilderName)
	g.Printf("return &%s{", procBuilderName)
	g.Printf("group:group,\n")
	// for _, edge := range g.topo.edges {
	// 	g.Printf("%s:%s,\n", edge.builderFieldName(), edge.builderFieldName())
	// }
	g.Printf("}\n")
	g.Printf("}\n")

	// TODO: rename
	inputPrinter := g.NewPrinter()
	for _, edge := range g.topo.edges {
		edgeName := edge.name
		valueType := edge.valueTypes[len(edge.valueTypes)-1]
		edgeInitializerName := edge.edgeInitializerMethodName()
		switch edge.edgeType {
		case "GInput":
			err := inputPrinter.PrintTemplate(`
			func (p*{{.builder}}) {{.edgeInitializerName}}(topic goka.Stream, keyCodec typed.GCodec[{{.keyType}}], valueCodec typed.GCodec[{{.valueType}}], handler func(ctx *{{.ctxName}},key {{.keyType}}, msg {{.valueType}}), opts... typed.GOption) *{{.builder}} {
				codecBridge := typed.NewCodecBridge(valueCodec)
				p.{{.edgeField}} = typed.DefineInput(topic, keyCodec, valueCodec)
				edge := goka.Input(topic, codecBridge, func(ctx goka.Context, _msg interface{}) {

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
				return p
			}
			`, m{
				"builder":             procBuilderName,
				"edgeName":            edgeName,
				"ctxName":             ctxName,
				"keyType":             edge.keyType,
				"valueType":           valueType,
				"edgeField":           edge.builderFieldName(),
				"edgeInitializerName": edgeInitializerName,
			})
			if err != nil {
				return err
			}
		case "GState":
			err := inputPrinter.PrintTemplate(`
			func (p*{{.builder}}) {{.edgeInitializerName}}(keyCodec typed.GCodec[{{.keyType}}], valueCodec typed.GCodec[{{.valueType}}], opts... typed.GOption) *{{.builder}} {
				p.{{.edgeField}} = typed.DefineState(keyCodec, valueCodec)
				return p
			}
			`, m{
				"builder":             procBuilderName,
				"edgeName":            edgeName,
				"keyType":             edge.keyType,
				"valueType":           valueType,
				"edgeField":           edge.builderFieldName(),
				"edgeInitializerName": edgeInitializerName,
			})
			if err != nil {
				return err
			}
		case "GJoin":
			err := inputPrinter.PrintTemplate(`
			func (p*{{.builder}}) {{.edgeInitializerName}}(topic goka.Stream, keyCodec typed.GCodec[{{.keyType}}], valueCodec typed.GCodec[{{.valueType}}], opts... typed.GOption) *{{.builder}} {
				p.{{.edgeField}} = typed.DefineJoin(topic, keyCodec, valueCodec)
				return p
			}
			`, m{
				"builder":             procBuilderName,
				"edgeName":            edgeName,
				"keyType":             edge.keyType,
				"valueType":           valueType,
				"edgeField":           edge.builderFieldName(),
				"edgeInitializerName": edgeInitializerName,
			})
			if err != nil {
				return err
			}
		case "GLookup":
			err := inputPrinter.PrintTemplate(`
			func (p*{{.builder}}) {{.edgeInitializerName}}(table goka.Table, keyCodec typed.GCodec[{{.keyType}}], valueCodec typed.GCodec[{{.valueType}}], opts... typed.GOption) *{{.builder}} {
			
				p.{{.edgeField}} = typed.DefineLookup(table, keyCodec, valueCodec)
				return p
			}
			`, m{
				"builder":             procBuilderName,
				"edgeName":            edgeName,
				"keyType":             edge.keyType,
				"valueType":           valueType,
				"edgeField":           edge.builderFieldName(),
				"edgeInitializerName": edgeInitializerName,
			})
			if err != nil {
				return err
			}
		case "GMappedLookup":
			log.Printf("mapped lookup is not supported yet")
			err := inputPrinter.PrintTemplate(`
			func (p*{{.builder}}) {{.edgeInitializerName}}(table goka.Table, keyCodec typed.GCodec[{{.keyType}}], valueCodec typed.GCodec[{{.valueType}}], opts... typed.GOption) *{{.builder}} {
				p.{{.edgeField}} = typed.DefineLookup(table, keyCodec, valueCodec)
				return p
			}
			`, m{
				"builder":             procBuilderName,
				"edgeName":            edgeName,
				"keyType":             edge.keyType,
				"valueType":           valueType,
				"edgeField":           edge.builderFieldName(),
				"edgeInitializerName": edgeInitializerName,
			})
			if err != nil {
				return err
			}
		case "GOutput":
			err := inputPrinter.PrintTemplate(`
			func (p*{{.builder}}) {{.edgeInitializerName}}(topic goka.Stream, keyCodec typed.GCodec[{{.keyType}}], valueCodec typed.GCodec[{{.valueType}}], opts... typed.GOption) *{{.builder}} {
				p.{{.edgeField}} = typed.DefineOutput(topic, keyCodec, valueCodec)
				return p
			}
			`, m{
				"builder":             procBuilderName,
				"edgeName":            edgeName,
				"keyType":             edge.keyType,
				"valueType":           valueType,
				"edgeField":           edge.builderFieldName(),
				"edgeInitializerName": edgeInitializerName,
			})
			if err != nil {
				return err
			}
		}
	}
	inputPrinter.write()

	g.Printf("func (p*%s)Build()(*goka.GroupGraph, error){\n", procBuilderName)
	// check if all edges are not nil
	for _, edge := range g.topo.edges {
		g.Printf("if p.%s == nil{\n", edge.builderFieldName())
		g.Printf("return nil, fmt.Errorf(\"Uninitialized graph edge '%s'. Did you call '%s(...)'?\")\n", edge.name, edge.edgeInitializerMethodName())
		g.Printf("}\n")
	}

	g.Printf("var edges []goka.Edge\n")
	for _, edge := range g.topo.edges {
		switch edge.edgeType {
		case "GState":
			// set up the codec-bridges and add a "Persist"-edge
			// follow the pattern with all other edge types that need edge-initialization
			g.Printf("edges = append(edges, goka.Persist(typed.NewCodecBridge(p.%s.ValueCodec())))\n", edge.builderFieldName())

		case "GJoin":
			g.Printf("edges = append(edges, goka.Join(goka.Table(p.%s.TopicName()), typed.NewCodecBridge(p.%s.ValueCodec())))\n", edge.builderFieldName(), edge.builderFieldName())
		case "GLookup":
			g.Printf("edges = append(edges, goka.Lookup(goka.Table(p.%s.TopicName()), typed.NewCodecBridge(p.%s.ValueCodec())))\n", edge.builderFieldName(), edge.builderFieldName())
		case "GMappedLookup":
			log.Printf("mapped lookup is not supported yet")
			g.Printf("edges = append(edges, goka.Lookup(goka.Table(p.%s.TopicName()), typed.NewCodecBridge(p.%s.ValueCodec())))\n", edge.builderFieldName(), edge.builderFieldName())

		case "GOutput":
			g.Printf("edges = append(edges, goka.Output(goka.Stream(p.%s.TopicName()), typed.NewCodecBridge(p.%s.ValueCodec())))\n", edge.builderFieldName(), edge.builderFieldName())
		}
	}
	g.Printf("\n")
	g.Printf("return goka.DefineGroup(p.group, append(edges, p.edges...)...), nil\n")

	g.Printf("}\n")

	return nil
}

func makeExported(value string) string {
	return cases.Title(language.Und, cases.NoLower).String(value)
}

type m map[string]interface{}

func (p *printer) PrintTemplate(tpl string, values m) error {
	// valueMap := make(map[string]interface{}, len(values))

	// for _, value := range values {
	// 	valueMap[value.Key] = value.Value
	// }

	templateFuncs := template.FuncMap{}

	content := template.Must(template.New("").Funcs(templateFuncs).Parse(tpl))

	return content.Execute(&p.buf, values)
}
