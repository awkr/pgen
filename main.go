package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/go-yaml/yaml"
)

var model = flag.String("model", "", "model file")

func main() {
	flag.Parse()

	if *model == "" {
		return
	}

	exitIfErr(checkFile(*model))

	raw, err := readFile(*model)
	exitIfErr(err)

	// parse to get structured data
	p := parser{
		Data: &Metadata{
			Enums:  []*Enum{},
			Tables: []*Table{},
		},
	}
	exitIfErr(p.parse(raw))

	exitIfErr(render(p.Data, os.Stdout))
}

func exitIfErr(e error) {
	if e == nil {
		return
	}
	println(e.Error())
	os.Exit(1)
}

func checkFile(f string) error {
	if stat, err := os.Stat(f); os.IsNotExist(err) {
		return errors.New("error: model not found")
	} else if err != nil {
		return fmt.Errorf("unknown error: %s", err.Error())
	} else if stat.IsDir() {
		return errors.New("error: model should be a YAML file")
	}
	return nil
}

func readFile(path string) (yaml.MapSlice, error) {
	var raw yaml.MapSlice

	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if err = yaml.Unmarshal(b, &raw); err != nil {
		return nil, err
	}

	return raw, nil
}

type parser struct {
	Data *Metadata
}

type Metadata struct {
	Enums  []*Enum
	Tables []*Table
}

func (p *parser) parse(raw yaml.MapSlice) error {
	parse := func(raw yaml.MapSlice, f func(t string, s yaml.MapItem) error) error {
		for _, s := range raw {
			attrs := s.Value.(yaml.MapSlice)
			if len(attrs) == 0 {
				continue
			}

			if attrs[0].Key.(string) != "type" {
				return fmt.Errorf("%s: the first attribute must be 'type'", s.Key.(string))
			}

			if err := f(attrs[0].Value.(string), s); err != nil {
				return err
			}
		}
		return nil
	}

	// parse enum
	if err := parse(raw, func(t string, s yaml.MapItem) error {
		if t != "enum" {
			return nil
		}

		e, err := p.parseEnum(s)
		if err != nil {
			return err
		}
		p.Data.Enums = append(p.Data.Enums, e)

		return nil
	}); err != nil {
		return err
	}

	// after all other types parsed, parse table
	if err := parse(raw, func(t string, s yaml.MapItem) error {
		if t != "table" {
			return nil
		}

		table, err := p.parseTable(s)
		if err != nil {
			return err
		}
		p.Data.Tables = append(p.Data.Tables, table)

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (p *parser) parseEnum(s yaml.MapItem) (*Enum, error) {
	e := Enum{
		Name:   s.Key.(string),
		Values: []string{},
	}
	for _, attr := range s.Value.(yaml.MapSlice) {
		switch n := attr.Key.(string); n {
		case "comment":
			e.Comment = attr.Value.(string)
		case "value":
			if attr.Value == nil {
				return nil, fmt.Errorf("enum %s should have at least one value", e.Name)
			}

			for _, val := range attr.Value.([]interface{}) {
				e.Values = append(e.Values, val.(string))
			}
		}
	}
	return &e, nil
}

func (p *parser) parseTable(s yaml.MapItem) (*Table, error) {
	t := Table{
		Name: s.Key.(string),
	}
	for _, attr := range s.Value.(yaml.MapSlice) {
		switch n := attr.Key.(string); n {
		case "db":
			t.DB = attr.Value.(string)
		case "comment":
			t.Comment = attr.Value.(string)
		case "fields":
			if attr.Value == nil {
				continue
			}

			for _, field := range attr.Value.([]interface{}) {
				f, err := p.parseField(field)
				if err != nil {
					return nil, err
				}

				t.Fields = append(t.Fields, f)
			}
		case "uniques":
			t.Uniques = p.parseIndexes(attr.Value)
		case "indexes":
			t.Indexes = p.parseIndexes(attr.Value)
		}
	}

	// validation
	if t.DB == "" {
		return nil, fmt.Errorf("%s: db should be provided", t.Name)
	}

	return &t, nil
}

func (p *parser) parseIndexes(in interface{}) []Index {
	if in == nil {
		return nil
	}

	var indexes []Index
	for _, val := range in.([]interface{}) {
		var index Index
		for _, idx := range val.([]interface{}) {
			index = append(index, idx.(string))
		}
		indexes = append(indexes, index)
	}
	return indexes
}

func (p *parser) parseField(in interface{}) (*Field, error) {
	m := in.(yaml.MapSlice)

	t, err := p.parseDataType(m[0].Value.(string))
	if err != nil {
		return nil, err
	}

	f := Field{
		Name: m[0].Key.(string),
		Type: t,
	}

	for _, item := range m[1:] {
		switch key := item.Key.(string); key {
		case "default":
			switch f.Type.T {
			case DataTypeVarchar, DataTypeInteger, DataTypeBigint, DataTypeBool,
				DataTypeDouble, DataTypeText:
				f.Default = item.Value
			case DataTypeTimestamptz:
				if val := item.Value.(string); val != "now" {
					return nil, fmt.Errorf("%s: invalid default value '%s'", f.Name, val)
				}
				f.Default = "current_timestamp"
			default:
				if !f.Type.IsEnum {
					return nil, fmt.Errorf("%s: data type '%s' can not have 'default' attribute", f.Name, f.Type.T)
				}
			}

		case "size":
			switch f.Type.T {
			case DataTypeVarchar:
			default:
				return nil, fmt.Errorf("%s: data type '%s' can not have 'size' attribute", f.Name, f.Type.T)
			}
			f.Size = item.Value.(int)
		case "comment":
			f.Comment = item.Value.(string)
		case "nullable":
			f.Nullable = item.Value.(bool)
		case "pk":
			switch f.Type.T {
			case DataTypeInteger, DataTypeBigint, DataTypeSerial:
			default:
				return nil, fmt.Errorf("%s: primary key must be integer, bigint, serial", f.Name)
			}
			f.PK = item.Value.(bool)
		default:
			return nil, fmt.Errorf("%s: invalid attribute: %s", f.Name, key)
		}
	}

	// some validation
	switch f.Type.T {
	case DataTypeVarchar:
		if f.Size == 0 {
			return nil, fmt.Errorf("%s should have size. if size is not a consideration, 'text' should be used", f.Name)
		}
	}

	if f.PK {
		if f.Nullable {
			return nil, fmt.Errorf("%s: primary key can not be nullable", f.Name)
		}
	}

	return &f, nil
}

func (p *parser) parseDataType(t string) (*Type, error) {
	switch t {
	case "i32":
		return &Type{T: DataTypeInteger}, nil
	case "i64":
		return &Type{T: DataTypeBigint}, nil
	case "str":
		return &Type{T: DataTypeVarchar}, nil
	case "bool":
		return &Type{T: DataTypeBool}, nil
	case "t":
		return &Type{T: DataTypeTime}, nil
	case "tsz":
		return &Type{T: DataTypeTimestamptz}, nil
	case "double":
		return &Type{T: DataTypeDouble}, nil
	case "text", "serial", "jsonb":
		return &Type{T: t}, nil
	default:
		for _, e := range p.Data.Enums {
			if t == e.Name {
				return &Type{
					T:      t,
					IsEnum: true,
				}, nil
			}
		}

		return nil, fmt.Errorf("invalid data type: %s", t)
	}
}

func render(data *Metadata, w io.Writer) error {
	g := &gen{}
	g.P("-- Auto generated by pgen, DO NOT MODIFY.").Ln().Ln()

	g.P("-- Enums").Ln().Ln()

	for i, e := range data.Enums {
		if len(e.Values) == 0 {
			continue
		}

		if i > 0 {
			g.Ln()
		}

		g.P("create type", e.Name, "as enum(")

		for j, val := range e.Values {
			if j > 0 {
				g.P(", ")
			}
			g.Pf("'%s'", val)
		}

		g.P(");").Ln()

		// comment
		if e.Comment != "" {
			g.Pf("comment on type %s is '%s';", e.Name, e.Comment).Ln()
		}
	}

	g.Ln().P("-- Tables").Ln().Ln()

	for i, t := range data.Tables {
		if len(t.Fields) == 0 {
			continue
		}

		if i > 0 {
			g.Ln()
		}

		// DDL
		g.P("create table if not exists", t.Name, "(").Ln()

		for j, f := range t.Fields {
			g.P(" ", f.Name, f.Type.T)

			// size
			if f.Size > 0 {
				switch f.Type.T {
				case DataTypeVarchar:
					g.Pf("(%d)", f.Size)
				}
			}

			// default
			if f.Default != nil {
				switch f.Type.T {
				case DataTypeVarchar:
					g.Pf(" default '%s'", f.Default.(string))
				case DataTypeTimestamptz:
					g.Pf(" default %s", f.Default.(string))
				case DataTypeInteger, DataTypeBigint:
					g.Pf(" default %d", f.Default.(int))
				case DataTypeBool:
					g.Pf(" default %t", f.Default.(bool))
				default:
					if f.Type.IsEnum {
						g.Pf(" default '%s'", f.Default.(string))
					}
				}
			}

			if !f.Nullable && !f.PK {
				g.P(" not null")
			}

			if f.PK {
				g.P(" primary key")
			}

			if j < len(t.Fields)-1 {
				g.P(",")
			}
			g.Ln()
		}

		g.P(");").Ln()

		// indexes
		for _, index := range t.Uniques {
			// todo check if column exists
			g.Pf("create unique index %s_%s_key on %s (%s);", t.Name, strings.Join(index, "_"), t.Name, strings.Join(index, ", ")).Ln()
		}

		for _, index := range t.Indexes {
			// todo check if column exists
			g.Pf("create index %s_%s_idx on %s (%s);", t.Name, strings.Join(index, "_"), t.Name, strings.Join(index, ", ")).Ln()
		}

		// comments
		if t.Comment != "" {
			g.Pf("comment on table %s is '%s';", t.Name, t.Comment).Ln()
		}

		for _, f := range t.Fields {
			if f.Comment != "" {
				g.Pf("comment on column %s.%s is '%s';", t.Name, f.Name, f.Comment).Ln()
			}
		}
	}

	return g.Write(w)
}

type gen struct {
	buf bytes.Buffer
}

func (g *gen) Pf(format string, args ...interface{}) *gen {
	g.buf.WriteString(fmt.Sprintf(format, args...))
	return g
}

func (g *gen) P(s ...string) *gen {
	g.buf.WriteString(strings.Join(s, " "))
	return g
}

func (g *gen) Ln() *gen {
	return g.P("\n")
}

func (g *gen) String() string {
	return g.buf.String()
}

func (g *gen) Write(w io.Writer) error {
	_, err := w.Write(g.buf.Bytes())
	return err
}

type Enum struct {
	Name    string
	Comment string
	Values  []string
}

type Table struct {
	DB      string
	Name    string
	Comment string
	Fields  []*Field
	Indexes []Index
	Uniques []Index
}

type Index []string

type Field struct {
	Name     string
	Type     *Type
	Comment  string
	Nullable bool
	Default  interface{}
	Size     int // only 'varchar' has size attribute
	PK       bool
}

type Type struct {
	T      string
	IsEnum bool
}

const (
	DataTypeInteger     = "integer"
	DataTypeBigint      = "bigint"
	DataTypeVarchar     = "varchar"
	DataTypeBool        = "bool"
	DataTypeTime        = "time"
	DataTypeTimestamptz = "timestamptz"
	DataTypeSerial      = "serial"
	DataTypeDouble      = "float8"
	DataTypeText        = "text"
)
