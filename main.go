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

var model string

func main() {
	flag.StringVar(&model, "model", "", "model file")
	flag.Parse()

	if model = strings.TrimSpace(model); model == "" {
		return
	}

	exitIfErr(checkFile(model))

	raw, err := readFile(model)
	exitIfErr(err)

	// parse to get structured data
	data, err := parse(raw)
	exitIfErr(err)

	exitIfErr(render(data, os.Stdout))
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

func parse(raw yaml.MapSlice) ([]*Table, error) {
	data := make([]*Table, 0, len(raw))

	for _, s := range raw {
		attrs := s.Value.(yaml.MapSlice)

		t := Table{
			Name: s.Key.(string),
		}

		for _, attr := range attrs {
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
					f, err := parseField(field)
					if err != nil {
						return nil, err
					}

					t.Fields = append(t.Fields, f)
				}
			case "uniques":
				t.Uniques = parseIndexes(attr.Value)
			case "indexes":
				t.Indexes = parseIndexes(attr.Value)
			}
		}

		// validation
		if t.DB == "" {
			return nil, fmt.Errorf("%s: db should be provided", t.Name)
		}

		data = append(data, &t)
	}

	return data, nil
}

func parseIndexes(in interface{}) []Index {
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

func parseField(in interface{}) (*Field, error) {
	m := in.(yaml.MapSlice)

	col := m[0]

	t, err := castDataType(col.Value.(string))
	if err != nil {
		return nil, err
	}

	f := Field{
		Name: col.Key.(string),
		Type: t,
	}

	for _, item := range m[1:] {
		switch key := item.Key.(string); key {
		case "default":
			f.Default = item.Value
		case "size":
			f.Size = item.Value.(int)
		case "comment":
			f.Comment = item.Value.(string)
		case "nullable":
			f.Nullable = item.Value.(bool)
		case "pk":
			if t != DataTypeInteger && t != DataTypeBigint {
				return nil, fmt.Errorf("%s: primary key must be integer or bigint", f.Name)
			}
			f.PK = item.Value.(bool)
		case "serial":
			f.Serial = item.Value.(bool)
		default:
			return nil, fmt.Errorf("%s: invalid attribute: %s", f.Name, key)
		}
	}

	// do some validation
	switch f.Type {
	case DataTypeVarchar:
		if f.Size == 0 {
			return nil, fmt.Errorf("%s should have size. if size is not a consideration, 'text' should be used", f.Name)
		}
	}

	return &f, nil
}

func render(data []*Table, w io.Writer) error {
	g := &gen{}
	g.P("-- Auto generated by pgen, DO NOT MODIFY.").Ln().Ln()

	for i, t := range data {
		if len(t.Fields) == 0 {
			continue
		}

		if i > 0 {
			g.Ln()
		}

		// DDL
		g.P("create table if not exists", t.Name, "(").Ln()

		for j, f := range t.Fields {
			g.P(" ", f.Name, f.Type)

			// size
			if f.Size > 0 {
				switch f.Type {
				case DataTypeVarchar:
					g.Pf("(%d)", f.Size)
				default:
					return fmt.Errorf("%s: data type '%s' can not have 'size' attribute", f.Name, f.Type)
				}
			}

			// default
			if f.Default != nil {
				switch f.Type {
				case DataTypeVarchar:
					g.Pf(" default '%s'", f.Default.(string))
				case DataTypeInteger, DataTypeBigint:
					g.Pf(" default %d", f.Default.(int))
				case DataTypeBool:
					g.Pf(" default %t", f.Default.(bool))
				default:
					return fmt.Errorf("%s: data type '%s' can not have 'default' attribute", f.Name, f.Type)
				}
			}

			if !f.Nullable && !f.PK {
				g.P(" not null")
			}

			if f.Serial {
				g.P(" serial")
			}

			if f.PK {
				if f.Nullable {
					return fmt.Errorf("%s: primary key can not nullable", f.Name)
				}
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
			g.Pf("create unique index %s_idx on %s (%s);", strings.Join(index, "_"), t.Name, strings.Join(index, ", ")).Ln()
		}

		for _, index := range t.Indexes {
			// todo check if column exists
			g.Pf("create index %s_idx on %s (%s);", strings.Join(index, "_"), t.Name, strings.Join(index, ", ")).Ln()
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
	Type     string
	Comment  string
	Nullable bool
	Default  interface{}
	Size     int // only 'varchar' has size attribute
	PK       bool
	Serial   bool
}

func castDataType(t string) (string, error) {
	switch t {
	case "i32":
		return DataTypeInteger, nil
	case "i64":
		return DataTypeBigint, nil
	case "str":
		return DataTypeVarchar, nil
	case "text":
		return t, nil
	case "bool":
		return DataTypeBool, nil
	case "t":
		return DataTypeTime, nil
	case "tsz":
		return "timestamptz", nil
	default:
		return "", fmt.Errorf("invalid data type: %s", t)
	}
}

const (
	DataTypeInteger = "integer"
	DataTypeBigint  = "bigint"
	DataTypeVarchar = "varchar"
	DataTypeBool    = "bool"
	DataTypeTime    = "time"
)
