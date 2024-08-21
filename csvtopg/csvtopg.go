package csvtopg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/jackc/pgtype"
	shopspring "github.com/jackc/pgtype/ext/shopspring-numeric"
	"github.com/jackc/pgx/v5"
)

type transcoder interface {
	pgtype.TextDecoder
	pgtype.BinaryEncoder
	pgtype.Value
}

type Column struct {
	Name       string
	DataType   string
	NotNull    bool
	transcoder transcoder
}

type Table struct {
	Name    string
	Columns []Column
}

type columnAnalyzer struct {
	ci              *pgtype.ConnInfo
	acceptableTypes []transcoder
	nullsFound      int64
	nonNullsFound   int64
}

func newColumnAnalyzer() *columnAnalyzer {
	return &columnAnalyzer{
		acceptableTypes: []transcoder{
			&pgtype.Int4{},
			&pgtype.Int8{},
			&shopspring.Numeric{},
			&pgtype.Date{},
			&pgtype.Bool{},
		},
	}
}

func (ca *columnAnalyzer) analyzeValue(s string) {
	if s == "" {
		ca.nullsFound += 1
		return
	}
	ca.nonNullsFound += 1

	var newAcceptableTypes []transcoder

	for _, t := range ca.acceptableTypes {
		err := t.DecodeText(ca.ci, []byte(s))
		if err == nil {
			newAcceptableTypes = append(newAcceptableTypes, t)
		}
	}

	ca.acceptableTypes = newAcceptableTypes
}

func (ca *columnAnalyzer) result() (dataType string, transcoder transcoder, notNull bool) {
	if len(ca.acceptableTypes) == 0 || ca.nonNullsFound == 0 {
		transcoder = &pgtype.Text{}
	} else {
		transcoder = ca.acceptableTypes[0]
	}

	switch transcoder.(type) {
	case *pgtype.Int4:
		dataType = "integer"
	case *pgtype.Int8:
		dataType = "bigint"
	case *shopspring.Numeric:
		dataType = "numeric"
	case *pgtype.Date:
		dataType = "date"
	case *pgtype.Bool:
		dataType = "bool"
	case *pgtype.Text:
		dataType = "text"
	}

	return dataType, transcoder, ca.nullsFound == 0
}

func AnalyzeColumns(ci *pgtype.ConnInfo, read func() ([]string, error)) ([]Column, error) {
	lineNumber := 1
	headerRow, err := read()
	if err != nil {
		return nil, fmt.Errorf("line %d: %w", lineNumber, err)
	}
	columns := make([]Column, len(headerRow))
	for i := range headerRow {
		columns[i].Name = NormalizeIdentifier(headerRow[i])
	}
	columnAnalyzers := make([]*columnAnalyzer, len(headerRow))
	for i := range headerRow {
		columnAnalyzers[i] = newColumnAnalyzer()
	}

	for {
		lineNumber += 1
		row, err := read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("line %d: %w", lineNumber, err)
		}

		for i := range row {
			columnAnalyzers[i].analyzeValue(row[i])
		}
	}

	for i := range columns {
		columns[i].DataType, columns[i].transcoder, columns[i].NotNull = columnAnalyzers[i].result()
	}

	return columns, nil
}

func CreateTable(ctx context.Context, tx pgx.Tx, tableName string, columns []Column) error {
	sb := &strings.Builder{}
	fmt.Fprintf(sb, "create table %s (", tableName)
	for i, c := range columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(sb, "%s %s", c.Name, c.DataType)
		if c.NotNull {
			sb.WriteString(" not null")
		}
	}
	sb.WriteString(");")

	_, err := tx.Exec(ctx, sb.String())
	return err
}

type copyFromSource struct {
	ci       *pgtype.ConnInfo
	columns  []Column
	readFunc func() ([]string, error)
	rawRow   []string
	values   []interface{}
	err      error
}

func (cfs *copyFromSource) Next() bool {
	row, err := cfs.readFunc()
	if err != nil {
		if err != io.EOF {
			cfs.err = err
		}
		return false
	}

	cfs.rawRow = row

	return true
}

func (cfs *copyFromSource) Values() ([]interface{}, error) {
	for i, s := range cfs.rawRow {
		var buf []byte
		if len(s) > 0 {
			buf = []byte(s)
		}
		err := cfs.columns[i].transcoder.DecodeText(cfs.ci, buf)
		if err != nil {
			cfs.err = err
			return nil, err
		}
	}

	return cfs.values, nil
}

func (cfs *copyFromSource) Err() error {
	return cfs.err
}

func CopyRows(ctx context.Context, tx pgx.Tx, tableName string, columns []Column, read func() ([]string, error)) (int64, error) {
	lineNumber := 1
	_, err := read()
	if err != nil {
		return 0, fmt.Errorf("line %d: %w", lineNumber, err)
	}

	columnNames := make([]string, len(columns))
	columnTranscodersAsEmptyInterfaces := make([]interface{}, len(columns))
	for i := range columns {
		columnNames[i] = columns[i].Name
		columnTranscodersAsEmptyInterfaces[i] = columns[i].transcoder
	}

	cfs := &copyFromSource{
		ci:       pgtype.NewConnInfo(),
		columns:  columns,
		readFunc: read,
		values:   columnTranscodersAsEmptyInterfaces,
	}

	return tx.CopyFrom(ctx, pgx.Identifier{tableName}, columnNames, cfs)
}

var normalizeIdentifierRegexp = regexp.MustCompile(`\W+`)

func NormalizeIdentifier(s string) string {
	s = normalizeIdentifierRegexp.ReplaceAllLiteralString(s, "_")
	s = strings.ToLower(s)
	return s
}
