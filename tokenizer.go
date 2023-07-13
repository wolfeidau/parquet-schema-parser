package parquetschemaparser

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

type SchemaTokenizer struct {
	buffer      []string
	line        int
	currentLine strings.Builder
	st          *StringTokenizer
}

func NewSchemaTokenizer(rdr io.Reader, delimiters string) *SchemaTokenizer {
	return &SchemaTokenizer{
		line: 1,
		st:   NewStringTokenizer(rdr, delimiters, true),
	}
}

func (sct *SchemaTokenizer) PeakToken() (string, error) {
	for sct.st.HasMoreTokens() {
		t := sct.st.NextToken()

		sct.buffer = append(sct.buffer, t)

		if !isWhitespace(t) {
			return t, nil
		}
	}

	return "", errors.New("unexpected end of schema")
}

func (sct *SchemaTokenizer) pop() (t string) {
	t, sct.buffer = sct.buffer[len(sct.buffer)-1], sct.buffer[:len(sct.buffer)-1]
	return
}

func (sct *SchemaTokenizer) NextToken() (string, error) {
	if len(sct.buffer) > 0 {
		t := sct.pop()

		if !isWhitespace(t) {
			return t, nil
		}
	}

	for sct.st.HasMoreTokens() {
		t := sct.st.NextToken()

		sct.appendToLine(t)

		if !isWhitespace(t) {
			return t, nil
		}
	}

	return "", errors.New("unexpected end of schema")
}

func (sct *SchemaTokenizer) appendToLine(t string) {
	if t == "\n" {
		sct.line++
		sct.currentLine = strings.Builder{}
	} else {
		sct.currentLine.WriteString(t)
	}
}

func (sct *SchemaTokenizer) AssertNextToken(msg string, expected ...string) (string, error) {
	t, err := sct.NextToken()
	if err != nil {
		return "", err
	}

	for _, e := range expected {
		if e == t {
			return t, nil
		}
	}

	return "", fmt.Errorf("%s, expected %s but found %s", msg, expected, t)
}

func (sct *SchemaTokenizer) GetLocation() string {
	return fmt.Sprintf("line %d : %s", sct.line, sct.currentLine.String())
}

func isWhitespace(t string) bool {
	return t == " " || t == "\t" || t == "\n" || t == "\r"
}
