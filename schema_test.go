package parquetschemaparser

import (
	"bytes"
	"os"
	"testing"

	"github.com/apache/arrow/go/v13/parquet/schema"
	"github.com/stretchr/testify/require"
)

func TestParseSchema(t *testing.T) {

	t.Run("Parse nested schema", func(t *testing.T) {
		assert := require.New(t)

		data, parsed, err := parseSchema("data/nested.schema")
		assert.NoError(err)

		assert.Equal(data, parsed)
	})

	t.Run("Parse test schema", func(t *testing.T) {
		assert := require.New(t)

		data, parsed, err := parseSchema("data/test.schema")
		assert.NoError(err)

		assert.Equal(string(data), string(parsed))
	})
}

func parseSchema(name string) ([]byte, []byte, error) {
	data, err := os.ReadFile(name)
	if err != nil {
		return nil, nil, err
	}

	ps, err := ParseSchema(bytes.NewBuffer(data))
	if err != nil {
		return nil, nil, err
	}

	var buf bytes.Buffer
	schema.PrintSchema(ps, &buf, 2)

	return data, buf.Bytes(), nil
}
