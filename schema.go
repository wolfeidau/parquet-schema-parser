package parquetschemaparser

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/schema"
)

func ParseSchema(rdr io.Reader) (schema.Node, error) {
	sct := NewSchemaTokenizer(rdr, " ,;{}()\n\t=")

	t, err := sct.NextToken()
	if err != nil {
		return nil, fmt.Errorf("reading token failed at %s with: %w", sct.GetLocation(), err)
	}

	repetition, err := asRepetition(t, sct)
	if err != nil {
		return nil, fmt.Errorf("reading token failed at %s with: %w", sct.GetLocation(), err)
	}

	t, err = sct.NextToken()
	if err != nil {
		return nil, fmt.Errorf("reading token failed at %s with: %w", sct.GetLocation(), err)
	}

	if t != "group" {
		return nil, fmt.Errorf("expected top level group node, got: %s", t)
	}

	group, err := asGroup(repetition, sct)
	if err != nil {
		return nil, fmt.Errorf("reading token failed at %s with: %w", sct.GetLocation(), err)
	}

	return group, nil
}

func asGroup(repetition parquet.Repetition, sct *SchemaTokenizer) (*schema.GroupNode, error) {

	// grab next token
	t, err := sct.NextToken()
	if err != nil {
		return nil, err
	}

	var id int32 = -1

	if t == "field_id" {
		// read the field id
		id, err = asFieldID(t, sct)
		if err != nil {
			return nil, err
		}
	}

	// read the identifier
	name, err := sct.NextToken()
	if err != nil {
		return nil, err
	}

	// t, err = sct.NextToken()
	// if err != nil {
	// 	return nil, err
	// }

	// if t == "(" {
	// 	// can be a Map or a List

	// }

	peak, err := sct.PeakToken()
	if err != nil {
		return nil, err
	}

	if peak == "(" {
		return nil, fmt.Errorf("wanted something, got: %s", peak)
	}

	// read the left brace
	_, err = sct.AssertNextToken("check opening brace", "{")
	if err != nil {
		return nil, err
	}

	fields, err := readFields(sct)
	if err != nil {
		return nil, err
	}

	return schema.NewGroupNode(name /* name */, repetition, fields, int32(id) /* fieldID */)
}

func readFields(sct *SchemaTokenizer) (schema.FieldList, error) {

	fields := schema.FieldList{}

	for {

		// grab next token
		t, err := sct.NextToken()
		if err != nil {
			return nil, err
		}

		if t == "}" {
			break
		}

		repetition, err := asRepetition(t, sct)
		if err != nil {
			return nil, err
		}

		// grab next token
		t, err = sct.NextToken()
		if err != nil {
			return nil, err
		}

		if t == "group" {
			group, err := asGroup(repetition, sct)
			if err != nil {
				return nil, err
			}

			fields = append(fields, group)
		} else {

			fld, err := asField(t, repetition, sct)
			if err != nil {
				return nil, err
			}

			fields = append(fields, fld)
		}
	}

	return fields, nil
}

func asField(t string, repetition parquet.Repetition, sct *SchemaTokenizer) (schema.Node, error) {
	// type info
	typ, err := asType(t, sct)
	if err != nil {
		return nil, err
	}

	t, err = sct.NextToken()
	if err != nil {
		return nil, err
	}

	var id int32 = -1

	if t == "field_id" {
		// read the field id
		id, err = asFieldID(t, sct)
		if err != nil {
			return nil, err
		}
	}

	name, err := sct.NextToken()
	if err != nil {
		return nil, err
	}

	t, err = sct.NextToken()
	if err != nil {
		return nil, err
	}

	var fld schema.Node

	// does this field have a logical type
	if t == "(" {
		ltype, err := asLogicalType(sct)
		if err != nil {
			return nil, err
		}

		fld, err = schema.NewPrimitiveNodeLogical(name, repetition, ltype, typ, -1, id)
		if err != nil {
			return nil, err
		}

		t, err = sct.NextToken()
		if err != nil {
			return nil, err
		}

	} else {
		// fall back to primitive node
		fld, err = schema.NewPrimitiveNode(name, repetition, typ, id, -1)
		if err != nil {
			return nil, err
		}
	}

	if t != ";" {
		return nil, fmt.Errorf("unable to locate closing semi for field: %s", t)
	}

	return fld, nil
}

func asLogicalType(sct *SchemaTokenizer) (schema.LogicalType, error) {
	logicalType, err := sct.NextToken()
	if err != nil {
		return nil, err
	}

	var lgt schema.LogicalType

	fmt.Println(logicalType)

	switch logicalType {
	case "String":
		lgt = schema.StringLogicalType{}
	case "Map":
		lgt = schema.MapLogicalType{}
	case "Timestamp":
		// read the params for the timestamp type
		params, err := readLogicalTypeParams(sct)
		if err != nil {
			return nil, err
		}

		fmt.Println(params)

		tu, err := asTimeUnit(params["timeUnit"])
		if err != nil {
			return nil, err
		}

		if params["is_from_converted_type"] == "true" {
			lgt = schema.NewTimestampLogicalTypeForce(asBool(params["isAdjustedToUTC"]), tu)
		} else {
			lgt = schema.NewTimestampLogicalType(asBool(params["isAdjustedToUTC"]), tu)
		}
	default:
		return nil, fmt.Errorf("unknown logical type: %s", logicalType)
	}

	_, err = sct.AssertNextToken("check closing bracket for logical type", ")")
	if err != nil {
		return nil, err
	}

	return lgt, nil
}

func readLogicalTypeParams(sct *SchemaTokenizer) (map[string]string, error) {
	params := make(map[string]string)

	t, err := sct.NextToken()
	if err != nil {
		return nil, err
	}

	if t != "(" {
		return nil, fmt.Errorf("expected '(' got: %s", t)
	}

	// read till the end bracket
	for {
		name, err := sct.NextToken()
		if err != nil {
			return nil, err
		}

		_, err = sct.AssertNextToken("check equal for attribute", "=")
		if err != nil {
			return nil, err
		}

		val, err := sct.NextToken()
		if err != nil {
			return nil, err
		}

		params[name] = val

		t, err = sct.NextToken()
		if err != nil {
			return nil, err
		}

		if t == ")" {
			break
		}

		if t != "," {
			return nil, fmt.Errorf("expected ',' got: %s", t)
		}
	}

	return params, nil
}

func asFieldID(t string, sct *SchemaTokenizer) (int32, error) {
	if t != "field_id" {
		return 0, errors.New("expected field_id")
	}

	_, err := sct.AssertNextToken("check equal for field id", "=")
	if err != nil {
		return 0, err
	}

	t, err = sct.NextToken()
	if err != nil {
		return 0, err
	}

	i, err := strconv.ParseInt(t, 10, 32)
	if err != nil {
		return 0, err
	}

	return int32(i), nil // field id is a 32 bit int
}

func asRepetition(t string, sct *SchemaTokenizer) (parquet.Repetition, error) {
	switch t {
	case parquet.Repetitions.Optional.String():
		return parquet.Repetitions.Optional, nil
	case parquet.Repetitions.Required.String():
		return parquet.Repetitions.Required, nil
	case parquet.Repetitions.Repeated.String():
		return parquet.Repetitions.Repeated, nil
	default:
		return 0, fmt.Errorf("expected one of %s but found %s", parquet.Repetitions, t)
	}
}

func asType(t string, sct *SchemaTokenizer) (parquet.Type, error) {
	switch strings.ToUpper(t) {
	case parquet.Types.Boolean.String():
		return parquet.Types.Boolean, nil
	case parquet.Types.Int32.String():
		return parquet.Types.Int32, nil
	case parquet.Types.Int64.String():
		return parquet.Types.Int64, nil
	case parquet.Types.Int96.String():
		return parquet.Types.Int96, nil
	case parquet.Types.Float.String():
		return parquet.Types.Float, nil
	case parquet.Types.Double.String():
		return parquet.Types.Double, nil
	case parquet.Types.ByteArray.String():
		return parquet.Types.ByteArray, nil
	case parquet.Types.FixedLenByteArray.String():
		return parquet.Types.FixedLenByteArray, nil
	default:
		return 0, fmt.Errorf("expected one of %s but found %s", parquet.Types, t)
	}
}

func asBool(val string) bool {
	return strings.ToLower(val) == "true"
}

func asTimeUnit(val string) (schema.TimeUnitType, error) {
	switch strings.ToLower(val) {
	case "milliseconds":
		return schema.TimeUnitMillis, nil
	case "microseconds":
		return schema.TimeUnitMicros, nil
	case "nanoseconds":
		return schema.TimeUnitNanos, nil
	}
	return 0, fmt.Errorf("unknown time unit: %s", val)
}
