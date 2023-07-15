package parquetschemaparser

import (
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/schema"
)

func ParseSchema(rdr io.Reader) (schema.Node, error) {
	sct := NewSchemaTokenizer(rdr, " ,;{}()\n\t=")

	repetition, err := asRepetition(sct)
	if err != nil {
		return nil, fmt.Errorf("reading token failed at %s with: %w", sct.GetLocation(), err)
	}

	t := sct.NextToken()
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

	var (
		id  int32 = -1
		err error
	)

	if peak := sct.PeakToken(); peak == "field_id" {
		// read the field id
		id, err = asFieldID(sct)
		if err != nil {
			return nil, err
		}
	}

	// read the identifier
	name := sct.NextToken()

	if peak := sct.PeakToken(); peak == "(" {
		// can be Map or List
		return asLogicalGroup(name, repetition, sct)
	}

	fields, err := readFields(sct)
	if err != nil {
		return nil, err
	}

	return schema.NewGroupNode(name /* name */, repetition, fields, int32(id) /* fieldID */)
}

func readFields(sct *SchemaTokenizer) (schema.FieldList, error) {
	// read the left brace
	err := sct.AssertNextToken("check opening brace", "{")
	if err != nil {
		return nil, err
	}

	fields := schema.FieldList{}

	for {

		// grab next token

		if peak := sct.PeakToken(); peak == "}" {
			break
		}

		fld, err := readField(sct)
		if err != nil {
			return nil, err
		}

		fields = append(fields, fld)
	}

	// read the right brace
	err = sct.AssertNextToken("check closing brace", "}")
	if err != nil {
		return nil, err
	}

	return fields, nil
}

func readField(sct *SchemaTokenizer) (schema.Node, error) {
	repetition, err := asRepetition(sct)
	if err != nil {
		return nil, err
	}

	// grab next token
	t := sct.NextToken()

	if t == "group" {
		group, err := asGroup(repetition, sct)
		if err != nil {
			return nil, err
		}

		return group, nil
	}

	fld, err := asFieldNode(t, repetition, sct)
	if err != nil {
		return nil, err
	}

	return fld, nil
}

func asFieldNode(t string, repetition parquet.Repetition, sct *SchemaTokenizer) (schema.Node, error) {
	// type info
	typ, err := asType(t, sct)
	if err != nil {
		return nil, err
	}

	var id int32 = -1

	if peak := sct.PeakToken(); peak == "field_id" {
		// read the field id
		id, err = asFieldID(sct)
		if err != nil {
			return nil, err
		}
	}

	name := sct.NextToken()
	if err != nil {
		return nil, err
	}

	var fld schema.Node

	// does this field have a logical type
	if peak := sct.PeakToken(); peak == "(" {
		ltype, err := asLogicalType(sct)
		if err != nil {
			return nil, err
		}

		fld, err = schema.NewPrimitiveNodeLogical(name, repetition, ltype, typ, -1, id)
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

	err = sct.AssertNextToken("check closing semi colon", ";")
	if err != nil {
		return nil, err
	}

	return fld, nil
}

func asLogicalGroup(name string, repetition parquet.Repetition, sct *SchemaTokenizer) (*schema.GroupNode, error) {
	err := sct.AssertNextToken("check first token is bracket", "(")
	if err != nil {
		return nil, err
	}

	lt := sct.NextToken()

	log.Println("parsing logical group:", lt)

	err = sct.AssertNextToken("check is closing bracket", ")")
	if err != nil {
		return nil, err
	}

	switch lt {
	case "Map":
		return asMap(name, repetition, sct)
	case "List":
		return asList(name, repetition, sct)
	default:
		return nil, fmt.Errorf("unknown logical type: %s", lt)
	}
}

func asMap(name string, repetition parquet.Repetition, sct *SchemaTokenizer) (*schema.GroupNode, error) {
	// <map-repetition> group <name> (MAP) {
	//   repeated group key_value {
	//     required <key-type> key;
	//     <value-repetition> <value-type> value;
	//   }
	// }

	err := sct.AssertNextToken("check first token is brace", "{")
	if err != nil {
		return nil, err
	}

	err = sct.AssertNextToken("check repetition", "repeated")
	if err != nil {
		return nil, err
	}

	err = sct.AssertNextToken("check group", "group")
	if err != nil {
		return nil, err
	}

	var id int32 = -1

	if peak := sct.PeakToken(); peak == "field_id" {
		// read the field id
		id, err = asFieldID(sct)
		if err != nil {
			return nil, err
		}
	}

	err = sct.AssertNextToken("check name is key_value", "key_value")
	if err != nil {
		return nil, err
	}

	fields, err := readFields(sct)
	if err != nil {
		return nil, err
	}

	if len(fields) != 2 {
		return nil, fmt.Errorf("expected 2 fields for map, got: %d", len(fields))
	}

	err = sct.AssertNextToken("check end brace", "}")
	if err != nil {
		return nil, err
	}

	return schema.MapOf(name, fields[0], fields[1], repetition, int32(id))
}

func asList(name string, repetition parquet.Repetition, sct *SchemaTokenizer) (*schema.GroupNode, error) {

	// <list-repetition> group <name> (LIST) {
	//   repeated group list {
	//     <element-repetition> <element-type> element;
	//   }
	// }

	err := sct.AssertNextToken("check first token is brace", "{")
	if err != nil {
		return nil, err
	}

	err = sct.AssertNextToken("check repetition", "repeated")
	if err != nil {
		return nil, err
	}

	err = sct.AssertNextToken("check group", "group")
	if err != nil {
		return nil, err
	}

	var id int32 = -1

	if peak := sct.PeakToken(); peak == "field_id" {
		// read the field id
		id, err = asFieldID(sct)
		if err != nil {
			return nil, err
		}
	}

	err = sct.AssertNextToken("check name is list", "list")
	if err != nil {
		return nil, err
	}

	fields, err := readFields(sct)
	if err != nil {
		return nil, err
	}

	if len(fields) != 1 {
		return nil, fmt.Errorf("expected 1 field for list, got: %d", len(fields))
	}

	err = sct.AssertNextToken("check end brace", "}")
	if err != nil {
		return nil, err
	}

	return ListOf(name, fields[0], repetition, int32(id))
}

func asLogicalType(sct *SchemaTokenizer) (schema.LogicalType, error) {
	err := sct.AssertNextToken("check first token is bracket", "(")
	if err != nil {
		return nil, err
	}

	var lgt schema.LogicalType

	switch lt := sct.NextToken(); {
	case lt == "String":
		lgt = schema.StringLogicalType{}
	case lt == "Timestamp":
		// read the params for the timestamp type
		params, err := readLogicalTypeParams(sct)
		if err != nil {
			return nil, err
		}

		log.Println("timestamp", params)

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
		return nil, fmt.Errorf("unknown logical type: %s", lt)
	}

	err = sct.AssertNextToken("check closing bracket for logical type", ")")
	if err != nil {
		return nil, err
	}

	return lgt, nil
}

func readLogicalTypeParams(sct *SchemaTokenizer) (map[string]string, error) {
	params := make(map[string]string)

	t := sct.NextToken()

	if t != "(" {
		return nil, fmt.Errorf("expected '(' got: %s", t)
	}

	// read till the end bracket
	for {
		name := sct.NextToken()

		err := sct.AssertNextToken("check equal for attribute", "=")
		if err != nil {
			return nil, err
		}

		val := sct.NextToken()

		params[name] = val

		t = sct.NextToken()
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

func asFieldID(sct *SchemaTokenizer) (int32, error) {
	err := sct.AssertNextToken("check next token is field_id", "field_id")
	if err != nil {
		return 0, err
	}

	err = sct.AssertNextToken("check equal for field id", "=")
	if err != nil {
		return 0, err
	}

	val := sct.NextToken()

	i, err := strconv.ParseInt(val, 10, 32)
	if err != nil {
		return 0, err
	}

	return int32(i), nil // field id is a 32 bit int
}

func asRepetition(sct *SchemaTokenizer) (parquet.Repetition, error) {
	rep := sct.NextToken()

	switch rep {
	case parquet.Repetitions.Optional.String():
		return parquet.Repetitions.Optional, nil
	case parquet.Repetitions.Required.String():
		return parquet.Repetitions.Required, nil
	case parquet.Repetitions.Repeated.String():
		return parquet.Repetitions.Repeated, nil
	default:
		return 0, fmt.Errorf("expected one of %s but found %s", parquet.Repetitions, rep)
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

func ListOf(listName string, n schema.Node, rep parquet.Repetition, fieldID int32) (*schema.GroupNode, error) {
	if rep == parquet.Repetitions.Repeated || n.RepetitionType() == parquet.Repetitions.Repeated {
		return nil, fmt.Errorf("parquet: listof repetition and element repetition must not be repeated, got %s", rep)
	}

	list, err := schema.NewGroupNode("list" /* name */, parquet.Repetitions.Repeated, schema.FieldList{n}, -1 /* fieldID */)
	if err != nil {
		return nil, err
	}
	return schema.NewGroupNodeLogical(listName, rep, schema.FieldList{list}, schema.ListLogicalType{}, fieldID)
}
