package parquet

import (
	"fmt"
	"iter"
	"slices"

	"github.com/apache/iceberg-go"
	"github.com/parquet-go/parquet-go/format"
)

type schemaElementPullFunc func() (format.SchemaElement, bool)

func ToIcebergSchema(elems []format.SchemaElement) (*iceberg.Schema, error) {
	var next, stop = iter.Pull(slices.Values(elems))

	defer stop()

	rootElem, ok := next()

	if !ok {
		return nil, fmt.Errorf("no root element")
	}

	rootField, err := getField(rootElem, next)

	if err != nil {
		return nil, err
	}

	if rootField.Name != "schema" {
		return nil, fmt.Errorf("root field must always be called `schema`")
	}

	structField, ok := rootField.Type.(*iceberg.StructType)

	if !ok {
		return nil, fmt.Errorf("root field must always be of `struct` type")
	}

	return iceberg.NewSchema(0, structField.Fields()...), nil
}

func getField(elem format.SchemaElement, next schemaElementPullFunc) (iceberg.NestedField, error) {
	var (
		field iceberg.NestedField
		err   error
	)

	switch {
	case elem.NumChildren == 0:
		field, err = getScalarField(elem)

	case elem.Type == nil && elem.LogicalType != nil && elem.LogicalType.List != nil:
		field, err = getListField(next)

	case elem.Type == nil && elem.LogicalType == nil && elem.RepetitionType == nil && elem.NumChildren > 0:
		field, err = getStructField(int(elem.NumChildren), next)

	default:
		err = fmt.Errorf("don't know what to do with this field")
	}

	if err != nil {
		return field, err
	}

	field.Name = elem.Name
	field.Required = isRequired(elem)

	return field, nil
}

func getStructField(numChilds int, next schemaElementPullFunc) (iceberg.NestedField, error) {
	var (
		res     iceberg.NestedField
		resType iceberg.StructType
	)

	res.Type = &resType

	for {
		if numChilds == len(resType.FieldList) {
			return res, nil
		}

		elem, ok := next()

		if !ok {
			return res, fmt.Errorf("not enought children: should be %d, but only have %d", numChilds, len(resType.FieldList))
		}

		field, err := getField(elem, next)

		if err != nil {
			return res, err
		}

		field.ID = len(resType.FieldList) + 1
		resType.FieldList = append(resType.FieldList, field)
	}
}

func getListField(next schemaElementPullFunc) (iceberg.NestedField, error) {
	var field iceberg.NestedField

	listElem, ok := next()

	if !ok {
		return field, fmt.Errorf("wanted `list` group but reached enf of elements.")
	}

	if listElem.Type != nil {
		return field, fmt.Errorf("malformed list: `list` group must not have a type")
	}

	if listElem.RepetitionType == nil || *listElem.RepetitionType != format.Repeated {
		return field, fmt.Errorf("malformed list: `list` group must be repeated")
	}

	if listElem.Name != "list" {
		return field, fmt.Errorf("malformed list: `list` group must be named `list`")
	}

	if listElem.NumChildren != 1 {
		return field, fmt.Errorf("malformed list: `list` group must have a single child")

	}

	elementElem, ok := next()

	if !ok {
		return field, fmt.Errorf("wanted `element` element but reached enf of elements.")
	}

	if elementElem.Name != "element" {
		return field, fmt.Errorf("malformed list: `element` field must be named `element`")
	}

	elemField, err := getField(elementElem, next)

	if err != nil {
		return field, err
	}

	field.Type = &iceberg.ListType{
		Element:         elemField.Type,
		ElementRequired: elemField.Required,
	}

	return field, nil
}

func getScalarField(elem format.SchemaElement) (iceberg.NestedField, error) {
	var field iceberg.NestedField

	t, err := getScalarType(elem)

	if err != nil {
		return field, err
	}

	field.ID = int(elem.FieldID)
	field.Name = elem.Name
	field.Type = t
	field.Required = isRequired(elem)

	return field, nil
}

func getScalarType(elem format.SchemaElement) (iceberg.Type, error) {
	switch *elem.Type {
	case format.Boolean:
		return iceberg.BooleanType{}, nil

	case format.Int32:
		return iceberg.Int32Type{}, nil

	case format.Int64:
		return iceberg.Int64Type{}, nil

	case format.Float:
		return iceberg.Float32Type{}, nil

	case format.Double:
		return iceberg.Float64Type{}, nil

	case format.ByteArray:
		if elem.LogicalType == nil {
			return iceberg.BinaryType{}, nil
		}

		switch {
		case elem.LogicalType.UTF8 != nil:
			return iceberg.StringType{}, nil
		default:
			return nil, fmt.Errorf("unknown logical type annotation")
		}

	case format.FixedLenByteArray:
		return iceberg.FixedTypeOf(int(*elem.TypeLength)), nil

	default:
		return nil, fmt.Errorf("unkown type: %s", elem.Type.String())
	}
}

func isRequired(elem format.SchemaElement) bool {
	return (elem.RepetitionType != nil) && (*elem.RepetitionType == format.Required)
}
