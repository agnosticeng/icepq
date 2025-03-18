package parquet

import (
	"fmt"

	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/iceberg-go"
)

func ToIcebergSchema(sch *schema.Schema) (*iceberg.Schema, error) {
	rootField, err := getField(sch.Root())

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

func getField(n schema.Node) (iceberg.NestedField, error) {
	var (
		field iceberg.NestedField
		err   error
	)

	switch n.Type() {
	case schema.Primitive:
		field, err = getPrimitiveField(n.(*schema.PrimitiveNode))
	case schema.Group:
		var lt = n.LogicalType()

		switch lt := lt.(type) {
		case schema.ListLogicalType:
			field, err = getListField(n.(*schema.GroupNode))
		case schema.NoLogicalType:
			field, err = getStructField(n.(*schema.GroupNode))
		default:
			if !lt.IsValid() || lt.IsNone() {
				field, err = getStructField(n.(*schema.GroupNode))
			} else {
				return field, fmt.Errorf("unknown logical type: %s", lt)
			}
		}
	default:
		return field, fmt.Errorf("unknown type: %v", n.Type())
	}

	if err != nil {
		return field, err
	}

	field.Name = n.Name()
	field.Required = isRequired(n)

	return field, nil
}

func getStructField(g *schema.GroupNode) (iceberg.NestedField, error) {
	var (
		res     iceberg.NestedField
		resType iceberg.StructType
	)

	res.Type = &resType

	for i := 0; i < g.NumFields(); i++ {
		field, err := getField(g.Field(i))

		if err != nil {
			return res, err
		}

		field.ID = len(resType.FieldList) + 1
		resType.FieldList = append(resType.FieldList, field)
	}

	return res, nil
}

func getListField(g *schema.GroupNode) (iceberg.NestedField, error) {
	var field iceberg.NestedField

	var _, ok = g.LogicalType().(*schema.ListLogicalType)

	if !ok {
		return field, fmt.Errorf("invalid list node: %v", g)
	}

	if g.NumFields() != 1 {
		return field, fmt.Errorf("`LIST` group must have a single repeated child field named `list`")
	}

	var listField = g.Field(0)

	if listField.Name() != "list" || listField.RepetitionType() != 2 {
		return field, fmt.Errorf("`LIST` group must have a single repeated child field named `list`")
	}

	listGroup, ok := listField.(*schema.GroupNode)

	if !ok {
		return field, fmt.Errorf("`LIST` group must have a single repeated child field named `list`")
	}

	if listGroup.NumFields() != 1 {
		return field, fmt.Errorf("list inner group must have a single child field name `element`")
	}

	var elemField = listGroup.Field(0)

	if elemField.Name() != "element" {
		return field, fmt.Errorf("list inner group must have a single child field name `element`")
	}

	elem, err := getField(elemField)

	if err != nil {
		return field, err
	}

	field.Type = &iceberg.ListType{
		Element:         elem.Type,
		ElementRequired: elem.Required,
	}

	return field, nil
}

func getPrimitiveField(n *schema.PrimitiveNode) (iceberg.NestedField, error) {
	var field iceberg.NestedField

	t, err := getPrimitiveType(n)

	if err != nil {
		return field, err
	}

	field.ID = int(n.FieldID())
	field.Name = n.Name()
	field.Type = t
	field.Required = isRequired(n)

	return field, nil
}

func getPrimitiveType(n *schema.PrimitiveNode) (iceberg.Type, error) {
	switch n.PhysicalType() {
	// BOOLEAN
	case 0:
		return iceberg.BooleanType{}, nil
	// INT32
	case 1:
		return iceberg.Int32Type{}, nil
	// INT64
	case 2:
		return iceberg.Int64Type{}, nil
	// FLOAT
	case 4:
		return iceberg.Float32Type{}, nil
	// DOUBLE
	case 5:
		return iceberg.Float64Type{}, nil
	// BYTE_ARRAY
	case 6:
		var lt = n.LogicalType()

		if !lt.IsValid() || lt.IsNone() {
			return iceberg.BinaryType{}, nil
		}

		switch lt := lt.(type) {
		case schema.StringLogicalType:
			return iceberg.StringType{}, nil
		default:
			return nil, fmt.Errorf("unknown logical type annotation: %s", lt)
		}

	// FIXED_LEN_BYTE_ARRAY
	case 7:
		return iceberg.FixedTypeOf(n.TypeLength()), nil

	default:
		return nil, fmt.Errorf("unkown physical type: %s", n.PhysicalType())
	}
}

func isRequired(n schema.Node) bool {
	return n.RepetitionType() == 0
}

// func getField(elem format.SchemaElement, next schemaElementPullFunc) (iceberg.NestedField, error) {
// 	var (
// 		field iceberg.NestedField
// 		err   error
// 	)

// 	switch {
// 	case elem.NumChildren == 0:
// 		field, err = getScalarField(elem)

// 	case elem.Type == nil && elem.LogicalType != nil && elem.LogicalType.List != nil:
// 		field, err = getListField(next)

// 	case elem.Type == nil && elem.LogicalType == nil && elem.RepetitionType == nil && elem.NumChildren > 0:
// 		field, err = getStructField(int(elem.NumChildren), next)

// 	default:
// 		err = fmt.Errorf("don't know what to do with this field")
// 	}

// 	if err != nil {
// 		return field, err
// 	}

// 	field.Name = elem.Name
// 	field.Required = isRequired(elem)

// 	return field, nil
// }

// func getStructField(numChilds int, next schemaElementPullFunc) (iceberg.NestedField, error) {
// 	var (
// 		res     iceberg.NestedField
// 		resType iceberg.StructType
// 	)

// 	res.Type = &resType

// 	for {
// 		if numChilds == len(resType.FieldList) {
// 			return res, nil
// 		}

// 		elem, ok := next()

// 		if !ok {
// 			return res, fmt.Errorf("not enought children: should be %d, but only have %d", numChilds, len(resType.FieldList))
// 		}

// 		field, err := getField(elem, next)

// 		if err != nil {
// 			return res, err
// 		}

// 		field.ID = len(resType.FieldList) + 1
// 		resType.FieldList = append(resType.FieldList, field)
// 	}
// }

// func getListField(next schemaElementPullFunc) (iceberg.NestedField, error) {
// 	var field iceberg.NestedField

// 	listElem, ok := next()

// 	if !ok {
// 		return field, fmt.Errorf("wanted `list` group but reached enf of elements.")
// 	}

// 	if listElem.Type != nil {
// 		return field, fmt.Errorf("malformed list: `list` group must not have a type")
// 	}

// 	if listElem.RepetitionType == nil || *listElem.RepetitionType != format.Repeated {
// 		return field, fmt.Errorf("malformed list: `list` group must be repeated")
// 	}

// 	if listElem.Name != "list" {
// 		return field, fmt.Errorf("malformed list: `list` group must be named `list`")
// 	}

// 	if listElem.NumChildren != 1 {
// 		return field, fmt.Errorf("malformed list: `list` group must have a single child")

// 	}

// 	elementElem, ok := next()

// 	if !ok {
// 		return field, fmt.Errorf("wanted `element` element but reached enf of elements.")
// 	}

// 	if elementElem.Name != "element" {
// 		return field, fmt.Errorf("malformed list: `element` field must be named `element`")
// 	}

// 	elemField, err := getField(elementElem, next)

// 	if err != nil {
// 		return field, err
// 	}

// 	field.Type = &iceberg.ListType{
// 		Element:         elemField.Type,
// 		ElementRequired: elemField.Required,
// 	}

// 	return field, nil
// }

// func getScalarField(elem format.SchemaElement) (iceberg.NestedField, error) {
// 	var field iceberg.NestedField

// 	t, err := getScalarType(elem)

// 	if err != nil {
// 		return field, err
// 	}

// 	field.ID = int(elem.FieldID)
// 	field.Name = elem.Name
// 	field.Type = t
// 	field.Required = isRequired(elem)

// 	return field, nil
// }

// func getScalarType(elem format.SchemaElement) (iceberg.Type, error) {
// 	switch *elem.Type {
// 	case format.Boolean:
// 		return iceberg.BooleanType{}, nil

// 	case format.Int32:
// 		return iceberg.Int32Type{}, nil

// 	case format.Int64:
// 		return iceberg.Int64Type{}, nil

// 	case format.Float:
// 		return iceberg.Float32Type{}, nil

// 	case format.Double:
// 		return iceberg.Float64Type{}, nil

// 	case format.ByteArray:
// 		if elem.LogicalType == nil {
// 			return iceberg.BinaryType{}, nil
// 		}

// 		switch {
// 		case elem.LogicalType.UTF8 != nil:
// 			return iceberg.StringType{}, nil
// 		default:
// 			return nil, fmt.Errorf("unknown logical type annotation")
// 		}

// 	case format.FixedLenByteArray:
// 		return iceberg.FixedTypeOf(int(*elem.TypeLength)), nil

// 	default:
// 		return nil, fmt.Errorf("unkown type: %s", elem.Type.String())
// 	}
// }

// func isRequired(elem format.SchemaElement) bool {
// 	return (elem.RepetitionType != nil) && (*elem.RepetitionType == format.Required)
// }
