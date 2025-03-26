package iceberg

import (
	"context"
	"fmt"
	"net/url"

	"github.com/agnosticeng/icepq/internal/io"
	"github.com/agnosticeng/objstr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/samber/lo"
	"github.com/sourcegraph/conc/iter"
)

type Schemas []*iceberg.Schema

func (schs Schemas) Equals(target *iceberg.Schema) bool {
	return lo.EveryBy(schs, func(sch *iceberg.Schema) bool {
		return target.Equals(sch)
	})
}

func SchemaFromParquetDataFiles(ctx context.Context, location *url.URL, files []string) (*iceberg.Schema, error) {
	schemas, err := iter.MapErr(files, func(path *string) (*iceberg.Schema, error) {
		var u = location.JoinPath("data", *path)
		return SchemaFromParquetFile(ctx, u)
	})

	if err != nil {
		return nil, err
	}

	if !Schemas(schemas).Equals(schemas[0]) {
		return nil, fmt.Errorf("not all provided Parquet files have the same schema")
	}

	return schemas[0], nil
}

func SchemaFromParquetFile(ctx context.Context, u *url.URL) (*iceberg.Schema, error) {
	var os = objstr.FromContextOrDefault(ctx)

	md, err := os.ReadMetadata(ctx, u)

	if err != nil {
		return nil, err
	}

	r, err := os.ReaderAt(ctx, u)

	if err != nil {
		return nil, err
	}

	defer r.Close()

	pqr, err := file.NewParquetReader(io.NewReadSeekerAdapter(r, int64(md.Size)))

	if err != nil {
		return nil, err
	}

	defer pqr.Close()

	fr, err := pqarrow.NewFileReader(pqr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)

	if err != nil {
		return nil, err
	}

	arrowSch, err := fr.Schema()

	if err != nil {
		return nil, err
	}

	var v = nameMappingArrowSchemaVisitor{}

	mapping, err := table.VisitArrowSchema(arrowSch, &v)

	if err != nil {
		return nil, err
	}

	return table.ArrowSchemaToIceberg(arrowSch, true, mapping.Fields)
}

type nameMappingArrowSchemaVisitor struct {
	latestFieldId int
}

func (v *nameMappingArrowSchemaVisitor) nextFieldId() int {
	v.latestFieldId++
	return v.latestFieldId
}

func (v *nameMappingArrowSchemaVisitor) unwrap(t arrow.Type, mf iceberg.MappedField) []iceberg.MappedField {
	switch t {
	case arrow.STRUCT:
		return mf.Fields
	case arrow.LIST:
		return mf.Fields
	case arrow.MAP:
		return mf.Fields
	default:
		return nil
	}
}

func (v *nameMappingArrowSchemaVisitor) Schema(sch *arrow.Schema, mf iceberg.MappedField) iceberg.MappedField {
	return mf
}

func (v *nameMappingArrowSchemaVisitor) Struct(st *arrow.StructType, mfs []iceberg.MappedField) iceberg.MappedField {
	return iceberg.MappedField{
		Fields: mfs,
	}
}

func (v *nameMappingArrowSchemaVisitor) Field(f arrow.Field, mf iceberg.MappedField) iceberg.MappedField {
	var id = v.nextFieldId()

	return iceberg.MappedField{
		FieldID: &id,
		Names:   []string{f.Name},
		Fields:  v.unwrap(f.Type.ID(), mf),
	}
}

func (v *nameMappingArrowSchemaVisitor) List(lt arrow.ListLikeType, mf iceberg.MappedField) iceberg.MappedField {
	var elemId = v.nextFieldId()

	return iceberg.MappedField{
		Fields: []iceberg.MappedField{
			{
				Names:   []string{"element"},
				FieldID: &elemId,
				Fields:  v.unwrap(lt.ID(), mf),
			},
		},
	}
}

func (v *nameMappingArrowSchemaVisitor) Map(mt *arrow.MapType, keyResult iceberg.MappedField, valueResult iceberg.MappedField) iceberg.MappedField {
	var (
		keyId   = v.nextFieldId()
		valueId = v.nextFieldId()
	)

	return iceberg.MappedField{
		Fields: []iceberg.MappedField{
			{
				Names:   []string{"key"},
				FieldID: &keyId,
				Fields:  v.unwrap(mt.KeyType().ID(), keyResult),
			},
			{
				Names:   []string{"value"},
				FieldID: &valueId,
				Fields:  v.unwrap(mt.ValueType().ID(), valueResult),
			},
		},
	}
}

func (v *nameMappingArrowSchemaVisitor) Primitive(dt arrow.DataType) iceberg.MappedField {
	return iceberg.MappedField{}
}
