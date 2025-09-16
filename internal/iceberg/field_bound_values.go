package iceberg

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/agnosticeng/icepq/internal/io"
	"github.com/agnosticeng/objstr"
	"github.com/apache/iceberg-go"
	"github.com/samber/lo"
	"github.com/sourcegraph/conc/iter"
)

type FieldBoundValuesItem struct {
	FieldName string `json:"field_name"`
	FieldId   int    `json:"field_id"`
	FilePath  string `json:"file_path"`
	FileCount int64  `json:"file_count"`
	Lower     any    `json:"lower"`
	Upper     any    `json:"upper"`
}

func DecodeBoundValue(field iceberg.NestedField, v []byte) (any, error) {
	if len(v) == 0 {
		return nil, nil
	}

	switch field.Type {
	case iceberg.Int64Type{}:
		return int64(binary.LittleEndian.Uint64(v)), nil
	default:
		return nil, fmt.Errorf("unsupported type: %s", field.Type.String())
	}
}

type FieldBoundValuesConfig struct {
	FailOnDeleteFiles   bool
	FailOnMissingValues bool
}

func FieldBoundValues(
	ctx context.Context,
	tableLocation string,
	fieldName string,
	conf FieldBoundValuesConfig,
) ([]FieldBoundValuesItem, error) {
	var (
		os = objstr.FromContextOrDefault(ctx)
		io = io.NewObjectStoreIO(os)
	)

	cat, err := NewVersionHintCatalog(tableLocation)
	if err != nil {
		return nil, err
	}

	t, err := cat.LoadTable(ctx, nil, nil)
	if err != nil {
		return nil, err
	}

	field, found := t.Schema().FindFieldByName(fieldName)
	if !found {
		return nil, fmt.Errorf("field %s not found", fieldName)
	}

	mans, err := t.CurrentSnapshot().Manifests(io)
	if err != nil {
		return nil, err
	}

	res, err := iter.MapErr(mans, func(man *iceberg.ManifestFile) ([]FieldBoundValuesItem, error) {
		entries, err := (*man).FetchEntries(io, false)
		if err != nil {
			return nil, err
		}

		var res []FieldBoundValuesItem

		for _, entry := range entries {
			var contentType = entry.DataFile().ContentType()

			if conf.FailOnDeleteFiles &&
				(contentType == iceberg.EntryContentEqDeletes || contentType == iceberg.EntryContentPosDeletes) {
				return nil, fmt.Errorf("snapshot has delete files")
			}

			upper, found := entry.DataFile().UpperBoundValues()[field.ID]
			if conf.FailOnMissingValues && !found {
				return nil, fmt.Errorf("upper bound value not found for field %s in datafile %s", fieldName, entry.DataFile().FilePath())
			}

			lower, found := entry.DataFile().LowerBoundValues()[field.ID]
			if conf.FailOnMissingValues && !found {
				return nil, fmt.Errorf("lower bound value not found for field %s in datafile %s", fieldName, entry.DataFile().FilePath())
			}

			decodedLower, err := DecodeBoundValue(field, lower)
			if err != nil {
				return nil, err
			}

			decodedUpper, err := DecodeBoundValue(field, upper)
			if err != nil {
				return nil, err
			}

			res = append(res, FieldBoundValuesItem{
				FieldName: field.Name,
				FieldId:   field.ID,
				FilePath:  entry.DataFile().FilePath(),
				FileCount: entry.DataFile().Count(),
				Lower:     decodedLower,
				Upper:     decodedUpper,
			})
		}

		return res, nil
	})

	return lo.Flatten(res), err
}
