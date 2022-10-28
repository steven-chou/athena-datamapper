package athenaconv

import (
	"context"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/pkg/errors"
)

type dataMapper struct {
	modelType             reflect.Type
	modelDefinitionSchema modelDefinitionMap
}

// DataMapper provides abstraction to convert athena ResultSet object to arbitrary user-defined struct
type DataMapper interface {
	FromAthenaResultSetV2(ctx context.Context, input *types.ResultSet) ([]interface{}, error)
}

// NewMapperFor creates new DataMapper for given reflect.Type
// reflect.Type should be of struct value type, not pointer to struct.
//
// Example:
//
// mapper, err := athenaconv.NewMapperFor(reflect.TypeOf(MyStruct{}))
func NewMapperFor(modelType reflect.Type) (DataMapper, error) {
	modelDefinitionSchema, err := newModelDefinitionMap(modelType)
	if err != nil {
		return nil, err
	}

	mapper := &dataMapper{
		modelType:             modelType,
		modelDefinitionSchema: modelDefinitionSchema,
	}
	return mapper, nil
}

// FromAthenaResultSetV2 converts ResultSet from aws-sdk-go-v2/service/athena/types into strongly-typed array[mapper.modelType]
// Returns conversion error if header values are passed, i.e. first row of your athena ResultSet in page 1.
// Returns error if the athena ResultSetMetadata does not match the mapper definition.
//
// Example:
// if page == 1 && len(queryResultOutput.ResultSet.Rows) > 0 {
// 		queryResultOutput.ResultSet.Rows = queryResultOutput.ResultSet.Rows[1:]		// skip header row
// }
// mapped, err := mapper.FromAthenaResultSetV2(ctx, queryResultOutput.ResultSet)
func (m *dataMapper) FromAthenaResultSetV2(ctx context.Context, resultSet *types.ResultSet) ([]interface{}, error) {
	resultSetSchema, err := newResultSetDefinitionMap(ctx, resultSet.ResultSetMetadata)
	if err != nil {
		return nil, err
	}

	err = validateResultSetSchema(ctx, resultSetSchema, m.modelDefinitionSchema)
	if err != nil {
		return nil, err
	}

	fieldNameToStructField := make(map[string]reflect.StructField)

	result := make([]interface{}, 0)
	for _, row := range resultSet.Rows {
		model := reflect.New(m.modelType)
		for athenaColName, modelDefColInfo := range m.modelDefinitionSchema {
			mappedColumnInfo := resultSetSchema[athenaColName]
			fieldName := modelDefColInfo.fieldName

			var targetVal interface{}
			fieldData := row.Data[mappedColumnInfo.index]

			if fieldData.VarCharValue != nil {
				// log.Printf("SET model.%s = row.Data[%d] with athena col name = '%s'", fieldName, mappedColumnInfo.index, athenaColName)
				targetVal, err = castAthenaRowData(ctx, fieldData, mappedColumnInfo.athenaColumnType)
				if err != nil {
					return nil, err
				}
			}
			var sf reflect.StructField
			if v, ok := fieldNameToStructField[fieldName]; !ok {
				sf, _ = m.modelType.FieldByName(fieldName)
				fieldNameToStructField[fieldName] = sf
			} else {
				sf = v
			}

			err := setRowFieldValToTargetField(model, sf, targetVal)
			if err != nil {
				return nil, err
			}
		}

		result = append(result, model.Interface())
	}

	return result, nil
}

func setRowFieldValToTargetField(targetObj reflect.Value, targetField reflect.StructField, val interface{}) error {
	if val == nil {
		// ignore the nil val, so the target field will remain as initial zero value
		return nil
	}

	srcVal := reflect.ValueOf(val)

	if srcVal.Kind() == targetField.Type.Kind() {
		targetObj.Elem().FieldByName(targetField.Name).Set(srcVal)
		return nil
	}

	// src and target are different types
	switch targetField.Type.Kind() {
	case reflect.Pointer:
		switch srcVal.Kind() {
		case reflect.String, reflect.Bool,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64, reflect.Struct:
			v := reflect.New(targetField.Type.Elem())
			v.Elem().Set(srcVal)
			targetObj.Elem().FieldByName(targetField.Name).Set(v)
		default:
			return errors.Errorf("can't set source value %v type to the ptr field", srcVal.Kind().String())
		}
	case reflect.String, reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Struct:
		switch srcVal.Kind() {
		case reflect.Pointer:
			targetObj.Elem().FieldByName(targetField.Name).Set(reflect.Indirect(srcVal))
		default:
			return errors.Errorf("can't set source value %v type to the %v field", srcVal.Kind().String(), targetField.Type.Kind().String())
		}
	default:
		return errors.Errorf("incompatible source value and target field type: [%v -> %v]", srcVal.Kind().String(), targetField.Type.Kind().String())
	}

	return nil
}
