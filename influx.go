package influx

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
)

var emptyTags = map[string]string{} // always empty

func parseInt(i interface{}) int64 {
	switch v := reflect.ValueOf(i); v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(v.Uint())
	case reflect.Float32, reflect.Float64:
		return int64(v.Float())
	case reflect.String:
		val, err := strconv.ParseInt(v.String(), 10, 64)
		if err != nil { // maybe time
			t, err := time.Parse(time.RFC3339, v.String())
			if err == nil {
				return t.UnixNano()
			}
		}
		return val
	}
	return 0
}

func parseFloat(i interface{}) float64 {
	switch v := reflect.ValueOf(i); v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(v.Uint())
	case reflect.Float32, reflect.Float64:
		return v.Float()
	case reflect.String:
		val, _ := strconv.ParseFloat(v.String(), 64)
		return val
	}
	return 0
}

func parseString(i interface{}) string {
	switch v := reflect.ValueOf(i); v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(v.Float(), 'E', -1, 64)
	case reflect.String:
		return v.String()
	case reflect.Interface:
		return ""
	}
	// Stringer or Error
	return fmt.Sprint(i)
}

func parseTime(i interface{}) time.Time {
	if s, ok := i.(string); ok && s != "" {
		t, _ := time.Parse(time.RFC3339, s)
		return t
	}

	var nano int64
	switch v := reflect.ValueOf(i); v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		nano = v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		nano = int64(v.Uint())
	case reflect.Float32, reflect.Float64:
		nano = int64(v.Float())
	}
	return time.Unix(nano/1e9, nano%1e9)
}

// MeasurementName --> measurement_name
func titleToSnake(s string) string {
	r := []rune(s)
	lastIsUpper := true
	for i := 0; i < len(r); i++ {
		if unicode.IsUpper(r[i]) {
			if !lastIsUpper {
				r = append(append(r[:i:i], '_'), r[i:]...)
				i++
				lastIsUpper = true
			}
			r[i] = unicode.ToLower(r[i])
		} else {
			lastIsUpper = false
		}
	}
	return string(r)
}

func snakeToTitle(s string) string {
	segs := strings.Split(s, "_")
	for i := range segs {
		segs[i] = strings.Title(segs[i])
	}
	return strings.Join(segs, "")
}

func makePtrDstVal(dst reflect.Value) reflect.Value {
	for dst.Kind() == reflect.Ptr && dst.IsNil() {
		dst.Set(reflect.New(dst.Type().Elem()))
		dst = reflect.Indirect(dst)
	}
	return dst
}

func makeSliceDstVal(dst reflect.Value, n int) reflect.Value {
	if dst.Len() < n {
		ori := dst
		dst.Set(reflect.MakeSlice(ori.Type(), n, n))
		reflect.Copy(dst, ori)
	}
	return dst
}

func alignToStruct(cols []string, vals []interface{}, tags map[string]string, dst reflect.Value, columns ...string) error {
	if dst.Type().String() == "time.Time" {
		dst.Set(reflect.ValueOf(parseTime(vals[0])))
		return nil
	}
	if len(cols) != len(vals) {
		return errors.New("columns size not equal values size")
	}

	typ := dst.Type()
	parse := func(col string, val interface{}) error {
		if !inColumns(col, columns) {
			return nil
		}
		var field reflect.Value
		for f := 0; f < typ.NumField(); f++ {
			if strings.Split(typ.Field(f).Tag.Get("inf"), ",")[0] == col {
				field = dst.Field(f)
				break
			}
		}
		if !field.CanSet() {
			field = dst.FieldByName(snakeToTitle(col))
		}
		if !field.CanSet() {
			return nil
		}

		return parseSingle([]string{col}, []interface{}{val}, emptyTags, field)
	}

	for i, col := range cols {
		if err := parse(col, vals[i]); err != nil {
			return err
		}
	}
	for t, v := range tags {
		if err := parse(t, v); err != nil {
			return err
		}
	}
	return nil
}

func alignToSlice(cols []string, vals []interface{}, tags map[string]string, dst reflect.Value, columns ...string) error {
	if len(columns) > 0 {
		var val interface{}
		dst = makeSliceDstVal(dst, len(columns))
		for i, col := range columns {
			idx := columnIndex(col, cols)
			if idx >= 0 {
				val = vals[idx]
			} else if v, ok := tags[col]; ok {
				val = v
			} else {
				continue
			}
			if err := parseSingle([]string{col}, []interface{}{val}, emptyTags, dst.Index(i), columns[i]); err != nil {
				return err
			}
		}
	} else {
		// ignore tags
		dst = makeSliceDstVal(dst, len(cols))
		for i := range cols {
			if err := parseSingle(cols[i:i+1], vals[i:i+1], emptyTags, dst.Index(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func alignToMap(cols []string, vals []interface{}, tags map[string]string, dst reflect.Value, columns ...string) error {
	if dst.Type().Key().Kind() != reflect.String {
		return errors.New("invalid key type")
	}
	if len(cols) != len(vals) {
		return errors.New("columns size not equal values size")
	}

	parse := func(k string, v interface{}) error {
		if !inColumns(k, columns) {
			return nil
		}
		val := reflect.Indirect(reflect.New(dst.Type().Elem()))
		if err := parseSingle([]string{k}, []interface{}{v}, emptyTags, val); err != nil {
			return err
		}
		dst.SetMapIndex(reflect.ValueOf(k), val)
		return nil
	}

	for i, col := range cols {
		if err := parse(col, vals[i]); err != nil {
			return err
		}
	}

	for t, v := range tags {
		if err := parse(t, v); err != nil {
			return err
		}
	}
	return nil
}

func parseSingle(cols []string, vals []interface{}, tags map[string]string, dst reflect.Value, columns ...string) error {
	if len(cols) == 0 {
		return nil
	}

	var val interface{} = vals[0]
	if len(columns) > 0 {
		idx := columnIndex(columns[0], cols)
		if idx >= 0 {
			val = vals[idx]
		} else if v, ok := tags[columns[0]]; ok {
			val = v
		}
	}

	switch dst.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dst.SetInt(parseInt(val))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		dst.SetUint(uint64(parseInt(val)))
	case reflect.Float32, reflect.Float64:
		dst.SetFloat(parseFloat(val))
	case reflect.String:
		dst.SetString(parseString(val))
	case reflect.Ptr:
		return parseSingle(cols, vals, tags, makePtrDstVal(dst), columns...)
	case reflect.Struct:
		return alignToStruct(cols, vals, tags, dst, columns...)
	case reflect.Slice:
		return alignToSlice(cols, vals, tags, dst, columns...)
	case reflect.Map:
		if dst.IsNil() {
			dst.Set(reflect.MakeMap(dst.Type()))
		}
		return alignToMap(cols, vals, tags, dst, columns...)
	case reflect.Interface:
		if len(vals) == 1 || len(columns) == 1 {
			dst.Set(reflect.ValueOf(val))
		} else {
			mp := reflect.MakeMap(reflect.MapOf(reflect.TypeOf(""), dst.Type()))
			dst.Set(mp)
			return alignToMap(cols, vals, tags, mp, columns...)
		}
	default:
		return errors.New("unrecognized type")
	}
	return nil
}

func columnIndex(column string, columns []string) int {
	if len(columns) == 0 {
		return 0
	}
	for i, col := range columns {
		if column == col {
			return i
		}
	}
	return -1
}

func inColumns(column string, columns []string) bool {
	if len(columns) == 0 {
		return true // namely all
	}
	for _, col := range columns {
		if column == col {
			return true
		}
	}
	return false
}

func ParseResult(dst interface{}, serie models.Row, columns ...string) error {
	cols := serie.Columns
	vals := serie.Values
	tags := serie.Tags
	if tags == nil {
		tags = make(map[string]string)
	}
	if len(columns) == 1 {
		if _, ok := tags[columns[0]]; !ok && !inColumns(columns[0], serie.Columns) {
			return fmt.Errorf("column not exists: `%v`", columns[0])
		}
	}

	dstVal := reflect.Indirect(reflect.ValueOf(dst))
	if !dstVal.CanSet() {
		return errors.New("dst cannot be setted")
	}
	dstVal = makePtrDstVal(dstVal)

	toslice := func(dstVal reflect.Value) error {
		for i, vs := range vals {
			if err := parseSingle(cols, vs, tags, dstVal.Index(i), columns...); err != nil {
				return err
			}
		}
		return nil
	}

	switch dstVal.Kind() {
	case reflect.Interface:
		if len(vals) == 0 {
			return nil
		}
		if len(vals) == 1 {
			return parseSingle(cols, vals[0], tags, dstVal)
		}
		slice := reflect.MakeSlice(reflect.SliceOf(dstVal.Type()), len(vals), len(vals))
		if err := toslice(slice); err != nil {
			return err
		}
		dstVal.Set(slice)
	case reflect.Slice:
		dstVal = makeSliceDstVal(dstVal, len(vals))
		if err := toslice(dstVal); err != nil {
			return err
		}
	default:
		return parseSingle(cols, vals[0], tags, dstVal, columns...)
	}
	return nil
}

// - - - - - - - - - - - - - - - - - - - - -

func ToPoint(structure interface{}) *client.Point {
	val := reflect.ValueOf(structure)
	method := val.MethodByName("Measurement")
	val = reflect.Indirect(val)
	if val.Kind() != reflect.Struct {
		return nil
	}
	if !method.IsValid() {
		method = val.MethodByName("Measurement")
	}

	measurement := ""
	if method.IsValid() {
		measurement = method.Call(nil)[0].Interface().(string)
	} else {
		name := val.Type().Name()
		if idx := strings.LastIndexByte(name, '.'); idx >= 0 {
			name = name[idx+1:]
		}
		measurement = titleToSnake(name)
	}

	typ := val.Type()
	tags := make(map[string]string)
	fields := make(map[string]interface{})
	now := time.Now()

	for i := 0; i < val.NumField(); i++ {
		fv := val.Field(i)
		ft := typ.Field(i)
		if ft.Tag.Get("inf") == "-" {
			continue
		}
		tagstr := ft.Tag.Get("inf")
		if ft.Name == "Time" || tagstr == "time" {
			now = fv.Interface().(time.Time)
			continue
		}

		name := strings.Split(tagstr, ",")[0]
		if name == "" {
			name = titleToSnake(ft.Name)
		}
		if strings.HasSuffix(tagstr, ",tag") {
			tags[name] = parseString(fv.Interface())
		} else { // fields
			fields[name] = fv.Interface()
		}
	}
	point, _ := client.NewPoint(measurement, tags, fields, now)
	return point
}
