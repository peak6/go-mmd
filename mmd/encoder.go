/*-----------------------------------------------------------------------

Copyright 2022 PEAK6 INVESTMENTS LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

-----------------------------------------------------------------------*/

package mmd

import (
	"encoding"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"
)

// Encode serializes 'thing' into buffer
func Encode(buffer *Buffer, thing interface{}) error {
	switch i := thing.(type) {
	case nil:
		buffer.WriteByte('N')
	case string:
		buffer.WriteByte('s')
		writeSz(buffer, len(i))
		buffer.WriteString(i)
	case ChannelCreate:
		buffer.WriteByte('c')
		buffer.Write([]byte(i.ChannelId))
		switch i.Type {
		case CallChan:
			buffer.WriteByte('C')
		case SubChan:
			buffer.WriteByte('S')
		default:
			return fmt.Errorf("Unknown type: %v", i.Type)
		}
		buffer.WriteByte(byte(len(i.Service)))
		buffer.WriteString(i.Service)
		ta := make([]byte, 2)
		binary.BigEndian.PutUint16(ta, uint16(i.Timeout))
		buffer.Write(ta)
		buffer.Write([]byte(i.AuthToken))
		Encode(buffer, i.Body)
	case ChannelClose:
		buffer.WriteByte('X')
		buffer.Write([]byte(i.ChannelId))
		Encode(buffer, i.Body)
	case ChannelMsg:
		if i.IsClose {
			buffer.WriteByte('X')
		} else {
			buffer.WriteByte('M')
		}
		buffer.Write([]byte(i.Channel))
		Encode(buffer, i.Body)
	case float32:
		buffer.WriteByte('d')
		buffer.WriteFloat32(i)
	case float64:
		buffer.WriteByte('D')
		buffer.WriteFloat64(i)
	case int:
		return encodeInt(buffer, int64(i))
	case uint:
		return encodeUint(buffer, uint64(i))
	case time.Time:
		buffer.WriteByte('z')
		buffer.WriteInt64(int64(i.UnixNano() / 1000))
	case []byte:
		buffer.WriteByte('q')
		writeSz(buffer, len(i))
		buffer.Write(i)
	case []interface{}: // common case, don't reflect
		buffer.WriteByte('a')
		writeSz(buffer, len(i))
		for _, item := range i {
			err := Encode(buffer, item)
			if err != nil {
				return fmt.Errorf("Error encoding: %v - %v", item, err)
			}
		}
	case net.IPAddr:
		return Encode(buffer, i.String())
	case bool:
		if i {
			buffer.WriteByte('T')
		} else {
			buffer.WriteByte('F')
		}
	default:
		return reflectEncode(thing, buffer)
	}
	return nil
}

func encodeInt(buffer *Buffer, i int64) error {
	if i == 0 {
		buffer.WriteByte(0)
	} else if i >= math.MinInt8 && i <= math.MaxInt8 {
		buffer.WriteByte(0x01)
		buffer.WriteByte(byte(i))
	} else if i >= math.MinInt16 && i <= math.MaxInt16 {
		buffer.WriteByte(0x02)
		buffer.order.PutUint16(buffer.GetWritable(2), uint16(i))
	} else if i >= math.MinInt32 && i <= math.MaxInt32 {
		buffer.WriteByte(0x04)
		buffer.order.PutUint32(buffer.GetWritable(4), uint32(i))
	} else if i >= math.MinInt64 && i <= math.MaxInt64 {
		buffer.WriteByte(0x08)
		buffer.order.PutUint64(buffer.GetWritable(8), uint64(i))
	} else {
		return fmt.Errorf("Don't know how to encode int(%d)", i)
	}
	return nil
}

func encodeUint(buffer *Buffer, i uint64) error {
	if i == 0 {
		buffer.WriteByte(0)
	} else if i <= math.MaxUint8 {
		buffer.WriteByte(0x11)
		buffer.WriteByte(byte(i))
	} else if i <= math.MaxUint16 {
		buffer.WriteByte(0x12)
		buffer.order.PutUint16(buffer.GetWritable(2), uint16(i))
	} else if i <= math.MaxUint32 {
		buffer.WriteByte(0x14)
		buffer.order.PutUint32(buffer.GetWritable(4), uint32(i))
	} else if i <= math.MaxUint64 {
		buffer.WriteByte(0x18)
		buffer.order.PutUint64(buffer.GetWritable(8), i)
	} else {
		return fmt.Errorf("Don't know how to encode int(%d)", i)
	}
	return nil
}

type fieldInfo struct {
	key       string
	num       int
	omitEmpty bool
}
type structInfo struct {
	size   int
	fields []fieldInfo
}

var structMap = make(map[reflect.Type]*structInfo)
var structMapLock sync.RWMutex

func getStructInfo(st reflect.Type) *structInfo {
	structMapLock.RLock()
	sinfo, ok := structMap[st]
	structMapLock.RUnlock()
	if ok {
		return sinfo
	}
	var si structInfo
	for i := 0; i < st.NumField(); i++ {
		f := st.Field(i)
		if f.PkgPath != "" {
			continue
		}

		si.size++
		fi := fieldInfo{key: f.Name, num: i}
		n := f.Tag.Get("mmd")
		if n == "" {
			n = f.Tag.Get("json")
		}
		if n != "" {
			if n == "-" {
				continue
			}
			parts := strings.Split(n, ",")
			if parts[0] != "" {
				fi.key = parts[0]
			}
			if len(parts) > 1 {
				for _, p := range parts[1:] {
					if p == "omitempty" {
						fi.omitEmpty = true
					}
				}
			}
		}
		si.fields = append(si.fields, fi)
	}
	structMapLock.Lock()
	structMap[st] = &si
	structMapLock.Unlock()
	return &si
}

type zeroChecker interface {
	IsZero() bool
}

func fieldByIndex(src reflect.Value, idx int) reflect.Value {
	if src.Kind() == reflect.Ptr {
		if src.IsNil() {
			return reflect.Value{}
		}
		src = src.Elem()
	}
	return src.Field(idx)
}
func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	case reflect.Struct:
		t, ok := v.Interface().(zeroChecker)
		if ok {
			return t.IsZero()
		} else {
			panic(fmt.Sprintf("don't know how to check empty struct: %v", v))
		}
	}
	return false
}
func reflectEncode(thing interface{}, buffer *Buffer) error {
	tm, ok := thing.(encoding.TextMarshaler)
	if ok {
		b, err := tm.MarshalText()
		if err != nil {
			return err
		}
		return Encode(buffer, string(b))
	}
	val := reflect.ValueOf(thing)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	kind := val.Kind()
	switch kind {
	case reflect.Struct:
		start := buffer.GetPos()
		buffer.WriteByte('r')
		buffer.WriteByte(0x04)
		si := getStructInfo(val.Type())

		// Store the absolute position since buffer can resize and reserve space to add size later
		sz_pos := buffer.GetPos()
		buffer.GetWritable(4)

		count := 0
		for _, f := range si.fields {
			fv := fieldByIndex(val, f.num)
			if !fv.IsValid() || (f.omitEmpty && isEmptyValue(fv)) {
				continue
			}
			count++
			err := Encode(buffer, f.key)
			if err != nil {
				return err
			}
			err = Encode(buffer, fv.Interface())
			if err != nil {
				return err
			}

		}
		if count > 0 {
			// Fill in size at the reserved space
			cur_pos := buffer.GetPos()
			buffer.Position(sz_pos)
			buffer.order.PutUint32(buffer.GetWritable(4), uint32(count))
			buffer.Position(cur_pos)
		} else {
			buffer.Position(start)
			buffer.WriteByte('N')
		}
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return encodeInt(buffer, val.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return encodeUint(buffer, val.Uint())
	case reflect.String:
		return Encode(buffer, val.String())
	case reflect.Slice, reflect.Array:
		buffer.WriteByte('a')
		buffer.WriteByte(0x04)
		buffer.order.PutUint32(buffer.GetWritable(4), uint32(val.Len()))
		for i := 0; i < val.Len(); i++ {
			item := val.Index(i)
			if !item.CanInterface() {
				return fmt.Errorf("Can't Interface() %s", val)
			}
			err := Encode(buffer, item.Interface())
			if err != nil {
				return err
			}
		}
		return nil
	case reflect.Map:
		buffer.WriteByte('r') // fast map
		writeSz(buffer, val.Len())
		for _, k := range val.MapKeys() {
			ki := k.Interface()
			vi := val.MapIndex(k).Interface()
			err := Encode(buffer, ki)
			if err != nil {
				return err
			}
			err = Encode(buffer, vi)
			if err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("Don't know how to encode (%s) %v", kind, thing)
	}
}

func writeSz(buffer *Buffer, sz int) {
	buffer.WriteByte(0x04)
	buffer.WriteInt32(int32(sz))
}
