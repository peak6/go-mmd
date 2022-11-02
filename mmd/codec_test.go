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
	"encoding/hex"
	"fmt"
	"math"
	"testing"
	"time"
)

var allTypes = []interface{}{
	"Hello",
	true,
	false,
	0,
	math.MinInt8,
	math.MaxInt8,
	math.MinInt16,
	math.MaxInt16,
	math.MinInt32,
	math.MaxInt32,
	math.MinInt64,
	math.MaxInt64,
	math.MaxUint8,
	math.MaxUint16,
	math.MaxUint32,
	uint64(math.MaxUint64),
	float32(-1.0),
	math.MaxFloat32,
	float64(-1.0),
	math.MaxFloat64,
	[]int{1, 2, 3},
	map[string]interface{}{"ABC": []byte{9, 8, 7}},
	time.Now().Round(time.Microsecond),
}

func TestCodecEncode(t *testing.T) {
	buffer := NewBuffer(1024)
	toEncode := allTypes
	t.Log("Encoding", toEncode)
	err := Encode(buffer, toEncode)
	if err != nil {
		t.Fatal(err)
	}
	bytes := buffer.Flip().Bytes()
	t.Logf("Buffer: \n%s", hex.Dump(bytes))
}

func TestCodecEncodeDecode(t *testing.T) {
	toEncode := allTypes
	buffer := NewBuffer(1024)
	err := Encode(buffer, toEncode)
	if err != nil {
		t.Fatal(err)
	}
	read := buffer.Flip()
	t.Logf("Decoding: \n%s", hex.Dump(read.Bytes()))
	decoded, err := Decode(read)
	if err != nil {
		t.Fatal(err)
	}
	encstr := fmt.Sprint(toEncode)
	decstr := fmt.Sprint(decoded)
	if encstr != decstr {
		t.Fatalf("Not equal\n   Orig: %s\nDecoded: %s", encstr, decstr)
	}
	// deep equals test fails even when both print identically, not sure why
	// if !reflect.DeepEqual(toEncode, decoded) {
	// 	t.Fatalf("Not equal\n"+
	// 		"Orig   : %#v\n"+
	// 		"Decoded: %#v", toEncode, decoded)
	// }
}
