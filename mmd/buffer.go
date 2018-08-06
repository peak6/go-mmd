package mmd

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
)

type Buffer struct {
	data  []byte
	index int
	order binary.ByteOrder
}

//Creates a new buffer of the specified size, with index of zero and big endian byte order
func NewBuffer(size int) *Buffer {
	return Wrap(make([]byte, size))
}

//Wraps a given []byte, defaults to index 0 and big endian byte order
func Wrap(bytes []byte) *Buffer {
	return &Buffer{bytes, 0, binary.BigEndian}
}

//Returns a new buffer who's limit is the index of this current buffer
func (b *Buffer) Flip() *Buffer {
	return &Buffer{b.data[:b.index], 0, b.order}
}

func (b *Buffer) Clear() {
	b.index = 0
}

// Returns a new buffer using the same backing slice,
// but with an independant index and byte order
func (b *Buffer) Duplicate() *Buffer {
	return &Buffer{b.data, b.index, b.order}
}

func (b *Buffer) Bytes() []byte {
	return b.data
}

func (b *Buffer) BytesRemaining() []byte {
	return b.Bytes()[b.index:]
}

func (buff *Buffer) WriteByte(b byte) error {
	buff.ensureSpace(1)
	buff.data[buff.index] = b
	buff.index++
	return nil
}

func (b *Buffer) ReadByte() (ret byte, err error) {
	ret = b.data[b.index]
	b.index++
	return
}

func (b *Buffer) Compact() {
	copy(b.data, b.data[b.index:])
	b.index = 0

}

func (b *Buffer) Write(bytes []byte) error {
	l := len(bytes)
	b.ensureSpace(l)
	copy(b.data[b.index:], bytes)
	b.index += l
	return nil
}

func (b *Buffer) WriteString(str string) {
	l := len(str)
	b.ensureSpace(l)
	copy(b.data[b.index:], str)
	b.index += l
}

func (b *Buffer) Position(idx int) {
	b.index = idx
}
func (b *Buffer) GetPos() int {
	return b.index
}
func (b *Buffer) ReadString(sz int) (string, error) {
	bytes, err := b.Next(sz)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func (b *Buffer) GetWritable(sz int) []byte {
	return b.data[b.advance(sz):b.index]
}
func (b *Buffer) String() string {
	return fmt.Sprintf("Buffer{index: %d, len: %d, cap: %d, order: %v}", b.index, len(b.data), cap(b.data), b.order)
}

func (b *Buffer) DumpRemaining() string {
	return hex.Dump(b.BytesRemaining())
}

func (b *Buffer) Next(n int) ([]byte, error) {
	if b.index+n > len(b.data) {
		return nil, fmt.Errorf("Can't read %d bytes from %s", n, b)
	}
	ret := make([]byte, n)
	start := b.index
	b.index += n
	copy(ret, b.data[start:b.index])
	return ret, nil
}

func (b *Buffer) advance(sz int) int {
	b.ensureSpace(sz)
	ret := b.index
	b.index += sz
	return ret
}

func (b *Buffer) ensureSpace(sz int) {
	need := b.index + sz
	for cap(b.data) <= need {
		b.doubleCapacity()
	}
}

func (b *Buffer) doubleCapacity() {
	newCapacity := cap(b.data) * 2
	if newCapacity == 0 {
		newCapacity = 1
	}
	tmp := make([]byte, newCapacity)
	copy(tmp, b.data)
	b.data = tmp
}

func (b *Buffer) ReadVaruint() (uint, error) {
	u, err := binary.ReadUvarint(b)
	return uint(u), err
}

func (b *Buffer) ReadVarint() (int, error) {
	i, err := binary.ReadVarint(b)
	return int(i), err
}
func (b *Buffer) WriteInt64(i int64) {
	b.order.PutUint64(b.GetWritable(8), uint64(i))
}
func (b *Buffer) WriteInt32(i int32) {
	b.order.PutUint32(b.GetWritable(4), uint32(i))
}
func (b *Buffer) WriteFloat32(f float32) {
	binary.BigEndian.PutUint32(b.GetWritable(4), math.Float32bits(f))
}
func (b *Buffer) WriteFloat64(f float64) {
	binary.BigEndian.PutUint64(b.GetWritable(8), math.Float64bits(f))
}
func (b *Buffer) ReadInt64() (int64, error) {
	v, err := b.Next(8)
	if err != nil {
		return 0, err
	}
	return int64(b.order.Uint64(v)), nil
}
