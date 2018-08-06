package mmd

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"time"
)

const microsInSecond = 1000 * 1000

// Decode decodes the next mmd message from a given buffer
func Decode(buff *Buffer) (interface{}, error) {
	tag, err := buff.ReadByte()
	if err != nil {
		return nil, err
	}
	fn := decodeTable[tag]
	if fn == nil {
		return nil, fmt.Errorf("Unsupported type tag: %c:%d", tag, tag)
	}
	return fn(tag, buff)
}

var decodeTable []func(byte, *Buffer) (interface{}, error)

func init() { // this init() is here due to compiler bug(?) on init loop
	decodeTable = []func(byte, *Buffer) (interface{}, error){
		0x00: decodeFastInt,
		0x01: decodeFastInt,
		0x02: decodeFastInt,
		0x04: decodeFastInt,
		0x08: decodeFastInt,
		0x10: decodeFastUInt,
		0x11: decodeFastUInt,
		0x12: decodeFastUInt,
		0x14: decodeFastUInt,
		0x18: decodeFastUInt,
		'S':  varString,
		's':  fastString,
		'C':  decodeVarIntCreate,
		'X':  decodeClose,
		'M':  decodeMessage,
		'i':  decodeVaruint,
		'I':  decodeVarint,
		'l':  decodeVaruint,
		'L':  decodeVarint,
		'r':  fastMap,
		'm':  varIntMap,
		'a':  fastArray,
		'A':  varIntArray,
		'b':  varBytes,
		'q':  fastBytes,
		'e':  fastError,
		'E':  varError,
		// 'a':  ack,
		// 'p':  pong,
		// 'P':  ping,
		'D': decodeDouble,
		'd': decodeFloat,
		'N': decodeNil,
		'T': decodeTrue,
		'F': decodeFalse,
		'U': decodeUUID,
		'$': decodeSecID,
		'B': decodeByte,
		'#': decodeVarintTime,
		'z': decodeFastTime,
	}
}

func decodeVarIntCreate(tag byte, buff *Buffer) (interface{}, error) {

	chanID, err := decodeChanID(buff)
	if err != nil {
		return nil, err
	}
	chanType, err := decodeChannelType(buff)
	if err != nil {
		return nil, err
	}
	service, err := readVarString(buff)
	if err != nil {
		return nil, err
	}
	timeout, err := buff.ReadVarint()
	if err != nil {
		return nil, err
	}
	authToken, err := decodeAuthToken(buff)
	if err != nil {
		return nil, err
	}
	body, err := Decode(buff)
	if err != nil {
		return nil, err
	}
	return ChannelCreate{
		ChannelId: ChannelId(chanID),
		Type:      chanType,
		Service:   service,
		Timeout:   int64(timeout),
		AuthToken: AuthToken(authToken),
		Body:      body,
	}, nil
}

func decodeFastInt(tag byte, buff *Buffer) (interface{}, error) {
	return fastInt(tag&0x0F, buff)
}
func decodeFastUInt(tag byte, buff *Buffer) (interface{}, error) {
	return fastUInt(tag&0x0F, buff)
}

func decodeVarintTime(tag byte, buff *Buffer) (ret interface{}, err error) {
	t, err := buff.ReadVarint()
	if err == nil {
		ret = convertTime(int64(t))
	}
	return
}

func convertTime(t int64) time.Time {
	sec := t / microsInSecond
	nsec := (t % microsInSecond) * 1000
	return time.Unix(sec, nsec)

}

func decodeFastTime(tag byte, buff *Buffer) (ret interface{}, err error) {
	t, err := buff.ReadInt64()
	if err != nil {
		return
	}
	ret = convertTime(t)
	return
}
func decodeChannelType(buff *Buffer) (ChannelType, error) {
	b, err := buff.ReadByte()
	if err != nil {
		return CallChan, err
	}
	switch b {
	case 'C':
		return CallChan, nil
	case 'S':
		return SubChan, nil
	default:
		return CallChan, fmt.Errorf("Unknown channel type; %c:%d", b, b)
	}
}
func decodeByte(tag byte, buff *Buffer) (interface{}, error) {
	return buff.ReadByte()
}
func decodeChanID(buff *Buffer) (ChannelId, error) {
	b, err := buff.Next(16)
	if err != nil {
		return "<ERROR>", err
	}
	return ChannelId(b), nil
}
func decodeAuthToken(buff *Buffer) (AuthToken, error) {
	b, err := buff.Next(16)
	if err != nil {
		return "<ERROR>", err
	}
	return AuthToken(b), nil
}
func decodeUUID(tag byte, buff *Buffer) (interface{}, error) {
	return buff.Next(16)
}
func decodeSecID(tag byte, buff *Buffer) (interface{}, error) {
	return buff.Next(16)
}
func decodeNil(tag byte, buff *Buffer) (interface{}, error) {
	return nil, nil
}
func decodeTrue(tag byte, buff *Buffer) (interface{}, error) {
	return true, nil
}
func decodeFalse(tag byte, buff *Buffer) (interface{}, error) {
	return false, nil
}

func decodeFloat(tag byte, buff *Buffer) (ret interface{}, err error) {
	v, err := buff.Next(4)
	if err == nil {
		ret = math.Float32frombits(buff.order.Uint32(v))
	}
	return
}
func decodeDouble(tag byte, buff *Buffer) (ret interface{}, err error) {
	v, err := buff.Next(8)
	if err == nil {
		ret = math.Float64frombits(buff.order.Uint64(v))
	}
	return
}
func fastError(tag byte, buff *Buffer) (ret interface{}, err error) {
	code, err := taggedFastInt(buff)
	if err != nil {
		return nil, err
	}
	body, err := Decode(buff)
	if err != nil {
		return nil, err
	}
	return MMDError{code, body}, nil
}

func varError(tag byte, buff *Buffer) (interface{}, error) {
	code, err := varInt(buff)
	if err != nil {
		return nil, err
	}
	body, err := Decode(buff)
	if err != nil {
		return nil, err
	}
	return MMDError{code, body}, nil
}

func fastBytes(tag byte, buff *Buffer) (interface{}, error) {
	sz, err := fastSz(buff)
	if err != nil {
		return nil, err
	}
	return buff.Next(sz)
}

func varBytes(tag byte, buff *Buffer) (interface{}, error) {
	sz, err := buff.ReadVaruint()
	if err != nil {
		return nil, err
	}
	return buff.Next(int(sz))
}

func fastMap(tag byte, buff *Buffer) (interface{}, error) {
	sz, err := fastSz(buff)
	if err != nil {
		return nil, err
	}
	return decodeMap(buff, uint(sz))
}

func varIntMap(tag byte, buff *Buffer) (interface{}, error) {
	sz, err := buff.ReadVaruint()
	if err != nil {
		return nil, err
	}
	return decodeMap(buff, sz)
}

func decodeMap(buff *Buffer, num uint) (interface{}, error) {
	if num < 0 {
		str := hex.Dump(buff.data[:200])
		return nil, fmt.Errorf("Invalid map size: %d in:\n%s", num, str)
	}
	ret := make(map[interface{}]interface{}, num)
	var i uint
	for ; i < num; i++ {
		key, kerr := Decode(buff)
		if kerr != nil {
			return nil, fmt.Errorf("Failed to decode key: %s", kerr)
		}
		val, verr := Decode(buff)
		if verr != nil {
			return nil, fmt.Errorf("Failed to decode value: %s", verr)
		}
		ret[key] = val
	}
	return ret, nil
}

func fastArray(tag byte, buff *Buffer) (interface{}, error) {
	sz, err := fastSz(buff)
	if err != nil {
		return nil, err
	}
	return decodeArray(buff, sz)
}

func varIntArray(tag byte, buff *Buffer) (interface{}, error) {
	sz, err := varSz(buff)
	if err != nil {
		return nil, err
	}
	return decodeArray(buff, sz)
}

func decodeArray(buff *Buffer, num int) (interface{}, error) {
	ret := make([]interface{}, num)
	for i := 0; i < num; i++ {
		obj, err := Decode(buff)
		if err != nil {
			return nil, err
		}
		ret[i] = obj
	}
	return ret, nil
}

func decodeVarint(tag byte, buff *Buffer) (interface{}, error) {
	return varInt(buff)
}
func decodeVaruint(tag byte, buff *Buffer) (interface{}, error) {
	return varUInt(buff)
}

func varInt(buff *Buffer) (ret int, err error) {
	if n, err := binary.ReadVarint(buff); err == nil {
		ret = int(n)
	}
	return
}
func varUInt(buff *Buffer) (ret uint, err error) {
	if n, err := binary.ReadUvarint(buff); err == nil {
		ret = uint(n)
	}
	return
}

func taggedFastInt(buff *Buffer) (ret int, err error) {
	if tag, err := buff.ReadByte(); err == nil {
		ret, err = fastInt(tag, buff)
	}
	return
}
func taggedFastUInt(buff *Buffer) (ret uint, err error) {
	if tag, err := buff.ReadByte(); err == nil {
		ret, err = fastUInt(tag, buff)
	}
	return
}
func fastInt(sz byte, buff *Buffer) (ret int, err error) {
	switch sz {
	case 0x00:
		ret = int(0)
	case 0x01:
		var v byte
		v, err = buff.ReadByte()
		ret = int(int8(v))
	case 0x02:
		var v []byte
		v, err = buff.Next(2)
		ret = int(int16(buff.order.Uint16(v)))
	case 0x04:
		var v []byte
		v, err = buff.Next(4)
		ret = int(int32(buff.order.Uint32(v)))
	case 0x08:
		var v []byte
		v, err = buff.Next(8)
		ret = int(int64(buff.order.Uint64(v)))
	}
	return
}

func fastUInt(sz byte, buff *Buffer) (ret uint, err error) {
	switch sz {
	case 0x00:
		ret = 0
	case 0x01:
		var v byte
		if v, err = buff.ReadByte(); err == nil {
			ret = uint(v)
		}
	case 0x02:
		var v []byte
		if v, err = buff.Next(2); err == nil {
			ret = uint(buff.order.Uint16(v))
		}
	case 0x04:
		var v []byte
		if v, err = buff.Next(4); err == nil {
			ret = uint(buff.order.Uint32(v))
		}
	case 0x08:
		var v []byte
		if v, err = buff.Next(8); err == nil {
			ret = uint(buff.order.Uint64(v))
		}
	}
	return
}

func varString(tag byte, buff *Buffer) (interface{}, error) {
	return readVarString(buff)
}

func readVarString(buff *Buffer) (string, error) {
	sz, err := binary.ReadUvarint(buff)
	if err != nil {
		return "", err
	}
	return buff.ReadString(int(sz))
}
func varSz(buff *Buffer) (int, error) {
	i, err := varUInt(buff)
	if err != nil {
		return 0, err
	}
	return int(i), nil
}

func fastSz(buff *Buffer) (int, error) {
	i, err := taggedFastUInt(buff)
	if err != nil {
		return 0, err
	}
	return int(i), nil
}

func badTag(msg string, b byte) string {
	return fmt.Sprintf("%s %c:%d", msg, b, b)
}

func fastString(tag byte, buff *Buffer) (interface{}, error) {
	sz, err := taggedFastInt(buff)
	if err != nil {
		return nil, err
	}
	s, e := buff.ReadString(sz)
	return s, e
}

func decodeClose(tag byte, buff *Buffer) (interface{}, error) {
	chanID, err := buff.Next(16)
	if err != nil {
		return nil, err
	}
	body, err := Decode(buff)
	if err != nil {
		return nil, err
	}
	// log.Println("Got chan id", err, chanId, ChannelId(chanId))
	ret := ChannelMsg{
		IsClose: true,
		Channel: ChannelId(chanID),
		Body:    body,
	}
	return ret, nil
}

func decodeMessage(tag byte, buff *Buffer) (ret interface{}, err error) {
	if chanID, err := buff.Next(16); err == nil {
		if body, err := Decode(buff); err == nil {
			ret = ChannelMsg{
				Channel: ChannelId(chanID),
				Body:    body,
			}
		}
	}
	return
}
