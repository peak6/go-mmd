package mmd

import (
	"errors"
	"fmt"
)

// ServiceFunc Handler callback for registered services
type ServiceFunc func(Conn, *Chan, *ChannelCreate)

// Chan MMD Channel
type Chan struct {
	Ch  chan ChannelMsg
	con *ConnImpl
	Id  ChannelId
}

// EOC Signals close of MMD channel
var EOC = errors.New("End Of Channel")

func (c *Chan) NextMessage() (ChannelMsg, error) {
	a, ok := <-c.Ch
	if !ok {
		return ChannelMsg{}, EOC
	}
	return a, nil
}

func (c *Chan) Close(body interface{}) error {
	cm := ChannelMsg{Channel: c.Id, Body: body, IsClose: true}
	if ch := c.con.unregisterChannel(c.Id); ch != nil {
		close(ch)
	}

	buff := NewBuffer(1024)
	err := Encode(buff, cm)
	if err != nil {
		return err
	}
	c.con.writeOnSocket(buff.Flip().Bytes())
	return nil
}

func (c *Chan) Send(body interface{}) error {
	cm := ChannelMsg{Channel: c.Id, Body: body}
	buff := NewBuffer(1024)
	err := Encode(buff, cm)
	if err != nil {
		return err
	}
	c.con.writeOnSocket(buff.Flip().Bytes())
	return nil
}

func (c *Chan) Errorf(code int, format string, args ...interface{}) error {
	return c.Error(code, fmt.Sprintf(format, args...))
}

func (c *Chan) Error(code int, body interface{}) error {
	return c.Close(&MMDError{code, body})
}

func (c *Chan) ErrorInvalidRequest(body interface{}) error {
	return c.Error(Err_INVALID_REQUEST, body)
}
