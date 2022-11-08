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

// EOC signals close of MMD channel
var EOC = errors.New("end of channel")

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
		closeRecover(ch)
	}

	buff := NewBuffer(1024)
	err := Encode(buff, cm)
	if err != nil {
		return err
	}
	_ = c.con.writeOnSocket(buff.Flip().Bytes())
	return nil
}

func (c *Chan) Send(body interface{}) error {
	cm := ChannelMsg{Channel: c.Id, Body: body}
	buff := NewBuffer(1024)
	err := Encode(buff, cm)
	if err != nil {
		return err
	}
	_ = c.con.writeOnSocket(buff.Flip().Bytes())
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
