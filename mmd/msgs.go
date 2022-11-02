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
	"crypto/rand"
	"encoding/hex"
)

var NO_AUTH_TOKEN = string(make([]byte, 16))

//Probably not useful as a public function
//TODO: switch to raw struct and set defaults upon usage
func NewChannelCreate(chanType ChannelType, service string, body interface{}) ChannelCreate {
	return ChannelCreate{
		ChannelId: ChannelId(newUUID()),
		Type:      chanType,
		Service:   service,
		Timeout:   3,
		AuthToken: AuthToken(NO_AUTH_TOKEN),
		Body:      body,
	}
}

func NewChannelClose(id ChannelId, body interface{}) ChannelClose {
	return ChannelClose{
		ChannelId: id,
		Body:      body,
	}
}

func GenerateUUID() UUID {
	return newUUID()
}

func newUUID() UUID {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return UUID(b)
}

type MMDError struct {
	code int
	msg  interface{}
}

type UUID string
type ChannelId UUID
type AuthToken UUID

// Typesafe enum
type ChannelType int

/*type MMDMessage interface {
	Encode(buffer *bytes.Buffer)
}
*/
func (u UUID) Bytes() []byte {
	return []byte(u)
}
func (u UUID) String() string {
	return hex.EncodeToString([]byte(u))
}

func (c AuthToken) String() string {
	return UUID(c).String()
}
func (c ChannelId) String() string {
	return UUID(c).String()
}

const (
	CallChan ChannelType = iota
	SubChan  ChannelType = iota
)

// Familiar MMD Message types
type ChannelCreate struct {
	ChannelId ChannelId
	Type      ChannelType
	Service   string
	Timeout   int64
	AuthToken AuthToken
	Body      interface{}
}

type ChannelClose struct {
	ChannelId ChannelId
	Body      interface{}
}

type ChannelMsg struct {
	IsClose bool
	Channel ChannelId
	Body    interface{}
}

const (
	Err_UNKNOWN                         = 0
	Err_SERVICE_NOT_FOUND               = 1
	Err_IMPROPER_RESPONSE_TYPE          = 2
	Err_BROKER_CONNECTION_CLOSED        = 3
	Err_SERVICE_ERROR                   = 4
	Err_UNEXPECTED_REMOTE_CHANNEL_CLOSE = 5
	Err_INVALID_REQUEST                 = 6
	Err_AUTHENTICATION_ERROR            = 7
	Err_CHANNEL_ADMIN_CLOSED            = 8
	Err_INVALID_CHANNEL                 = 9
	Err_TIMEOUT                         = 10
	Err_SERVICE_BUSY                    = 11
)
