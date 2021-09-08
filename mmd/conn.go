package mmd

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"sync"
	"time"
)

const DefaultRetryInterval = 5 * time.Second
const LocalhostUrl = "localhost:9999"

type OnConnection func(Conn) error
type OnDisconnect func()

type Conn interface {
	Subscribe(service string, body interface{}) (*Chan, error)
	Unsubscribe(cid ChannelId, body interface{}) error
	Call(service string, body interface{}) (interface{}, error)
	CallAuthenticated(service string, token AuthToken, body interface{}) (interface{}, error)
	SetDefaultCallTimeout(dur time.Duration)
	GetDefaultCallTimeout() time.Duration
	RegisterLocalService(name string, fn ServiceFunc) error
	RegisterService(name string, fn ServiceFunc) error

	String() string

	createSocketConnection(isRetryConnection bool, notifyOnConnect bool) error
	close() error
}

// ConnImpl Connection and channel dispatch map
type ConnImpl struct {
	socket       *net.TCPConn
	dispatch     map[ChannelId]chan ChannelMsg
	dispatchLock sync.RWMutex
	socketLock   sync.Mutex
	callTimeout  time.Duration
	services     map[string]ServiceFunc
	config       *ConnConfig
}

func (c *ConnImpl) Subscribe(service string, body interface{}) (*Chan, error) {
	ch := make(chan ChannelMsg, 1)
	cc := NewChannelCreate(SubChan, service, body)
	c.registerChannel(cc.ChannelId, ch)

	err := c.sendChannelMsg(cc)
	if err != nil {
		return nil, err
	}
	return &Chan{Ch: ch, con: c, Id: cc.ChannelId}, nil
}

func (c *ConnImpl) Unsubscribe(cid ChannelId, body interface{}) error {
	ch := c.unregisterChannel(cid)
	if ch != nil {
		close(ch)
	} else {
		return fmt.Errorf("Failed close channel: %v", cid)
	}

	return c.sendChannelMsg(NewChannelClose(cid, body))
}

func (c *ConnImpl) Call(service string, body interface{}) (interface{}, error) {
	return c.CallAuthenticated(service, AuthToken(NO_AUTH_TOKEN), body)
}

func (c *ConnImpl) CallAuthenticated(service string, token AuthToken, body interface{}) (interface{}, error) {
	buff := NewBuffer(1024)
	cc := NewChannelCreate(CallChan, service, body)
	cc.AuthToken = token
	err := Encode(buff, cc)
	if err != nil {
		return nil, err
	}
	ch := make(chan ChannelMsg, 1)
	c.registerChannel(cc.ChannelId, ch)
	defer c.unregisterChannel(cc.ChannelId)
	err = c.writeOnSocket(buff.Flip().Bytes())
	if err != nil {
		return nil, err
	}
	select {
	case ret, ok := <-ch:
		if !ok {
			return nil, fmt.Errorf("Call Error: channel closed while waiting for return message")
		}

		e, ok := ret.Body.(MMDError)
		if ok {
			return nil, fmt.Errorf("MMD Error: %d: %v", e.code, e.msg)
		}

		return ret.Body, nil
	case <-time.After(c.callTimeout):
		return nil, fmt.Errorf("Timeout waiting for: %s", service)
	}
}

func (c *ConnImpl) SetDefaultCallTimeout(dur time.Duration) {
	c.callTimeout = dur
}

func (c *ConnImpl) GetDefaultCallTimeout() time.Duration {
	return c.callTimeout
}

func (c ConnImpl) String() string {
	return fmt.Sprintf("ConnImpl{remote: %s, local: %s}", c.socket.RemoteAddr(), c.socket.LocalAddr())
}

func (c *ConnImpl) RegisterLocalService(name string, fn ServiceFunc) error {
	return c.registerServiceUtil(name, fn, "registerLocal")
}

func (c *ConnImpl) RegisterService(name string, fn ServiceFunc) error {
	return c.registerServiceUtil(name, fn, "register")
}

// default to local connection to call a service
func Call(service string, body interface{}) (interface{}, error) {
	lc, err := LocalConnect()
	if err != nil {
		return nil, err
	}
	return lc.Call(service, body)
}

// Creates a default URL connection (-mmd to override)
func Connect() (Conn, error) {
	return ConnectTo(mmdUrl)
}

func LocalConnect() (Conn, error) {
	return ConnectTo(LocalhostUrl)
}

func ConnectTo(url string) (Conn, error) {
	return NewConnConfig(url).Connect()
}

func ConnectWithTags(myTags []string, theirTags []string) (Conn, error) {
	return ConnectWithTagsTo(mmdUrl, myTags, theirTags)
}

func ConnectWithTagsTo(url string, myTags []string, theirTags []string) (Conn, error) {
	conn := NewConnConfig(url)
	conn.ExtraMyTags = myTags
	conn.ExtraTheirTags = theirTags
	return conn.Connect()
}

func ConnectWithRetry(reconnectInterval time.Duration, onConnect OnConnection) (Conn, error) {
	return ConnectWithRetryTo(mmdUrl, reconnectInterval, onConnect)
}

func ConnectWithRetryTo(url string, reconnectInterval time.Duration, onConnect OnConnection) (Conn, error) {
	cfg := NewConnConfig(url)
	cfg.ReconnectInterval = reconnectInterval
	cfg.AutoRetry = true
	cfg.OnConnect = onConnect
	return cfg.Connect()
}

func ConnectWithTagsWithRetry(myTags []string, theirTags []string, reconnectInterval time.Duration, onConnect OnConnection) (Conn, error) {
	return ConnectWithTagsWithRetryTo(mmdUrl, myTags, theirTags, reconnectInterval, onConnect)
}

func ConnectWithTagsWithRetryTo(url string, myTags []string, theirTags []string, reconnectInterval time.Duration, onConnect OnConnection) (Conn, error) {
	cfg := NewConnConfig(url)
	cfg.ExtraMyTags = myTags
	cfg.ExtraTheirTags = theirTags
	cfg.ReconnectInterval = reconnectInterval
	cfg.AutoRetry = true
	cfg.OnConnect = onConnect
	return cfg.Connect()
}

// internal to package --

func (c *ConnImpl) startReader() {
	go c.reader()
}

func (c *ConnImpl) cleanupReader() {
	log.Println("Cleaning up reader")
	defer c.dispatchLock.Unlock()
	c.socket.CloseRead()
	c.dispatchLock.Lock()
	for k, v := range c.dispatch {
		log.Println("Auto-closing channel", k)
		delete(c.dispatch, k)
		close(v)
	}
}

func (c *ConnImpl) cleanupSocket() {
	c.socket.CloseWrite()
}

func (c *ConnImpl) sendChannelMsg(cc interface{}) error {
	buff := NewBuffer(1024)
	err := Encode(buff, cc)
	if err != nil {
		return err
	}

	return c.writeOnSocket(buff.Flip().Bytes())
}

func (c *ConnImpl) reconnect() {
	err := c.close()
	if err != nil {
		log.Panicln("Failed to close socket: ", err)
	}

	start := time.Now()
	err = c.createSocketConnection(true, true)
	elapsed := time.Since(start)

	log.Println("Socket reset. Connected to mmd after :", elapsed)
}

func (c *ConnImpl) close() error {
	if c.socket != nil {
		err := c.socket.Close()
		return err
	}

	log.Println("Cannot close a nil socket")
	return nil
}

func (c *ConnImpl) createSocketConnection(isRetryConnection bool, notifyOnConnect bool) error {
	if isRetryConnection && c.config.ReconnectDelay > 0 {
		log.Printf("Sleeping for %.2f seconds before trying next connection\n", c.config.ReconnectDelay.Seconds())
		time.Sleep(c.config.ReconnectDelay)
	}

	dialer := net.Dialer{}
	if c.config.ConnTimeout > 0 {
		dialer.Timeout = time.Second * time.Duration(c.config.ConnTimeout)
	}

	for {
		conn, err := dialer.Dial("tcp", c.config.Url)
		if err != nil && c.config.AutoRetry {
			log.Printf("Failed to connect, will sleep for %.2f seconds before trying again : %v\n", c.config.ReconnectInterval.Seconds(), err)
			time.Sleep(c.config.ReconnectInterval)
			continue
		}

		if err == nil {
			tcpConn := conn.(*net.TCPConn)

			tcpConn.SetWriteBuffer(c.config.WriteSz)
			tcpConn.SetReadBuffer(c.config.ReadSz)
			c.socket = tcpConn

			return c.onSocketConnection(notifyOnConnect)
		}

		return err
	}
}

func (c *ConnImpl) onSocketConnection(notifyOnConnect bool) error {
	//either write or read the handshake
	if c.config.WriteHandshake {
		err := c.handshake()
		if err != nil {
			return err
		}
	} else {
		err, _ := c.readSingleFrame()
		if err != nil {
			return err
		}
	}

	c.startReader()

	if len(c.config.ExtraTheirTags) > 0 {
		c.Call("$mmd", map[string]interface{}{"extraTheirTags": c.config.ExtraTheirTags})
	}

	if c.config.OnConnect != nil && notifyOnConnect {
		return c.config.OnConnect(c)
	}

	return nil
}

func (c *ConnImpl) onDisconnect() {
	c.cleanupReader()
	c.cleanupSocket()
	if c.config.AutoRetry {
		c.reconnect()
	}
}

func (c *ConnImpl) handshake() error {
	handshake := []byte{1, 1}
	handshake = append(handshake, c.config.AppName...)
	return c.writeOnSocket(handshake)
}

func (c *ConnImpl) registerServiceUtil(name string, fn ServiceFunc, registryAction string) error {
	c.services[name] = fn
	ok, err := c.Call("serviceregistry", map[string]interface{}{
		"action": registryAction,
		"name":   name,
		"tag":    c.config.ExtraMyTags,
	})
	if err == nil && ok != "ok" {
		err = fmt.Errorf("Unexpected return: %v", ok)
	}
	if err != nil {
		delete(c.services, name)
	}
	return err
}

func (c *ConnImpl) registerChannel(cid ChannelId, ch chan ChannelMsg) {
	c.dispatchLock.Lock()
	c.dispatch[cid] = ch
	c.dispatchLock.Unlock()
}

func (c *ConnImpl) unregisterChannel(cid ChannelId) chan ChannelMsg {
	c.dispatchLock.Lock()
	ret, ok := c.dispatch[cid]
	if ok {
		delete(c.dispatch, cid)
	}
	c.dispatchLock.Unlock()
	return ret
}

func (c *ConnImpl) lookupChannel(cid ChannelId) chan ChannelMsg {
	c.dispatchLock.RLock()
	ret := c.dispatch[cid]
	c.dispatchLock.RUnlock()
	return ret
}

func (c *ConnImpl) writeOnSocket(data []byte) error {
	c.socketLock.Lock()
	defer c.socketLock.Unlock()

	fsz := make([]byte, 4)
	binary.BigEndian.PutUint32(fsz, uint32(len(data)))

	_, err := c.socket.Write(fsz)
	if err != nil {
		return fmt.Errorf("Failed to write header: %s %s", fsz, err)
	} else {
		_, err = c.socket.Write(data)
		if err != nil {
			return fmt.Errorf("Failed to write data: %s", err)
		}
	}

	return nil
}

func (c *ConnImpl) reader() {
	fszb := make([]byte, 4)
	buff := make([]byte, 256)
	defer func() {
		log.Println("exiting reader loop")
		if c.config.OnDisconnect != nil {
			c.config.OnDisconnect()
		} else {
			c.onDisconnect()
		}
	}()

	for {
		err, b := c.readFrame(fszb, buff)
		if err != nil {
			return
		}
		m, err := Decode(b)
		if err != nil {
			if c.config.AutoRetry == true {
				log.Println("Error decoding buffer:", err)
				return
			} else {
				log.Panic("Error decoding buffer:", err)
			}
		} else {
			c.dispatchMessage(m)
		}
	}
}

func (c *ConnImpl) readSingleFrame() (error, *Buffer) {
	buf := make([]byte, 1024)
	fszbf := make([]byte, 4)
	return c.readFrame(fszbf, buf)
}

func (c *ConnImpl) readFrame(fszb []byte, buff []byte) (error, *Buffer) {
	num, err := io.ReadFull(c.socket, fszb)
	if err != nil {
		if err != io.EOF {
			log.Println("Error reading frame size:", err)
		}
		return err, nil
	}
	if num != 4 {
		log.Println("Short read for size:", num)
		return fmt.Errorf("Short read for size: %d", num), nil
	}
	fsz := int(binary.BigEndian.Uint32(fszb))
	if len(buff) < fsz {
		buff = make([]byte, fsz)
	}

	reads := 0
	offset := 0
	for offset < fsz {
		sz, err := c.socket.Read(buff[offset:fsz])
		if err != nil {
			if c.config.AutoRetry == true {
				log.Println("Failed to read from socket", err)
				return fmt.Errorf("failed to read from socket: %v", err.Error()), nil
			} else {
				log.Panic("Error reading message:", err)
			}
		}
		reads++
		offset += sz
	}
	return nil, Wrap(buff[:fsz])
}

func (c *ConnImpl) dispatchMessage(m interface{}) {
	switch msg := m.(type) {
	case ChannelMsg:
		if msg.IsClose {
			ch := c.unregisterChannel(msg.Channel)
			if ch != nil {
				ch <- msg
				close(ch)
			} else {
				log.Println("Unknown channel:", msg.Channel, "discarding message")
			}
		} else {
			ch := c.lookupChannel(msg.Channel)
			if ch != nil {
				ch <- msg
			} else {
				log.Println("Unknown channel:", msg.Channel, "discarding message")
			}
		}
	case ChannelCreate:
		fn, ok := c.services[msg.Service]
		if !ok {
			log.Println("Unknown service:", msg.Service, "cannot process", msg)
		}
		ch := make(chan ChannelMsg, 1)
		c.registerChannel(msg.ChannelId, ch)
		fn(c, &Chan{Ch: ch, con: c, Id: msg.ChannelId}, &msg)
	default:
		if c.config.AutoRetry == true {
			log.Println("Unknown message type:", reflect.TypeOf(msg), msg)
		} else {
			log.Panic("Unknown message type:", reflect.TypeOf(msg), msg)
		}

	}
}
