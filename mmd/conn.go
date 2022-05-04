package mmd

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"reflect"
	"sync"
	"time"
)

const DefaultRetryInterval = 5 * time.Second
const LocalhostUrl = "localhost:9999"

type OnConnection func(Conn) error
type OnDisconnect func(int32)

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
	c.unregisterChannel(cid)
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

func (c *ConnImpl) startReader(wg *sync.WaitGroup) {
	go c.reader(wg)
}

func (c *ConnImpl) cleanupReader() {
	defer c.dispatchLock.Unlock()
	c.socket.CloseRead()
	c.dispatchLock.Lock()
	for k, v := range c.dispatch {
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
	c.socketLock.Lock()
	defer c.socketLock.Unlock()

	if c.socket != nil {
		err := c.socket.Close()
		return err
	}

	return nil
}

func (c *ConnImpl) createSocketConnection(isRetryConnection bool, notifyOnConnect bool) error {
	if isRetryConnection && c.config.ReconnectDelay > 0 {
		time.Sleep(c.config.ReconnectDelay)
	}

	dialer := net.Dialer{}
	if c.config.ConnTimeout > 0 {
		dialer.Timeout = time.Second * time.Duration(c.config.ConnTimeout)
	}

	logReconnect := true
	for {
		conn, err := dialer.Dial("tcp", c.config.Url)
		if err != nil && c.config.AutoRetry {
			if logReconnect {
				log.Printf("Failed to connect, will sleep for %.2f seconds before trying again : %v\n", c.config.ReconnectInterval.Seconds(), err)
				logReconnect = false
			}
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

	// This is a convoluted way to ensure that Subscribe calls to this mmd client will return an error if the
	// service is behind the Istio ingress, but is actually not healthy.
	//
	// We create a WaitGroup that the reader thread can signal when it is complete with its first pass of reading.
	//
	// This will ensure the TCP connection is good, because the Istio ingress will accept TCP connections
	// for services that may not be available. Additionally, we cannot detect that the service is not there simply by
	// Writing data to the socket due to the fact that  the network stack will return a successful Write call
	// so long as the data makes it to the kernel's network buffer.
	//
	// If the reader detects an error, it will shutdown the socket, and will cause any subsequent Writes
	// to fail as well. This gives the opportunity to return an Error from the Subscribe call.
	wg := &sync.WaitGroup{}
	wg.Add(1)

	c.startReader(wg)

	wg.Wait()

	if len(c.config.ExtraTheirTags) > 0 {
		c.Call("$mmd", map[string]interface{}{"extraTheirTags": c.config.ExtraTheirTags})
	}

	if c.config.OnConnect != nil && notifyOnConnect {
		return c.config.OnConnect(c)
	}

	return nil
}

func (c *ConnImpl) onDisconnect() {
	log.Println("exited reader loop and disconnecting")
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

func (c *ConnImpl) unregisterChannel(cid ChannelId) {
	c.dispatchLock.Lock()
	ret, ok := c.dispatch[cid]
	if ok {
		delete(c.dispatch, cid)
		close(ret)
	}
	c.dispatchLock.Unlock()
}

func (c *ConnImpl) unregisterChannelAndSendMsg(cid ChannelId, msg ChannelMsg) {
	c.dispatchLock.Lock()
	ret, ok := c.dispatch[cid]
	if ok {
		delete(c.dispatch, cid)
		ret <- msg
		close(ret)
	} else {
		log.Printf("Unknown channel: %v discarding message", cid)
	}
	c.dispatchLock.Unlock()
}


func (c *ConnImpl) lookupChannelAndSendMsg(cid ChannelId, msg ChannelMsg) {
	c.dispatchLock.RLock()
	ret, ok := c.dispatch[cid]
	if ok {
		ret <- msg
	} else {
		log.Printf("Unknown channel: %v discarding message", cid)
	}
	c.dispatchLock.RUnlock()
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

func (c *ConnImpl) reader(wg *sync.WaitGroup) {
	firstPass := true
	fszb := make([]byte, 4)
	buff := make([]byte, 256)
	defer func() {
		if c.config.OnDisconnect != nil {
			c.config.OnDisconnect(c.config.Version)
		} else {
			c.onDisconnect()
		}
	}()

	for {
		if firstPass {
			// On the first pass of the reader we want to use the Read to check the state of the
			// TCP connection, so we must set a ReadDeadline, otherwise the Read call will block until data is present.
			// If the TCP connection is bad, the Read call will return an error other than ErrDeadlineExceeded,
			// most likely an EOF
			_ = c.socket.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		}
		err, b := c.readFrame(fszb, buff)
		if firstPass {
			// After the first pass of the Read, we reset the deadline to 0, which disables
			// it, any subsequent Read calls on the TCP connection will block until data is
			// available.
			_ = c.socket.SetReadDeadline(time.Time{})
			// We also signal the WaitGroup that we are done so the goroutine that is setting
			// up this connection can finish its processing
			wg.Done()
			firstPass = false
		}
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				// We are expecting a DeadlineExceeded error on the first pass, so we just
				// continue at the top of the for loop.
				// This block of code should never run on subsequent iterations of
				// the loop because we have disabled the deadline above.
				continue
			}
			// Any other error received, regardless of first pass or not, indicates
			// there is a problem with the TCP connection, and we should exit the reader loop.
			// The deferred function will take care of the cleanup.
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
			// error expected for composite connection or first read with a Deadline Exceeded,
			// so no need to log this error.
			if c.config.OnDisconnect == nil && !errors.Is(err, os.ErrDeadlineExceeded){
				log.Println("Error reading frame size:", err)
			}
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
			c.unregisterChannelAndSendMsg(msg.Channel, msg)
		} else {
			c.lookupChannelAndSendMsg(msg.Channel, msg)
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
