// +build integration

package mmd

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"sync"
	"testing"
)

var integrationTests = false

func init() {
	integrationTests, _ = strconv.ParseBool(os.Getenv("INTEGRATION"))
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
}

func TestEchoCall(t *testing.T) {
	if !integrationTests {
		t.Skip("integration tests disabled")
	}

	mmdc, err := Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer closeConnection(t, mmdc)

	t.Log("Created mmd connection:", mmdc)
	resp, err := mmdc.Call("echo", "Howdy Doody")
	t.Logf("Response: %+v\nError: %v\n", resp, err)
}

func TestRegister(t *testing.T) {
	if !integrationTests {
		t.Skip("integration tests disabled")
	}

	mmdc, err := Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer closeConnection(t, mmdc)

	t.Log("Created mmd connection:", mmdc)

	err = mmdc.RegisterService("test.service", func(conn Conn, channel *Chan, channelCreate *ChannelCreate) {
		t.Logf("Service received channel create: %#v", channelCreate)
		if channelCreate.Type == CallChan {
			err := channel.Close("call response")
			if err != nil {
				t.Logf("Service error seding call response: %s", err)
			}
		} else {
			go func() {
				next := <-channel.Ch
				t.Logf("Service received sub channel message: %#v", next)
			}()

			err := channel.Send("sub response")
			if err != nil {
				t.Logf("Service error sending sub response: %s", err)
			}
		}
	})

	t.Logf("Register error response: %v", err)

	resp, err := mmdc.Call("test.service", "call message")
	t.Logf("Call response: %+v\nError: %v\n", resp, err)

	subChan, err := mmdc.Subscribe("test.service", "sub message")
	t.Logf("Sub response: %+v\nError: %v\n", subChan, err)
	err = subChan.Send("sub channel message")
	if err != nil {
		t.Logf("Client error sending sub channel message: %s", err)
	}
	resp, err = subChan.NextMessage()
	t.Logf("Client received sub channel message response: %+v\nError: %v\n", resp, err)
}

func TestCloseChannelRecover(t *testing.T) {
	if !integrationTests {
		t.Skip("integration tests disabled")
	}
	
	wg := &sync.WaitGroup{}
	wg.Add(1)

	mmdc, err := Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := recover(); closeErr != nil {
			fmt.Println("Expected, recovered from panic on closeConnection")
		}
	} ()
	defer closeConnection(t, mmdc)

	t.Log("Created mmd connection:", mmdc)

	err = mmdc.RegisterService("test.service", func(conn Conn, channel *Chan, channelCreate *ChannelCreate) {
		if channelCreate.Type == SubChan {
			wg.Wait()
			err := channel.Send("sub response")
			if err != nil {
				t.Logf("Service error sending sub response: %s", err)
			}
		}
	})
	
	subChan, err := mmdc.Subscribe("test.service", "sub message")
	close(subChan.Ch)
	wg.Done()
}

func closeConnection(t *testing.T, mmdc Conn) {
	t.Log("Shutting down MMD connection")
	err := mmdc.close()
	t.Logf("Close error: %v\n", err)
}
