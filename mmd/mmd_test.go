// +build integration

package mmd

import (
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
	integration := os.Getenv("INTEGRATION")
	log.Println("integration tests are enabled: ", integration)
	integrationTests, _ = strconv.ParseBool(integration)
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

	mmdcc, err := Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer closeConnection(t, mmdcc)
	t.Log("Created mmd client connection:", mmdcc)

	mmdsc, err := Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer closeConnection(t, mmdsc)
	t.Log("Created mmd service connection:", mmdsc)

	err = mmdsc.RegisterService("test.service", func(conn Conn, channel *Chan, channelCreate *ChannelCreate) {
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

	resp, err := mmdcc.Call("test.service", "call message")
	t.Logf("Call response: %+v\nError: %v\n", resp, err)

	subChan, err := mmdcc.Subscribe("test.service", "sub message")
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

	channelWait := &sync.WaitGroup{}
	channelWait.Add(1)

	finishWait := &sync.WaitGroup{}
	finishWait.Add(1)

	mmdsc, err := Connect()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Created mmd service connection:", mmdsc)

	mmdcc, err := Connect()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Created mmd client connection:", mmdcc)

	defer closeConnection(t, mmdcc)
	defer closeConnection(t, mmdsc)

	t.Log("Created mmd connection:", mmdsc)

	err = mmdsc.RegisterService("test.service.recover", func(conn Conn, channel *Chan, channelCreate *ChannelCreate) {
		if channelCreate.Type == SubChan {
			channelWait.Wait()
			err := channel.Send("sub response")
			if err != nil {
				t.Logf("Service error sending sub response: %s", err)
			}
			finishWait.Done()
		}
	})

	subChan, err := mmdcc.Subscribe("test.service.recover", "sub message")

	t.Log("closing sub channel")
	close(subChan.Ch)

	channelWait.Done()

	t.Log("wg done, waiting for test to finish")
	finishWait.Wait()
}

func closeConnection(t *testing.T, mmdc Conn) {
	t.Log("Shutting down MMD connection")
	err := mmdc.close()
	t.Logf("Close error: %v\n", err)
}
