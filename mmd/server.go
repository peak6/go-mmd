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
	"fmt"
	"log"
	"net"
	"time"
)

type Server struct {
	serviceName string
	listenPort  int
	cfg         *ConnConfig
	serviceFunc ServiceFunc
	listener    net.Listener
	closeChan   chan bool
}

func (s *Server) start(started chan error) {
	log.Printf("Starting Server for service %s  on port %d", s.serviceName, s.listenPort)

	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", s.listenPort))

	if err != nil {
		log.Printf("Error starting server for service %s on port %d: %v", s.serviceName, s.listenPort, err)
		started <- err
	} else {
		log.Printf("Started Server for service %s on port %d", s.serviceName, s.listenPort)

		s.listener = listener
		started <- nil

		for {
			select {
			case <- s.closeChan:
				log.Println("Server stopped, exiting.")
				return
			default:
				conn, err := listener.Accept()
				if err != nil {
					log.Println("Server: Error accepting connection:", err)
					continue
				}

				err = s.handleConnection(conn.(*net.TCPConn))
				if err != nil {
					log.Println("Server: Error handling connection:", err)
				}
			}
		}
	}


}

func (s *Server) stop() error {
	err := s.listener.Close()
	s.closeChan <- true
	return err
}

func (s *Server) handleConnection(tcpConn *net.TCPConn) (err error) {
	defer func() {
		if connErr := recover(); connErr != nil {
			log.Println("Connection panic: ", connErr)
			err = fmt.Errorf("recovered from panic while handling connection. Details: %v", connErr)
		}
	}()

	serverConfig := createServerSideConnCfg(s.cfg)
	mmdConn := createConnectionForTcpConn(serverConfig, tcpConn)

	mmdConn.services[s.serviceName] = s.serviceFunc
	return mmdConn.onSocketConnection(true)
}

func createServerSideConnCfg(clientConfig *ConnConfig) *ConnConfig {
	newCfg := *clientConfig
	newCfg.WriteHandshake = false
	return &newCfg
}

func createConnectionForTcpConn(cfg *ConnConfig, conn *net.TCPConn) *ConnImpl {
	return &ConnImpl{
		socket:      conn,
		dispatch:    make(map[ChannelId]chan ChannelMsg, 1024),
		callTimeout: time.Second * 30,
		services:    make(map[string]ServiceFunc),
		config:      cfg,
	}
}
