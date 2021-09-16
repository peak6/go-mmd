package mmd

import (
	"fmt"
	"go.uber.org/atomic"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const reconnectInterval = time.Second * 10
const reconnectDelay = time.Second * 1

type ConnConfig struct {
	Url               string
	ReadSz            int
	WriteSz           int
	Verson            int32
	AppName           string
	AutoRetry         bool
	ReconnectInterval time.Duration
	ReconnectDelay    time.Duration
	OnConnect         OnConnection
	OnDisconnect      OnDisconnect
	ExtraMyTags       []string
	ExtraTheirTags    []string
	ConnTimeout       int
	WriteHandshake    bool
}

func NewConnConfig(url string) *ConnConfig {
	return &ConnConfig{
		Url:               url,
		ReadSz:            64 * 1024,
		WriteSz:           64 * 1024,
		AppName:           fmt.Sprintf("Go:%s", filepath.Base(os.Args[0])),
		AutoRetry:         false,
		ReconnectInterval: reconnectInterval,
		ReconnectDelay:    reconnectDelay,
		ExtraMyTags:       findExtraTags("MMD_EXTRA_MY_TAGS"),
		ExtraTheirTags:    findExtraTags("MMD_EXTRA_THEIR_TAGS"),
		ConnTimeout:       -1,
		WriteHandshake:    true,
	}
}

func (c *ConnConfig) Connect() (Conn, error) {
	return _create_connection(c)
}

var enableIstio = getEnvBool("ENABLE_ISTIO_CONNECTION", false)

func _create_connection(cfg *ConnConfig) (Conn, error) {
	var mmdc Conn
	if enableIstio {
		mmdc = createCompositeConnection(cfg)
	} else {
		mmdc = createConnection(cfg)
	}

	err := mmdc.createSocketConnection(false, true)
	if err != nil {
		return nil, err
	}

	return mmdc, err
}

func createCompositeConnection(cfg *ConnConfig) *CompositeConn {
	compositeCfg := *cfg
	result := &CompositeConn{
		conns:               make(map[string]*ConnImpl),
		mmdConn:             createConnection(&compositeCfg),
		cfg:                 &compositeCfg,
		servers:             make([]*Server, 0),
		connVersionCounter:  atomic.NewInt32(0),
		lastReconnect:       time.Now(),
	}
	if (cfg.OnConnect != nil) {
		compositeCfg.OnConnect = func(Conn) error {
			return cfg.OnConnect(result)
		}
	}
	return result
}

func createConnection(cfg *ConnConfig) *ConnImpl {
	return &ConnImpl{
		dispatch:    make(map[ChannelId]chan ChannelMsg, 1024),
		callTimeout: time.Second * 30,
		services:    make(map[string]ServiceFunc),
		config:      cfg,
	}
}

func findExtraTags(envVar string) []string {
	extraTagsEnv := strings.TrimSpace(os.Getenv(envVar))
	if len(extraTagsEnv) == 0 {
		return []string{}
	} else {
		extraTags := make([]string, 0)
		for _, tag := range strings.Split(extraTagsEnv, ",") {
			extraTags = append(extraTags, strings.TrimSpace(tag))
		}
		return extraTags
	}
}
