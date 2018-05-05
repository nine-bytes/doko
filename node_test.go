package doko

import (
	"testing"
	"crypto/tls"
	"github.com/nine-bytes/util/log"
	"github.com/gin-gonic/gin"
	"net/http"
)

func TestNode(t *testing.T) {
	var brokerAddr = "localhost:4443"
	var targetAddr = "192.168.2.142:8088"
	var tlsConfig *tls.Config = nil
	var getReqPermission ReqPermissionFunc = func(startProxy *StartTunnel) error {
		return nil
	}
	var node1 = NewNode("1", brokerAddr, tlsConfig, getReqPermission)
	var node2 = NewNode("2", brokerAddr, tlsConfig, getReqPermission)
	var node3 = NewNode("3", brokerAddr, tlsConfig, getReqPermission)

	if err := node1.Start(); err != nil {
		t.Fatalf("node1 run error: %v", err)
	}

	if err := node2.Start(); err != nil {
		t.Fatalf("node2 run error: %v", err)
	}

	if err := node3.Start(); err != nil {
		t.Fatalf("node3 run error: %v", err)
	}

	listener1, err := NewListener("tcp", ":1234", nil)
	if err != nil {
		t.Fatalf("create listener 1 error: %v", err)
	}
	go func() {
		for {
			localConn, ok := <-listener1.ConnChan
			if !ok {
				t.Logf("listener1.ConnChan closed")
				return
			}

			log.Debug("node 1 reqBroker for target 2")
			go node1.ReqBroker("2", targetAddr, localConn)
		}
	}()

	listener3, err := NewListener("tcp", ":5678", nil)
	if err != nil {
		t.Fatalf("create listener 2 error: %v", err)
	}
	go func() {
		for {
			localConn, ok := <-listener3.ConnChan
			if !ok {
				t.Logf("listener1.ConnChan closed")
				return
			}

			log.Debug("node 3 reqBroker for target 2")
			go node3.ReqBroker("2", targetAddr, localConn)
		}
	}()

	e := gin.Default()
	e.GET("/start1", func(context *gin.Context) {
		node1.Start()
		context.String(http.StatusOK, "start 1 success")
	})
	e.GET("/stop1", func(context *gin.Context) {
		node1.Stop()
		context.String(http.StatusOK, "stop 1 success")
	})

	e.GET("/start2", func(context *gin.Context) {
		node2.Start()
		context.String(http.StatusOK, "start 2 success")
	})
	e.GET("/stop2", func(context *gin.Context) {
		node2.Stop()
		context.String(http.StatusOK, "stop 2 success")
	})

	e.GET("/start3", func(context *gin.Context) {
		node3.Start()
		context.String(http.StatusOK, "start 2 success")
	})
	e.GET("/stop3", func(context *gin.Context) {
		node3.Stop()
		context.String(http.StatusOK, "stop 2 success")
	})

	if err := e.Run(":8080"); err != nil {
		t.Errorf("run gin app error: %v", err)
	}
}
