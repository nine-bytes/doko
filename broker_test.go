package doko

import (
	"testing"
	"crypto/tls"
)

func TestBroker_Run(t *testing.T) {
	var listenAddr = ":4443"
	var tlsConfig *tls.Config = nil

	var getAuthenticate AuthenticateFunc = func(authMsg *Auth) error {
		return nil
	}

	var reqBrokerPermission ReqBrokerPermission = func(reqBrokerMsg *ReqBroker) error {
		return nil
	}

	if err := NewBroker(getAuthenticate, reqBrokerPermission).Run(listenAddr, tlsConfig); err != nil {
		t.Fatalf("run broker error: %v", err)
	}

	t.Log("bloking")
	select {}
}
