package doko

import (
	"testing"
	"crypto/tls"
)

func TestBroker_Run(t *testing.T) {
	var listenAddr = ":4443"
	var tlsConfig *tls.Config = nil

	var getAuthenticate AuthenticateFunc = func(tokenString string) (controlId string, err error) {
		return tokenString, nil
	}

	if err := NewBroker(getAuthenticate).Run(listenAddr, tlsConfig); err != nil {
		t.Errorf("run broker error: %v", err)
	}

	t.Logf("bloking")
	select {}
}
