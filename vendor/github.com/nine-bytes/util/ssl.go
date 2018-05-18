package util

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
)

func TLSServerConfig(crt, key []byte) (tlsConfig *tls.Config, err error) {
	var cert tls.Certificate
	if cert, err = tls.X509KeyPair(crt, key); err != nil {
		return
	}

	tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	return
}

func TLSClientConfig(rootCA []byte) (*tls.Config, error) {
	pemBlock, _ := pem.Decode(rootCA)
	if pemBlock != nil {
		rootCA = pemBlock.Bytes
	}

	certs, err := x509.ParseCertificates(rootCA)
	if err != nil {
		return nil, err
	}

	pool := x509.NewCertPool()
	pool.AddCert(certs[0])

	return &tls.Config{RootCAs: pool}, nil
}
