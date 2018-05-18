package util

import (
	"bytes"
	"net/http"
	"io/ioutil"
	"crypto/tls"
	"io"
)

func ReadResponseBody(response *http.Response) ([]byte, error) {
	defer response.Body.Close()
	return ioutil.ReadAll(response.Body)
}

func HTTPRequest(method, url string, header map[string]string, body []byte, config *tls.Config) (*http.Response, error) {
	var client *http.Client
	if config == nil {
		client = http.DefaultClient
	} else {
		client = &http.Client{Transport: &http.Transport{TLSClientConfig: config}}
	}

	var reader io.Reader = nil
	if body != nil {
		reader = bytes.NewReader(body)
	}

	request, err := http.NewRequest(method, url, reader)
	if err != nil {
		return nil, err
	}

	if header != nil {
		for key, value := range header {
			request.Header.Set(key, value)
		}
	}

	return client.Do(request)
}

type BodyHandler func(body []byte) []byte
type StatusHandler func(status int) int
type ResponseWriter struct {
	bodyHandler   BodyHandler
	statusHandler StatusHandler
	http.ResponseWriter
}

func NewResponseWriter(responseWriter http.ResponseWriter, bodyHandler BodyHandler, statusHandler StatusHandler) *ResponseWriter {
	return &ResponseWriter{bodyHandler, statusHandler, responseWriter}
}

func (rw *ResponseWriter) Write(data []byte) (int, error) {
	if rw.bodyHandler == nil {
		return rw.ResponseWriter.Write(data)
	}

	return rw.ResponseWriter.Write(rw.bodyHandler(data))
}

func (rw *ResponseWriter) WriteHeader(statusCode int) {
	if rw.statusHandler == nil {
		rw.ResponseWriter.WriteHeader(statusCode)
	}

	rw.ResponseWriter.WriteHeader(rw.statusHandler(statusCode))
}
