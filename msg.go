package doko

import (
	"encoding/json"
	"reflect"
)

var TypeMap map[string]reflect.Type

func init() {
	TypeMap = make(map[string]reflect.Type)

	t := func(obj interface{}) reflect.Type {
		return reflect.TypeOf(obj).Elem()
	}

	TypeMap["AccessResp"] = t((*AccessResp)(nil))
	TypeMap["Auth"] = t((*Auth)(nil))
	TypeMap["ReqBroker"] = t((*ReqBroker)(nil))
	TypeMap["ReqTunnel"] = t((*ReqTunnel)(nil))
	TypeMap["RegTunnel"] = t((*RegTunnel)(nil))
	TypeMap["StartTunnel"] = t((*StartTunnel)(nil))
	TypeMap["Ping"] = t((*Ping)(nil))
	TypeMap["Pong"] = t((*Pong)(nil))
	TypeMap["Bye"] = t((*Bye)(nil))
}

type Message interface{}

type Envelope struct {
	Type    string
	Payload json.RawMessage
}

type AccessResp struct{ Error string }

type Auth struct{ Id, TokenString, ProtocolVersion string }

type ReqBroker struct {
	Auth
	DstId, DstAddr string
}

type ReqTunnel struct{}

type RegTunnel struct{ Auth }

type StartTunnel struct{ ReqBroker }

type Ping struct{}

type Pong struct{}

type Bye struct{}
