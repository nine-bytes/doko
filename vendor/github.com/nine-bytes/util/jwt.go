package util

import (
	"errors"
	"github.com/dgrijalva/jwt-go"
	"strings"
)

var ErrNotMatchWithSigningMethod = errors.New("not match with signing method")

const (
	HS256 = "HS256"
	HS384 = "HS384"
	HS512 = "HS512"
	RS256 = "RS256"
	RS384 = "RS384"
	RS512 = "RS512"
	ES256 = "ES256"
	ES384 = "ES384"
	ES512 = "ES512"
	PS256 = "PS256"
	PS384 = "PS384"
	PS512 = "PS512"
)

func MethodStr2Obj(str string) (obj jwt.SigningMethod) {
	switch strings.ToUpper(str) {
	case HS256:
		return jwt.SigningMethodHS256
	case HS384:
		return jwt.SigningMethodHS384
	case HS512:
		return jwt.SigningMethodHS512
	case RS256:
		return jwt.SigningMethodRS256
	case RS384:
		return jwt.SigningMethodRS384
	case RS512:
		return jwt.SigningMethodRS512
	case ES256:
		return jwt.SigningMethodES256
	case ES384:
		return jwt.SigningMethodES384
	case ES512:
		return jwt.SigningMethodES512
	case PS256:
		return jwt.SigningMethodPS256
	case PS384:
		return jwt.SigningMethodPS384
	case PS512:
		return jwt.SigningMethodPS512
	}
	return jwt.SigningMethodNone
}

func ParseSignKeyFromPEM(bufKey []byte, method jwt.SigningMethod) (signKey interface{}, err error) {
	switch method {
	case jwt.SigningMethodHS256, jwt.SigningMethodHS384, jwt.SigningMethodHS512:
		return bufKey, nil
	case jwt.SigningMethodRS256, jwt.SigningMethodRS384, jwt.SigningMethodRS512,
		jwt.SigningMethodPS256, jwt.SigningMethodPS384, jwt.SigningMethodPS512:
		return jwt.ParseRSAPrivateKeyFromPEM(bufKey)
	case jwt.SigningMethodES256, jwt.SigningMethodES384, jwt.SigningMethodES512:
		return jwt.ParseECPrivateKeyFromPEM(bufKey)
	}

	return jwt.UnsafeAllowNoneSignatureType, nil
}

func ParseVerifyKeyFromPEM(bufKey []byte, method jwt.SigningMethod) (verifyKey interface{}, err error) {
	switch method {
	case jwt.SigningMethodHS256, jwt.SigningMethodHS384, jwt.SigningMethodHS512:
		return bufKey, nil
	case jwt.SigningMethodRS256, jwt.SigningMethodRS384, jwt.SigningMethodRS512,
		jwt.SigningMethodPS256, jwt.SigningMethodPS384, jwt.SigningMethodPS512:
		return jwt.ParseRSAPublicKeyFromPEM(bufKey)
	case jwt.SigningMethodES256, jwt.SigningMethodES384, jwt.SigningMethodES512:
		return jwt.ParseECPublicKeyFromPEM(bufKey)
	}

	return jwt.UnsafeAllowNoneSignatureType, nil
}

type JWT struct {
	method    jwt.SigningMethod
	signKey   interface{}
	verifyKey interface{}
}

func NewJWT(method jwt.SigningMethod, signKey, verifyKey interface{}) *JWT {
	return &JWT{method: method, signKey: signKey, verifyKey: verifyKey}
}

//Generate JWT token
func (t *JWT) GenerateWithClaims(claims jwt.Claims) (string, error) {
	return jwt.NewWithClaims(t.method, claims).SignedString(t.signKey)
}

func (t *JWT) ParseWithClaims(tokenString string, claims jwt.Claims) (*jwt.Token, error) {
	//validata the token
	return jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		//verify the token with public key , which is the counter part of private key
		if token.Method != t.method {
			return nil, ErrNotMatchWithSigningMethod
		}
		return t.verifyKey, nil
	})
}
