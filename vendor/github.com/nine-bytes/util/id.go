package util

import (
	"crypto/rand"
	"fmt"
	mrand "math/rand"
)

// creates a random identifier of the specified length
func RandId(length int) string {
	b := make([]byte, length)
	var randVal uint32
	for i := 0; i < length; i++ {
		byteIdx := i % 4
		if byteIdx == 0 {
			randVal = mrand.Uint32()
		}
		b[i] = byte((randVal >> (8 * uint(byteIdx))) & 0xFF)
	}
	return fmt.Sprintf("%x", b)[:length]
}

// like RandId, but uses a crypto/rand for secure random identifiers
func SecureRandId(length int) (id string, err error) {
	b := make([]byte, length)
	n, err := rand.Read(b)

	if n != length {
		err = fmt.Errorf("only generated %d random bytes, %d requested", n, length)
		return
	}

	if err != nil {
		return
	}

	id = fmt.Sprintf("%x", b)[:length]
	return
}

func SecureRandIdOrPanic(length int) string {
	id, err := SecureRandId(length)
	if err != nil {
		panic(err)
	}
	return id[:length]
}

func RandNumId(length int) string {
	var numRunes = []rune("1234567890")
	b := make([]rune, length)
	for i := range b {
		b[i] = numRunes[mrand.Intn(len(numRunes))]
	}
	return string(b)
}
