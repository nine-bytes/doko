package util

import (
	"path/filepath"
	"os"
	"log"
	"encoding/binary"
	mrand "math/rand"
	"crypto/rand"
)

func init() {
	var err error
	if executableFile, err = os.Executable(); err != nil {
		log.Fatalf("[util.os.Executable]: %v", err)
	}

	executableDirectory = filepath.Dir(executableFile)

	randomSeed := func() (seed int64, err error) {
		err = binary.Read(rand.Reader, binary.LittleEndian, &seed)
		return
	}

	// seed random number generator
	if seed, err := randomSeed(); err == nil {
		mrand.Seed(seed)
	} else {
		panic(err)
	}
}
