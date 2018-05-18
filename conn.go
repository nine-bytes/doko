package doko

import (
	"github.com/nine-bytes/util"
	"github.com/nine-bytes/log"
	"crypto/tls"
	"net"
	"time"
	"encoding/binary"
	"strings"
	"sync"
	"io"
	"github.com/juju/ratelimit"
)

func readMsg(c net.Conn) (buffer []byte, err error) {
	var sz int32
	err = binary.Read(c, binary.LittleEndian, &sz)
	if err != nil {
		return
	}

	buffer = make([]byte, sz)
	var n int
	for received := 0; received < int(sz); received += n {
		if n, err = c.Read(buffer[received:]); err != nil {
			if err == io.ErrUnexpectedEOF {

			}
			return
		}
	}

	return
}

func ReadMsg(c net.Conn) (Message, error) {
	buffer, err := readMsg(c)
	if err != nil {
		return nil, err
	}

	return Unpack(buffer)
}

func WriteMsg(c net.Conn, message interface{}) (err error) {
	buffer, err := Pack(message)
	if err != nil {
		return
	}

	sz := len(buffer)
	if err = binary.Write(c, binary.LittleEndian, int32(sz)); err != nil {
		return
	}

	var n int
	for sent := 0; sent < sz; sent += n {
		if n, err = c.Write(buffer[sent:]); err != nil {
			return
		}
	}

	return nil
}

func ReadMsgInto(c net.Conn, message Message) (err error) {
	buffer, err := readMsg(c)
	if err != nil {
		return
	}
	return UnpackInto(buffer, message)
}

type Listener struct {
	net.Listener
	ConnChan chan net.Conn
}

func NewListener(network, addr string, tlsCfg *tls.Config) (listener *Listener, err error) {
	var l net.Listener
	if tlsCfg != nil {
		l, err = tls.Listen(network, addr, tlsCfg)
	} else {
		l, err = net.Listen(network, addr)
	}
	if err != nil {
		return nil, err
	}

	listener = &Listener{
		Listener: l,
		ConnChan: make(chan net.Conn),
	}

	go listener.run()

	return
}

func (l *Listener) run() {
	for {
		conn, err := l.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") {
				log.Debug("listener closed: %v", err)
				// listener is closed, return the routine
				util.PanicToError(func() { close(l.ConnChan) })
				return
			}

			log.Debug("failed to accept new TCP connection: %v", err)
			continue
		}

		if err := util.PanicToError(func() { l.ConnChan <- conn }); err != nil {
			// ch may be closed by outside function
			// close listener
			l.Close()
			return
		}
	}
}

func Dial(network, addr string, timeout time.Duration, tlsCfg *tls.Config) (net.Conn, error) {
	if tlsCfg != nil {
		return tls.DialWithDialer(&net.Dialer{Timeout: timeout}, network, addr, tlsCfg)
	}

	return net.DialTimeout(network, addr, timeout)
}

type Plumber struct {
	c1, c2 net.Conn
}

func NewPlumber(c1, c2 net.Conn) *Plumber {
	return &Plumber{c1: c1, c2: c2}
}

func (p *Plumber) Pipe(bytesPerSec int64) (toC1bytes, toC2Bytes int64) {
	var wg = new(sync.WaitGroup)

	pipe := func(to, from net.Conn, bytesPerSec int64, bytesCopied *int64, wg *sync.WaitGroup) {
		defer to.Close()
		defer from.Close()
		defer wg.Done()

		bucket := ratelimit.NewBucketWithRate(float64(bytesPerSec), bytesPerSec)
		*bytesCopied, _ = io.Copy(to, ratelimit.Reader(from, bucket))

		//*bytesCopied, _ = io.Copy(to, from)
	}

	wg.Add(2)
	go pipe(p.c1, p.c2, bytesPerSec, &toC1bytes, wg)
	go pipe(p.c2, p.c1, bytesPerSec, &toC2Bytes, wg)
	wg.Wait()

	return
}

func (p *Plumber) Close() {
	p.c1.Close()
	p.c2.Close()
}
