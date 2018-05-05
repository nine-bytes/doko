package doko

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"
	"runtime/debug"
	"github.com/nine-bytes/util"
	"github.com/nine-bytes/util/log"
	"io"
	"errors"
	"sync"
)

type ReqPermissionFunc func(startProxy *StartTunnel) error

type Node struct {
	// broker info
	tokenString      string
	brokerAddr       string
	tlsConfig        *tls.Config
	getReqPermission ReqPermissionFunc

	shutdown  *util.Shutdown // running instance shutdown
	plumbers  *util.Set      // unlimited capacity set
	isRunning bool
	sync.RWMutex
}

func NewNode(tokenString, brokerAddr string, tlsConfig *tls.Config, getReqPermission ReqPermissionFunc) (node *Node) {
	return &Node{
		tokenString:      tokenString,
		brokerAddr:       brokerAddr,
		tlsConfig:        tlsConfig,
		getReqPermission: getReqPermission,
		plumbers:         util.NewSet(0),
	}
}

func (n *Node) Start() (err error) {
	n.Lock()
	defer n.Unlock()

	if n.isRunning {
		log.Debug("Node::Run an instance is running")
		return
	}

	if n.shutdown, err = n.run(); err != nil {
		log.Error("Client::Run run error: %v", err)
		return
	}

	n.isRunning = true
	// run daemon
	go n.daemon()
	return
}

func (n *Node) daemon() {
	var err error
	var shutdown *util.Shutdown

	for {
		n.shutdown.WaitComplete()
		for {
			n.Lock()
			if !n.isRunning {
				n.Unlock()
				log.Debug("Client::daemon Stop() called and stop daemon")
				return
			}
			if shutdown, err = n.run(); err != nil {
				n.Unlock()

				if util.IsNetworkError(err) {
					log.Error("Client::daemon re-run network error: %v", err)
					time.Sleep(haltDuration)
					continue
				}

				log.Info("Client::daemon re-run err: %v", err)
				return
			}
			n.shutdown = shutdown
			n.Unlock()
			break
		}
	}
}

func (n *Node) run() (shutdown *util.Shutdown, err error) {
	// connect to broker
	var ctlConn net.Conn
	if ctlConn, err = Dial("tcp", n.brokerAddr, dialTimeout, n.tlsConfig); err != nil {
		log.Error("Client::run connect to %s with error: %v", n.brokerAddr, err)
		return
	}

	// authenticate with the broker
	ctlConn.SetWriteDeadline(time.Now().Add(rwTimeout))
	if err = WriteMsg(ctlConn, &Auth{TokenString: n.tokenString, ProtocolVersion: protocolVersion}); err != nil {
		ctlConn.Close()
		log.Error("Client::run send authenticate message error: %v", err)
		return
	}

	// wait for the broker to authenticate
	resp := new(AccessResp)
	ctlConn.SetReadDeadline(time.Now().Add(rwTimeout))
	if err = ReadMsgInto(ctlConn, resp); err != nil {
		ctlConn.Close()
		log.Error("Client::run receive authenticate response message error: %v", err)
		return
	}
	ctlConn.SetReadDeadline(time.Time{})

	if resp.Error != "" {
		ctlConn.Close()
		log.Info("Client::run authenticate denied: %s", resp.Error)
		err = errors.New(resp.Error)
		return
	}

	shutdown = util.NewShutdown()
	readerShutdown, writerShutdown, pingShutdown, managerShutdown :=
		util.NewShutdown(), util.NewShutdown(), util.NewShutdown(), util.NewShutdown()
	in, out := make(chan Message), make(chan Message)
	pong := make(chan bool)

	// run reader
	go func(ctlConn net.Conn, in chan Message, readerShutdown *util.Shutdown, shutdown *util.Shutdown) {
		defer func() {
			if err := recover(); err != nil {
				log.Error("Client::run::reader recover from panic: %v: %s", err, debug.Stack())
			}
		}()

		// kill everything if the reader stops
		defer shutdown.Begin()

		// notify that we're done
		defer readerShutdown.Complete()

		// read messages from the Client channel
		for {
			if message, err := ReadMsg(ctlConn); err != nil {
				log.Debug("Client::run::reader read message from control connection error: %v", err)
				if err != io.EOF {
					log.Error("Client::run::reader read message from control connection error: %v", err)
				}
				return
			} else {
				// this can also panic during shutdown
				if util.PanicToError(func() { in <- message }) != nil {
					log.Debug("Client::run::reader channel c.in closed")
					return
				}
			}
		}
	}(ctlConn, in, readerShutdown, shutdown)

	// run writer
	go func(ctlConn net.Conn, out chan Message, writerShutdown *util.Shutdown, shutdown *util.Shutdown) {
		defer func() {
			if err := recover(); err != nil {
				log.Error("Client::run::writer recover from panic: %v: %s", err, debug.Stack())
			}
		}()

		// kill everything if the writer() stops
		defer shutdown.Begin()

		// notify that we've flushed all messages
		defer writerShutdown.Complete()

		// write messages to the Client channel
		for m := range out {
			ctlConn.SetWriteDeadline(time.Now().Add(rwTimeout))
			if err := WriteMsg(ctlConn, m); err != nil {
				log.Error(fmt.Sprintf("Client::run::writer error while writing:%v", err))
				return
			}
		}
	}(ctlConn, out, writerShutdown, shutdown)

	// run manager
	go func(n *Node, pong chan bool, in chan Message, managerShutdown *util.Shutdown, shutdown *util.Shutdown) {
		defer func() {
			if err := recover(); err != nil {
				log.Error("Client::run::manager recover from panic %v: %s", err, debug.Stack())
			}
		}()

		// kill everything if the Client manager stops
		defer shutdown.Begin()

		// notify that manager() has shutdown
		defer managerShutdown.Complete()

		// main Client loop
		var ok bool
		var rawMsg Message
		for {
			if rawMsg, ok = <-in; !ok {
				log.Debug("Client::run::manager channel c.in closed")
				return
			}

			switch m := rawMsg.(type) {
			case *ReqTunnel:
				go n.regTunnel()

			case *Pong:
				pong <- true

			case *Bye:
				go n.Stop()
				log.Debug("Client::run::manager time to say goodbye")

			default:
				log.Debug("Client::run::manager ignore unknown Client message %v ", m)
			}
		}
	}(n, pong, in, managerShutdown, shutdown)

	// run heartbeat
	go func(pong chan bool, out chan Message, pingShutdown *util.Shutdown, shutdown *util.Shutdown) {
		defer func() {
			// don't crash panic
			if r := recover(); r != nil {
				log.Error("Client::run::ping recover from panic: %v stack: %s", r, debug.Stack())
			}
		}()

		defer shutdown.Begin()

		defer pingShutdown.Complete()

		lastPing := time.Now()
		lastPong := time.Now()

		ping := time.NewTicker(pingInterval)
		defer ping.Stop()

		pongCheck := time.NewTicker(time.Second)
		defer pongCheck.Stop()

		for {
			select {
			case _, ok := <-pong:
				if !ok {
					log.Debug("Client::run::ping channel c.pong closed")
					return
				}

				// log.Debug("Client::run::ping PONG")
				lastPong = time.Now()

			case <-pongCheck.C:
				needPong := lastPong.Sub(lastPing) < 0
				pongLatency := time.Since(lastPing)

				if needPong && (pongLatency > rwTimeout) {
					log.Debug("Client::run::ping last ping: %v, last pong: %v", lastPing, lastPong)
					log.Debug("Client::run::ping lost pong in %.2f seconds", pongLatency.Seconds())
					return
				}

			case <-ping.C:
				out <- new(Ping)
				// log.Debug("Client::run::ping PING ")
				lastPing = time.Now()
			}
		}
	}(pong, out, pingShutdown, shutdown)

	// run stopper
	go func(ctlConn net.Conn, in, out chan Message, pong chan bool,
		managerShutdown, writerShutdown, readerShutdown, pingShutdown *util.Shutdown, shutdown *util.Shutdown) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("Client::run::stopper recover from panic: %v stack: %s", r, debug.Stack())
			}
		}()

		// wait until we're instructed to shutdown
		shutdown.WaitBegin()

		// shutdown manager() so that we have no more work to do
		close(in)
		managerShutdown.WaitComplete()

		// shutdown writer()
		close(out)
		writerShutdown.WaitComplete()

		// shutdown heartbeat
		close(pong)
		pingShutdown.WaitComplete()

		// close connection fully
		ctlConn.Close()
		readerShutdown.WaitComplete()

		shutdown.Complete()
	}(ctlConn, in, out, pong, managerShutdown, writerShutdown, readerShutdown, pingShutdown, shutdown)

	log.Info("Client::run authenticated with broker: %s", n.brokerAddr)
	return
}

func (n *Node) regTunnel() {
	var (
		pxyConn net.Conn
		err     error
	)

	if pxyConn, err = Dial("tcp", n.brokerAddr, dialTimeout, n.tlsConfig); err != nil {
		log.Error("Client::regProxy failed to establish proxy connection: %v", err)
		return
	}

	defer pxyConn.Close()

	pxyConn.SetWriteDeadline(time.Now().Add(rwTimeout))
	if err = WriteMsg(pxyConn, &RegTunnel{TokenString: n.tokenString, ProtocolVersion: protocolVersion}); err != nil {
		log.Error("Client::regProxy send RegProxy message error: %v", err)
		return
	}

	resp := new(AccessResp)
	pxyConn.SetReadDeadline(time.Now().Add(rwTimeout))
	if err = ReadMsgInto(pxyConn, resp); err != nil {
		log.Error("Client::regProxy receive authenticate response error: %v", err)
		return
	}

	if resp.Error != "" {
		log.Info("Client::regProxy authenticate denied: %v", resp.Error)
		return
	}

	// wait for StartProxy message
	//log.Debug("Client::regProxy wait for StartProxy")
	pxyConn.SetDeadline(time.Now().Add(pxyStaleDuration))
	startProxy := new(StartTunnel)
	if err = ReadMsgInto(pxyConn, startProxy); err != nil {
		log.Debug("Client::regProxy try receive StartProxy error: %v", err)
		return
	}

	log.Debug("Client::regProxy received startProxy: %v", startProxy)
	if err := n.getReqPermission(startProxy); err != nil {
		log.Debug("Client::regProxy request permission denied: %v", err)
		pxyConn.SetWriteDeadline(time.Now().Add(rwTimeout))
		WriteMsg(pxyConn, &AccessResp{Error: err.Error()})
		return
	}

	localConn, err := Dial("tcp", startProxy.DstAddr, dialTimeout, nil)
	if err != nil {
		log.Error("Client::regProxy dial dst error: %v", err)
		pxyConn.SetWriteDeadline(time.Now().Add(rwTimeout))
		WriteMsg(pxyConn, &AccessResp{Error: err.Error()})
		return
	}
	defer localConn.Close()

	pxyConn.SetWriteDeadline(time.Now().Add(rwTimeout))
	if err = WriteMsg(pxyConn, new(AccessResp)); err != nil {
		log.Error("Client::regProxy send success message error: %v", err)
		return
	}

	pxyConn.SetDeadline(time.Time{})

	log.Debug("Client::regProxy start joining localConn, pxyConn")
	p := NewPlumber(localConn, pxyConn)
	bytes2LocalConn, bytes2PxyConn := p.Pipe(bytesForRate)
	log.Debug("Client::regProxy bytes2LocalConn:%d bytes2PxyConn:%d", bytes2LocalConn, bytes2PxyConn)
}

func (n *Node) ReqBroker(dstId, dstAddr string, localConn net.Conn) {
	n.RLock()

	defer localConn.Close()

	if !n.isRunning {
		log.Debug("Client::ReqBroker client is not running")
		n.RUnlock()
		return
	}

	var (
		reqConn net.Conn
		err     error
	)

	if reqConn, err = Dial("tcp", n.brokerAddr, dialTimeout, n.tlsConfig); err != nil {
		log.Error("Client::ReqBroker failed to establish proxy connection: %v", err)
		n.RUnlock()
		return
	}

	defer reqConn.Close()

	// send ReqBroker message
	reqConn.SetWriteDeadline(time.Now().Add(rwTimeout))
	if err = WriteMsg(reqConn, &ReqBroker{
		TokenString:     n.tokenString,
		DstId:           dstId,
		DstAddr:         dstAddr,
		ProtocolVersion: protocolVersion,
	}); err != nil {
		log.Error("Client::ReqBroker send RegProxy message error: %v", err)
		n.RUnlock()
		return
	}

	resp := new(AccessResp)
	reqConn.SetReadDeadline(time.Now().Add(rwTimeout))
	if err = ReadMsgInto(reqConn, resp); err != nil {
		log.Error("Client::ReqBroker read response message error: %v", err)
		n.RUnlock()
		return
	}

	if resp.Error != "" {
		log.Info("Client::ReqBroker request for broker denied: %s", resp.Error)
		n.RUnlock()
		return
	}

	reqConn.SetDeadline(time.Time{})
	log.Debug("Client::ReqBroker start joining localConn, reqConn")

	// create a routine to pipe
	p := NewPlumber(localConn, reqConn)
	n.plumbers.Add(p)
	n.RUnlock()

	// block until pipe complete
	bytes2LocalConn, bytes2reqConn := p.Pipe(bytesForRate)
	log.Debug("Client::regProxy bytes2LocalConn:%d bytes2reqConn:%d", bytes2LocalConn, bytes2reqConn)
}

func (n *Node) Stop() {
	n.Lock()
	defer n.Unlock()

	if !n.isRunning {
		log.Debug("Client::Stop is not running")
		return
	}

	n.shutdown.Begin()
	n.shutdown.WaitComplete()
	n.plumbers.Each(func(elem interface{}) { elem.(*Plumber).Close() })
	n.plumbers.Clean()
	n.shutdown = nil
	n.isRunning = false
}
