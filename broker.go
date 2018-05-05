package doko

import (
	"github.com/nine-bytes/util/log"
	"github.com/nine-bytes/util"
	"crypto/tls"
	"net"
	"time"
	"runtime/debug"
	"errors"
	"sync"
)

type AuthenticateFunc func(tokenString string) (controlId string, err error)

type Broker struct {
	getAuthenticate AuthenticateFunc
	listener        *Listener
	registry        *util.Registry
}

func NewBroker(getAuthenticate AuthenticateFunc) *Broker {
	return &Broker{
		getAuthenticate: getAuthenticate,
		registry:        util.NewRegistry(),
	}
}

func (b *Broker) Run(listenerAddr string, tlsConfig *tls.Config) (err error) {
	if b.listener, err = NewListener("tcp", listenerAddr, tlsConfig); err != nil {
		return log.Error("listening %s error: %v", listenerAddr, err)
	}

	go b.daemon()
	return nil
}

func (b *Broker) daemon() {
	for {
		conn, ok := <-b.listener.ConnChan
		if !ok {
			log.Debug("listener.Conn closed")
			return
		}

		go func(conn net.Conn) {
			var rawMsg Message

			var err error
			conn.SetReadDeadline(time.Now().Add(rwTimeout))
			if rawMsg, err = ReadMsg(conn); err != nil {
				conn.Close()
				return
			}
			conn.SetReadDeadline(time.Time{})

			switch msg := rawMsg.(type) {
			case *Auth:
				b.control(conn, msg)

			case *RegTunnel:
				b.proxy(conn, msg)

			case *ReqBroker:
				b.broker(conn, msg)

			default:
				conn.Close()
			}
		}(conn)
	}
}

func (b *Broker) control(ctlConn net.Conn, authMsg *Auth) {
	// authenticate firstly
	id, err := b.getAuthenticate(authMsg.TokenString)
	if err != nil {
		ctlConn.SetWriteDeadline(time.Now().Add(rwTimeout))
		WriteMsg(ctlConn, &AccessResp{Error: err.Error()})
		return
	}

	// create the object
	bc := &nodeControl{
		id:              id,
		ctlConn:         ctlConn,
		out:             make(chan Message),
		in:              make(chan Message),
		proxies:         make(chan net.Conn, pxyPoolSize),
		lastPing:        time.Now(),
		writerShutdown:  util.NewShutdown(),
		readerShutdown:  util.NewShutdown(),
		managerShutdown: util.NewShutdown(),
		shutdown:        util.NewShutdown(),
		broker:          b,
		plumbers:        util.NewSet(0),
	}

	// register the control
	if old := b.registry.Add(id, bc); old != nil {
		// old had been kicked out
		// routine for shutdown the old one
		go func(old *nodeControl) {
			// send bye message to avoid control reconnect
			if err := util.PanicToError(func() { old.out <- new(Bye) }); err != nil {
				log.Debug("send Bye message error: %v", err)
			}

			// change id to empty string
			old.id = ""

			// tell the old one to shutdown
			old.shutdown.Begin()
		}(old.(*nodeControl))
	}

	// start four goroutines
	go bc.writer()
	go bc.manager()
	go bc.reader()
	go bc.stopper()

	// send success message
	util.PanicToError(func() { bc.out <- new(AccessResp) })

	log.Debug("Broker::control authenticate with id: %v", id)
}

func (b *Broker) proxy(pxyConn net.Conn, authMsg *RegTunnel) {
	// authenticate firstly
	id, err := b.getAuthenticate(authMsg.TokenString)
	if err != nil {
		pxyConn.SetWriteDeadline(time.Now().Add(rwTimeout))
		WriteMsg(pxyConn, &AccessResp{Error: err.Error()})
		return
	}

	pxyConn.SetWriteDeadline(time.Now().Add(rwTimeout))
	if err := WriteMsg(pxyConn, new(AccessResp)); err != nil {
		pxyConn.Close()
		return
	}

	// look up the control for this proxy conn
	var ctl *nodeControl
	if tmp, ok := b.registry.Get(id); !ok {
		log.Debug("Broker::proxy registering new proxy for %s", id)
		pxyConn.Close()
		return
	} else {
		ctl = tmp.(*nodeControl)
	}

	log.Debug("Broker::proxy registering new proxy for %s", id)
	ctl.regProxy(pxyConn)
}

func (b *Broker) broker(srcConn net.Conn, reqBrokerMsg *ReqBroker) {
	defer srcConn.Close()

	srcId, err := b.getAuthenticate(reqBrokerMsg.TokenString)
	if err != nil {
		srcConn.SetWriteDeadline(time.Now().Add(rwTimeout))
		WriteMsg(srcConn, &AccessResp{Error: err.Error()})
		return
	}

	// look up the control connection for this proxy
	var dstCtl *nodeControl
	if tmp, ok := b.registry.Get(reqBrokerMsg.DstId); !ok {
		log.Debug("Broker::broker no control found for target control id: %s", reqBrokerMsg.DstId)
		return
	} else {
		dstCtl = tmp.(*nodeControl)
	}

	log.Debug("Broker:broker get dstConn for %s", dstCtl.id)
	dstConn, err := dstCtl.getProxy(srcId, reqBrokerMsg.DstAddr)
	if err != nil {
		srcConn.SetWriteDeadline(time.Now().Add(rwTimeout))
		WriteMsg(srcConn, &AccessResp{Error: "failed to get proxy connection of target control"})
		return
	}
	defer dstConn.Close()

	srcConn.SetWriteDeadline(time.Now().Add(rwTimeout))
	if err := WriteMsg(srcConn, new(AccessResp)); err != nil {
		log.Error("Broker::broker send broker success message error: %v", err)
		return
	}

	srcConn.SetDeadline(time.Time{})
	// join the public and proxy connections
	log.Debug("start joining srcConn, dstConn")

	p := NewPlumber(srcConn, dstConn)
	dstCtl.stopRWMutex.RLock()
	if dstCtl.stopping {
		dstCtl.stopRWMutex.RUnlock()
		log.Debug("Broker::broker dstCtl %s is stopping", dstCtl.id)
		return
	}
	dstCtl.plumbers.Add(p)
	defer dstCtl.plumbers.Remove(p)
	dstCtl.stopRWMutex.RUnlock()

	bytes2Src, bytes2Dst := p.Pipe(bytesForRate)
	log.Debug("Broker::broker bytes2Src :%d bytes2Dst:%d", bytes2Src, bytes2Dst)
}

type nodeControl struct {
	// id of the control
	id string

	// main control connection
	ctlConn net.Conn

	// broker of the control
	broker *Broker

	// unlimited capacity set
	plumbers    *util.Set
	stopping    bool
	stopRWMutex sync.RWMutex

	// put a message in this channel to send it over conn to the control
	out chan Message

	// read from this channel to get the next message sent to us over conn by the control
	in chan Message

	// the last time we received a ping from the control - for heartbeats
	lastPing time.Time

	// proxy connections
	proxies chan net.Conn

	// synchronizer for controlled shutdown of writer()
	writerShutdown *util.Shutdown

	// synchronizer for controlled shutdown of reader()
	readerShutdown *util.Shutdown

	// synchronizer for controlled shutdown of manager()
	managerShutdown *util.Shutdown

	// synchronizer for controller shutdown of entire control
	shutdown *util.Shutdown
}

func (nc *nodeControl) manager() {
	defer func() {
		if err := recover(); err != nil {
			log.Error("nodeControl::manager recover with error: %v, stack: %s", err, debug.Stack())
		}
	}()

	// kill everything if the control manager stops
	defer nc.shutdown.Begin()

	// notify that manager() has shutdown
	defer nc.managerShutdown.Complete()

	// reaping timer for detecting heartbeat failure
	pingCheck := time.NewTicker(time.Second)
	defer pingCheck.Stop()

	for {
		select {
		case <-pingCheck.C:
			if time.Since(nc.lastPing) > (pingInterval + rwTimeout) {
				log.Debug("nodeControl::manager lost heartbeat")
				return
			}

		case mRaw, ok := <-nc.in:
			// c.in closes to indicate shutdown
			if !ok {
				log.Debug("nodeControl::manager chan bc.in closed")
				return
			}

			//log.Debug("nodeControl::manager PING")
			if _, ok := mRaw.(*Ping); ok {
				nc.lastPing = time.Now()
				// don't crash on panic
				if err := util.PanicToError(func() { nc.out <- new(Pong) }); err != nil {
					log.Debug("nodeControl::manager send message to bc.out error: %v", err)
					return
				}
				//log.Debug("nodeControl::manager PONG")
			}
		}
	}
}

func (nc *nodeControl) writer() {
	defer func() {
		if err := recover(); err != nil {
			log.Error("nodeControl::writer recover with error: %v, stack: %s", err, debug.Stack())
		}
	}()

	// kill everything if the writer() stops
	defer nc.shutdown.Begin()

	// notify that we've flushed all messages
	defer nc.writerShutdown.Complete()

	// write messages to the control channel
	for m := range nc.out {
		nc.ctlConn.SetWriteDeadline(time.Now().Add(rwTimeout))
		if err := WriteMsg(nc.ctlConn, m); err != nil {
			// bc.conn may be closed
			log.Debug("nodeControl::writer WriteMsg error: %v", err)
			return
		}
	}
}

func (nc *nodeControl) reader() {
	defer func() {
		if err := recover(); err != nil {
			log.Error("nodeControl::reader recover with error: %v, stack: %s", err, debug.Stack())
		}
	}()

	// kill everything if the reader stops
	defer nc.shutdown.Begin()

	// notify that we're done
	defer nc.readerShutdown.Complete()

	// read messages from the control channel
	for {
		if message, err := ReadMsg(nc.ctlConn); err != nil {
			log.Debug("nodeControl::read message: %v", err)
			return
		} else {
			// this can also panic during shutdown
			if err := util.PanicToError(func() { nc.in <- message }); err != nil {
				log.Debug("nodeControl::reader bc.in <- message error: %v", err)
				return
			}
		}
	}
}

func (nc *nodeControl) stopper() {
	defer func() {
		if err := recover(); err != nil {
			log.Error("nodeControl::stopper recover with error: %v, stack: %s", err, debug.Stack())
		}
	}()

	defer nc.shutdown.Complete()

	// wait until we're instructed to shutdown
	nc.shutdown.WaitBegin()

	nc.stopRWMutex.Lock()
	nc.stopping = true
	nc.stopRWMutex.Unlock()

	// close all plumbers
	nc.plumbers.Each(func(elem interface{}) { elem.(*Plumber).Close() })
	nc.plumbers.Clean()

	// remove myself from the control registry
	nc.broker.registry.Del(nc.id, nc)

	// shutdown manager() so that we have no more work to do
	close(nc.in)
	nc.managerShutdown.WaitComplete()

	// shutdown writer()
	close(nc.out)
	nc.writerShutdown.WaitComplete()

	// close connection fully
	nc.ctlConn.Close()

	// close all of the proxy connections
	close(nc.proxies)
	wg := new(sync.WaitGroup)
	for p := range nc.proxies {
		wg.Add(1)
		go func(p net.Conn, wg *sync.WaitGroup) {
			defer wg.Done()
			p.Close()
		}(p, wg)
	}
	wg.Wait()

	log.Debug("nodeControl::stopper shutdown %s complete!", nc.id)
}

// Remove a proxy connection from the pool and return it
// If not proxy connections are in the pool, request one
// and wait until it is available
// Returns an error if we couldn't get a proxy because it took too long
// or the tunnel is closing
func (nc *nodeControl) getProxy(srcId, dstAddr string) (pxyConn net.Conn, err error) {
	var ok bool
	for {
		for {
			// get a proxy connection from the pool
			select {
			case pxyConn, ok = <-nc.proxies:
				if !ok {
					return nil, errors.New("no proxy connections available, control is closing")
				}
				log.Debug("nodeControl::getProxy get a proxy connection from pool")
				goto end
			default:
				// no proxy available in the pool, ask for one over the control channel
				log.Debug("nodeControl::getProxy no proxy in pool, send ReqProxy message...")
				if err = util.PanicToError(func() { nc.out <- new(ReqTunnel) }); err != nil {
					// c.out is closed, impossible to get a proxy connection
					log.Debug("nodeControl::getProxy send message to c.out error: %v", err)
					return
				}

				select {
				case <-time.After(dialTimeout):
					// try again, never stop believing
					continue
				case pxyConn, ok = <-nc.proxies:
					if !ok {
						return nil, errors.New("no proxy connections available, control is closing")
					}
				}
				log.Debug("nodeControl::getProxy get a proxy connection after sending ReqProxy")
				goto end
			}
		}

	end:
		{
			// try to send StartProxy message
			log.Debug("nodeControl::getProxy try to send StartProxy.TargetAddr: %s", dstAddr)
			if err := WriteMsg(pxyConn, &StartTunnel{SrcId: srcId, DstAddr: dstAddr}); err != nil {
				// this proxy connection is reached deadline
				log.Debug("nodeControl::getProxy failed to send ping: %v", err)
				pxyConn.Close()
				pxyConn = nil
				continue
			}

			// receive response message
			resp := new(AccessResp)
			pxyConn.SetReadDeadline(time.Now().Add(rwTimeout))
			if err := ReadMsgInto(pxyConn, resp); err != nil {
				log.Debug("nodeControl::getProxy failed to receive response message: %v", err)
				pxyConn.Close()
				pxyConn = nil
				continue
			}

			if resp.Error != "" {
				log.Debug("nodeControl::getProxy failed with response: %s", resp.Error)
				pxyConn.Close()
				return nil, errors.New(resp.Error)
			}

			pxyConn.SetDeadline(time.Time{})

			util.PanicToError(func() { nc.out <- new(ReqTunnel) })
			break
		}
	}

	return
}

func (nc *nodeControl) regProxy(pxyConn net.Conn) {
	select {
	case nc.proxies <- pxyConn:
		pxyConn.SetDeadline(time.Now().Add(pxyStaleDuration))
	default:
		log.Debug("nodeControl::regProxy proxies buffer is full, discarding.")
		pxyConn.Close()
	}
}
