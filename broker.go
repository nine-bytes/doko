package doko

import (
	"github.com/nine-bytes/log"
	"github.com/nine-bytes/util"
	"crypto/tls"
	"net"
	"time"
	"runtime/debug"
	"errors"
	"sync"
)

type AuthenticateFunc func(authMsg *Auth) error
type ReqBrokerPermission func(reqBrokerMsg *ReqBroker) error

type Broker struct {
	getAuthenticate     AuthenticateFunc
	reqBrokerPermission ReqBrokerPermission
	listener            *Listener
	controllers         *util.Registry
}

func NewBroker(getAuthenticate AuthenticateFunc, reqBrokerPermission ReqBrokerPermission) *Broker {
	return &Broker{
		getAuthenticate:     getAuthenticate,
		reqBrokerPermission: reqBrokerPermission,
		controllers:         util.NewRegistry(),
	}
}

func (b *Broker) ListenAddr() net.Addr {
	return b.listener.Addr()
}

func (b *Broker) Run(listenerAddr string, tlsConfig *tls.Config) (err error) {
	if b.listener, err = NewListener("tcp", listenerAddr, tlsConfig); err != nil {
		return log.Error("listening %s error: %v", listenerAddr, err)
	}

	go b.daemon()
	return nil
}

func (b *Broker) AllController(f func(id string, controller *NodeController)) {
	b.controllers.All(func(key, value interface{}) { f(key.(string), value.(*NodeController)) })
}

func (b *Broker) EachController(f func(id string, controller *NodeController, stop *bool)) {
	b.controllers.Each(func(key, value interface{}, stop *bool) { f(key.(string), value.(*NodeController), stop) })
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
				b.tunnel(conn, msg)

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
	if err := b.getAuthenticate(authMsg); err != nil {
		ctlConn.SetWriteDeadline(time.Now().Add(rwTimeout))
		WriteMsg(ctlConn, &AccessResp{Error: err.Error()})
		ctlConn.Close()
		return
	}

	// create the object
	bc := &NodeController{
		id:              authMsg.Id,
		authMsg:         authMsg,
		ctlConn:         ctlConn,
		out:             make(chan Message),
		in:              make(chan Message),
		tunnels:         make(chan net.Conn, pxyPoolSize),
		lastPing:        time.Now(),
		writerShutdown:  util.NewShutdown(),
		readerShutdown:  util.NewShutdown(),
		managerShutdown: util.NewShutdown(),
		shutdown:        util.NewShutdown(),
		broker:          b,
		plumbers:        util.NewSet(0),
	}

	// register the control
	if old := b.controllers.Add(authMsg.Id, bc); old != nil {
		// old had been kicked out
		// routine for shutdown the old one
		go func(old *NodeController) {
			// send bye message to avoid control reconnect
			if err := util.PanicToError(func() { old.out <- new(Bye) }); err != nil {
				log.Debug("send Bye message error: %v", err)
			}

			// change id to empty string
			old.id = ""

			// tell the old one to shutdown
			old.shutdown.Begin()
		}(old.(*NodeController))
	}

	// start four goroutines
	go bc.writer()
	go bc.manager()
	go bc.reader()
	go bc.stopper()

	// send success message
	util.PanicToError(func() { bc.out <- new(AccessResp) })

	log.Debug("Broker::control authenticate with id: %v", authMsg.Id)
}

func (b *Broker) tunnel(pxyConn net.Conn, reqTunnel *RegTunnel) {
	// authenticate firstly
	if err := b.getAuthenticate(&reqTunnel.Auth); err != nil {
		pxyConn.SetWriteDeadline(time.Now().Add(rwTimeout))
		WriteMsg(pxyConn, &AccessResp{Error: err.Error()})
		pxyConn.Close()
		return
	}

	pxyConn.SetWriteDeadline(time.Now().Add(rwTimeout))
	if err := WriteMsg(pxyConn, new(AccessResp)); err != nil {
		pxyConn.Close()
		return
	}

	// look up the control for this tunnel conn
	var ctl *NodeController
	if tmp, ok := b.controllers.Get(reqTunnel.Auth.Id); !ok {
		log.Debug("Broker::tunnel registering new tunnel for %s", reqTunnel.Auth.Id)
		pxyConn.Close()
		return
	} else {
		ctl = tmp.(*NodeController)
	}

	log.Debug("Broker::tunnel registering new tunnel for %s", reqTunnel.Auth.Id)
	ctl.regTunnel(pxyConn)
}

func (b *Broker) Broker(srcConn net.Conn, reqBrokerMsg *ReqBroker) {
	b.broker(srcConn, reqBrokerMsg)
}

func (b *Broker) broker(srcConn net.Conn, reqBrokerMsg *ReqBroker) {
	defer srcConn.Close()

	// authenticate
	if err := b.reqBrokerPermission(reqBrokerMsg); err != nil {
		srcConn.SetWriteDeadline(time.Now().Add(rwTimeout))
		WriteMsg(srcConn, &AccessResp{Error: err.Error()})
		return
	}

	// look up the control connection for this tunnel
	var dstCtl *NodeController
	if tmp, ok := b.controllers.Get(reqBrokerMsg.DstId); !ok {
		log.Debug("Broker::broker no control found for target control id: %s", reqBrokerMsg.DstId)
		return
	} else {
		dstCtl = tmp.(*NodeController)
	}

	log.Debug("Broker:broker get dstConn for %s", dstCtl.id)
	dstConn, err := dstCtl.getTunnel(reqBrokerMsg)
	if err != nil {
		srcConn.SetWriteDeadline(time.Now().Add(rwTimeout))
		WriteMsg(srcConn, &AccessResp{Error: "failed to get tunnel connection of target control"})
		return
	}
	defer dstConn.Close()

	srcConn.SetWriteDeadline(time.Now().Add(rwTimeout))
	if err := WriteMsg(srcConn, new(AccessResp)); err != nil {
		log.Error("Broker::broker send broker success message error: %v", err)
		return
	}

	srcConn.SetDeadline(time.Time{})
	// join the public and tunnel connections
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

type NodeController struct {
	// id of the control
	id string

	// authMsg
	authMsg *Auth

	// main controller connection
	ctlConn net.Conn

	// broker of the controller
	broker *Broker

	// unlimited capacity set
	plumbers    *util.Set
	stopping    bool
	stopRWMutex sync.RWMutex

	// put a message in this channel to send it over conn to the controller
	out chan Message

	// read from this channel to get the next message sent to us over conn by the controller
	in chan Message

	// the last time we received a ping from the controller - for heartbeats
	lastPing time.Time

	// tunnel connections
	tunnels chan net.Conn

	// synchronizer for writer()
	writerShutdown *util.Shutdown

	// synchronizer for reader()
	readerShutdown *util.Shutdown

	// synchronizer for manager()
	managerShutdown *util.Shutdown

	// synchronizer for entire controller
	shutdown *util.Shutdown
}

func (nc *NodeController) AuthMsg() *Auth {
	return nc.authMsg
}

func (nc *NodeController) manager() {
	defer func() {
		if err := recover(); err != nil {
			log.Error("NodeController::manager recover with error: %v, stack: %s", err, debug.Stack())
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
				log.Debug("NodeController::manager lost heartbeat")
				return
			}

		case mRaw, ok := <-nc.in:
			// c.in closes to indicate shutdown
			if !ok {
				log.Debug("NodeController::manager chan bc.in closed")
				return
			}

			//log.Debug("NodeController::manager PING")
			if _, ok := mRaw.(*Ping); ok {
				nc.lastPing = time.Now()
				// don't crash on panic
				if err := util.PanicToError(func() { nc.out <- new(Pong) }); err != nil {
					log.Debug("NodeController::manager send message to bc.out error: %v", err)
					return
				}
				//log.Debug("NodeController::manager PONG")
			}
		}
	}
}

func (nc *NodeController) writer() {
	defer func() {
		if err := recover(); err != nil {
			log.Error("NodeController::writer recover with error: %v, stack: %s", err, debug.Stack())
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
			log.Debug("NodeController::writer WriteMsg error: %v", err)
			return
		}
	}
}

func (nc *NodeController) reader() {
	defer func() {
		if err := recover(); err != nil {
			log.Error("NodeController::reader recover with error: %v, stack: %s", err, debug.Stack())
		}
	}()

	// kill everything if the reader stops
	defer nc.shutdown.Begin()

	// notify that we're done
	defer nc.readerShutdown.Complete()

	// read messages from the control channel
	for {
		if message, err := ReadMsg(nc.ctlConn); err != nil {
			log.Debug("NodeController::read message: %v", err)
			return
		} else {
			// this can also panic during shutdown
			if err := util.PanicToError(func() { nc.in <- message }); err != nil {
				log.Debug("NodeController::reader bc.in <- message error: %v", err)
				return
			}
		}
	}
}

func (nc *NodeController) stopper() {
	defer func() {
		if err := recover(); err != nil {
			log.Error("NodeController::stopper recover with error: %v, stack: %s", err, debug.Stack())
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
	nc.broker.controllers.Del(nc.id, nc)

	// shutdown manager() so that we have no more work to do
	close(nc.in)
	nc.managerShutdown.WaitComplete()

	// shutdown writer()
	close(nc.out)
	nc.writerShutdown.WaitComplete()

	// close connection fully
	nc.ctlConn.Close()

	// close all of the tunnel connections
	close(nc.tunnels)
	wg := new(sync.WaitGroup)
	for p := range nc.tunnels {
		wg.Add(1)
		go func(p net.Conn, wg *sync.WaitGroup) {
			defer wg.Done()
			p.Close()
		}(p, wg)
	}
	wg.Wait()

	log.Debug("NodeController::stopper shutdown %s complete!", nc.id)
}

// Remove a tunnel connection from the pool and return it
// If not tunnel connections are in the pool, request one
// and wait until it is available
// Returns an error if we couldn't get a tunnel because it took too long
// or the tunnel is closing
func (nc *NodeController) getTunnel(reqBrokerMsg *ReqBroker) (tunConn net.Conn, err error) {
	var ok bool
	for {
		for {
			// get a tunnel connection from the pool
			select {
			case tunConn, ok = <-nc.tunnels:
				if !ok {
					return nil, errors.New("no tunnel connections available, control is closing")
				}
				log.Debug("NodeController::getTunnel get a tunnel connection from pool")
				goto end
			default:
				// no tunnel available in the pool, ask for one over the control channel
				log.Debug("NodeController::getTunnel no tunnel in pool, send ReqTunnel message...")
				if err = util.PanicToError(func() { nc.out <- new(ReqTunnel) }); err != nil {
					// c.out is closed, impossible to get a tunnel connection
					log.Debug("NodeController::getTunnel send message to c.out error: %v", err)
					return
				}

				select {
				case <-time.After(dialTimeout):
					// try again, never stop believing
					continue
				case tunConn, ok = <-nc.tunnels:
					if !ok {
						return nil, errors.New("no tunnel connections available, control is closing")
					}
				}
				log.Debug("NodeController::getTunnel get a tunnel connection after sending ReqTunnel")
				goto end
			}
		}

	end:
		{
			// try to send StartTunnel message
			if err := WriteMsg(tunConn, &StartTunnel{*reqBrokerMsg}); err != nil {
				// this tunnel connection is reached deadline
				log.Debug("NodeController::getTunnel failed to send ping: %v", err)
				tunConn.Close()
				tunConn = nil
				continue
			}

			// receive response message
			resp := new(AccessResp)
			tunConn.SetReadDeadline(time.Now().Add(rwTimeout))
			if err := ReadMsgInto(tunConn, resp); err != nil {
				log.Debug("NodeController::getTunnel failed to receive response message: %v", err)
				tunConn.Close()
				tunConn = nil
				continue
			}

			if resp.Error != "" {
				log.Debug("NodeController::getTunnel failed with response: %s", resp.Error)
				tunConn.Close()
				return nil, errors.New(resp.Error)
			}

			tunConn.SetDeadline(time.Time{})

			util.PanicToError(func() { nc.out <- new(ReqTunnel) })
			break
		}
	}

	return
}

func (nc *NodeController) regTunnel(pxyConn net.Conn) {
	select {
	case nc.tunnels <- pxyConn:
		pxyConn.SetDeadline(time.Now().Add(pxyStaleDuration))
	default:
		log.Debug("NodeController::regTunnel proxies buffer is full, discarding.")
		pxyConn.Close()
	}
}
