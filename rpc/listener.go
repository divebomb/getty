package rpc

import (
	"github.com/AlexStocks/getty/rpc/mq"
	"sync"
	"time"
)

import (
	log "github.com/AlexStocks/log4go"
	jerrors "github.com/juju/errors"
)

import (
	"github.com/AlexStocks/getty"
)

var (
	errTooManySessions = jerrors.New("too many sessions")
)

type rpcSession struct {
	session getty.Session
	reqNum  int32
}

////////////////////////////////////////////
// RpcServerHandler
////////////////////////////////////////////

type MQPacketHandler func(packet *mq.Packet) error

type RpcServerHandler struct {
	maxSessionNum  int
	sessionTimeout time.Duration
	mqPkgHandler   MQPacketHandler
	rwlock         sync.RWMutex
	sessionMap     map[getty.Session]*rpcSession
}

func NewRpcServerHandler(maxSessionNum int, sessionTimeout time.Duration, handler MQPacketHandler) *RpcServerHandler {
	return &RpcServerHandler{
		maxSessionNum:  maxSessionNum,
		sessionTimeout: sessionTimeout,
		mqPkgHandler:   handler,
		sessionMap:     make(map[getty.Session]*rpcSession),
	}
}

func (h *RpcServerHandler) OnOpen(session getty.Session) error {
	var err error
	h.rwlock.RLock()
	if h.maxSessionNum <= len(h.sessionMap) {
		err = errTooManySessions
	}
	h.rwlock.RUnlock()
	if err != nil {
		return jerrors.Trace(err)
	}

	log.Info("got session:%s", session.Stat())
	h.rwlock.Lock()
	h.sessionMap[session] = &rpcSession{session: session}
	h.rwlock.Unlock()
	return nil
}

func (h *RpcServerHandler) OnError(session getty.Session, err error) {
	log.Info("session{%s} got error{%v}, will be closed.", session.Stat(), err)
	h.rwlock.Lock()
	delete(h.sessionMap, session)
	h.rwlock.Unlock()
}

func (h *RpcServerHandler) OnClose(session getty.Session) {
	log.Info("session{%s} is closing......", session.Stat())
	h.rwlock.Lock()
	delete(h.sessionMap, session)
	h.rwlock.Unlock()
}

func (h *RpcServerHandler) OnMessage(session getty.Session, pkg interface{}) {
	h.rwlock.Lock()
	if _, ok := h.sessionMap[session]; ok {
		h.sessionMap[session].reqNum++
	}
	h.rwlock.Unlock()

	req, ok := pkg.(*mq.Packet)
	if !ok {
		log.Error("illegal package{%#v}", pkg)
		return
	}
	err := h.mqPkgHandler(req)
	if err != nil {
		log.Error("h.callService(session:%#v, req:%#v) = %s", session, req, jerrors.ErrorStack(err))
	}
}

func (h *RpcServerHandler) OnCron(session getty.Session) {
	var (
		flag   bool
		active time.Time
	)

	h.rwlock.RLock()
	if _, ok := h.sessionMap[session]; ok {
		active = session.GetActive()
		if h.sessionTimeout.Nanoseconds() < time.Since(active).Nanoseconds() {
			flag = true
			log.Warn("session{%s} timeout{%s}, reqNum{%d}",
				session.Stat(), time.Since(active).String(), h.sessionMap[session].reqNum)
		}
	}
	h.rwlock.RUnlock()

	if flag {
		h.rwlock.Lock()
		delete(h.sessionMap, session)
		h.rwlock.Unlock()
		session.Close()
	}
}

////////////////////////////////////////////
// RpcClientHandler
////////////////////////////////////////////

type RpcClientHandler struct {
	conn *gettyRPCClient
}

func NewRpcClientHandler(client *gettyRPCClient) *RpcClientHandler {
	return &RpcClientHandler{conn: client}
}

func (h *RpcClientHandler) OnOpen(session getty.Session) error {
	h.conn.addSession(session)
	return nil
}

func (h *RpcClientHandler) OnError(session getty.Session, err error) {
	log.Info("session{%s} got error{%v}, will be closed.", session.Stat(), err)
	h.conn.removeSession(session)
}

func (h *RpcClientHandler) OnClose(session getty.Session) {
	log.Info("session{%s} is closing......", session.Stat())
	h.conn.removeSession(session)
}

func (h *RpcClientHandler) OnMessage(session getty.Session, pkg interface{}) {
	p, ok := pkg.(*mq.Packet)
	if !ok {
		log.Error("illegal package{%#v}", pkg)
		return
	}
	log.Debug("get rpc response{%#v}", p)
	h.conn.updateSession(session)

	pendingResponse := h.conn.pool.rpcClient.removePendingResponse(SequenceType(p.PacketId))
	if pendingResponse == nil {
		return
	}

	pendingResponse.reply = p
	if pendingResponse.callback == nil {
		pendingResponse.done <- struct{}{}
	} else {
		pendingResponse.callback(pendingResponse.GetCallResponse())
	}
}

func (h *RpcClientHandler) OnCron(session getty.Session) {
	rpcSession, err := h.conn.getClientRpcSession(session)
	if err != nil {
		log.Error("client.getClientSession(session{%s}) = error{%s}",
			session.Stat(), jerrors.ErrorStack(err))
		return
	}
	if h.conn.pool.rpcClient.conf.sessionTimeout.Nanoseconds() < time.Since(session.GetActive()).Nanoseconds() {
		log.Warn("session{%s} timeout{%s}, reqNum{%d}",
			session.Stat(), time.Since(session.GetActive()).String(), rpcSession.reqNum)
		h.conn.removeSession(session) // -> h.conn.close() -> h.conn.pool.remove(h.conn)
		return
	}

	codecType := GetCodecType(h.conn.protocol)

	h.conn.pool.rpcClient.heartbeat(session, codecType)
}
