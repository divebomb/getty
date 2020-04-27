package rpc

import (
	"time"
)

import (
	log "github.com/AlexStocks/log4go"
	jerrors "github.com/juju/errors"
	uberAtomic "go.uber.org/atomic"
)

import (
	"github.com/divebomb/getty"
	"github.com/divebomb/getty/rpc/mq"
)

var (
	errTooManySessions = jerrors.New("too many sessions")
)

type rpcSession struct {
	getty.Session
	reqNum  uberAtomic.Int32
}

func (s *rpcSession) AddReqNum(num int32) {
	s.reqNum.Add(num)
}

func (s *rpcSession) GetReqNum() int32 {
	return s.reqNum.Load()
}

////////////////////////////////////////////
// RpcServerHandler
////////////////////////////////////////////

type MQPacketHandler func(ss getty.Session, packet *mq.Packet) error

type RpcServerHandler struct {
	*getty.SessionMap
	maxSessionNum  int
	sessionTimeout time.Duration
	mqPkgHandler   MQPacketHandler
}

func NewRpcServerHandler(maxSessionNum int, sessionTimeout time.Duration, handler MQPacketHandler) *RpcServerHandler {
	return &RpcServerHandler{
		maxSessionNum:  maxSessionNum,
		sessionTimeout: sessionTimeout,
		mqPkgHandler:   handler,
		SessionMap:     getty.NewSessionMap(),
	}
}

//func (h *RpcServerHandler) SessionSet() []getty.Session {
//	h.rwlock.RLock()
//	defer h.rwlock.RUnlock()
//
//	arr := make([]getty.Session, 0, len(h.sessionMap))
//	for s := range h.sessionMap {
//		arr = append(arr, s)
//	}
//
//	return arr
//}

func (h *RpcServerHandler) OnOpen(session getty.Session) error {
	sz := h.SessionMap.Size()
	if sz >= h.maxSessionNum {
		return errTooManySessions
	}

	rs := rpcSession{
		Session: session,
	}
	return jerrors.Trace(h.SessionMap.AddSession(&rs))
}

func (h *RpcServerHandler) OnError(session getty.Session, err error) {
	log.Info("session{%s} got error{%v}, will be closed.", session.Stat(), err)

	h.SessionMap.RemoveSession(session)
}

func (h *RpcServerHandler) OnClose(session getty.Session) {
	log.Info("session{%s} is closing......", session.Stat())

	h.SessionMap.RemoveSession(session)
}

func (h *RpcServerHandler) OnMessage(session getty.Session, pkg interface{}) {
	var rs *rpcSession
	if session != nil {
		func() {
			s := h.SessionMap.GetSessionBySessionID(session.ID())
			ss, ok := s.(*rpcSession)
			if ok && ss != nil {
				rs = ss
			}
		}()
		if rs != nil {
			rs.AddReqNum(1)
		}
	}

	req, ok := pkg.(*mq.Packet)
	if !ok {
		log.Error("illegal package{%#v}", pkg)
		return
	}
	err := h.mqPkgHandler(session, req)
	if err != nil {
		log.Error("h.callService(session:%#v, req:%#v) = %s", session, req, jerrors.ErrorStack(err))
	}
}

func (h *RpcServerHandler) OnCron(session getty.Session) {
	var (
		flag   bool
		active time.Time
	)

	if rs := h.SessionMap.GetSessionBySessionID(session.ID()); rs != nil {
		active = session.GetActive()
		if h.sessionTimeout.Nanoseconds() < time.Since(active).Nanoseconds() {
			flag = true
			log.Warn("session{%s} timeout{%s}", session.Stat(), time.Since(active).String())
		}
	}

	if flag {
		h.SessionMap.RemoveSession(session)
		session.Close()
	}
}

////////////////////////////////////////////
// RpcClientHandler
////////////////////////////////////////////

type HandleServerRequest MQPacketHandler

type RpcClientHandler struct {
	conn                *gettyRPCClient
	handleServerRequest MQPacketHandler
}

func NewRpcClientHandler(client *gettyRPCClient) *RpcClientHandler {
	h := &RpcClientHandler{conn: client}
	if client != nil && client.pool != nil && client.pool.handleServerRequest != nil {
		h.handleServerRequest = client.pool.handleServerRequest
	}

	return h
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
	h.conn.updateSession(session)

	pendingResponse := h.conn.pool.rpcClient.removePendingResponse(SequenceType(p.PacketId))
	if pendingResponse == nil {
		if h.handleServerRequest == nil {
			log.Error("can not handle server package %+v", p)
			return
		}
		if err := h.handleServerRequest(session, p); err != nil {
			log.Error("handleServerRequest(session:%s, p:%+v) = error:%+v", session.Stat(), p, err)
		}

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
		log.Warn("session{%s} timeout{%s}", session.Stat(), time.Since(session.GetActive()).String())
		h.conn.removeSession(session) // -> h.conn.close() -> h.conn.pool.remove(h.conn)
		return
	}

	h.conn.pool.rpcClient.heartbeat(session)
}
