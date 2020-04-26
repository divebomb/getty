package rpc

import (
	"sync"
	"sync/atomic"
	"time"
)

import (
	log "github.com/AlexStocks/log4go"
	jerrors "github.com/juju/errors"
)

import (
	"gitlab.alipay-inc.com/alipay-com/getty"
	"gitlab.alipay-inc.com/alipay-com/getty/rpc/mq"
)

var (
	errTooManySessions = jerrors.New("too many sessions")
)

type rpcSession struct {
	session getty.Session
	reqNum  int32
}

func (s *rpcSession) AddReqNum(num int32) {
	atomic.AddInt32(&s.reqNum, num)
}

func (s *rpcSession) GetReqNum() int32 {
	return atomic.LoadInt32(&s.reqNum)
}

////////////////////////////////////////////
// RpcSessionMap
////////////////////////////////////////////

type sessionMap struct {
	rwlock         sync.RWMutex
	sessionMap     map[string][]*rpcSession // peer address -> rpcSession array
}

func newSessionMap() *sessionMap {
	return &sessionMap{
		sessionMap: make(map[string][]*rpcSession, 32),
	}
}

func (m *sessionMap) size() int {
	m.rwlock.RLock()
	defer m.rwlock.RUnlock()

	return len(m.sessionMap)
}

func (m *sessionMap) addSession(session getty.Session) error {
	rs := &rpcSession{session: session}
	addr := session.RemoteAddr()

	m.rwlock.Lock()
	defer m.rwlock.Unlock()

	arr, ok := m.sessionMap[addr]
	if !ok {
		arr = make([]*rpcSession, 0, 4)
	}
	arr = append(arr, rs)
	m.sessionMap[addr] = arr

	return nil
}

func (m *sessionMap) removeSession(session getty.Session) {
	addr := session.RemoteAddr()

	m.rwlock.Lock()
	defer m.rwlock.Unlock()

	arr, ok := m.sessionMap[addr]
	if !ok {
		return
	}
	for idx, rs := range arr {
		if rs.session == session {
			arr = append(arr[:idx], arr[idx+1:]...)
			break
		}
	}

	// this for-loop is impossible
	for idx, rs := range arr {
		if rs.session == session {
			log.Error("the same session %s exist in the same session array %+v", session.Stat(), arr)
			arr = append(arr[:idx], arr[idx+1:]...)
			break
		}
	}

	if len(arr) == 0 {
		delete(m.sessionMap, addr)
	} else {
		m.sessionMap[addr] = arr
	}
}

func (m *sessionMap) getSession(session getty.Session) *rpcSession {
	addr := session.RemoteAddr()

	m.rwlock.RLock()
	defer m.rwlock.RUnlock()

	arr, ok := m.sessionMap[addr]
	if !ok || len(arr) == 0 {
		return nil
	}

	for _, rs := range arr {
		if rs.session == session {
			return rs
		}
	}

	return nil
}

////////////////////////////////////////////
// RpcServerHandler
////////////////////////////////////////////

type MQPacketHandler func(ss getty.Session, packet *mq.Packet) error

type RpcServerHandler struct {
	*sessionMap
	maxSessionNum  int
	sessionTimeout time.Duration
	mqPkgHandler   MQPacketHandler
}

func NewRpcServerHandler(maxSessionNum int, sessionTimeout time.Duration, handler MQPacketHandler) *RpcServerHandler {
	return &RpcServerHandler{
		maxSessionNum:  maxSessionNum,
		sessionTimeout: sessionTimeout,
		mqPkgHandler:   handler,
		sessionMap:     newSessionMap(),
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
	return jerrors.Trace(h.addSession(session))
}

func (h *RpcServerHandler) OnError(session getty.Session, err error) {
	log.Info("session{%s} got error{%v}, will be closed.", session.Stat(), err)

	h.removeSession(session)
}

func (h *RpcServerHandler) OnClose(session getty.Session) {
	log.Info("session{%s} is closing......", session.Stat())

	h.removeSession(session)
}

func (h *RpcServerHandler) OnMessage(session getty.Session, pkg interface{}) {
	if session != nil {
		rs := h.getSession(session)
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

	if rs := h.getSession(session); rs != nil {
		active = session.GetActive()
		if h.sessionTimeout.Nanoseconds() < time.Since(active).Nanoseconds() {
			flag = true
			log.Warn("session{%s} timeout{%s}, reqNum{%d}",
				session.Stat(), time.Since(active).String(), rs.GetReqNum())
		}
	}

	if flag {
		h.removeSession(session)
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
		log.Warn("session{%s} timeout{%s}, reqNum{%d}",
			session.Stat(), time.Since(session.GetActive()).String(), rpcSession.GetReqNum())
		h.conn.removeSession(session) // -> h.conn.close() -> h.conn.pool.remove(h.conn)
		return
	}

	h.conn.pool.rpcClient.heartbeat(session)
}
