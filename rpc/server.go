package rpc

import (
	"fmt"
	"net"
)

import (
	log "github.com/AlexStocks/log4go"
	jerrors "github.com/juju/errors"
)

import (
	"github.com/divebomb/getty"
)

type ServerOptions struct {
	handler MQPacketHandler
}

type ServerOption func(options *ServerOptions)

func WithPackageHandler(h MQPacketHandler) ServerOption {
	return func(o *ServerOptions) {
		o.handler = h
	}
}

type Server struct {
	conf          ServerConfig
	tcpServerList []getty.Server
	rpcHandler    *RpcServerHandler
	pkgHandler    *RpcServerPackageHandler
}

func NewServer(conf *ServerConfig, opts ...ServerOption) (*Server, error) {
	if err := conf.CheckValidity(); err != nil {
		return nil, jerrors.Trace(err)
	}

	var sopts ServerOptions
	for _, o := range opts {
		o(&sopts)
	}

	s := &Server{
		conf: *conf,
	}
	s.rpcHandler = NewRpcServerHandler(s.conf.SessionNumber, s.conf.sessionTimeout, sopts.handler)
	s.pkgHandler = NewRpcServerPackageHandler(s)

	return s, nil
}

//func (s *Server) SessionSet() []getty.Session {
//	return s.rpcHandler.SessionSet()
//}

func (s *Server) newSession(session getty.Session) error {
	var (
		ok      bool
		tcpConn *net.TCPConn
	)

	if s.conf.GettySessionParam.CompressEncoding {
		session.SetCompressType(getty.CompressZip)
	}

	if tcpConn, ok = session.Conn().(*net.TCPConn); !ok {
		panic(fmt.Sprintf("%s, session.conn{%#v} is not tcp connection\n", session.Stat(), session.Conn()))
	}

	tcpConn.SetNoDelay(s.conf.GettySessionParam.TcpNoDelay)
	tcpConn.SetKeepAlive(s.conf.GettySessionParam.TcpKeepAlive)
	if s.conf.GettySessionParam.TcpKeepAlive {
		tcpConn.SetKeepAlivePeriod(s.conf.GettySessionParam.keepAlivePeriod)
	}
	tcpConn.SetReadBuffer(s.conf.GettySessionParam.TcpRBufSize)
	tcpConn.SetWriteBuffer(s.conf.GettySessionParam.TcpWBufSize)

	session.SetName(s.conf.GettySessionParam.SessionName)
	session.SetMaxMsgLen(s.conf.GettySessionParam.MaxMsgLen)
	session.SetPkgHandler(s.pkgHandler)
	session.SetEventListener(s.rpcHandler)
	session.SetRQLen(s.conf.GettySessionParam.PkgRQSize)
	session.SetWQLen(s.conf.GettySessionParam.PkgWQSize)
	session.SetReadTimeout(s.conf.GettySessionParam.tcpReadTimeout)
	session.SetWriteTimeout(s.conf.GettySessionParam.tcpWriteTimeout)
	session.SetCronPeriod((int)(s.conf.sessionTimeout.Nanoseconds() / 1e6))
	session.SetWaitTime(s.conf.GettySessionParam.waitTimeout)
	log.Debug("app accepts new session:%s\n", session.Stat())

	return nil
}

func (s *Server) Start() {
	var (
		addr      string
		portList  []string
		tcpServer getty.Server
	)

	portList = s.conf.Ports
	if len(portList) == 0 {
		panic("portList is nil")
	}
	for _, port := range portList {
		addr = net.JoinHostPort(s.conf.Host, port)
		tcpServer = getty.NewTCPServer(
			getty.WithLocalAddress(addr),
		)
		tcpServer.RunEventLoop(s.newSession)
		log.Debug("s bind addr{%s} ok!", addr)
		s.tcpServerList = append(s.tcpServerList, tcpServer)
	}
}

func (s *Server) Stop() {
	list := s.tcpServerList
	s.tcpServerList = nil
	if list != nil {
		for _, tcpServer := range list {
			tcpServer.Close()
		}
	}
}
