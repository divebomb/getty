package rpc

import (
	log "github.com/AlexStocks/log4go"
	"github.com/dubbogo/gost/bytes"
	jerrors "github.com/juju/errors"
)

import (
	"gitlab.alipay-inc.com/alipay-com/getty"
	"gitlab.alipay-inc.com/alipay-com/getty/rpc/mq"
)

////////////////////////////////////////////
// RpcServerPackageHandler
////////////////////////////////////////////

type RpcServerPackageHandler struct {
	server *Server
}

func NewRpcServerPackageHandler(server *Server) *RpcServerPackageHandler {
	return &RpcServerPackageHandler{
		server: server,
	}
}

func (p *RpcServerPackageHandler) Read(ss getty.Session, data []byte) (interface{}, int, error) {
	buf := gxbytes.GetBytesBuffer()
	buf.Write(data)
	defer gxbytes.PutBytesBuffer(buf)

	pkg := &mq.Packet{}
	length, err := pkg.Unmarshal(buf)
	if err != nil {
		if err == mq.ErrNotEnoughStream {
			return nil, 0, nil
		}
		return nil, 0, jerrors.Trace(err)
	}

	return pkg, length, nil
}

func (p *RpcServerPackageHandler) Write(ss getty.Session, pkg interface{}) ([]byte, error) {
	resp, ok := pkg.(*mq.Packet)
	if !ok {
		log.Error("illegal pkg:%+v\n", pkg)
		return nil, jerrors.New("invalid rpc response")
	}

	buf, err := resp.Marshal()
	if err != nil {
		log.Warn("binary.Write(resp{%#v}) = err{%#v}", resp, err)
		return nil, jerrors.Trace(err)
	}

	return buf.Bytes(), nil
}

////////////////////////////////////////////
// RpcClientPackageHandler
////////////////////////////////////////////

var (
	rpcClientPackageHandler = &RpcClientPackageHandler{}
)

type RpcClientPackageHandler struct{}

func (p *RpcClientPackageHandler) Read(ss getty.Session, data []byte) (interface{}, int, error) {
	buf := gxbytes.GetBytesBuffer()
	buf.Write(data)
	defer gxbytes.PutBytesBuffer(buf)

	pkg := &mq.Packet{}
	length, err := pkg.Unmarshal(buf)
	if err != nil {
		if err == mq.ErrNotEnoughStream {
			return nil, 0, nil
		}
		return nil, 0, jerrors.Trace(err)
	}

	return pkg, length, nil
}

func (p *RpcClientPackageHandler) Write(ss getty.Session, pkg interface{}) ([]byte, error) {
	req, ok := pkg.(*mq.Packet)
	if !ok {
		log.Error("illegal pkg:%+v\n", pkg)
		return nil, jerrors.New("invalid rpc request")
	}

	buf, err := req.Marshal()
	if err != nil {
		log.Warn("binary.Write(req{%#v}) = err{%#v}", req, jerrors.ErrorStack(err))
		return nil, jerrors.Trace(err)
	}

	return buf.Bytes(), nil
}
