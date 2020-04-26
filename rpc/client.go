package rpc

import (
	"math/rand"
	"sync"
	"time"
)

import (
	jerrors "github.com/juju/errors"
)

import (
	"gitlab.alipay-inc.com/alipay-com/getty"
	"gitlab.alipay-inc.com/alipay-com/getty/rpc/mq"
)

var (
	errInvalidCodecType  = jerrors.New("illegal CodecType")
	errInvalidAddress    = jerrors.New("remote address invalid or empty")
	errSessionNotExist   = jerrors.New("session not exist")
	errClientClosed      = jerrors.New("client closed")
	errClientReadTimeout = jerrors.New("client read timeout")
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type CallOptions struct {
	// request timeout
	RequestTimeout time.Duration
	// response timeout
	ResponseTimeout time.Duration
	Meta            map[interface{}]interface{}
}

type CallOption func(*CallOptions)

func WithCallRequestTimeout(d time.Duration) CallOption {
	return func(o *CallOptions) {
		o.RequestTimeout = d
	}
}

func WithCallResponseTimeout(d time.Duration) CallOption {
	return func(o *CallOptions) {
		o.ResponseTimeout = d
	}
}

func WithCallMeta(k, v interface{}) CallOption {
	return func(o *CallOptions) {
		if o.Meta == nil {
			o.Meta = make(map[interface{}]interface{})
		}
		o.Meta[k] = v
	}
}

type CallResponse struct {
	Opts      CallOptions
	Start     time.Time // invoke(call) start time == write start time
	ReadStart time.Time // read start time, write duration = ReadStart - Start
	Reply     interface{}
}

type AsyncCallback func(response CallResponse)

type ClientOptions struct {
	// handle server mq packet request
	handleServerRequest MQPacketHandler
}

type ClientOption func(options *ClientOptions)

func WithServerPackageHandler(h MQPacketHandler) ClientOption {
	return func(o *ClientOptions) {
		o.handleServerRequest = h
	}
}

type Client struct {
	*sender

	conf ClientConfig
	pool *gettyRPCClientPool
	//sequence uint64

	pendingLock      sync.Mutex
	pendingResponses map[SequenceType]*PendingResponse
}

func NewClient(conf *ClientConfig, opts ...ClientOption) (*Client, error) {
	if err := conf.CheckValidity(); err != nil {
		return nil, jerrors.Trace(err)
	}

	var copts ClientOptions
	for _, o := range opts {
		o(&copts)
	}

	sender := newSender()

	c := &Client{
		sender: sender,
		conf: *conf,
	}
	c.pool = newGettyRPCClientConnPool(c, conf.PoolSize, time.Duration(int(time.Second)*conf.PoolTTL), copts.handleServerRequest)

	return c, nil
}

func (c *Client) selectSession(addr string) (*gettyRPCClient, getty.Session, error) {
	rpcConn, err := c.pool.getConn(addr)
	if err != nil {
		return nil, nil, jerrors.Trace(err)
	}
	return rpcConn, rpcConn.selectSession(), nil
}

func (c *Client) call(ct CallType, addr string, pkg *mq.Packet,
	reply interface{}, callback AsyncCallback, opts CallOptions) error {

	if opts.RequestTimeout == 0 {
		opts.RequestTimeout = c.conf.GettySessionParam.tcpWriteTimeout
	}
	if opts.ResponseTimeout == 0 {
		opts.ResponseTimeout = c.conf.GettySessionParam.tcpReadTimeout
	}

	var (
		err     error
		session getty.Session
		conn    *gettyRPCClient
	)
	conn, session, err = c.selectSession(addr)
	if err != nil || session == nil {
		return errSessionNotExist
	}
	defer c.pool.release(conn, err)

	return jerrors.Trace(c.sender.call(session, ct, pkg, reply, callback, opts))
}

// call one way
func (c *Client) CallOneway(typ CodecType, addr string, pkg *mq.Packet, opts ...CallOption) error {
	var copts CallOptions

	for _, o := range opts {
		o(&copts)
	}

	return jerrors.Trace(c.call(CT_OneWay, addr, pkg, nil, nil, copts))
}

// synchronously invoke
// if @reply is nil, the transport layer will get the response without notify the invoker.
func (c *Client) Call(addr string, pkg *mq.Packet, reply interface{}, opts ...CallOption) error {
	var copts CallOptions

	for _, o := range opts {
		o(&copts)
	}

	ct := CT_TwoWay
	if reply == nil {
		ct = CT_TwoWayNoReply
	}

	return jerrors.Trace(c.call(ct, addr, pkg, reply, nil, copts))
}

// send request asynchronously
func (c *Client) AsyncCall(addr string, pkg *mq.Packet,
	callback AsyncCallback, reply interface{}, opts ...CallOption) error {

	var copts CallOptions
	for _, o := range opts {
		o(&copts)
	}

	return jerrors.Trace(c.call(CT_TwoWay, addr, pkg, reply, callback, copts))
}

func (c *Client) Close() {
	if c.pool != nil {
		c.pool.close()
	}
	c.pool = nil
}

func (c *Client) heartbeat(session getty.Session) error {
	//return c.transfer(session, typ, nil, NewPendingResponse(), CallOptions{})
	return nil
}