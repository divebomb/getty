package rpc

import (
	"encoding/json"
	"testing"
	"time"
)

import (
	log "github.com/AlexStocks/log4go"
	gxbytes "github.com/dubbogo/gost/bytes"
	jerrors "github.com/juju/errors"
	"github.com/stretchr/testify/assert"
	"gitlab.alipay-inc.com/alipay-com/getty"
	"gitlab.alipay-inc.com/alipay-com/getty/rpc/mq"
)

const (
	ServerHost = "127.0.0.1"
	ServerPort = "65432"
)

func buildServerConfig() *ServerConfig {
	return &ServerConfig{
		AppName:         "RPC-SERVER",
		Host:            ServerHost,
		Ports:           []string{ServerPort},
		ProfilePort:     10086,
		SessionTimeout:  "180s",
		sessionTimeout:  time.Second * 180,
		SessionNumber:   1,
		FailFastTimeout: "3s",
		failFastTimeout: time.Second * 3,
		GettySessionParam: GettySessionParam{
			CompressEncoding: false,
			TcpNoDelay:       true,
			TcpKeepAlive:     true,
			KeepAlivePeriod:  "120s",
			keepAlivePeriod:  time.Second * 120,
			TcpRBufSize:      262144,
			TcpWBufSize:      524288,
			PkgRQSize:        1024,
			PkgWQSize:        512,
			TcpReadTimeout:   "1s",
			tcpReadTimeout:   time.Second * 1,
			TcpWriteTimeout:  "3s",
			tcpWriteTimeout:  time.Second * 3,
			WaitTimeout:      "1s",
			waitTimeout:      time.Second * 1,
			MaxMsgLen:        102400,
			SessionName:      "getty-rpc-server",
		},
	}
}

func HandleTopicMetaRequest(ss getty.Session, packet *mq.Packet) error {
	log.Info("get client request:%s", packet)

	meta := &mq.TopicMetadata{
		Topic: mq.Topic{
			Id:         0,
			Name:       "TP_DS_TEST",
			FixedQueue: false,
			Cluster:    "",
			Perm:       6,
		},
		Version: 0,
		MessageQueues: []mq.MessageQueue{
			mq.MessageQueue{
				Id:               0,
				Topic:            "TP_DS_TEST",
				Address:          "100.88.61.115:9512",
				Broker:           "antq-eu95-0.cz00b.stable.alipay.net",
				Permission:       6,
				GlobalFixedQueue: false,
			},
			mq.MessageQueue{
				Id:               0,
				Topic:            "TP_DS_TEST",
				Address:          "100.88.128.198:9512",
				Broker:           "antq-eu95-1.cz00b.stable.alipay.net",
				Permission:       6,
				GlobalFixedQueue: false,
			},
			mq.MessageQueue{
				Id:               0,
				Topic:            "TP_DS_TEST",
				Address:          "11.166.70.10:9512",
				Broker:           "antq-zth-1.cz00b.stable.alipay.net",
				Permission:       6,
				GlobalFixedQueue: false,
			},
		},
		WritableQueues: []mq.MessageQueue{
			mq.MessageQueue{
				Id:               0,
				Topic:            "TP_DS_TEST",
				Address:          "100.88.61.115:9512",
				Broker:           "antq-eu95-0.cz00b.stable.alipay.net",
				Permission:       6,
				GlobalFixedQueue: false,
			},
			mq.MessageQueue{
				Id:               0,
				Topic:            "TP_DS_TEST",
				Address:          "100.88.128.198:9512",
				Broker:           "antq-eu95-1.cz00b.stable.alipay.net",
				Permission:       6,
				GlobalFixedQueue: false,
			},
			mq.MessageQueue{
				Id:               0,
				Topic:            "TP_DS_TEST",
				Address:          "11.166.70.10:9512",
				Broker:           "antq-zth-1.cz00b.stable.alipay.net",
				Permission:       6,
				GlobalFixedQueue: false,
			},
		},
		ReadableQueues: []mq.MessageQueue{
			mq.MessageQueue{
				Id:               0,
				Topic:            "TP_DS_TEST",
				Address:          "100.88.61.115:9512",
				Broker:           "antq-eu95-0.cz00b.stable.alipay.net",
				Permission:       6,
				GlobalFixedQueue: false,
			},
			mq.MessageQueue{
				Id:               0,
				Topic:            "TP_DS_TEST",
				Address:          "100.88.128.198:9512",
				Broker:           "antq-eu95-1.cz00b.stable.alipay.net",
				Permission:       6,
				GlobalFixedQueue: false,
			},
			mq.MessageQueue{
				Id:               0,
				Topic:            "TP_DS_TEST",
				Address:          "11.166.70.10:9512",
				Broker:           "antq-zth-1.cz00b.stable.alipay.net",
				Permission:       6,
				GlobalFixedQueue: false,
			},
		},
		WriteIndex: 0,
		ReadIndex:  0,
	}

	rs := mq.NewResponse(mq.SUCCESS, packet.PacketId)
	var metaRs mq.TopicMetadataResponse
	metaRs.Packet = *rs
	metaRs.SetMetadata(meta)

	rsBytes, err := metaRs.Marshal()
	if err != nil {
		return jerrors.Annotatef(err, "metaRs.Marshal(rs:%#v) = err:%s", metaRs, err)
	}
	defer gxbytes.PutBytesBuffer(rsBytes)

	return jerrors.Trace(ss.WriteBytes(rsBytes.Bytes()))
}

func buildServer(t *testing.T) *Server {
	var (
		err    error
		conf   *ServerConfig
		server *Server
	)

	conf = buildServerConfig()
	err = conf.CheckValidity()
	assert.Nil(t, err)

	server, err = NewServer(conf, WithPackageHandler(HandleTopicMetaRequest))
	assert.Nil(t, err)
	return server
}

func buildClientConfig() *ClientConfig {
	return &ClientConfig{
		AppName:         "mq",
		Host:            "127.0.0.1",
		ProfilePort:     38001,
		ConnectionNum:   1,
		HeartbeatPeriod: "30s",
		SessionTimeout:  "300s",
		FailFastTimeout: "3s",
		PoolSize:        64,
		PoolTTL:         600,
		GettySessionParam: GettySessionParam{
			CompressEncoding: false,
			TcpNoDelay:       true,
			TcpKeepAlive:     true,
			KeepAlivePeriod:  "180s",
			TcpRBufSize:      262144,
			TcpWBufSize:      65536,
			PkgRQSize:        0,
			PkgWQSize:        512,
			TcpReadTimeout:   "1s",
			TcpWriteTimeout:  "3s",
			WaitTimeout:      "1s",
			MaxMsgLen:        8388608,
			SessionName:      "mq-rcp-client",
		},
	}
}

func HandleBrokerRequest(ss getty.Session, pkg *mq.Packet) error {
	switch pkg.CommandID() {
	case mq.CONSUMER_LIST_CHANGE:
		var Req mq.ConsumerListChangeRequest
		Req.Packet = *pkg
		rq := &Req
		if !rq.Validate() {
			log.Error("broker url %s, @command %+v is not ConsumerListChangeRequest", ss.RemoteAddr(), rq)
			return jerrors.Errorf("illegal @pkg %+v", pkg)
		}

		_, err := rq.GetHeader()
		if err != nil {
			log.Error("commands.ConsumerListChangeRequest.GetHeader() = error:%v", err)
			return err
		}

		rs := mq.NewResponse(mq.SUCCESS, rq.PacketId)
		rs.SetCommandID(pkg.CommandID())
		rsBytes, err := rs.Marshal()
		if err != nil {
			return jerrors.Annotatef(err, "metaRs.Marshal(rs:%#v) = err:%s", rs, err)
		}
		defer gxbytes.PutBytesBuffer(rsBytes)

		return jerrors.Trace(ss.WriteBytes(rsBytes.Bytes()))

	default:
		log.Error("@ss:%s, illegal brokerClient(url:%s) message %+v", ss.Stat(), ss.RemoteAddr(), pkg)
	}

	return nil
}

func TestClient(t *testing.T) {
	// build server
	server := buildServer(t)
	go server.Start()
	time.Sleep(1e9)
	defer server.Stop()

	// build client
	clientConfig := buildClientConfig()
	assert.Nil(t, clientConfig.CheckValidity())
	client, err := NewClient(clientConfig,
		WithServerPackageHandler(HandleBrokerRequest))
	assert.Nil(t, err)
	defer client.Close()

	// send request from client to server
	rqHeader := mq.GetTopicMetadataHeader{
		Topic:    "TP_DS_TEST",
		ClientId: "62821@C02XW5SLJHD2.local@S_dongshi_test@0",
	}
	headerData, err := json.Marshal(rqHeader)
	assert.Nil(t, err)
	metaRq := mq.NewRequest(mq.GET_TOPIC_METADATA, headerData)

	rs := mq.TopicMetadataResponse{}
	addr := ServerHost + ":" + ServerPort
	err = client.Call(CodecMQ,
		addr,
		metaRq,
		&(rs.Packet),
		WithCallRequestTimeout(1e9),
		WithCallResponseTimeout(3e9),
	)
	assert.Nil(t, err)

	header, err := rs.GetHeader()
	assert.NotNil(t, err)
	assert.Nil(t, header)
	meta, err := rs.GetMetadata()
	assert.Nil(t, err)
	assert.NotNil(t, meta)

	// send request from server to client
	cidListChangeRqHeader := mq.ConsumerListChangeHeader{
		Consumer:    "TP_DS_TEST",
		ClientId: "62821@C02XW5SLJHD2.local@S_dongshi_test@0",
	}
	headerData, err = json.Marshal(cidListChangeRqHeader)
	assert.Nil(t, err)
	cidListChangeRq := mq.NewRequest(mq.CONSUMER_LIST_CHANGE, headerData)

	ssArray := server.SessionSet()
	for _, ss := range ssArray {
		err := ss.WritePkg(cidListChangeRq, 3e9)
		if err != nil {
			t.Errorf("session.WritePkg(rq:%+v) = error:%+v", cidListChangeRq, err)
			continue
		}
		select {
		case <- time.After(3e9):
			case
		}
	}
}
