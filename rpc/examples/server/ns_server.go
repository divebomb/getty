/******************************************************
# DESC    :
# AUTHOR  : Alex Stocks
# VERSION : 1.0
# LICENCE : Apache License 2.0
# EMAIL   : alexstocks@foxmail.com
# MOD     : 2020-01-10 11:35
# FILE    : ns.go
******************************************************/

package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"
)

import (
	log "github.com/AlexStocks/log4go"
	gxbytes "github.com/dubbogo/gost/bytes"
	jerrors "github.com/juju/errors"
)

import (
	"github.com/divebomb/getty"
	"github.com/divebomb/getty/rpc"
	"github.com/divebomb/getty/rpc/mq"
)

var (
	server *rpc.Server
)

func main() {
	initServer()

	initSignal()
}

func MQPacketHandler(ss getty.Session, packet *mq.Packet) error {
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

func initSignal() {
	signals := make(chan os.Signal, 1)
	// It is not possible to block SIGKILL or syscall.SIGSTOP
	signal.Notify(signals, os.Interrupt, os.Kill, syscall.SIGHUP,
		syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		sig := <-signals
		log.Info("get signal %s", sig.String())
		switch sig {
		case syscall.SIGHUP:
			// reload()
		default:
			time.AfterFunc(time.Duration(3e9), func() {
				log.Warn("app exit now by force...")
				os.Exit(1)
			})

			// The program exits normally or timeout forcibly exits.
			uninitServer()
			// fmt.Println("app exit now...")
			log.Exit("app exit now...")
			return
		}
	}
}

func initServer() {
	svrConf := rpc.ServerConfig{
		AppName:         "mq-nameserver",
		Host:            "0.0.0.0",
		Ports:           []string{"9509", "9511"},
		ProfilePort:     38002,
		SessionTimeout:  "300s",
		SessionNumber:   1,
		FailFastTimeout: "3s",
		GettySessionParam: rpc.GettySessionParam{
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
			SessionName:      "mq-nameserver-server",
		},
	}

	var err error
	server, err = rpc.NewServer(&svrConf, rpc.WithPackageHandler(MQPacketHandler))
	if err != nil {
		panic(jerrors.ErrorStack(err))
	}
	server.Start()
}

func uninitServer() {
	server.Stop()
}
