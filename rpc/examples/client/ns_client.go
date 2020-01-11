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
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

import (
	log "github.com/AlexStocks/log4go"
	jerrors "github.com/juju/errors"
)

import (
	"github.com/AlexStocks/getty/rpc"
	"github.com/AlexStocks/getty/rpc/mq"
)

var (
	client *rpc.Client
)

func main() {
	initClient()
	go getTopicMeta()

	initSignal()
}

func initClient() {
	cltConf := rpc.ClientConfig{
		AppName:         "mq",
		Host:            "127.0.0.1",
		ProfilePort:     38001,
		ConnectionNum:   1,
		HeartbeatPeriod: "30s",
		SessionTimeout:  "300s",
		FailFastTimeout: "3s",
		PoolSize:        64,
		PoolTTL:         600,
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
			SessionName:      "mq-rcp-client",
		},
	}

	var err error
	client, err = rpc.NewClient(&cltConf)
	if err != nil {
		panic(jerrors.ErrorStack(err))
	}
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
			fmt.Println("app exit now...")
			return
		}
	}
}

func getTopicMeta() {
	rqHeader := mq.GetTopicMetadataHeader{
		Topic:      "TP_DS_TEST",
		ClientId:   "62821@C02XW5SLJHD2.local@S_dongshi_test@0",
	}
	headerData, err := json.Marshal(rqHeader)
	if err != nil {
		log.Error("commands.GetConsumersHeader.MarshalJson(header:%+v) = error:%+v", rqHeader, err)
		return
	}
	metaRq := mq.NewRequest(mq.GET_TOPIC_METADATA, headerData)

	log.Info("request header:%#v", rqHeader)
	rs := mq.TopicMetadataResponse{}
	// nameserverAddr := "11.166.49.180:9511"
	nameserverAddr := "localhost:9511"
	err = client.Call(rpc.CodecMQ,
		nameserverAddr,
		metaRq,
		&(rs.Packet),
		rpc.WithCallRequestTimeout(1e9),
		rpc.WithCallResponseTimeout(3e9),
	)
	if err != nil {
		log.Error("client.Call(rq:%+v) = error:%+v", rqHeader, err)
		return
	}
	//log.Info("client.Call(rq:%+v) = rsp:%+v", rqHeader, rs)

	header, err := rs.GetHeader()
	if err == nil {
		log.Info("rsp:{header:%+v}", header)
	}
	meta, err := rs.GetMetadata()
	if err == nil {
		log.Info("rsp:{meta:%+v}", meta)
	}
}
