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
	getTopicMeta()

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
	rq := mq.TopicMetadataRequest{Packet: mq.Packet{PacketLength: 132, Magic: -626843481, Version: 0x1, CRC: 0, PacketId: 0, Code: 1003, HeaderLength: 107, HeaderData: []uint8{0x7b, 0x22, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x22, 0x3a, 0x22, 0x36, 0x32, 0x38, 0x32, 0x31, 0x40, 0x43, 0x30, 0x32, 0x58, 0x57, 0x35, 0x53, 0x4c, 0x4a, 0x48, 0x44, 0x32, 0x2e, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x40, 0x53, 0x5f, 0x64, 0x6f, 0x6e, 0x67, 0x73, 0x68, 0x69, 0x5f, 0x74, 0x65, 0x73, 0x74, 0x40, 0x30, 0x22, 0x2c, 0x22, 0x6d, 0x6f, 0x64, 0x65, 0x22, 0x3a, 0x22, 0x44, 0x45, 0x46, 0x41, 0x55, 0x4c, 0x54, 0x22, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x22, 0x54, 0x50, 0x5f, 0x44, 0x53, 0x5f, 0x54, 0x45, 0x53, 0x54, 0x22, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x49, 0x64, 0x22, 0x3a, 0x2d, 0x31, 0x7d}, Body: []uint8(nil), Flag: 0}}
	rs := mq.TopicMetadataResponse{}
	nameserverAddr := "11.166.49.180:9511"
	err := client.Call(rpc.CodecMQ,
		nameserverAddr,
		&rq.Packet,
		&(rs.Packet),
		rpc.WithCallRequestTimeout(1e9),
		rpc.WithCallResponseTimeout(3e9),
	)
	if err != nil {
		log.Error("client.Call(rq:%+v) = error:%+v", rq, err)
		return
	}
	header, _ := rs.GetHeader()
	meta, _ := rs.GetMetadata()
	log.Info("client.Call(rq:%+v) = rsp:{header:%+v, meta:%+v}", rq, header, meta)
}
