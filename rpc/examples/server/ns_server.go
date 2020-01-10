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
	jerrors "github.com/juju/errors"
)

import (
	"github.com/AlexStocks/getty/rpc"
	"github.com/AlexStocks/getty/rpc/mq"
)

var (
	server *rpc.Server
)

func main() {
	initServer()

	initSignal()
}

func MQPacketHandler(packet *mq.Packet) error {
	log.Info("get client request:%s", packet)
	return nil
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
		AppName:           "mq-nameserver",
		Host:              "0.0.0.0",
		Ports:             []string{"9509", "9511"},
		ProfilePort:       38002,
		SessionTimeout:    "100s",
		SessionNumber:     1,
		FailFastTimeout:   "3s",
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