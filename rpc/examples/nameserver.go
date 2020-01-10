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
    jerrors "github.com/juju/errors"
    "os"
    "os/signal"
    "syscall"
    "time"
)

import (
    log "github.com/AlexStocks/log4go"
)

import (
    "github.com/AlexStocks/getty/rpc"
)

var (
    client *rpc.Client
)

// they are necessary:
// 		export CONF_CONSUMER_FILE_PATH="xxx"
// 		export APP_LOG_CONF_FILE="xxx"
func main() {
    initClient()

    initSignal()
}

func initClient() {
    cltConf := rpc.ClientConfig{
        AppName:           "mq",
        Host:              "127.0.0.1",
        ProfilePort:       38001,
        ConnectionNum:     1,
        HeartbeatPeriod:   "30s",
        SessionTimeout:    "300s",
        FailFastTimeout:   "3s",
        PoolSize:          64,
        PoolTTL:           600,
        GettySessionParam : rpc.GettySessionParam{
            CompressEncoding: false,
            TcpNoDelay:       true,
            TcpKeepAlive:     true,
            KeepAlivePeriod:  "30",
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
            time.AfterFunc(time.Duration(survivalTimeout), func() {
                log.Warn("app exit now by force...")
                os.Exit(1)
            })

            // The program exits normally or timeout forcibly exits.
            fmt.Println("app exit now...")
            return
        }
    }
}

