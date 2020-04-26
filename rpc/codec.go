package rpc

import (
	"time"
)

////////////////////////////////////////////
//  getty command
////////////////////////////////////////////

type gettyCommand int16

const (
	gettyDefaultCmd     gettyCommand = 0x00
	gettyCmdHbRequest                = 0x01
	gettyCmdHbResponse               = 0x02
	gettyCmdRPCRequest               = 0x03
	gettyCmdRPCResponse              = 0x04
)

type (
	SequenceType uint64
)

////////////////////////////////////////////
//  getty error code
////////////////////////////////////////////

type ErrorCode int16

const (
	GettyOK   ErrorCode = 0x00
	GettyFail           = 0x01
)

////////////////////////////////////////////
//  getty codec type
////////////////////////////////////////////

type CodecType int16

const (
	CodecUnknown CodecType = 0x00
	CodecMQ      CodecType = 0x01
)

var (
	gettyCodecStrings = [...]string{
		"unknown",
		"mq",
	}
)

func (c CodecType) String() string {
	if c == CodecMQ {
		return gettyCodecStrings[c]
	}

	return gettyCodecStrings[CodecUnknown]
}

func (c CodecType) CheckValidity() bool {
	if c == CodecMQ {
		return true
	}

	return false
}

func GetCodecType(codecType string) CodecType {
	switch codecType {
	case gettyCodecStrings[CodecMQ]:
		return CodecMQ
	}

	return CodecUnknown
}

////////////////////////////////////////////
// GettyPackageHandler
////////////////////////////////////////////

////////////////////////////////////////////
// PendingResponse
////////////////////////////////////////////

type PendingResponse struct {
	seq       SequenceType
	start     time.Time
	readStart time.Time
	callback  AsyncCallback
	reply     interface{}
	opts      CallOptions
	done      chan struct{}
}

func NewPendingResponse() *PendingResponse {
	return &PendingResponse{
		start: time.Now(),
		done:  make(chan struct{}),
	}
}

func (r PendingResponse) GetCallResponse() CallResponse {
	return CallResponse{
		Opts:      r.opts,
		Start:     r.start,
		ReadStart: r.readStart,
		Reply:     r.reply,
	}
}
