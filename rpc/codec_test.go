package rpc

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestCodecType(t *testing.T) {
	assert.Equal(t, gettyCodecStrings[CodecUnknown], CodecUnknown.String())
	assert.Equal(t, gettyCodecStrings[CodecMQ], CodecMQ.String())

	assert.True(t, CodecMQ.CheckValidity())
	assert.False(t, CodecUnknown.CheckValidity())

	assert.Equal(t, gettyCodecStrings[CodecUnknown], GetCodecType(gettyCodecStrings[CodecUnknown]).String())
	assert.Equal(t, gettyCodecStrings[CodecMQ], GetCodecType(gettyCodecStrings[CodecMQ]).String())
}

func TestPendingResponse_GetCallResponse(t *testing.T) {
	rsp := NewPendingResponse()
	assert.True(t, rsp.seq == 0)
}
