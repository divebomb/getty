package getty

import (
	"net"
	"net/http"
	"testing"
	"time"
)

import (
	"github.com/dubbogo/gost/net"
	"github.com/stretchr/testify/assert"
)

func TestNewSessionMap(t *testing.T) {
	sm := NewSessionMap()
	assert.NotNil(t, sm)

	svrFunc := func()(flag bool, addr *net.TCPAddr) {
		listener, err := gxnet.ListenOnTCPRandomPort("")
		if err != nil {
			return false, nil
		}

		flag = true
		addr = listener.Addr().(*net.TCPAddr)
		go http.Serve(listener, nil)
		return
	}

	// server 0
	flag, addr := svrFunc()
	time.Sleep(1e9)
	if !flag {
		return
	}
	serverAddr0 := addr.String()
	t.Logf("listen server0 addr %s", serverAddr0)

	c0 := newClient(TCP_CLIENT,
		WithServerAddress(serverAddr0),
		WithConnectionNumber(1))
	s0 := c0.dialTCP()
	defer c0.Close()

	// add session 0
	err := sm.AddSession(s0)
	assert.Nil(t, err)
	err = sm.AddSession(s0)
	assert.NotNil(t, err)
	assert.True(t, sm.Size() == 1)
	assert.NotNil(t, sm.GetSessionBySessionID(s0.ID()))
	assert.True(t, len(sm.GetSession(s0.RemoteAddr())) == 1)

	// add session 1
	c1 := newClient(TCP_CLIENT,
		WithServerAddress(serverAddr0),
		WithConnectionNumber(1))
	s1 := c1.dialTCP()
	defer c1.Close()

	err = sm.AddSession(s1)
	assert.Nil(t, err)
	err = sm.AddSession(s1)
	assert.NotNil(t, err)
	assert.True(t, sm.Size() == 2)
	assert.NotNil(t, sm.GetSessionBySessionID(s1.ID()))
	assert.True(t, len(sm.GetSession(s1.RemoteAddr())) == 2)

	// delete session 0
    sm.RemoveSession(s0)
	assert.True(t, sm.Size() == 1)
	assert.Nil(t, sm.GetSessionBySessionID(s0.ID()))
	assert.NotNil(t, sm.GetSessionBySessionID(s1.ID()))
	assert.True(t, len(sm.GetSession(s1.RemoteAddr())) == 1)

	// server1
	flag, addr = svrFunc()
	time.Sleep(1e9)
	serverAddr1 := addr.String()
	t.Logf("listen server1 addr %s", serverAddr1)

	// add session 2
	c2 := newClient(TCP_CLIENT,
		WithServerAddress(serverAddr1),
		WithConnectionNumber(1))
	s2 := c2.dialTCP()
	defer c2.Close()

	err = sm.AddSession(s2)
	assert.Nil(t, err)
	err = sm.AddSession(s2)
	assert.NotNil(t, err)
	assert.True(t, sm.Size() == 2)
	assert.NotNil(t, sm.GetSessionBySessionID(s2.ID()))
	assert.True(t, len(sm.GetSession(s2.RemoteAddr())) == 1)

	// delete session 1
	sm.RemoveSession(s1)
	assert.True(t, sm.Size() == 1)
	assert.Nil(t, sm.GetSessionBySessionID(s0.ID()))
	assert.Nil(t, sm.GetSessionBySessionID(s1.ID()))
	assert.True(t, len(sm.GetSession(s1.RemoteAddr())) == 0)

	// delete session 2
	sm.RemoveSession(s2)
	assert.True(t, sm.Size() == 0)
	assert.Nil(t, sm.GetSessionBySessionID(s0.ID()))
	assert.Nil(t, sm.GetSessionBySessionID(s1.ID()))
	assert.Nil(t, sm.GetSessionBySessionID(s2.ID()))
	assert.True(t, len(sm.GetSession(s2.RemoteAddr())) == 0)
}