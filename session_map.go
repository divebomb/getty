package getty

import (
	"sync"
)

import (
	log "github.com/AlexStocks/log4go"
	jerrors "github.com/juju/errors"
)

////////////////////////////////////////////
// Session Map
////////////////////////////////////////////

var (
	ErrSessionExist = jerrors.Errorf("session exist")
)

type SessionMap struct {
	rwlock         sync.RWMutex
	sessionMap     map[uint32]Session // session ID -> Session array
	addr2IDMap     map[string][]uint32  // address -> session ID
}

func NewSessionMap() *SessionMap {
	return &SessionMap{
		sessionMap: make(map[uint32]Session, 32),
		addr2IDMap: make(map[string][]uint32, 32),
	}
}

func (m *SessionMap) Size() int {
	m.rwlock.RLock()
	defer m.rwlock.RUnlock()

	return len(m.sessionMap)
}

func (m *SessionMap) Exist(session Session) bool {
	id := session.ID()

	m.rwlock.RLock()
	defer m.rwlock.RUnlock()
	_, ok := m.sessionMap[id]

	return ok
}

func (m *SessionMap) AddSession(session Session) error {
	if m.Exist(session) {
		return ErrSessionExist
	}

	addr := session.RemoteAddr()

	m.rwlock.Lock()
	defer m.rwlock.Unlock()

	flag := false
	sid := session.ID()

	arr, ok := m.addr2IDMap[addr]
	if !ok {
		arr = make([]uint32, 0, 4)
	} else {
		for _, id := range arr {
			if id == sid {
				flag = true
			}
		}
	}

	if !flag {
		arr = append(arr, sid)
	}

	m.addr2IDMap[addr] = arr
	m.sessionMap[sid] = session

	return nil
}

func (m *SessionMap) RemoveSession(session Session) {
	sid := session.ID()
	addr := session.RemoteAddr()

	m.rwlock.Lock()
	defer m.rwlock.Unlock()

	delete(m.sessionMap, sid)
	arr, ok := m.addr2IDMap[addr]
	if !ok {
		return
	}

	for idx, id := range arr {
		if id == sid {
			arr = append(arr[:idx], arr[idx+1:]...)
			break
		}
	}

	// this for-loop is impossible
	for idx, id := range arr {
		if id == sid {
			log.Error("the same session %s exist in the same session array %+v", session.Stat(), arr)
			arr = append(arr[:idx], arr[idx+1:]...)
			break
		}
	}

	if len(arr) == 0 {
		delete(m.addr2IDMap, addr)
	} else {
		m.addr2IDMap[addr] = arr
	}
}

func (m *SessionMap) GetSession(addr string) []Session {
	m.rwlock.RLock()
	defer m.rwlock.RUnlock()

	if arr, ok := m.addr2IDMap[addr]; ok {
		ssArray := make([]Session, 0, len(arr))
		for _, id := range arr {
			if ss, ok := m.sessionMap[id]; ok {
				ssArray = append(ssArray, ss)
			}
		}

		return ssArray
	}

	return nil
}

func (m *SessionMap) GetSessionBySessionID(sid uint32) Session {
	m.rwlock.RLock()
	defer m.rwlock.RUnlock()

	if s, ok := m.sessionMap[sid]; ok {
		return s
	}

	return nil
}
