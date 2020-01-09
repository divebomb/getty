package mq

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

import (
	"github.com/dubbogo/gost/bytes"
	jerrors "github.com/juju/errors"
)

const (
	FIXED_FIELD_LENGTH int32  = 4 + 4 + 1 + 4 + 4 + 2 + 4 + 2
	VERSION            byte   = 1
	RESPONSE           uint32 = 0
	ONEWAY             uint32 = 1
)

var (
	ErrNilRspBody      = jerrors.Errorf("nil response body")
	ErrNotEnoughStream = jerrors.New("packet stream is not enough")
	ErrTooLargePackage = jerrors.New("package length is exceed the getty package's legal maximum length.")
	ErrInvalidPackage  = jerrors.New("invalid rpc package")
	ErrIllegalMagic    = jerrors.New("package magic is not right.")
)

var (
	PacketIdGenerator int32
	crc32Table        = crc32.MakeTable(crc32.IEEE)
)

type Packet struct {
	PacketLength int32 // 数据包大小
	Magic        int32 // 识别是否MQ的包
	Version      byte  // 通讯协议的版本号
	CRC          int32 // 校验包
	PacketId     int32 // 数据包id
	// packet的业务类型标识，比如获取消息、获取Topic路由信息等
	Code         int16
	HeaderLength int16 // 包头大小
	// 用户数据，采用JSON序列化；
	// 第一类请求(metadata)，数据存储在header ext中
	HeaderData []byte
	// 用户数据，用户自定义序列化；
	// 第二类请求(message)，数据存储在body中
	Body []byte

	// packet type
	Flag int32
}

func (p *Packet) Clone() *Packet {
	newPacket := *p

	return &newPacket
}

func (p *Packet) CommandType() int32 {
	return p.Magic
}

func (p *Packet) CommandCode() uint32 {
	return uint32(p.Code)
}

func (p *Packet) CommandID() uint32 {
	return uint32(p.PacketId)
}

func (p *Packet) SetCommandID(ID uint32) {
	p.PacketId = int32(ID)
}

func (p *Packet) TotalSize() int {
	return int(FIXED_FIELD_LENGTH) + int(p.HeaderLength) + len(p.Body)
}

func (p *Packet) OutputHeaderHex(newLine bool) {
	fmt.Printf("header len: %d, header data: ", len(p.HeaderData))

	for i := range p.HeaderData {
		fmt.Printf("%#X, ", p.HeaderData[i])
	}
	if newLine {
		fmt.Println("")
	}
}

func (p *Packet) OutputBodyHex(newLine bool) {
	fmt.Printf("body len: %d, body data: ", len(p.Body))

	for i := range p.Body {
		fmt.Printf("%#X, ", p.Body[i])
	}
	if newLine {
		fmt.Println("")
	}
}

func (p *Packet) setHeader(header []byte) {
	p.HeaderData = header
	p.HeaderLength = int16(len(header))
}

func (p *Packet) setBody(body []byte) {
	p.Body = body
	p.PacketLength = FIXED_FIELD_LENGTH + int32(p.HeaderLength) + int32(len(body))
}

func (p *Packet) MarkOneWay() {
	bit := int32(1 << uint32(ONEWAY))
	p.Flag |= bit
}

func (requst *Packet) IsOneWay() bool {
	bit := int32(1 << ONEWAY)
	return (bit & requst.Flag) == bit
}

func (p *Packet) MarkRequest() {}

func (p *Packet) MarkResponse() {
	bit := int32(1 << RESPONSE)
	p.Flag |= bit
}

func (p *Packet) IsResponse() bool {
	bit := int32(1 << RESPONSE)
	return (bit & p.Flag) == bit
}

func (p *Packet) IsRequest() bool {
	return !p.IsResponse()
}

func (p *Packet) String() string {
	return fmt.Sprintf("version:%d, packetId:%d, code:%d, flag:%d, header:%s",
		int(p.Version), p.PacketId, p.Code, p.Flag, string(p.HeaderData))
}

// the user should release the return *bytes.Buffer by gxbytes.PutBytes.Buffer
func (p *Packet) Marshal() (*bytes.Buffer, error) {
	totalSize := int(FIXED_FIELD_LENGTH)
	headerLength := len(p.HeaderData)
	if headerLength > math.MaxInt16 {
		// return nil, fmt.Errorf("packet header exceeded 32767(short) bytes")
		return nil, ErrTooLargePackage
	}

	totalSize += headerLength + len(p.Body)
	p.PacketLength = int32(totalSize)
	p.HeaderLength = int16(headerLength)

	packetBuf := gxbytes.GetBytesBuffer()
	defer gxbytes.PutBytesBuffer(packetBuf)

	///////////////////////
	// skip PacketLength + Magic + Version + CRC
	///////////////////////

	var tempBuffer [4]byte
	// PacketId
	binary.BigEndian.PutUint32(tempBuffer[0:], (uint32)(p.PacketId))
	packetBuf.Write(tempBuffer[0:4])

	// Code
	binary.BigEndian.PutUint16(tempBuffer[0:], (uint16)(p.Code))
	packetBuf.Write(tempBuffer[0:2])

	// Flag
	binary.BigEndian.PutUint32(tempBuffer[0:], (uint32)(p.Flag))
	packetBuf.Write(tempBuffer[0:4])

	// HeaderLength
	binary.BigEndian.PutUint16(tempBuffer[0:], (uint16)(p.HeaderLength))
	packetBuf.Write(tempBuffer[0:2])

	// HeaderData
	packetBuf.Write(p.HeaderData[:])

	// Body
	packetBuf.Write(p.Body[:])

	///////////////////////
	// PacketLength + Magic + Version + CRC
	///////////////////////

	buf := gxbytes.GetBytesBuffer()

	// PacketLength
	binary.BigEndian.PutUint32(tempBuffer[0:], (uint32)(p.PacketLength))
	buf.Write(tempBuffer[0:4])

	// Magic
	binary.BigEndian.PutUint32(tempBuffer[0:], uint32(p.Magic))
	buf.Write(tempBuffer[0:4])

	// Version
	tempBuffer[0] = p.Version
	buf.Write(tempBuffer[0:1])

	// CRC
	p.CRC = int32(crc32.Checksum(packetBuf.Bytes(), crc32Table))
	binary.BigEndian.PutUint32(tempBuffer[0:], (uint32)(p.CRC))
	buf.Write(tempBuffer[0:4])

	// PacketId + Code + Flag + HeaderLength + HeaderData + Body
	buf.Write(packetBuf.Bytes())

	return buf, nil
}

func (p *Packet) Unmarshal(buf *bytes.Buffer) (int, error) {
	stream := buf.Bytes()
	totalSize := buf.Len()
	if totalSize < 4 {
		return 0, ErrNotEnoughStream
	}

	// PacketLength
	p.PacketLength = int32(binary.BigEndian.Uint32(stream[0:4]))
	if p.PacketLength > int32(totalSize) {
		// return 0, fmt.Errorf("@buf length %d != Packet.PacketLength %d", totalSize, p.PacketLength)
		return 0, ErrNotEnoughStream
	}

	// Magic
	p.Magic = int32(binary.BigEndian.Uint32(stream[4:8]))
	if p.Magic != MAGIC_CODE {
		return 0, fmt.Errorf("unmatched magic code:%d", p.Magic)
	}

	// Version
	p.Version = stream[8]
	if p.Version != VERSION {
		return 0, fmt.Errorf("unknown version. expected: %d actual: %d", VERSION, p.Version)
	}

	// CRC
	//buf.Read(stream[9:13])
	// length, err = buf.Read(tempBuffer[:])
	//if err != nil {
	//	return fmt.Errorf("buf.Read(CRC) = len:%d, err:%+v", length, err)
	//}
	//p.CRC = int32(binary.BigEndian.Uint32(tempBuffer[:]))
	//crc := int32(crc32.Checksum(buf.Bytes(), crc32Table))
	//if p.CRC != crc {
	//	return fmt.Errorf("unmatched crc. read: %d calculated: %d", p.CRC, crc)
	//}

	// PacketId
	p.PacketId = int32(binary.BigEndian.Uint32(stream[13:17]))

	// Code
	p.Code = int16(binary.BigEndian.Uint16(stream[17:19]))

	// Flag
	p.Flag = int32(binary.BigEndian.Uint32(stream[19:23]))

	// HeaderLength
	p.HeaderLength = int16(binary.BigEndian.Uint16(stream[23:25]))

	// HeaderData
	offset := FIXED_FIELD_LENGTH + int32(p.HeaderLength)
	if p.HeaderLength > 0 {
		p.HeaderData = make([]byte, 0, p.HeaderLength)
		p.HeaderData = append(p.HeaderData, stream[FIXED_FIELD_LENGTH:offset]...)
	}

	// Body
	bodyLen := p.PacketLength - offset
	if bodyLen > 0 {
		p.Body = make([]byte, 0, bodyLen)
		p.Body = append(p.Body, stream[offset:p.PacketLength]...)
	}

	return int(p.PacketLength), nil
}

////////////////////////////////////////////////
// Request/Response
////////////////////////////////////////////////

type Request = Packet

func NewRequest(code RequestCode, header []byte) *Packet {
	p := &Request{
		Magic:      MAGIC_CODE,
		Version:    VERSION,
		PacketId:   atomic.AddInt32(&PacketIdGenerator, 1),
		Code:       int16(code),
		HeaderData: header,
	}
	p.MarkRequest()

	return p
}

type Response = Packet

func NewResponse(code ResponseCode, requestPacketId int32) *Response {
	p := &Response{
		Magic:    MAGIC_CODE,
		Version:  VERSION,
		PacketId: requestPacketId,
		Code:     int16(code),
	}
	p.MarkResponse()

	return p
}

////////////////////////////////////////////////
// get topic metadata from nameserver
////////////////////////////////////////////////

type TopicMetadataRequest struct {
	Packet
	header *GetTopicMetadataHeader
}

func (rq TopicMetadataRequest) Validate() bool {
	if rq.Code == int16(GET_TOPIC_METADATA) {
		return true
	}

	return false
}

func (rq *TopicMetadataRequest) SetHeader(header *GetTopicMetadataHeader) {
	rq.header = header
}

func (rq *TopicMetadataRequest) GetHeader() (*GetTopicMetadataHeader, error) {
	if rq.header == nil {
		var header GetTopicMetadataHeader
		//err := header.json.Unmarshal(rq.HeaderData)
		err := json.Unmarshal(rq.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rq.header = &header
	}

	return rq.header, nil
}

type MessageQueueList []MessageQueue

func (l MessageQueueList) Len() int {
	return len(l)
}

func (l MessageQueueList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func (l MessageQueueList) Less(i, j int) bool {
	if l[i].Topic != l[j].Topic {
		return l[i].Topic < l[j].Topic
	}

	if l[i].Broker != l[j].Broker {
		return l[i].Broker < l[j].Broker
	}

	return l[i].Id < l[j].Id
}

type TopicMetadataResponse struct {
	Packet
	header *GetTopicMetadataHeader
	meta   *TopicMetadata
}

func (m *TopicMetadata) FeedQueue() {
	if 1 < len(m.MessageQueues) {
		sort.Sort(MessageQueueList(m.MessageQueues))
	}
	for i := range m.MessageQueues {
		if PermName(m.MessageQueues[i].Permission) == PermName_PERM_READ_AND_WRITE {
			m.WritableQueues = append(m.WritableQueues, m.MessageQueues[i])
			m.ReadableQueues = append(m.ReadableQueues, m.MessageQueues[i])
			continue
		}
		if (PermName(m.MessageQueues[i].Permission) & PermName_PERM_WRITE) == PermName_PERM_WRITE {
			m.WritableQueues = append(m.WritableQueues, m.MessageQueues[i])
			continue
		}
		if (PermName(m.MessageQueues[i].Permission) & PermName_PERM_READ) == PermName_PERM_READ {
			m.ReadableQueues = append(m.ReadableQueues, m.MessageQueues[i])
			continue
		}
	}
}

func Hashkey(key string) uint32 {
	return crc32.Checksum([]byte(key), crc32Table)
}

const (
	MQKeySeparator = '#'
)

func (m *TopicMetadata) GetMQKey(nsKey string, mq *MessageQueue) string {
	key := make([]byte, 0, len(nsKey)+len(m.Topic.Name)+10+len(mq.Address)+32)

	// nskey + '#'
	key = append(key, nsKey[:]...)
	key = append(key, MQKeySeparator)

	// topic + '#'
	topic := m.Topic.Name
	if len(topic) == 0 {
		topic = mq.Topic
	}
	key = append(key, topic[:]...)
	key = append(key, MQKeySeparator)

	// qid
	key = strconv.AppendInt(key, int64(mq.Id), 10)

	// broker address
	if !m.Topic.FixedQueue {
		key = append(key, MQKeySeparator)
		key = append(key, mq.Address[:]...)
	}

	return string(key)
}

func (rs TopicMetadataResponse) Validate() bool {
	if rs.Code == int16(SUCCESS) {
		return true
	}

	return false
}

func (rs *TopicMetadataResponse) GetHeader() (*GetTopicMetadataHeader, error) {
	if rs.header == nil {
		var header GetTopicMetadataHeader
		// err := header.json.Unmarshal(rs.HeaderData)
		err := json.Unmarshal(rs.HeaderData, &header)
		if err != nil {
			return nil, err
		}
		rs.header = &header
	}

	return rs.header, nil
}

func (rs *TopicMetadataResponse) GetMetadata() (*TopicMetadata, error) {
	if rs.meta == nil {
		var topicMetadata TopicMetadata
		// err := topicMetadata.json.Unmarshal(rs.Body)
		err := json.Unmarshal(rs.Body, &topicMetadata)
		if err != nil {
			return nil, err
		}
		topicMetadata.FeedQueue()

		rs.meta = &topicMetadata
	}

	return rs.meta, nil
}

////////////////////////////////////////////////
// get assigned queues
////////////////////////////////////////////////

type GetAssignedQueuesRequest struct {
	Packet
	header *GetAssignedQueuesHeader
}

func (rq GetAssignedQueuesRequest) Validate() bool {
	if rq.Code == int16(GET_ASSIGNED_QUEUES) {
		return true
	}

	return false
}

func (rq *GetAssignedQueuesRequest) GetHeader() (*GetAssignedQueuesHeader, error) {
	if rq.header == nil {
		var header GetAssignedQueuesHeader
		err := json.Unmarshal(rq.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rq.header = &header
	}

	return rq.header, nil
}

type GetAssignedQueuesResponse struct {
	Packet
	header *GetAssignedQueuesHeader
	list   *MessageQueueList
}

func (rs GetAssignedQueuesResponse) Validate() bool {
	if rs.Code == int16(SUCCESS) {
		return true
	}

	return false
}

func (rs *GetAssignedQueuesResponse) GetHeader() (*GetAssignedQueuesHeader, error) {
	if rs.header == nil {
		var header GetAssignedQueuesHeader
		err := json.Unmarshal(rs.HeaderData, &header)
		if err != nil {
			return nil, err
		}
		rs.header = &header
	}

	return rs.header, nil
}

func (rs *GetAssignedQueuesResponse) GetMetadata() (MessageQueueList, error) {
	if rs.list == nil {
		var metadata MessageQueueList
		err := json.Unmarshal(rs.Body, &metadata)
		if err != nil {
			return nil, err
		}

		rs.list = &metadata
	}

	return *rs.list, nil
}

func (rs *GetAssignedQueuesResponse) SetMetadata(list MessageQueueList) error {
	var err error

	pkgLen := rs.PacketLength
	pkgLen -= int32(len(rs.Body))
	rs.Body, err = json.Marshal(list)
	if err != nil {
		return err
	}
	pkgLen += int32(len(rs.Body))
	rs.PacketLength = pkgLen

	return nil
}

////////////////////////////////////////////////
// get consumers from broker
////////////////////////////////////////////////

type GetConsumersRequest struct {
	Packet
	header *GetConsumersHeader
}

func (rq GetConsumersRequest) Validate() bool {
	if rq.Code == int16(GET_CONSUMERS) {
		return true
	}

	return false
}

func (rq *GetConsumersRequest) GetHeader() (*GetConsumersHeader, error) {
	if rq.header == nil {
		var header GetConsumersHeader
		err := json.Unmarshal(rq.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rq.header = &header
	}

	return rq.header, nil
}

func (rq *GetConsumersRequest) SetHeader(header *GetConsumersHeader) {
	rq.header = header
}

type TopicConsumers = []string
type GetConsumersResponse struct {
	Packet
	header    *GetConsumersHeader
	consumers *TopicConsumers
}

func (rs GetConsumersResponse) Validate() bool {
	if rs.Code == int16(SUCCESS) {
		return true
	}

	return false
}

func (rs *GetConsumersResponse) GetHeader() (*GetConsumersHeader, error) {
	if rs.header == nil {
		var header GetConsumersHeader
		err := json.Unmarshal(rs.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rs.header = &header
	}

	return rs.header, nil
}

func (rs *GetConsumersResponse) GetTopicConsumers() (TopicConsumers, error) {
	if len(rs.Body) == 0 {
		return nil, ErrNilRspBody
	}

	if rs.consumers == nil {
		var consumers TopicConsumers
		err := json.Unmarshal(rs.Body, &consumers)
		if err != nil {
			return nil, err
		}
		sort.Sort(sort.StringSlice(consumers))
		rs.consumers = &consumers
	}

	return *rs.consumers, nil
}

////////////////////////////////////////////////
// consumers list change from broker
////////////////////////////////////////////////

type ConsumerListChangeRequest struct {
	Packet
	header *ConsumerListChangeHeader
}

func (rq ConsumerListChangeRequest) Validate() bool {
	if rq.Code == int16(CONSUMER_LIST_CHANGE) {
		return true
	}

	return false
}

func (rq *ConsumerListChangeRequest) GetHeader() (*ConsumerListChangeHeader, error) {
	if rq.header == nil {
		var header ConsumerListChangeHeader
		err := json.Unmarshal(rq.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rq.header = &header
	}

	return rq.header, nil
}

////////////////////////////////////////////////
// heartbeat
////////////////////////////////////////////////

type HeartbeatRequest struct {
	Packet
	header *ClientHeartbeatHeader
}

func (rq HeartbeatRequest) Validate() bool {
	if rq.Code == int16(CLIENT_HEARTBEAT) {
		return true
	}

	return false
}

func (rs *HeartbeatRequest) GetHeader() (*ClientHeartbeatHeader, error) {
	if rs.header == nil {
		var header ClientHeartbeatHeader
		err := json.Unmarshal(rs.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rs.header = &header
	}

	return rs.header, nil
}

type HeartbeatResponse struct {
	Packet
}

func (rs HeartbeatResponse) Validate() bool {
	if rs.Code == int16(SUCCESS) {
		return true
	}

	return false
}

////////////////////////////////////////////////
// Send Message
////////////////////////////////////////////////

type SendMessageRequest struct {
	Packet
	header *WriteMessageHeader
}

func (rq SendMessageRequest) Validate() bool {
	if rq.Code == int16(SEND_MESSAGE) {
		return true
	}

	return false
}

func (rq *SendMessageRequest) GetHeader() (*WriteMessageHeader, error) {
	if rq.header == nil {
		var header WriteMessageHeader
		err := json.Unmarshal(rq.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rq.header = &header
	}

	return rq.header, nil
}

func (rq SendMessageRequest) GetBody() []byte {
	return rq.Body
}

func (rq *SendMessageRequest) Rebuild(qid int32) error {
	rq.header.QueueId = qid
	var err error
	rq.Packet.HeaderData, err = json.Marshal(rq.header)
	if err != nil {
		return err
	}
	rq.Packet.PacketLength -= int32(rq.Packet.HeaderLength)
	rq.Packet.HeaderLength = int16(len(rq.Packet.HeaderData))
	rq.Packet.PacketLength += int32(rq.Packet.HeaderLength)

	return nil
}

type SendTxSubmitMessageRequest struct {
	Packet
	header *SubmitTransactionHeader
}

func (rq SendTxSubmitMessageRequest) Validate() bool {
	if rq.Code == int16(SUBMIT_TRANSACTIONAL_MESSAGE) {
		return true
	}

	return false
}

func (rq *SendTxSubmitMessageRequest) GetHeader() (*SubmitTransactionHeader, error) {
	if rq.header == nil {
		var header SubmitTransactionHeader
		err := json.Unmarshal(rq.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rq.header = &header
	}

	return rq.header, nil
}

func (rq SendTxSubmitMessageRequest) GetBody() []byte {
	return rq.Body
}

type SendMessageResponse struct {
	Packet
	header   *WriteMessageResponseHeader
	metadata *TopicMetadata
}

func (rs SendMessageResponse) Validate() bool {
	if rs.Code == int16(SUCCESS) {
		return true
	}

	return false
}

func (rs *SendMessageResponse) GetHeader() (*WriteMessageResponseHeader, error) {
	if rs.header == nil {
		var header WriteMessageResponseHeader
		err := json.Unmarshal(rs.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rs.header = &header
	}

	return rs.header, nil
}

func (rs *SendMessageResponse) GetTopicMetadata() (*TopicMetadata, error) {
	if len(rs.Body) == 0 {
		return nil, ErrNilRspBody
	}

	var meta TopicMetadata

	if rs.metadata == nil {
		err := json.Unmarshal(rs.Body, &meta)
		if err != nil {
			return nil, err
		}

		rs.metadata = &meta
	}

	return rs.metadata, nil
}

type TimeoutResponse struct {
	Packet
	header *CommonResponseHeader
}

func (rs TimeoutResponse) Validate() bool {
	if rs.Code == int16(TIMEOUT) {
		return true
	}

	return false
}

// header is producer message
func (rs *TimeoutResponse) GetHeader() (*CommonResponseHeader, error) {
	if rs.header == nil {
		var header CommonResponseHeader
		err := json.Unmarshal(rs.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rs.header = &header
	}

	return rs.header, nil
}

type SendFailedResponse struct {
	Packet
	header *CommonResponseHeader
}

func (rs SendFailedResponse) Validate() bool {
	if rs.Code == int16(UNKNOWN_ERROR) {
		return true
	}

	return false
}

// header is producer message
func (rs *SendFailedResponse) GetHeader() (*CommonResponseHeader, error) {
	if rs.header == nil {
		var header CommonResponseHeader
		err := json.Unmarshal(rs.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rs.header = &header
	}

	return rs.header, nil
}

type ConnectionClosedResponse struct {
	Packet
	header *CommonResponseHeader
}

func (rs ConnectionClosedResponse) Validate() bool {
	if rs.Code == int16(CONNECTION_ERROR) {
		return true
	}

	return false
}

func (rs *ConnectionClosedResponse) GetHeader() (*CommonResponseHeader, error) {
	if rs.header == nil {
		var header CommonResponseHeader
		err := json.Unmarshal(rs.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rs.header = &header
	}

	return rs.header, nil
}

////////////////////////////////////////////////
// Consume Check Message
////////////////////////////////////////////////

type TxCheckMessageRequest struct {
	Packet
	header *CheckTransactionalMessageHeader
}

func (rq TxCheckMessageRequest) Validate() bool {
	if rq.Code == int16(CHECK_TRANSACTIONAL_MESSAGE) {
		return true
	}

	return false
}

func (rq *TxCheckMessageRequest) GetHeader() (*CheckTransactionalMessageHeader, error) {
	if rq.header == nil {
		var header CheckTransactionalMessageHeader
		err := json.Unmarshal(rq.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rq.header = &header
	}

	return rq.header, nil
}

////////////////////////////////////////////////
// Send Batch Message
////////////////////////////////////////////////

type WriteBatchMessageHeaderEx struct {
	WriteBatchMessageHeader
	PropertiesMap       []map[string]string `json:"propertiesMap,omitempty"`
	SystemPropertiesMap []map[string]string `json:"systemPropertiesMap,omitempty"`
}

type SendBatchMessageRequest struct {
	Packet
	header *WriteBatchMessageHeaderEx
}

func (rq SendBatchMessageRequest) Validate() bool {
	if rq.Code == int16(SEND_BATCH_MESSAGE) {
		return true
	}

	return false
}

func (rq *SendBatchMessageRequest) GetHeader() (*WriteBatchMessageHeaderEx, error) {
	if rq.header == nil {
		var header WriteBatchMessageHeaderEx
		//err := header.json.Unmarshal(rq.HeaderData)
		err := json.Unmarshal(rq.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rq.header = &header
	}

	return rq.header, nil
}

func (rq SendBatchMessageRequest) GetBody() []byte {
	return rq.Body
}

func (rq *SendBatchMessageRequest) Rebuild(qid int32) error {
	rq.header.QueueId = qid
	var err error
	rq.Packet.HeaderData, err = json.Marshal(rq.header)
	if err != nil {
		return err
	}
	rq.Packet.PacketLength -= int32(rq.Packet.HeaderLength)
	rq.Packet.HeaderLength = int16(len(rq.Packet.HeaderData))
	rq.Packet.PacketLength += int32(rq.Packet.HeaderLength)

	return nil
}

type SendBatchMessageResponse struct {
	Packet
	header   *WriteBatchMessageResponseHeader
	metadata *TopicMetadata
}

func (rs SendBatchMessageResponse) Validate() bool {
	if rs.Code == int16(SUCCESS) {
		return true
	}

	return false
}

func (rs *SendBatchMessageResponse) GetHeader() (*WriteBatchMessageResponseHeader, error) {
	if rs.header == nil {
		var header WriteBatchMessageResponseHeader
		err := json.Unmarshal(rs.HeaderData, &header)
		if err != nil {
			return nil, err
		}
		rs.header = &header
	}

	return rs.header, nil
}

func (rs *SendBatchMessageResponse) GetMetadata() (*TopicMetadata, error) {
	if rs.metadata == nil {
		var metadata TopicMetadata
		err := json.Unmarshal(rs.Body, &metadata)
		if err != nil {
			return nil, err
		}
		rs.metadata = &metadata
	}

	return rs.metadata, nil
}

////////////////////////////////////////////////
// Consume Message
////////////////////////////////////////////////

type FetchMessageRequest struct {
	Packet
	header *ReadMessageHeader
}

func (rq FetchMessageRequest) Validate() bool {
	if rq.Code == int16(FETCH_MESSAGE) {
		return true
	}

	return false
}

func (rq *FetchMessageRequest) GetHeader() (*ReadMessageHeader, error) {
	if rq.header == nil {
		var header ReadMessageHeader
		err := json.Unmarshal(rq.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rq.header = &header
	}

	return rq.header, nil
}

func (rq *FetchMessageRequest) Rebuild(header *ReadMessageHeader) error {
	rq.header = header
	headerData, err := json.Marshal(header)
	if err != nil {
		return err
	}

	rq.Packet.HeaderData = headerData
	rq.PacketLength -= int32(rq.HeaderLength)
	rq.HeaderLength = int16(len(headerData))
	rq.PacketLength += int32(rq.HeaderLength)

	return nil
}

type FetchMessageResponse struct {
	Packet
	fixedQueue bool
	topicID    int
	header     *ReadMessageResponseHeader
}

func (rs FetchMessageResponse) Validate() bool {
	if rs.Code == int16(SUCCESS) {
		return true
	}

	return false
}

func (rs *FetchMessageResponse) GetHeader() (*ReadMessageResponseHeader, error) {
	if rs.header == nil {
		var header ReadMessageResponseHeader
		err := json.Unmarshal(rs.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rs.header = &header
	}

	return rs.header, nil
}

func (rs *FetchMessageResponse) Rebuild(header *ReadMessageResponseHeader) error {
	rs.header = header
	headerData, err := json.Marshal(header)
	if err != nil {
		return err
	}

	rs.Packet.HeaderData = headerData
	rs.PacketLength -= int32(rs.HeaderLength)
	rs.HeaderLength = int16(len(headerData))
	rs.PacketLength += int32(rs.HeaderLength)

	return nil
}

// for tracelog, to get messageID
func (rs *FetchMessageResponse) SetFixedQueue(fixedQueue bool) {
	rs.fixedQueue = fixedQueue
}

// for tracelog, to get messageID
func (rs *FetchMessageResponse) SetTopicID(topicID int) {
	rs.topicID = topicID
}

func (rs *FetchMessageResponse) GetMessages(topic string) ([]*FlexibleMessageAndContext, error) {
	if rs == nil {
		return nil, fmt.Errorf("@rs is nil")
	}

	var (
		offset       int
		bodyLen      int
		msgLen       int
		err          error
		retryMessage bool
	)

	/*
	 * 设置消息ID
	 * 1.如果是普通延迟消息，每次重新生成新的msgid
	 * 2.如果是retry消息，则每次使用REAL_MSG_ID
	 */
	offset = 0
	bodyLen = len(rs.Body)
	if bodyLen == 0 {
		return nil, fmt.Errorf("@rs.body == nil")
	}

	if strings.HasPrefix(topic, "%RETRY%") {
		retryMessage = true
	}

	mcArray := make([]*FlexibleMessageAndContext, 0, 16)
	for offset < bodyLen {
		var mc FlexibleMessageAndContext

		msgLen, err = mc.Decode(rs.Body[offset:bodyLen])
		if err != nil {
			return nil, err
		}
		if !retryMessage {
			mc.GetMessageID(rs.fixedQueue, rs.topicID)
		}

		offset += msgLen
		mcArray = append(mcArray, &mc)
	}

	return mcArray, nil
}

////////////////////////////////////////////////
// Query Offset
////////////////////////////////////////////////

type QueryOffsetRequest struct {
	Packet
	header *QueryOffsetHeader
}

func (rq QueryOffsetRequest) Validate() bool {
	if rq.Code == int16(QUERY_OFFSET) {
		return true
	}

	return false
}

func (rq *QueryOffsetRequest) GetHeader() (*QueryOffsetHeader, error) {
	if rq.header == nil {
		var header QueryOffsetHeader
		err := json.Unmarshal(rq.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rq.header = &header
	}

	return rq.header, nil
}

type QueryOffsetResponse struct {
	Packet
	header *QueryOffsetResponseHeader
}

func (rs QueryOffsetResponse) Validate() bool {
	if rs.Code == int16(SUCCESS) {
		return true
	}

	return false
}

func (rs *QueryOffsetResponse) GetHeader() (*QueryOffsetResponseHeader, error) {
	if rs.header == nil {
		var header QueryOffsetResponseHeader
		err := json.Unmarshal(rs.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rs.header = &header
	}

	return rs.header, nil
}

////////////////////////////////////////////////
// Reset Offset
////////////////////////////////////////////////

type ResetOffsetHeader struct {
	ResetOffsets  map[MessageQueue]int64 `json:"resetOffsets,omitempty"`
	ConsumerGroup string                 `json:"consumerGroup,omitempty"`
}

type ResetOffsetRequest struct {
	Packet
	header *ResetOffsetHeader
}

func (rq ResetOffsetRequest) Validate() bool {
	if rq.Code == int16(RESET_OFFSET) {
		return true
	}

	return false
}

func (rq *ResetOffsetRequest) GetHeader() (*ResetOffsetHeader, error) {
	if rq.header == nil {
		var header ResetOffsetHeader
		err := json.Unmarshal(rq.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rq.header = &header
	}

	return rq.header, nil
}

////////////////////////////////////////////////
// Query Offset by Timestamp
////////////////////////////////////////////////

type QueryOffsetByTimestampRequest struct {
	Packet
	header *QueryOffsetByTimestampHeader
}

func (rq QueryOffsetByTimestampRequest) Validate() bool {
	if rq.Code == int16(QUERY_OFFSET_BY_TIMESTAMP) {
		return true
	}

	return false
}

func (rq *QueryOffsetByTimestampRequest) GetHeader() (*QueryOffsetByTimestampHeader, error) {
	if rq.header == nil {
		var header QueryOffsetByTimestampHeader
		err := json.Unmarshal(rq.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rq.header = &header
	}

	return rq.header, nil
}

type QueryOffsetByTimestampResponse struct {
	Packet
	header *QueryOffsetByTimestampResponseHeader
}

func (rs QueryOffsetByTimestampResponse) Validate() bool {
	if rs.Code == int16(SUCCESS) {
		return true
	}

	return false
}

func (rs *QueryOffsetByTimestampResponse) GetHeader() (*QueryOffsetByTimestampResponseHeader, error) {
	if rs.header == nil {
		var header QueryOffsetByTimestampResponseHeader
		err := json.Unmarshal(rs.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rs.header = &header
	}

	return rs.header, nil
}

////////////////////////////////////////////////
// persist Offset
////////////////////////////////////////////////

type PersistOffsetRequest struct {
	Packet
	header *SaveSingleOffsetHeader
}

func (rq PersistOffsetRequest) Validate() bool {
	if rq.Code == int16(SAVE_OFFSET) {
		return true
	}

	return false
}

func (rq *PersistOffsetRequest) GetHeader() (*SaveSingleOffsetHeader, error) {
	if rq.header == nil {
		var header SaveSingleOffsetHeader
		err := json.Unmarshal(rq.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rq.header = &header
	}

	return rq.header, nil
}

type PersistOffsetResponse struct {
	Packet
}

func (rs PersistOffsetResponse) Validate() bool {
	if rs.Code == int16(SUCCESS) {
		return true
	}

	return false
}

////////////////////////////////////////////////
// persist Offset
////////////////////////////////////////////////

type RequestOffsetRequest struct {
	Packet
	header *GetOffsetHeader
}

func (rq RequestOffsetRequest) Validate() bool {
	if rq.Code == int16(GET_OFFSET) {
		return true
	}

	return false
}

func (rq *RequestOffsetRequest) GetHeader() (*GetOffsetHeader, error) {
	if rq.header == nil {
		var header GetOffsetHeader
		err := json.Unmarshal(rq.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rq.header = &header
	}

	return rq.header, nil
}

type RequestOffsetResponse struct {
	Packet
	header *GetOffsetResponseHeader
}

func (rs RequestOffsetResponse) Validate() bool {
	if rs.Code == int16(SUCCESS) {
		return true
	}

	return false
}

func (rs *RequestOffsetResponse) GetHeader() (*GetOffsetResponseHeader, error) {
	if rs.header == nil {
		var header GetOffsetResponseHeader
		err := json.Unmarshal(rs.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rs.header = &header
	}

	return rs.header, nil
}

////////////////////////////////////////////////
// schedule
////////////////////////////////////////////////

type ScheduleMessageRequest struct {
	Packet
	header *ScheduleMessageHeader
}

func (rq ScheduleMessageRequest) Validate() bool {
	if rq.Code == int16(SCHEDULE_MESSAGE) {
		return true
	}

	return false
}

func (rq *ScheduleMessageRequest) GetHeader() (*ScheduleMessageHeader, error) {
	if rq.header == nil {
		var header ScheduleMessageHeader
		err := json.Unmarshal(rq.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rq.header = &header
	}

	return rq.header, nil
}

type ScheduleMessageResponse struct {
	Packet
	header *ScheduleMessageResponseHeader
}

func (rs ScheduleMessageResponse) Validate() bool {
	if rs.Code == int16(SUCCESS) {
		return true
	}

	return false
}

func (rs *ScheduleMessageResponse) GetHeader() (*ScheduleMessageResponseHeader, error) {
	if rs.header == nil {
		var header ScheduleMessageResponseHeader
		err := json.Unmarshal(rs.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rs.header = &header
	}

	return rs.header, nil
}

////////////////////////////////////////////////
// unregister cloud engine
////////////////////////////////////////////////

type UnregisterClientRequest struct {
	Packet
	header *ClientLogoutHeader
}

func (rq UnregisterClientRequest) Validate() bool {
	if rq.Code == int16(CLIENT_UNREGISTER) {
		return true
	}

	return false
}

func (rq *UnregisterClientRequest) GetHeader() (*ClientLogoutHeader, error) {
	if rq.header == nil {
		var header ClientLogoutHeader
		err := json.Unmarshal(rq.HeaderData, &header)
		if err != nil {
			return nil, err
		}

		rq.header = &header
	}

	return rq.header, nil
}

func (rq *UnregisterClientRequest) Rebuild(header *ClientLogoutHeader) error {
	rq.header = header
	headerData, err := json.Marshal(header)
	if err != nil {
		return err
	}

	rq.Packet.HeaderData = headerData
	rq.PacketLength -= int32(rq.HeaderLength)
	rq.HeaderLength = int16(len(headerData))
	rq.PacketLength += int32(rq.HeaderLength)

	return nil
}

type UnregisterClientResponse struct {
	Packet
}

func (rs UnregisterClientResponse) Validate() bool {
	if rs.Code == int16(SUCCESS) {
		return true
	}

	return false
}
