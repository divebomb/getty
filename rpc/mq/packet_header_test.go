package mq

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestWriteMessageHeader_ProtoMessage(t *testing.T) {
	jsonStr := `{"bornTime":1557370891097,"delaySeconds":0,"producer":"MQ-PRODUCER-GROUP","properties":{"messageIndex":"1"},"queueId":1,"systemProperties":{"sysPenAttrs":"","VERSION":"1","sofaTraceId":"c0a80b011557370896146100130340","sofaPenAttrs":"","antq_entity_property":"{\"bornTime\":1557370891097,\"committed\":true,\"deliveryCount\":0,\"dlqTime\":-1,\"flag\":3,\"gMTCreate\":1557370906175,\"gMTLastDelivery\":1557370906175,\"groupId\":\"MQ-PRODUCER-GROUP\",\"hostName\":\"C02XW5SLJHD2.local\",\"messageId\":\"DF7D9DDC689C3E1A62F064DFCC4BD29F\",\"messageType\":\"MQ_DEFAULT_TAG\",\"nextDeliverTime\":0,\"postTimeOut\":10000,\"timeToLive\":-1}"},"topic":"TP_CHENGYI_TEST_QUEUE_4"}`

	var header WriteMessageHeader
	err := json.Unmarshal([]byte(jsonStr), &header)
	if err != nil {
		t.Errorf("json.Unmarshal() = err:%s", err)
	}
	t.Logf("header{bornTime:%v, delaySeconds:%v, producer:%v, properties:%+v, queueId:%v, topic:%v, systemProperties:%v}",
		header.BornTime, header.DelaySeconds, header.Producer, header.Properties, header.QueueId, header.Topic, header.SystemProperties)

	var antqProperty map[string]interface{}
	json.Unmarshal([]byte(header.SystemProperties["antq_entity_property"]), &antqProperty)
	t.Logf("antq property: %+v", antqProperty)
}

func TestGetTopicMetadataHeader_ProtoMessage(t *testing.T) {
	jsonStr := `{"mode":"GLOBAL_QUEUE_ORDERING","topic":"TP_CHENGYI_TEST_QUEUE_4"}`

	var header GetTopicMetadataHeader
	err := json.Unmarshal([]byte(jsonStr), &header)
	if err != nil {
		t.Errorf("json.Unmarshal() = err:%s", err)
	}
	t.Logf("header{mode:%v, topic:%v}", header.Mode, header.Topic)

	header = GetTopicMetadataHeader{
		Topic: "TP_YUYU",
		Mode:  TopicRouteMode_DEFAULT,
	}
	jsonBytes, _ := json.Marshal(header)
	t.Logf("Marshal(header:%#v) = %s", header, string(jsonBytes))

	header = GetTopicMetadataHeader{
		Topic: "TP_YUYU",
		Mode:  TopicRouteMode_GLOBAL_QUEUE_ORDERING,
	}
	jsonBytes, _ = json.Marshal(header)
	t.Logf("Marshal(header:%#v) = %s", header, string(jsonBytes))
}

func TestClientHeartbeatHeader_ProtoMessage(t *testing.T) {
	jsonStr := `{"instanceInfo":{"group":"MQ-PRODUCER-GROUP","id":"30340@C02XW5SLJHD2.local@0","type":"PRODUCER","version":"1.2.0"}}`

	var producerHeartbeatHeader ClientHeartbeatHeader
	err := json.Unmarshal([]byte(jsonStr), &producerHeartbeatHeader)
	if err != nil {
		t.Errorf("json.Unmarshal() = err:%s", err)
	}
	t.Logf("producerHeartbeatHeader{InstanceInfo{group:%v, id:%v, type:%v, version:%v}}",
		producerHeartbeatHeader.InstanceInfo.Group, producerHeartbeatHeader.InstanceInfo.Id,
		producerHeartbeatHeader.InstanceInfo.Type, producerHeartbeatHeader.InstanceInfo.Version)

	jsonStr = `{"instanceInfo":{"consumerType":"AUTO_PULL","consumingMode":"CLUSTERING","group":"MQ-PRODUCER-GROUP","id":"31133@C02XW5SLJHD2.local@0","startingPoint":"FIRST_OFFSET","subscribed":["TP_CHENGYI_TEST_QUEUE_4","%RETRY%MQ-PRODUCER-GROUP"],"type":"CONSUMER","version":"1.2.0"}}`

	var consumerHeartbeatHeader ClientHeartbeatHeader
	// err = json.Unmarshal([]byte(jsonStr), &consumerHeartbeatHeader)
	err = json.Unmarshal([]byte(jsonStr), &consumerHeartbeatHeader)
	if err != nil {
		t.Errorf("json.Unmarshal() = err:%s", err)
	}
	t.Logf("consumerHeartbeatHeader{InstanceInfo{consumerType:%v, consumingMode:%v, group:%v, id:%v, startingPoint:%v, subscribed:%+v, type:%v, version:%v}}",
		consumerHeartbeatHeader.InstanceInfo.ConsumerType, consumerHeartbeatHeader.InstanceInfo.ConsumingMode,
		consumerHeartbeatHeader.InstanceInfo.Group, consumerHeartbeatHeader.InstanceInfo.Id,
		consumerHeartbeatHeader.InstanceInfo.StartingPoint, consumerHeartbeatHeader.InstanceInfo.Subscribed,
		consumerHeartbeatHeader.InstanceInfo.Type, consumerHeartbeatHeader.InstanceInfo.Version)
}

func TestQueryOffsetHeader_ProtoMessage(t *testing.T) {
	jsonStr := `{"group":"MQ-PRODUCER-GROUP","queueId":0,"topic":"TP_CHENGYI_TEST_QUEUE_4"}`

	var header QueryOffsetHeader
	err := json.Unmarshal([]byte(jsonStr), &header)
	if err != nil {
		t.Errorf("json.Unmarshal() = err:%s", err)
	}
	t.Logf("header{group:%v, queueId:%v, topic:%v}", header.Group, header.QueueId, header.Topic)
}

func TestReadMessageHeader(t *testing.T) {
	jsonStr := `{"batch":32,"commit":1527,"group":"MQ-PRODUCER-GROUP","queue":0,"start":1528,"timeout":20000,"topic":"TP_CHENGYI_TEST_QUEUE_4"}`

	var header ReadMessageHeader
	err := json.Unmarshal([]byte(jsonStr), &header)
	if err != nil {
		t.Errorf("json.Unmarshal() = err:%s", err)
	}
	t.Logf("header{batch:%v, commit:%v, group:%v, queue:%v, start:%v, timeout:%v, topic:%v}",
		header.Batch, header.Commit, header.Group, header.Queue, header.Start, header.Timeout, header.Topic)
}

func TestGetConsumersHeader_ProtoMessage(t *testing.T) {
	jsonStr := `{"group":"MQ-PRODUCER-GROUP","topic":"TP_CHENGYI_TEST_QUEUE_4"}`

	var header GetConsumersHeader
	err := json.Unmarshal([]byte(jsonStr), &header)
	if err != nil {
		t.Errorf("json.Unmarshal() = err:%s", err)
	}
	t.Logf("header{group:%v, topic:%v}", header.Group, header.Topic)
}

func TestClientLoginHeader_Validate(t *testing.T) {
	var header ClientLoginHeader
	flag := header.Validate()
	if flag {
		t.Errorf("nil ClientLoginHeader Validate true")
	}

	header.ID = "1"
	header.Group = "TG"
	flag = header.Validate()
	if !flag {
		t.Errorf("non nil ClientLoginHeader Validate false")
	}
}

func TestClientLogoutHeader_Validate(t *testing.T) {
	var header ClientLogoutHeader
	flag := header.Validate()
	if flag {
		t.Errorf("nil ClientLogoutHeader Validate true")
	}

	header.Group = "TG"
	header.ClientId = "T"
	header.ClientType = InstanceType_PRODUCER.String()
	flag = header.Validate()
	if !flag {
		t.Errorf("non nil ClientLogoutHeader Validate false")
	}
}

func TestInstanceType_MarshalJSON(t *testing.T) {
	inst := InstanceType_PRODUCER
	jsonStream, err := inst.MarshalJSON()
	if err != nil {
		t.Errorf("can not marshal %v in json", inst)
	}
	err = inst.UnmarshalJSON(jsonStream)
	if err != nil {
		t.Errorf("can not unmarshal %v in json", string(jsonStream))
	}
	t.Logf("instance %s", inst.String())
}

func TestConsumerType_MarshalJSON(t *testing.T) {
	inst := ConsumerType_AUTO_PULL
	jsonStream, err := inst.MarshalJSON()
	if err != nil {
		t.Errorf("can not marshal %v in json", inst)
	}
	err = inst.UnmarshalJSON(jsonStream)
	if err != nil {
		t.Errorf("can not unmarshal %v in json", string(jsonStream))
	}
	t.Logf("instance %s", inst.String())
}

func TestConsumingMode_MarshalJSON(t *testing.T) {
	inst := ConsumingMode_BROADCASTING
	jsonStream, err := inst.MarshalJSON()
	if err != nil {
		t.Errorf("can not marshal %v in json", inst)
	}
	err = inst.UnmarshalJSON(jsonStream)
	if err != nil {
		t.Errorf("can not unmarshal %v in json", string(jsonStream))
	}
	t.Logf("instance %s", inst.String())
}

func TestStartingPoint_MarshalJSON(t *testing.T) {
	inst := StartingPoint_LAST_OFFSET
	jsonStream, err := inst.MarshalJSON()
	if err != nil {
		t.Errorf("can not marshal %v in json", inst)
	}
	err = inst.UnmarshalJSON(jsonStream)
	if err != nil {
		t.Errorf("can not unmarshal %v in json", string(jsonStream))
	}
	t.Logf("instance %s", inst.String())
}

func TestRouteMode_MarshalJSON(t *testing.T) {
	inst := TopicRouteMode_GLOBAL_QUEUE_ORDERING
	jsonStream, err := inst.MarshalJSON()
	if err != nil {
		t.Errorf("can not marshal %v in json", inst)
	}
	err = inst.UnmarshalJSON(jsonStream)
	if err != nil {
		t.Errorf("can not unmarshal %v in json", string(jsonStream))
	}
	t.Logf("instance %s", inst.String())
}

func TestGetOffsetType_MarshalJSON(t *testing.T) {
	inst := GetOffsetType_MAX
	jsonStream, err := inst.MarshalJSON()
	if err != nil {
		t.Errorf("can not marshal %v in json", inst)
	}
	err = inst.UnmarshalJSON(jsonStream)
	if err != nil {
		t.Errorf("can not unmarshal %v in json", string(jsonStream))
	}
	t.Logf("instance %s", inst.String())
}

func TestSerializationType_MarshalJSON(t *testing.T) {
	inst := SerializationType_JSON
	jsonStream, err := inst.MarshalJSON()
	if err != nil {
		t.Errorf("can not marshal %v in json", inst)
	}
	err = inst.UnmarshalJSON(jsonStream)
	if err != nil {
		t.Errorf("can not unmarshal %v in json", string(jsonStream))
	}
	t.Logf("instance %s", inst.String())
}

func TestSchemaCompatibilityStrategy_MarshalJSON(t *testing.T) {
	inst := SchemaCompatibilityStrategy_BACKWARD
	jsonStream, err := inst.MarshalJSON()
	if err != nil {
		t.Errorf("can not marshal %v in json", inst)
	}
	err = inst.UnmarshalJSON(jsonStream)
	if err != nil {
		t.Errorf("can not unmarshal %v in json", string(jsonStream))
	}
	t.Logf("instance %s", inst.String())
}

func TestSendState_MarshalJSON(t *testing.T) {
	inst := SendState_CONNECT_EXCEPTION
	jsonStream, err := inst.MarshalJSON()
	if err != nil {
		t.Errorf("can not marshal %v in json", inst)
	}
	err = inst.UnmarshalJSON(jsonStream)
	if err != nil {
		t.Errorf("can not unmarshal %v in json", string(jsonStream))
	}
	t.Logf("instance %s", inst.String())
}

func TestPermName_MarshalJSON(t *testing.T) {
	inst := PermName_PERM_READ
	jsonStream, err := inst.MarshalJSON()
	if err != nil {
		t.Errorf("can not marshal %v in json", inst)
	}
	err = inst.UnmarshalJSON(jsonStream)
	if err != nil {
		t.Errorf("can not unmarshal %v in json", string(jsonStream))
	}
	t.Logf("instance %s", inst.String())
}

func TestMessageStateEnum_MarshalJSON(t *testing.T) {
	inst := MessageStateEnum_WAITING
	jsonStream, err := inst.MarshalJSON()
	if err != nil {
		t.Errorf("can not marshal %v in json", inst)
	}
	err = inst.UnmarshalJSON(jsonStream)
	if err != nil {
		t.Errorf("can not unmarshal %v in json", string(jsonStream))
	}
	t.Logf("instance %s", inst.String())
}

func TestRequestCommandCode_MarshalJSON(t *testing.T) {
	inst := RequestCommandCode_CLIENT_HEARTBEAT
	jsonStream, err := inst.MarshalJSON()
	if err != nil {
		t.Errorf("can not marshal %v in json", inst)
	}
	err = inst.UnmarshalJSON(jsonStream)
	if err != nil {
		t.Errorf("can not unmarshal %v in json", string(jsonStream))
	}
	t.Logf("instance %s", inst.String())
}

func TestResponseCommandCode_MarshalJSON(t *testing.T) {
	inst := ResponseCommandCode_CONNECTION_ERROR
	jsonStream, err := inst.MarshalJSON()
	if err != nil {
		t.Errorf("can not marshal %v in json", inst)
	}
	err = inst.UnmarshalJSON(jsonStream)
	if err != nil {
		t.Errorf("can not unmarshal %v in json", string(jsonStream))
	}
	t.Logf("instance %s", inst.String())
}

func TestReadMessageResponseHeader_PullState_MarshalJSON(t *testing.T) {
	inst := ReadMessageResponseHeader_REDIRECT
	jsonStream, err := inst.MarshalJSON()
	if err != nil {
		t.Errorf("can not marshal %v in json", inst)
	}
	err = inst.UnmarshalJSON(jsonStream)
	if err != nil {
		t.Errorf("can not unmarshal %v in json", string(jsonStream))
	}
	t.Logf("instance %s", inst.String())
}

func TestMessageQueue_Equal(t *testing.T) {
	q0 := MessageQueue{Id: 0, Topic: "TP_S_TRADE", Address: "localhost:62000", Broker: "antq-eu95-1.gz00b.stable.alipay.net", Permission: 6, GlobalFixedQueue: false}
	q1 := MessageQueue{Id: 15, Topic: "TP_S_TRADE", Address: "localhost:62000", Broker: "antq-eu95-1.gz00b.stable.alipay.net", Permission: 6, GlobalFixedQueue: false}
	if q0 == q1 {
		t.Errorf("q0 == q1")
	}
	if reflect.DeepEqual(q0, q1) {
		t.Errorf("q0 == q1")
	}

	q0.Id = 15
	if q0 != q1 {
		t.Errorf("q0 != q1")
	}
	if !reflect.DeepEqual(q0, q1) {
		t.Errorf("q0 != q1")
	}

	q0.Id = q1.Id
	q0.GlobalFixedQueue = true
	if q0 == q1 {
		t.Errorf("q0 != q1")
	}
	if reflect.DeepEqual(q0, q1) {
		t.Errorf("q0 != q1")
	}

	q0.GlobalFixedQueue = q1.GlobalFixedQueue
	q0.Address = "localhost:1234"
	if q0 == q1 {
		t.Errorf("q0 != q1")
	}
	if reflect.DeepEqual(q0, q1) {
		t.Errorf("q0 != q1")
	}

	q0.Address = q1.Address
	if q0 != q1 {
		t.Errorf("q0 == q1")
	}
	if !reflect.DeepEqual(q0, q1) {
		t.Errorf("q0 == q1")
	}
}

func TestDefaultPullResult_PullState(t *testing.T) {
	state := DefaultPullResult_PullState(1)
	assert.NotNil(t, state.Enum())

	s, err := state.MarshalJSON()
	assert.NotNil(t, s)
	assert.Nil(t, err)

	err = state.UnmarshalJSON(s)
	assert.Nil(t, err)
}

func TestEnum(t *testing.T) {
	it := InstanceType(1)
	assert.NotNil(t, it.Enum())

	ct := ConsumerType(1)
	assert.NotNil(t, ct.Enum())

	cm := ConsumingMode(1)
	assert.NotNil(t, cm.Enum())

	sp := StartingPoint(1)
	assert.NotNil(t, sp.Enum())

	trm := TopicRouteMode(1)
	assert.NotNil(t, trm.Enum())

	got := GetOffsetType(1)
	assert.NotNil(t, got.Enum())

	st := SerializationType(1)
	assert.NotNil(t, st.Enum())

	scs := SchemaCompatibilityStrategy(1)
	assert.NotNil(t, scs.Enum())

	ss := SendState(1)
	assert.NotNil(t, ss.Enum())

	pn := PermName(1)
	assert.NotNil(t, pn.Enum())

	msgState := MessageStateEnum(1)
	assert.NotNil(t, msgState.Enum())

	rqCmdCode := RequestCommandCode(1)
	assert.NotNil(t, rqCmdCode.Enum())

	cmdCode := ResponseCommandCode(1)
	assert.NotNil(t, cmdCode.Enum())

	state := ReadMessageResponseHeader_PullState(1)
	assert.NotNil(t, state.Enum())
}

func TestFlexibleMessageAndContext_GetMessageID(t *testing.T) {
	msgCtx := FlexibleMessageAndContext{}
	msgCtx.GetMessageID(true, 1)
}

func TestClientConfiguration_String(t *testing.T) {
	conf := ClientConfiguration{}
	t.Logf("conf %s", conf)
}
