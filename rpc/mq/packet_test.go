package commands

import (
	//"hash/crc32"
	"reflect"
	"testing"
)

import (
	"sofastack.io/sofa-mosn/pkg/buffer"
)

func TestCRC32(t *testing.T) {
	//buf := "hello, world!"
	//t.Logf("crc32:%d", crc32.Checksum([]byte(buf), crc32Table))
	//t.Logf("crc32:%#X", crc32.Checksum([]byte("Hello World"), crc32Table))
}

func TestPacket_GetAssignedQueueResponse(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x1, 0x52, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0x9d, 0xa, 0x2e, 0x54, 0x0, 0x0, 0x12, 0x34, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x5a, 0x7b, 0x22, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x49, 0x64, 0x22, 0x3a, 0x22, 0x74, 0x43, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x49, 0x64, 0x22, 0x2c, 0x22, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x69, 0x6e, 0x67, 0x4d, 0x6f, 0x64, 0x65, 0x22, 0x3a, 0x22, 0x43, 0x4c, 0x55, 0x53, 0x54, 0x45, 0x52, 0x49, 0x4e, 0x47, 0x22, 0x2c, 0x22, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x22, 0x3a, 0x22, 0x74, 0x47, 0x72, 0x6f, 0x75, 0x70, 0x22, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x22, 0x74, 0x54, 0x65, 0x73, 0x74, 0x22, 0x7d, 0x5b, 0x7b, 0x22, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x22, 0x3a, 0x22, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31, 0x3a, 0x31, 0x32, 0x33, 0x34, 0x22, 0x2c, 0x22, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x22, 0x3a, 0x22, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x30, 0x22, 0x2c, 0x22, 0x67, 0x6c, 0x6f, 0x62, 0x61, 0x6c, 0x46, 0x69, 0x78, 0x65, 0x64, 0x51, 0x75, 0x65, 0x75, 0x65, 0x22, 0x3a, 0x74, 0x72, 0x75, 0x65, 0x2c, 0x22, 0x69, 0x64, 0x22, 0x3a, 0x30, 0x2c, 0x22, 0x70, 0x65, 0x72, 0x6d, 0x69, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x22, 0x3a, 0x36, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x22, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x30, 0x22, 0x7d, 0x2c, 0x7b, 0x22, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x22, 0x3a, 0x22, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x32, 0x3a, 0x31, 0x32, 0x33, 0x34, 0x22, 0x2c, 0x22, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x22, 0x3a, 0x22, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x31, 0x22, 0x2c, 0x22, 0x67, 0x6c, 0x6f, 0x62, 0x61, 0x6c, 0x46, 0x69, 0x78, 0x65, 0x64, 0x51, 0x75, 0x65, 0x75, 0x65, 0x22, 0x3a, 0x74, 0x72, 0x75, 0x65, 0x2c, 0x22, 0x69, 0x64, 0x22, 0x3a, 0x31, 0x2c, 0x22, 0x70, 0x65, 0x72, 0x6d, 0x69, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x22, 0x3a, 0x36, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x22, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x31, 0x22, 0x7d, 0x5d}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg TopicMetadataResponse
	pkg.Decode(ioBuffer)
	if !pkg.Validate() {
		t.Errorf("this is not a TopicMetadataResponse")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("TopicMetadataResponse can not decode itself")
	}

	// binary header
	if pkg.TotalSize() != 338 {
		t.Errorf("pkg total length != 338")
	}
	if pkg.HeaderLength != 90 {
		t.Errorf("pkg header length != 90")
	}
	if pkg.CRC != -1660277164 {
		t.Errorf("pkg crc32 value != -1660277164")
	}
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	if pkg.Code != 0 {
		t.Errorf("pkg code != %d", 0)
	}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id != %d", 0x1234)
	}

	// json header
	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}

	if header.Topic != "tTest" {
		t.Errorf("header.Topic != tTest")
	}

	/*
		if header.Group != "tGroup" {
			t.Errorf("header.Group != tGroup")
		}

		if header.ConsumerId != "tConsumerId" {
			t.Errorf("header.Group != tConsumerId")
		}

		if header.ConsumingMode != ConsumingMode_CLUSTERING {
			t.Errorf("header.ConsumingMode = ConsumingMode_CLUSTERING")
		}

			// json body
			meta, err := pkg.GetMetadata()
			if err != nil {
				t.Errorf("json.Unmarshal() = error %s", err)
			}
			queues := meta.MessageQueues
			if len(queues) != 2 {
				t.Errorf("len(queues) != 2")
			}

			if queues[0].Topic != "Topic0" {
				t.Errorf("queues[0].Topic %s != Topic0", queues[0].Topic)
			}
			if queues[0].Id != 0 {
				t.Errorf("queues[0].Id %d != 0", queues[0].Id)
			}
			if queues[0].Broker != "broker0" {
				t.Errorf("queues[0].Broker %s != broker0", queues[0].Broker)
			}
			if queues[0].Address != "127.0.0.1:1234" {
				t.Errorf("queues[0].Broker %s != '127.0.0.1:1234'", queues[0].Address)
			}

			if queues[1].Topic != "Topic1" {
				t.Errorf("queues[1].Topic %s != Topic1", queues[1].Topic)
			}
			if queues[1].Id != 1 {
				t.Errorf("queues[1].Id %d != 1", queues[1].Id)
			}
			if queues[1].Broker != "broker1" {
				t.Errorf("queues[1].Broker %s != broker0", queues[1].Broker)
			}
			if queues[1].Address != "127.0.0.2:1234" {
				t.Errorf("queues[1].Broker %s != '127.0.0.2:1234'", queues[1].Address)
			}
	*/
}

func TestGetConsumersHeader(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x0, 0x3b, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0x81, 0xa0, 0x1f, 0x7f, 0x0, 0x0, 0x12, 0x34, 0x3, 0xef, 0x0, 0x0, 0x0, 0x0, 0x0, 0x22, 0x7b, 0x22, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x22, 0x3a, 0x22, 0x74, 0x47, 0x72, 0x6f, 0x75, 0x70, 0x22, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x22, 0x74, 0x54, 0x65, 0x73, 0x74, 0x22, 0x7d}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg GetConsumersRequest
	err := pkg.Decode(ioBuffer)
	if err != nil {
		t.Errorf("GetConsumersRequest.Decode() = error %s", err)
	}
	if !pkg.Validate() {
		t.Errorf("this is not a GetConsumersRequest")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("GetConsumersRequest can not decode itself")
	}

	// binary header
	if pkg.TotalSize() != 59 {
		t.Errorf("pkg total length != 59")
	}
	if pkg.HeaderLength != 34 {
		t.Errorf("pkg header length != 34")
	}
	if pkg.CRC != -2120212609 {
		t.Errorf("pkg crc32 value != -2120212609")
	}
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	if pkg.Code != 1007 {
		t.Errorf("pkg code != %d", 1007)
	}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id != %d", 0x1234)
	}

	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}

	if header.Topic != "tTest" {
		t.Errorf("header.Topic != tTest")
	}

	if header.Group != "tGroup" {
		t.Errorf("header.Group != tGroup")
	}
}

func TestPacket_GetConsumersResponse(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x0, 0x48, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0xa8, 0x50, 0x4e, 0xba, 0x0, 0x0, 0x12, 0x34, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x22, 0x7b, 0x22, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x22, 0x3a, 0x22, 0x74, 0x47, 0x72, 0x6f, 0x75, 0x70, 0x22, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x22, 0x74, 0x54, 0x65, 0x73, 0x74, 0x22, 0x7d, 0x5b, 0x22, 0x63, 0x67, 0x30, 0x22, 0x2c, 0x22, 0x63, 0x67, 0x31, 0x22, 0x5d}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg GetConsumersResponse
	pkg.Decode(ioBuffer)
	if !pkg.Validate() {
		t.Errorf("this is not a GetConsumersResponse")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("GetConsumersResponse can not decode itself")
	}

	// binary header
	if pkg.TotalSize() != 72 {
		t.Errorf("pkg total length != 72")
	}
	if pkg.HeaderLength != 34 {
		t.Errorf("pkg header length != 34")
	}
	if pkg.CRC != -1471131974 {
		t.Errorf("pkg crc32 value != -1471131974")
	}
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	if pkg.Code != 0 {
		t.Errorf("pkg code != %d", 0)
	}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id != %d", 0x1234)
	}

	// json header
	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}

	if header.Topic != "tTest" {
		t.Errorf("header.Topic != tTest")
	}

	if header.Group != "tGroup" {
		t.Errorf("header.Group != tGroup")
	}

	// json body
	consumers, err := pkg.GetTopicConsumers()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}
	if len(consumers) != 2 {
		t.Errorf("len(queues) != 2")
	}
}

func TestPacket_ProducerHeartbeatHeader(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x0, 0x7c, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0xb, 0xbe, 0xa1, 0x83, 0x0, 0x0, 0x12, 0x34, 0x3, 0xea, 0x0, 0x0, 0x0, 0x0, 0x0, 0x63, 0x7b, 0x22, 0x69, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x22, 0x3a, 0x7b, 0x22, 0x61, 0x70, 0x70, 0x4e, 0x61, 0x6d, 0x65, 0x22, 0x3a, 0x22, 0x74, 0x41, 0x70, 0x70, 0x22, 0x2c, 0x22, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x22, 0x3a, 0x22, 0x74, 0x47, 0x72, 0x6f, 0x75, 0x70, 0x22, 0x2c, 0x22, 0x69, 0x64, 0x22, 0x3a, 0x22, 0x74, 0x49, 0x64, 0x22, 0x2c, 0x22, 0x74, 0x79, 0x70, 0x65, 0x22, 0x3a, 0x22, 0x50, 0x52, 0x4f, 0x44, 0x55, 0x43, 0x45, 0x52, 0x22, 0x2c, 0x22, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x22, 0x3a, 0x22, 0x31, 0x2e, 0x30, 0x2e, 0x30, 0x22, 0x7d, 0x7d}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg HeartbeatRequest
	err := pkg.Decode(ioBuffer)
	if err != nil {
		t.Errorf("HeartbeatRequest.Decode() = error %s", err)
	}
	if !pkg.Validate() {
		t.Errorf("this is not a HeartbeatRequest")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("TopicMetadataRequest can not decode itself")
	}

	// binary header
	if pkg.TotalSize() != 124 {
		t.Errorf("pkg total length != 59")
	}
	if pkg.HeaderLength != 99 {
		t.Errorf("pkg header length != 34")
	}
	if pkg.CRC != 197042563 {
		t.Errorf("pkg crc32 value != 197042563")
	}
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	if pkg.Code != 1002 {
		t.Errorf("pkg code != %d", 1002)
	}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id %#x != %#x", pkg.PacketId, 0x1234)
	}

	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}
	if header.InstanceInfo.Type.String() != "PRODUCER" {
		t.Errorf("header.InstanceInfo.Type != PRODUCER")
	}
	if header.InstanceInfo.Group != "tGroup" {
		t.Errorf("header.InstanceInfo.Group != tGroup")
	}
	if header.InstanceInfo.Id != "tId" {
		t.Errorf("header.InstanceInfo.Id != tId")
	}
	if header.InstanceInfo.Version != "1.0.0" {
		t.Errorf("header.InstanceInfo.Id != 1.0.0")
	}
	if header.InstanceInfo.AppName != "tApp" {
		t.Errorf("header.InstanceInfo.AppName != tApp")
	}
}

func TestPacket_ConsumerHeartbeatHeader(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x0, 0xe9, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0x32, 0xa4, 0xa5, 0x40, 0x0, 0x0, 0x12, 0x34, 0x3, 0xea, 0x0, 0x0, 0x0, 0x0, 0x0, 0xd0, 0x7b, 0x22, 0x69, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x22, 0x3a, 0x7b, 0x22, 0x61, 0x70, 0x70, 0x4e, 0x61, 0x6d, 0x65, 0x22, 0x3a, 0x22, 0x74, 0x41, 0x70, 0x70, 0x22, 0x2c, 0x22, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x54, 0x79, 0x70, 0x65, 0x22, 0x3a, 0x22, 0x41, 0x55, 0x54, 0x4f, 0x5f, 0x50, 0x55, 0x4c, 0x4c, 0x22, 0x2c, 0x22, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x69, 0x6e, 0x67, 0x4d, 0x6f, 0x64, 0x65, 0x22, 0x3a, 0x22, 0x43, 0x4c, 0x55, 0x53, 0x54, 0x45, 0x52, 0x49, 0x4e, 0x47, 0x22, 0x2c, 0x22, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x22, 0x3a, 0x22, 0x74, 0x47, 0x72, 0x6f, 0x75, 0x70, 0x22, 0x2c, 0x22, 0x69, 0x64, 0x22, 0x3a, 0x22, 0x74, 0x49, 0x64, 0x22, 0x2c, 0x22, 0x73, 0x74, 0x61, 0x72, 0x74, 0x69, 0x6e, 0x67, 0x50, 0x6f, 0x69, 0x6e, 0x74, 0x22, 0x3a, 0x22, 0x4c, 0x41, 0x53, 0x54, 0x5f, 0x4f, 0x46, 0x46, 0x53, 0x45, 0x54, 0x22, 0x2c, 0x22, 0x73, 0x75, 0x62, 0x73, 0x63, 0x72, 0x69, 0x62, 0x65, 0x64, 0x22, 0x3a, 0x5b, 0x22, 0x74, 0x54, 0x65, 0x73, 0x74, 0x22, 0x5d, 0x2c, 0x22, 0x74, 0x79, 0x70, 0x65, 0x22, 0x3a, 0x22, 0x43, 0x4f, 0x4e, 0x53, 0x55, 0x4d, 0x45, 0x52, 0x22, 0x2c, 0x22, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x22, 0x3a, 0x22, 0x31, 0x2e, 0x30, 0x2e, 0x30, 0x22, 0x7d, 0x7d}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg HeartbeatRequest
	err := pkg.Decode(ioBuffer)
	if err != nil {
		t.Errorf("HeartbeatRequest.Decode() = error %s", err)
	}
	if !pkg.Validate() {
		t.Errorf("this is not a HeartbeatRequest")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("TopicMetadataRequest can not decode itself")
	}

	// binary header
	if pkg.TotalSize() != 233 {
		t.Errorf("pkg total length != 233")
	}
	if pkg.HeaderLength != 208 {
		t.Errorf("pkg header length != 208")
	}
	if pkg.CRC != 849651008 {
		t.Errorf("pkg crc32 value != 849651008")
	}
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	if pkg.Code != 1002 {
		t.Errorf("pkg code != %d", 1002)
	}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id %#x != %#x", pkg.PacketId, 0x1234)
	}

	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}
	if header.InstanceInfo.Type.String() != "CONSUMER" {
		t.Errorf("header.InstanceInfo.Type != CONSUMER")
	}
	if header.InstanceInfo.Group != "tGroup" {
		t.Errorf("header.InstanceInfo.Group != tGroup")
	}
	if header.InstanceInfo.Id != "tId" {
		t.Errorf("header.InstanceInfo.Id != tId")
	}
	if header.InstanceInfo.Version != "1.0.0" {
		t.Errorf("header.InstanceInfo.Id != 1.0.0")
	}
	if header.InstanceInfo.AppName != "tApp" {
		t.Errorf("header.InstanceInfo.AppName != tApp")
	}
	if header.InstanceInfo.ConsumerType != ConsumerType_AUTO_PULL {
		t.Errorf("header.InstanceInfo.AppName != ConsumerType_AUTO_PULL")
	}
	if header.InstanceInfo.ConsumingMode != ConsumingMode_CLUSTERING {
		t.Errorf("header.InstanceInfo.ConsumingMode != ConsumingMode_CLUSTERING")
	}
	if header.InstanceInfo.StartingPoint != StartingPoint_LAST_OFFSET {
		t.Errorf("header.InstanceInfo.StartingPoint != StartingPoint_LAST_OFFSET")
	}
	topics := header.InstanceInfo.Subscribed
	if len(topics) != 1 {
		t.Errorf("header.InstanceInfo.Subscribed() length != 1")
	}
	if topics[0] != "tTest" {
		t.Errorf("header.InstanceInfo.Subscribed()[0] != tTest")
	}
}

func TestPacket_GetHeartbeatResponse(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x0, 0x19, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0x2a, 0x91, 0x6c, 0x53, 0x0, 0x0, 0x12, 0x34, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg HeartbeatResponse
	pkg.Decode(ioBuffer)
	if !pkg.Validate() {
		t.Errorf("this is not a HeartbeatResponse")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("GetConsumersResponse can not decode itself")
	}
}

func TestPacket_ProduceMessageRequest(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x0, 0xe6, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0xda, 0x39, 0xd5, 0x10, 0x0, 0x0, 0x12, 0x34, 0x7, 0xd0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc8, 0x7b, 0x22, 0x62, 0x61, 0x74, 0x63, 0x68, 0x22, 0x3a, 0x66, 0x61, 0x6c, 0x73, 0x65, 0x2c, 0x22, 0x62, 0x6f, 0x72, 0x6e, 0x54, 0x69, 0x6d, 0x65, 0x22, 0x3a, 0x31, 0x35, 0x36, 0x31, 0x38, 0x37, 0x33, 0x33, 0x35, 0x37, 0x37, 0x34, 0x34, 0x2c, 0x22, 0x64, 0x65, 0x6c, 0x61, 0x79, 0x53, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x73, 0x22, 0x3a, 0x31, 0x2c, 0x22, 0x70, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65, 0x72, 0x22, 0x3a, 0x22, 0x74, 0x50, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65, 0x72, 0x22, 0x2c, 0x22, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x22, 0x3a, 0x7b, 0x22, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x22, 0x3a, 0x22, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x22, 0x7d, 0x2c, 0x22, 0x71, 0x75, 0x65, 0x75, 0x65, 0x49, 0x64, 0x22, 0x3a, 0x33, 0x2c, 0x22, 0x73, 0x68, 0x61, 0x72, 0x64, 0x4b, 0x65, 0x79, 0x22, 0x3a, 0x22, 0x6b, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x30, 0x22, 0x2c, 0x22, 0x73, 0x79, 0x73, 0x74, 0x65, 0x6d, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x22, 0x3a, 0x7b, 0x7d, 0x2c, 0x22, 0x74, 0x61, 0x67, 0x22, 0x3a, 0x22, 0x45, 0x43, 0x5f, 0x54, 0x65, 0x73, 0x74, 0x22, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x22, 0x54, 0x50, 0x5f, 0x54, 0x65, 0x73, 0x74, 0x22, 0x7d, 0x68, 0x65, 0x6c, 0x6c, 0x6f}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg SendMessageRequest
	pkg.Decode(ioBuffer)
	if !pkg.Validate() {
		t.Errorf("this is not a SendMessageRequest")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("SendMessageRequest can not decode itself")
	}

	// binary header
	if pkg.TotalSize() != 230 {
		t.Errorf("pkg total length != 230")
	}
	if pkg.HeaderLength != 200 {
		t.Errorf("pkg header length != 200")
	}
	//if pkg.CRC != 1686134533 {
	//	t.Errorf("pkg crc32 value %d != 1686134533", pkg.CRC)
	//}
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	if pkg.Code != 2000 {
		t.Errorf("pkg code != %d", 2000)
	}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id %#x != %#x", pkg.PacketId, 0x1234)
	}

	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}
	if header.Topic != "TP_Test" {
		t.Errorf("header topic %s != TP_Test", header.Topic)
	}
	if header.Tag != "EC_Test" {
		t.Errorf("header tag %s != EC_Test", header.Tag)
	}
	if header.DelaySeconds != 1 {
		t.Errorf("header DelaySeconds %d != 1", header.DelaySeconds)
	}
	if header.ShardKey != "kHello0" {
		t.Errorf("header ShardKey %s != kHello0", header.ShardKey)
	}
	// *header.BornTime
	if header.Producer != "tProducer" {
		t.Errorf("header producer %s != tProducer", header.Producer)
	}
	pps := header.Properties
	if len(pps) != 1 {
		t.Errorf("header propertiyes length %d != 1", len(pps))
	}
	if pps["hello"] != "world" {
		t.Errorf("header propertiyes[hello]:%q != world", pps["hello"])
	}

	// message body
	msg := pkg.GetBody()
	if string(msg) != "hello" {
		t.Errorf("msg:%s != hello", string(msg))
	}
}

func TestPacket_SendMessageResponse(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x2, 0x79, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0x74, 0x9a, 0xa3, 0x75, 0x0, 0x0, 0x12, 0x34, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x71, 0x7b, 0x22, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x49, 0x64, 0x22, 0x3a, 0x22, 0x31, 0x32, 0x33, 0x22, 0x2c, 0x22, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x53, 0x69, 0x7a, 0x65, 0x22, 0x3a, 0x31, 0x30, 0x2c, 0x22, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x22, 0x3a, 0x31, 0x2c, 0x22, 0x71, 0x75, 0x65, 0x75, 0x65, 0x49, 0x64, 0x22, 0x3a, 0x31, 0x2c, 0x22, 0x72, 0x65, 0x6d, 0x61, 0x72, 0x6b, 0x22, 0x3a, 0x22, 0x72, 0x65, 0x6d, 0x61, 0x72, 0x6b, 0x22, 0x2c, 0x22, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x54, 0x69, 0x6d, 0x65, 0x22, 0x3a, 0x31, 0x30, 0x2c, 0x22, 0x74, 0x72, 0x61, 0x6e, 0x73, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x22, 0x3a, 0x22, 0x74, 0x78, 0x22, 0x7d, 0x7b, 0x22, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x73, 0x22, 0x3a, 0x5b, 0x7b, 0x22, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x22, 0x3a, 0x22, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31, 0x3a, 0x31, 0x32, 0x33, 0x34, 0x22, 0x2c, 0x22, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x22, 0x3a, 0x22, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x30, 0x22, 0x2c, 0x22, 0x67, 0x6c, 0x6f, 0x62, 0x61, 0x6c, 0x46, 0x69, 0x78, 0x65, 0x64, 0x51, 0x75, 0x65, 0x75, 0x65, 0x22, 0x3a, 0x74, 0x72, 0x75, 0x65, 0x2c, 0x22, 0x69, 0x64, 0x22, 0x3a, 0x30, 0x2c, 0x22, 0x70, 0x65, 0x72, 0x6d, 0x69, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x22, 0x3a, 0x36, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x22, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x30, 0x22, 0x7d, 0x2c, 0x7b, 0x22, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x22, 0x3a, 0x22, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x32, 0x3a, 0x31, 0x32, 0x33, 0x34, 0x22, 0x2c, 0x22, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x22, 0x3a, 0x22, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x31, 0x22, 0x2c, 0x22, 0x67, 0x6c, 0x6f, 0x62, 0x61, 0x6c, 0x46, 0x69, 0x78, 0x65, 0x64, 0x51, 0x75, 0x65, 0x75, 0x65, 0x22, 0x3a, 0x74, 0x72, 0x75, 0x65, 0x2c, 0x22, 0x69, 0x64, 0x22, 0x3a, 0x31, 0x2c, 0x22, 0x70, 0x65, 0x72, 0x6d, 0x69, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x22, 0x3a, 0x36, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x22, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x31, 0x22, 0x7d, 0x5d, 0x2c, 0x22, 0x72, 0x65, 0x61, 0x64, 0x61, 0x62, 0x6c, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x73, 0x22, 0x3a, 0x5b, 0x7b, 0x22, 0x24, 0x72, 0x65, 0x66, 0x22, 0x3a, 0x22, 0x24, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x73, 0x5b, 0x30, 0x5d, 0x22, 0x7d, 0x2c, 0x7b, 0x22, 0x24, 0x72, 0x65, 0x66, 0x22, 0x3a, 0x22, 0x24, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x73, 0x5b, 0x31, 0x5d, 0x22, 0x7d, 0x5d, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x7b, 0x22, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x22, 0x3a, 0x22, 0x54, 0x5f, 0x43, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x22, 0x2c, 0x22, 0x66, 0x69, 0x78, 0x65, 0x64, 0x51, 0x75, 0x65, 0x75, 0x65, 0x22, 0x3a, 0x74, 0x72, 0x75, 0x65, 0x2c, 0x22, 0x69, 0x64, 0x22, 0x3a, 0x31, 0x2c, 0x22, 0x6e, 0x61, 0x6d, 0x65, 0x22, 0x3a, 0x22, 0x54, 0x50, 0x5f, 0x54, 0x45, 0x53, 0x54, 0x22, 0x2c, 0x22, 0x70, 0x65, 0x72, 0x6d, 0x22, 0x3a, 0x30, 0x7d, 0x2c, 0x22, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x22, 0x3a, 0x31, 0x32, 0x2c, 0x22, 0x77, 0x72, 0x69, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x73, 0x22, 0x3a, 0x5b, 0x7b, 0x22, 0x24, 0x72, 0x65, 0x66, 0x22, 0x3a, 0x22, 0x24, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x73, 0x5b, 0x30, 0x5d, 0x22, 0x7d, 0x2c, 0x7b, 0x22, 0x24, 0x72, 0x65, 0x66, 0x22, 0x3a, 0x22, 0x24, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x73, 0x5b, 0x31, 0x5d, 0x22, 0x7d, 0x5d, 0x7d}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg SendMessageResponse
	pkg.Decode(ioBuffer)
	if !pkg.Validate() {
		t.Errorf("this is not a TimeoutResponse")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("SendMessageRequest can not decode itself")
	}

	// binary header
	if pkg.TotalSize() != 633 {
		t.Errorf("pkg total length != 633")
	}
	if pkg.HeaderLength != 113 {
		t.Errorf("pkg header length != 113")
	}
	if pkg.CRC != 1956291445 {
		t.Errorf("pkg crc32 value %d != 1956291445", pkg.CRC)
	}
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	if pkg.Code != 0 {
		t.Errorf("pkg code != 0")
	}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id %#x != %#x", pkg.PacketId, 0x1234)
	}
	if pkg.Flag != 1 {
		t.Errorf("pkg flag %#x != 1", pkg.Flag)
	}

	// header
	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}
	if header.QueueId != 1 {
		t.Errorf("header queue id %d != 1", header.QueueId)
	}
	if header.StoreTime != 10 {
		t.Errorf("header store time %d != 10", header.StoreTime)
	}
	if header.Remark != "remark" {
		t.Errorf("header remark %s != 'remark'", header.Remark)
	}
	if header.Offset != 1 {
		t.Errorf("header offset %d != 1", header.Offset)
	}
	if header.MessageId != "123" {
		t.Errorf("header message id %s != '123'", header.MessageId)
	}
	if header.MessageSize != 10 {
		t.Errorf("header message size %d != 10", header.MessageSize)
	}
	if header.TransactionId != "tx" {
		t.Errorf("header transaction id %s != 'tx'", header.TransactionId)
	}

	// body
	meta, err := pkg.GetTopicMetadata()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}

	if meta.Version != 12 {
		t.Errorf("topic.Version %d != 12", meta.Version)
	}

	topic := meta.Topic
	if topic.Id != 1 {
		t.Errorf("topic.Id %d != 1", topic.Id)
	}
	if topic.Name != "TP_TEST" {
		t.Errorf("topic.Name %s != 'TP_TEST'", topic.Name)
	}
	if !topic.FixedQueue {
		t.Errorf("topic.FixedQueue %v != true", topic.FixedQueue)
	}
	if topic.Cluster != "T_Cluster" {
		t.Errorf("topic.Cluster %v != 'T_Cluster'", topic.Cluster)
	}
	if topic.Perm != PermName_PERM_DEFAULT {
		t.Errorf("topic.Perm %v != PermName_PERM_READ_AND_WRITE", topic.Perm)
	}

	mqs := meta.MessageQueues
	if len(mqs) != 2 {
		t.Errorf("meta.GetMessageQueues() length %d != 2", len(mqs))
	}
	if mqs[0].Topic != "Topic0" {
		t.Errorf("mqs[0].Topic %s != 'Topic0'", mqs[0].Topic)
	}
	if mqs[0].Id != 0 {
		t.Errorf("mqs[0].Id %d != 0", mqs[0].Id)
	}
	if mqs[0].Address != "127.0.0.1:1234" {
		t.Errorf("mqs[0].Address %s != '127.0.0.1:1234'", mqs[0].Address)
	}
	if mqs[0].Broker != "broker0" {
		t.Errorf("mqs[0].Broker %s != 'broker0'", mqs[0].Broker)
	}

	if mqs[1].Topic != "Topic1" {
		t.Errorf("mqs[1].Topic %s != 'Topic1'", mqs[1].Topic)
	}
	if mqs[1].Id != 1 {
		t.Errorf("mqs[1].Id %d != 1", mqs[1].Id)
	}
	if mqs[1].Address != "127.0.0.2:1234" {
		t.Errorf("mqs[1].Address %s != '127.0.0.2:1234'", mqs[1].Address)
	}
	if mqs[1].Broker != "broker1" {
		t.Errorf("mqs[1].Broker %s != 'broker1'", mqs[1].Broker)
	}
}

// com/alipay/mq/remoting/bolt/BoltClient.java:243
func TestPacket_ProduceMessageTimeout(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x0, 0x39, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0xa9, 0x1e, 0xe3, 0x9b, 0x0, 0x0, 0x12, 0x34, 0x0, 0x1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x20, 0x7b, 0x22, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x22, 0x3a, 0x22, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x3a, 0x20, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31, 0x22, 0x7d}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg TimeoutResponse
	pkg.Decode(ioBuffer)
	if !pkg.Validate() {
		t.Errorf("this is not a TimeoutResponse")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("SendMessageRequest can not decode itself")
	}

	// binary header
	if pkg.TotalSize() != 57 {
		t.Errorf("pkg total length != 57")
	}
	if pkg.HeaderLength != 32 {
		t.Errorf("pkg header length != 32")
	}
	if pkg.CRC != -1457593445 {
		t.Errorf("pkg crc32 value %d != -1457593445", pkg.CRC)
	}
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	if pkg.Code != 1 {
		t.Errorf("pkg code != 1")
	}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id %#x != %#x", pkg.PacketId, 0x1234)
	}
	if pkg.Flag != 1 {
		t.Errorf("pkg flag %#x != 1", pkg.Flag)
	}

	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}
	if header.Message != "address: 127.0.0.1" {
		t.Errorf("header message %s != 'address: 127.0.0.1'", header.Message)
	}
}

// com/alipay/mq/remoting/bolt/BoltClient.java:229
// com/alipay/mq/remoting/bolt/BoltClient.java:237
func TestPacket_ProduceMessageSendFailed(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x0, 0x48, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0xfe, 0x18, 0x69, 0x2e, 0x0, 0x0, 0x12, 0x34, 0x0, 0x64, 0x0, 0x0, 0x0, 0x1, 0x0, 0x2f, 0x7b, 0x22, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x22, 0x3a, 0x22, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x3a, 0x20, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31, 0x2c, 0x20, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x3a, 0x20, 0x66, 0x61, 0x69, 0x6c, 0x22, 0x7d}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg SendFailedResponse
	pkg.Decode(ioBuffer)
	if !pkg.Validate() {
		t.Errorf("this is not a SendFailedResponse")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("SendMessageRequest can not decode itself")
	}

	// binary header
	if pkg.TotalSize() != 72 {
		t.Errorf("pkg total length != 72")
	}
	if pkg.HeaderLength != 47 {
		t.Errorf("pkg header length != 47")
	}
	if pkg.CRC != -31954642 {
		t.Errorf("pkg crc32 value %d != -31954642", pkg.CRC)
	}
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	if pkg.Code != 100 {
		t.Errorf("pkg code != 100")
	}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id %#x != %#x", pkg.PacketId, 0x1234)
	}
	if pkg.Flag != 1 {
		t.Errorf("pkg flag %#x != 1", pkg.Flag)
	}

	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}
	if header.Message != "address: 127.0.0.1, message: fail" {
		t.Errorf("header message %s != 'address: 127.0.0.1, message: fail'", header.Message)
	}
}

func TestPacket_ProduceMessageConnectionClosed(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x0, 0x48, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0xa6, 0x1a, 0xc7, 0xda, 0x0, 0x0, 0x12, 0x34, 0x0, 0x2, 0x0, 0x0, 0x0, 0x1, 0x0, 0x2f, 0x7b, 0x22, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x22, 0x3a, 0x22, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x3a, 0x20, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31, 0x2c, 0x20, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x3a, 0x20, 0x66, 0x61, 0x69, 0x6c, 0x22, 0x7d}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg ConnectionClosedResponse
	pkg.Decode(ioBuffer)
	if !pkg.Validate() {
		t.Errorf("this is not a ConnectionClosedResponse")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("SendMessageRequest can not decode itself")
	}

	// binary header
	if pkg.TotalSize() != 72 {
		t.Errorf("pkg total length != 72")
	}
	if pkg.HeaderLength != 47 {
		t.Errorf("pkg header length != 47")
	}
	if pkg.CRC != -1508194342 {
		t.Errorf("pkg crc32 value %d != -1508194342", pkg.CRC)
	}
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	if pkg.Code != 2 {
		t.Errorf("pkg code != 2")
	}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id %#x != %#x", pkg.PacketId, 0x1234)
	}
	if pkg.Flag != 1 {
		t.Errorf("pkg flag %#x != 1", pkg.Flag)
	}

	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}
	if header.Message != "address: 127.0.0.1, message: fail" {
		t.Errorf("header message %s != 'address: 127.0.0.1, message: fail'", header.Message)
	}
}

func TestPacket_ProduceBatchMessageRequest(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x1, 0x1, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0xfc, 0x7c, 0x1f, 0xf1, 0x0, 0x0, 0x12, 0x34, 0x7, 0xd4, 0x0, 0x0, 0x0, 0x0, 0x0, 0xdf, 0x7b, 0x22, 0x62, 0x61, 0x74, 0x63, 0x68, 0x22, 0x3a, 0x74, 0x72, 0x75, 0x65, 0x2c, 0x22, 0x62, 0x61, 0x74, 0x63, 0x68, 0x53, 0x69, 0x7a, 0x65, 0x22, 0x3a, 0x31, 0x2c, 0x22, 0x62, 0x6f, 0x72, 0x6e, 0x54, 0x69, 0x6d, 0x65, 0x22, 0x3a, 0x31, 0x35, 0x36, 0x31, 0x39, 0x36, 0x39, 0x32, 0x32, 0x34, 0x31, 0x38, 0x37, 0x2c, 0x22, 0x64, 0x65, 0x6c, 0x61, 0x79, 0x53, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x73, 0x22, 0x3a, 0x31, 0x2c, 0x22, 0x70, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65, 0x72, 0x22, 0x3a, 0x22, 0x74, 0x50, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65, 0x72, 0x22, 0x2c, 0x22, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x4d, 0x61, 0x70, 0x22, 0x3a, 0x5b, 0x7b, 0x22, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x22, 0x3a, 0x22, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x22, 0x7d, 0x5d, 0x2c, 0x22, 0x71, 0x75, 0x65, 0x75, 0x65, 0x49, 0x64, 0x22, 0x3a, 0x33, 0x2c, 0x22, 0x73, 0x68, 0x61, 0x72, 0x64, 0x4b, 0x65, 0x79, 0x22, 0x3a, 0x22, 0x6b, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x30, 0x22, 0x2c, 0x22, 0x73, 0x79, 0x73, 0x74, 0x65, 0x6d, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x4d, 0x61, 0x70, 0x22, 0x3a, 0x5b, 0x7b, 0x7d, 0x5d, 0x2c, 0x22, 0x74, 0x61, 0x67, 0x22, 0x3a, 0x22, 0x45, 0x43, 0x5f, 0x54, 0x65, 0x73, 0x74, 0x22, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x22, 0x54, 0x50, 0x5f, 0x54, 0x65, 0x73, 0x74, 0x22, 0x7d, 0x0, 0x0, 0x0, 0x5, 0x68, 0x65, 0x6c, 0x6c, 0x6f}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg SendBatchMessageRequest
	pkg.Decode(ioBuffer)
	if !pkg.Validate() {
		t.Errorf("this is not a SendMessageRequest")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("SendMessageRequest can not decode itself")
	}

	// binary header
	if pkg.TotalSize() != 257 {
		t.Errorf("pkg total length != 257")
	}
	if pkg.HeaderLength != 223 {
		t.Errorf("pkg header length != 223")
	}
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	if pkg.Code != 2004 {
		t.Errorf("pkg code != %d", 2004)
	}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id %#x != %#x", pkg.PacketId, 0x1234)
	}

	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}
	t.Logf("BatchMessageRequest header:%#v", header)
	if header.Topic != "TP_Test" {
		t.Errorf("header topic %s != TP_Test", header.Topic)
	}
	if header.Tag != "EC_Test" {
		t.Errorf("header tag %s != EC_Test", header.Tag)
	}
	if header.DelaySeconds != 1 {
		t.Errorf("header DelaySeconds %d != 1", header.DelaySeconds)
	}
	if header.ShardKey != "kHello0" {
		t.Errorf("header ShardKey %s != kHello0", header.ShardKey)
	}
	// *header.BornTime
	if header.Producer != "tProducer" {
		t.Errorf("header producer %s != tProducer", header.Producer)
	}
	pps := header.PropertiesMap
	if len(pps) != 1 {
		t.Errorf("header propertiyes length %d != 1", len(pps))
	}
	if pps[0]["hello"] != "world" {
		t.Errorf("header propertiyes[hello]:%q != world", pps[0]["hello"])
	}

	// message body
	msg := pkg.GetBody()
	if len(msg) <= len("hello") {
		t.Errorf("msg length %d <= len('hello')", len(msg))
	}
	t.Logf("BatchMessageRequest body:%#v", msg)
}

func TestPacket_SendBatchMessageResponse(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x2, 0x93, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0x6d, 0x8a, 0x43, 0x6, 0x0, 0x0, 0x12, 0x34, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x8b, 0x7b, 0x22, 0x62, 0x61, 0x74, 0x63, 0x68, 0x53, 0x69, 0x7a, 0x65, 0x22, 0x3a, 0x30, 0x2c, 0x22, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x49, 0x64, 0x73, 0x22, 0x3a, 0x5b, 0x22, 0x31, 0x32, 0x33, 0x22, 0x5d, 0x2c, 0x22, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x73, 0x22, 0x3a, 0x5b, 0x31, 0x32, 0x33, 0x5d, 0x2c, 0x22, 0x71, 0x75, 0x65, 0x75, 0x65, 0x49, 0x64, 0x22, 0x3a, 0x31, 0x2c, 0x22, 0x72, 0x65, 0x6d, 0x61, 0x72, 0x6b, 0x22, 0x3a, 0x22, 0x72, 0x65, 0x6d, 0x61, 0x72, 0x6b, 0x22, 0x2c, 0x22, 0x73, 0x74, 0x61, 0x74, 0x65, 0x22, 0x3a, 0x22, 0x53, 0x55, 0x43, 0x43, 0x45, 0x53, 0x53, 0x22, 0x2c, 0x22, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x54, 0x69, 0x6d, 0x65, 0x22, 0x3a, 0x31, 0x30, 0x2c, 0x22, 0x74, 0x72, 0x61, 0x6e, 0x73, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x73, 0x22, 0x3a, 0x5b, 0x22, 0x74, 0x78, 0x22, 0x5d, 0x7d, 0x7b, 0x22, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x73, 0x22, 0x3a, 0x5b, 0x7b, 0x22, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x22, 0x3a, 0x22, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31, 0x3a, 0x31, 0x32, 0x33, 0x34, 0x22, 0x2c, 0x22, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x22, 0x3a, 0x22, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x30, 0x22, 0x2c, 0x22, 0x67, 0x6c, 0x6f, 0x62, 0x61, 0x6c, 0x46, 0x69, 0x78, 0x65, 0x64, 0x51, 0x75, 0x65, 0x75, 0x65, 0x22, 0x3a, 0x74, 0x72, 0x75, 0x65, 0x2c, 0x22, 0x69, 0x64, 0x22, 0x3a, 0x30, 0x2c, 0x22, 0x70, 0x65, 0x72, 0x6d, 0x69, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x22, 0x3a, 0x36, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x22, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x30, 0x22, 0x7d, 0x2c, 0x7b, 0x22, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x22, 0x3a, 0x22, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x32, 0x3a, 0x31, 0x32, 0x33, 0x34, 0x22, 0x2c, 0x22, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x22, 0x3a, 0x22, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x31, 0x22, 0x2c, 0x22, 0x67, 0x6c, 0x6f, 0x62, 0x61, 0x6c, 0x46, 0x69, 0x78, 0x65, 0x64, 0x51, 0x75, 0x65, 0x75, 0x65, 0x22, 0x3a, 0x74, 0x72, 0x75, 0x65, 0x2c, 0x22, 0x69, 0x64, 0x22, 0x3a, 0x31, 0x2c, 0x22, 0x70, 0x65, 0x72, 0x6d, 0x69, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x22, 0x3a, 0x36, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x22, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x31, 0x22, 0x7d, 0x5d, 0x2c, 0x22, 0x72, 0x65, 0x61, 0x64, 0x61, 0x62, 0x6c, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x73, 0x22, 0x3a, 0x5b, 0x7b, 0x22, 0x24, 0x72, 0x65, 0x66, 0x22, 0x3a, 0x22, 0x24, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x73, 0x5b, 0x30, 0x5d, 0x22, 0x7d, 0x2c, 0x7b, 0x22, 0x24, 0x72, 0x65, 0x66, 0x22, 0x3a, 0x22, 0x24, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x73, 0x5b, 0x31, 0x5d, 0x22, 0x7d, 0x5d, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x7b, 0x22, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x22, 0x3a, 0x22, 0x54, 0x5f, 0x43, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x22, 0x2c, 0x22, 0x66, 0x69, 0x78, 0x65, 0x64, 0x51, 0x75, 0x65, 0x75, 0x65, 0x22, 0x3a, 0x74, 0x72, 0x75, 0x65, 0x2c, 0x22, 0x69, 0x64, 0x22, 0x3a, 0x31, 0x2c, 0x22, 0x6e, 0x61, 0x6d, 0x65, 0x22, 0x3a, 0x22, 0x54, 0x50, 0x5f, 0x54, 0x45, 0x53, 0x54, 0x22, 0x2c, 0x22, 0x70, 0x65, 0x72, 0x6d, 0x22, 0x3a, 0x30, 0x7d, 0x2c, 0x22, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x22, 0x3a, 0x31, 0x32, 0x2c, 0x22, 0x77, 0x72, 0x69, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x73, 0x22, 0x3a, 0x5b, 0x7b, 0x22, 0x24, 0x72, 0x65, 0x66, 0x22, 0x3a, 0x22, 0x24, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x73, 0x5b, 0x30, 0x5d, 0x22, 0x7d, 0x2c, 0x7b, 0x22, 0x24, 0x72, 0x65, 0x66, 0x22, 0x3a, 0x22, 0x24, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x73, 0x5b, 0x31, 0x5d, 0x22, 0x7d, 0x5d, 0x7d}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg SendBatchMessageResponse
	pkg.Decode(ioBuffer)
	if !pkg.Validate() {
		t.Errorf("this is not a SendBatchMessageResponse")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("SendMessageRequest can not decode itself")
	}

	// binary header
	if pkg.TotalSize() != 659 {
		t.Errorf("pkg total length != 659")
	}
	if pkg.HeaderLength != 139 {
		t.Errorf("pkg header length != 139")
	}
	//if pkg.CRC != 1956291445 {
	//	t.Errorf("pkg crc32 value %d != 1956291445", pkg.CRC)
	//}
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	if pkg.Code != 0 {
		t.Errorf("pkg code != 0")
	}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id %#x != %#x", pkg.PacketId, 0x1234)
	}
	if pkg.Flag != 1 {
		t.Errorf("pkg flag %#x != 1", pkg.Flag)
	}

	// header
	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}
	if header.QueueId != 1 {
		t.Errorf("header queue id %d != 1", header.QueueId)
	}
	if header.StoreTime != 10 {
		t.Errorf("header store time %d != 10", header.StoreTime)
	}
	if header.Remark != "remark" {
		t.Errorf("header remark %s != 'remark'", header.Remark)
	}
	offsets := header.Offsets
	if len(offsets) != 1 {
		t.Errorf("header offsets length %d != 1", len(offsets))
	}
	if offsets[0] != 123 {
		t.Errorf("header offsets[0] %d != 1", offsets[0])
	}
	messageIds := header.MessageIds
	if len(messageIds) != 1 {
		t.Errorf("header messageIds length %d != 1", len(messageIds))
	}
	if messageIds[0] != "123" {
		t.Errorf("header offsets[0] %s != '123'", messageIds[0])
	}
	txIds := header.TransactionIds
	if len(txIds) != 1 {
		t.Errorf("header txIds length %d != 1", len(txIds))
	}
	if txIds[0] != "tx" {
		t.Errorf("header txIds[0] %s != 'tx'", txIds[0])
	}

	// body
	meta, err := pkg.GetMetadata()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}
	if meta.Version != 12 {
		t.Errorf("topic.Version %d != 12", meta.Version)
	}
	topic := meta.Topic
	if topic.Id != 1 {
		t.Errorf("topic.Id %d != 1", topic.Id)
	}
	if topic.Name != "TP_TEST" {
		t.Errorf("topic.Name %s != 'TP_TEST'", topic.Name)
	}
	if !topic.FixedQueue {
		t.Errorf("topic.FixedQueue %v != true", topic.FixedQueue)
	}
	if topic.Cluster != "T_Cluster" {
		t.Errorf("topic.Cluster %v != 'T_Cluster'", topic.Cluster)
	}
	if topic.Perm != PermName_PERM_DEFAULT {
		t.Errorf("topic.Perm %v != PermName_PERM_READ_AND_WRITE", topic.Perm)
	}

	mqs := meta.MessageQueues
	if len(mqs) != 2 {
		t.Errorf("meta.GetMessageQueues() length %d != 2", len(mqs))
	}
	if mqs[0].Topic != "Topic0" {
		t.Errorf("mqs[0].Topic %s != 'Topic0'", mqs[0].Topic)
	}
	if mqs[0].Id != 0 {
		t.Errorf("mqs[0].Id %d != 0", mqs[0].Id)
	}
	if mqs[0].Address != "127.0.0.1:1234" {
		t.Errorf("mqs[0].Address %s != '127.0.0.1:1234'", mqs[0].Address)
	}
	if mqs[0].Broker != "broker0" {
		t.Errorf("mqs[0].Broker %s != 'broker0'", mqs[0].Broker)
	}

	if mqs[1].Topic != "Topic1" {
		t.Errorf("mqs[1].Topic %s != 'Topic1'", mqs[1].Topic)
	}
	if mqs[1].Id != 1 {
		t.Errorf("mqs[1].Id %d != 1", mqs[1].Id)
	}
	if mqs[1].Address != "127.0.0.2:1234" {
		t.Errorf("mqs[1].Address %s != '127.0.0.2:1234'", mqs[1].Address)
	}
	if mqs[1].Broker != "broker1" {
		t.Errorf("mqs[1].Broker %s != 'broker1'", mqs[1].Broker)
	}
}

func TestPacket_FetchMessageRequest(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x0, 0x90, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0x9c, 0xcf, 0xb9, 0x78, 0x0, 0x0, 0x12, 0x34, 0xb, 0xb8, 0x0, 0x0, 0x0, 0x0, 0x0, 0x77, 0x7b, 0x22, 0x62, 0x61, 0x74, 0x63, 0x68, 0x22, 0x3a, 0x31, 0x2c, 0x22, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x22, 0x3a, 0x32, 0x2c, 0x22, 0x64, 0x69, 0x73, 0x61, 0x62, 0x6c, 0x65, 0x4c, 0x6f, 0x6e, 0x67, 0x50, 0x6f, 0x6c, 0x6c, 0x69, 0x6e, 0x67, 0x22, 0x3a, 0x66, 0x61, 0x6c, 0x73, 0x65, 0x2c, 0x22, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x22, 0x3a, 0x22, 0x47, 0x5f, 0x54, 0x65, 0x73, 0x74, 0x22, 0x2c, 0x22, 0x71, 0x75, 0x65, 0x75, 0x65, 0x22, 0x3a, 0x31, 0x2c, 0x22, 0x73, 0x74, 0x61, 0x72, 0x74, 0x22, 0x3a, 0x34, 0x2c, 0x22, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x22, 0x3a, 0x31, 0x30, 0x30, 0x30, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x22, 0x54, 0x50, 0x5f, 0x54, 0x65, 0x73, 0x74, 0x22, 0x7d}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg FetchMessageRequest
	pkg.Decode(ioBuffer)
	if !pkg.Validate() {
		t.Errorf("this is not a FetchMessageRequest")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("FetchMessageRequest can not decode itself")
	}

	// binary header
	if pkg.TotalSize() != 144 {
		t.Errorf("pkg total length != 144")
	}
	if pkg.HeaderLength != 119 {
		t.Errorf("pkg header length != 119")
	}
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	if pkg.Code != 3000 {
		t.Errorf("pkg code != %d", 3000)
	}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id %#x != %#x", pkg.PacketId, 0x1234)
	}

	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}
	if header.Topic != "TP_Test" {
		t.Errorf("header topic %s != TP_Test", header.Topic)
	}
	if header.Queue != 1 {
		t.Errorf("header queue %d != 1", header.Queue)
	}
	if header.Group != "G_Test" {
		t.Errorf("header group %s != 'G_Test'", header.Group)
	}
	if header.Start != 4 {
		t.Errorf("header start %d != 4", header.Start)
	}
	if header.Commit != 2 {
		t.Errorf("header commit %d != 2", header.Commit)
	}
	if header.Batch != 1 {
		t.Errorf("header batch %d != 1", header.Batch)
	}
	if header.DisableLongPolling {
		t.Errorf("header disableLongPolling %v != false", header.DisableLongPolling)
	}
	if header.Timeout != 1000 {
		t.Errorf("header timeout %d != 1000", header.Timeout)
	}
}

func TestPacket_FetchMessasgeNoNewMessage(t *testing.T) {
	rs := NewResponse(SUCCESS, 123)
	var fetchMessageRsp FetchMessageResponse
	fetchMessageRsp.Packet = *rs

	rqHeader := ReadMessageResponseHeader{}
	rqHeader.State = ReadMessageResponseHeader_NO_NEW_MESSAGE
	fetchMessageRsp.Rebuild(&rqHeader)
	t.Logf("no new message response:%+v\n", fetchMessageRsp)
}

func TestPacket_ConsumerReadMessageResponse(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x0, 0x69, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0x3e, 0xbf, 0x1c, 0xad, 0x0, 0x0, 0x12, 0x34, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x50, 0x7b, 0x22, 0x6d, 0x61, 0x78, 0x4f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x22, 0x3a, 0x31, 0x35, 0x2c, 0x22, 0x6d, 0x69, 0x6e, 0x4f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x22, 0x3a, 0x31, 0x2c, 0x22, 0x6e, 0x65, 0x78, 0x74, 0x4f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x22, 0x3a, 0x32, 0x2c, 0x22, 0x72, 0x65, 0x6d, 0x61, 0x72, 0x6b, 0x22, 0x3a, 0x22, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x22, 0x2c, 0x22, 0x73, 0x74, 0x61, 0x74, 0x65, 0x22, 0x3a, 0x22, 0x53, 0x55, 0x43, 0x43, 0x45, 0x53, 0x53, 0x22, 0x7d}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg FetchMessageResponse
	pkg.Decode(ioBuffer)
	if !pkg.Validate() {
		t.Errorf("this is not a FetchMessageResponse")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("FetchMessageResponse can not decode itself")
	}

	// binary header
	//if pkg.TotalSize() != 659 {
	//	t.Errorf("pkg total length != 659")
	//}
	//if pkg.HeaderLength != 139 {
	//	t.Errorf("pkg header length != 139")
	//}
	//if pkg.CRC != 1956291445 {
	//	t.Errorf("pkg crc32 value %d != 1956291445", pkg.CRC)
	//}
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	//if pkg.Code != 0 {
	//	t.Errorf("pkg code != 0")
	//}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id %#x != %#x", pkg.PacketId, 0x1234)
	}
	//if pkg.Flag != 1 {
	//	t.Errorf("pkg flag %#x != 1", pkg.Flag)
	//}

	// header
	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}
	if header.State != ReadMessageResponseHeader_SUCCESS {
		t.Errorf("header queue id %d != ReadMessageResponseHeader_SUCCESS", header.State)
	}
	if header.NextOffset != 2 {
		t.Errorf("header next offset %d != 2", header.NextOffset)
	}
	if header.MinOffset != 1 {
		t.Errorf("header min offset %d != 1", header.MinOffset)
	}
	if header.MaxOffset != 15 {
		t.Errorf("header max offset %d != 15", header.MaxOffset)
	}
	if header.Remark != "hello" {
		t.Errorf("header remark %s != 'hello'", header.Remark)
	}
}

func TestPacket_QueryOffsetRequest(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x0, 0x4d, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0x59, 0x16, 0x24, 0xbe, 0x0, 0x0, 0x12, 0x34, 0x3, 0xed, 0x0, 0x0, 0x0, 0x0, 0x0, 0x34, 0x7b, 0x22, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x22, 0x3a, 0x22, 0x50, 0x75, 0x62, 0x5f, 0x53, 0x5f, 0x54, 0x65, 0x73, 0x74, 0x22, 0x2c, 0x22, 0x71, 0x75, 0x65, 0x75, 0x65, 0x49, 0x64, 0x22, 0x3a, 0x33, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x22, 0x54, 0x50, 0x5f, 0x54, 0x65, 0x73, 0x74, 0x22, 0x7d}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg QueryOffsetRequest
	pkg.Decode(ioBuffer)
	if !pkg.Validate() {
		t.Errorf("this is not a QueryOffsetRequest")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("QueryOffsetRequest can not decode itself")
	}

	//// binary header
	//if pkg.TotalSize() != 144 {
	//	t.Errorf("pkg total length != 144")
	//}
	//if pkg.HeaderLength != 119 {
	//	t.Errorf("pkg header length != 119")
	//}
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	//if pkg.Code != 3000 {
	//	t.Errorf("pkg code != %d", 3000)
	//}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id %#x != %#x", pkg.PacketId, 0x1234)
	}

	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}
	if header.Topic != "TP_Test" {
		t.Errorf("header topic %s != TP_Test", header.Topic)
	}
	if header.QueueId != 3 {
		t.Errorf("header query id %d != 1", header.QueueId)
	}
	if header.Group != "Pub_S_Test" {
		t.Errorf("header group %s != 'Pub_S_Test'", header.Group)
	}
}

func TestPacket_QueryOffsetResponse(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x0, 0x5a, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0xf8, 0x77, 0x50, 0x5f, 0x0, 0x0, 0x12, 0x34, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x41, 0x7b, 0x22, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x22, 0x3a, 0x22, 0x53, 0x75, 0x62, 0x5f, 0x47, 0x72, 0x6f, 0x75, 0x70, 0x22, 0x2c, 0x22, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x22, 0x3a, 0x31, 0x30, 0x30, 0x30, 0x2c, 0x22, 0x71, 0x75, 0x65, 0x75, 0x65, 0x49, 0x64, 0x22, 0x3a, 0x33, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x22, 0x54, 0x50, 0x5f, 0x54, 0x65, 0x73, 0x74, 0x22, 0x7d}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg QueryOffsetResponse
	pkg.Decode(ioBuffer)
	if !pkg.Validate() {
		t.Errorf("this is not a QueryOffsetResponse")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("QueryOffsetResponse can not decode itself")
	}

	// binary header
	//if pkg.TotalSize() != 659 {
	//	t.Errorf("pkg total length != 659")
	//}
	//if pkg.HeaderLength != 139 {
	//	t.Errorf("pkg header length != 139")
	//}
	//if pkg.CRC != 1956291445 {
	//	t.Errorf("pkg crc32 value %d != 1956291445", pkg.CRC)
	//}
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	//if pkg.Code != 0 {
	//	t.Errorf("pkg code != 0")
	//}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id %#x != %#x", pkg.PacketId, 0x1234)
	}
	//if pkg.Flag != 1 {
	//	t.Errorf("pkg flag %#x != 1", pkg.Flag)
	//}

	// header
	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}
	if header.Topic != "TP_Test" {
		t.Errorf("header topic %s != TP_Test", header.Topic)
	}
	if header.QueueId != 3 {
		t.Errorf("header query id %d != 1", header.QueueId)
	}
	if header.Group != "Sub_Group" {
		t.Errorf("header group %s != 'Sub_Group'", header.Group)
	}
	if header.Offset != 1000 {
		t.Errorf("header offset %d != 1000", header.Offset)
	}
}

func TestPacket_QueryOffsetRequestByTimestamp(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x0, 0x48, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0x11, 0x9f, 0x7, 0xc5, 0x0, 0x0, 0x12, 0x34, 0x3, 0xf0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2f, 0x7b, 0x22, 0x71, 0x75, 0x65, 0x75, 0x65, 0x49, 0x64, 0x22, 0x3a, 0x33, 0x2c, 0x22, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x22, 0x3a, 0x31, 0x32, 0x33, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x22, 0x54, 0x50, 0x5f, 0x54, 0x65, 0x73, 0x74, 0x22, 0x7d}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg QueryOffsetByTimestampRequest
	pkg.Decode(ioBuffer)
	if !pkg.Validate() {
		t.Errorf("this is not a QueryOffsetByTimestampRequest")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("QueryOffsetByTimestampRequest can not decode itself")
	}

	//// binary header
	//if pkg.TotalSize() != 144 {
	//	t.Errorf("pkg total length != 144")
	//}
	//if pkg.HeaderLength != 119 {
	//	t.Errorf("pkg header length != 119")
	//}
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	//if pkg.Code != 3000 {
	//	t.Errorf("pkg code != %d", 3000)
	//}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id %#x != %#x", pkg.PacketId, 0x1234)
	}

	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}
	if header.Topic != "TP_Test" {
		t.Errorf("header topic %s != TP_Test", header.Topic)
	}
	if header.QueueId != 3 {
		t.Errorf("header query id %d != 1", header.QueueId)
	}
	if header.Timestamp != 123 {
		t.Errorf("header timestamp %d != 123", header.Timestamp)
	}
}

func TestPacket_QueryOffsetByTimestampResponse(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x0, 0x46, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0x7f, 0x97, 0x7f, 0x71, 0x0, 0x0, 0x12, 0x34, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x2d, 0x7b, 0x22, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x22, 0x3a, 0x31, 0x30, 0x30, 0x30, 0x2c, 0x22, 0x71, 0x75, 0x65, 0x75, 0x65, 0x49, 0x64, 0x22, 0x3a, 0x33, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x22, 0x54, 0x50, 0x5f, 0x54, 0x65, 0x73, 0x74, 0x22, 0x7d}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg QueryOffsetByTimestampResponse
	pkg.Decode(ioBuffer)
	if !pkg.Validate() {
		t.Errorf("this is not a QueryOffsetByTimestampResponse")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("QueryOffsetByTimestampResponse can not decode itself")
	}

	// binary header
	//if pkg.TotalSize() != 659 {
	//	t.Errorf("pkg total length != 659")
	//}
	//if pkg.HeaderLength != 139 {
	//	t.Errorf("pkg header length != 139")
	//}
	//if pkg.CRC != 1956291445 {
	//	t.Errorf("pkg crc32 value %d != 1956291445", pkg.CRC)
	//}
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	//if pkg.Code != 0 {
	//	t.Errorf("pkg code != 0")
	//}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id %#x != %#x", pkg.PacketId, 0x1234)
	}
	//if pkg.Flag != 1 {
	//	t.Errorf("pkg flag %#x != 1", pkg.Flag)
	//}

	// header
	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}
	if header.Topic != "TP_Test" {
		t.Errorf("header topic %s != TP_Test", header.Topic)
	}
	if header.QueueId != 3 {
		t.Errorf("header query id %d != 1", header.QueueId)
	}
	if header.Offset != 1000 {
		t.Errorf("header offset %d != 1000", header.Offset)
	}
}

func TestPacket_PersistOffsetRequest(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x0, 0x58, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0x93, 0x98, 0x35, 0x7, 0x0, 0x0, 0x12, 0x34, 0x3, 0xee, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3f, 0x7b, 0x22, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x22, 0x3a, 0x22, 0x50, 0x5f, 0x47, 0x5f, 0x54, 0x65, 0x73, 0x74, 0x22, 0x2c, 0x22, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x22, 0x3a, 0x31, 0x32, 0x33, 0x2c, 0x22, 0x71, 0x75, 0x65, 0x75, 0x65, 0x49, 0x64, 0x22, 0x3a, 0x33, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x22, 0x54, 0x50, 0x5f, 0x54, 0x65, 0x73, 0x74, 0x22, 0x7d}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg PersistOffsetRequest
	pkg.Decode(ioBuffer)
	if !pkg.Validate() {
		t.Errorf("this is not a PersistOffsetRequest")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("PersistOffsetRequest can not decode itself")
	}

	//// binary header
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id %#x != %#x", pkg.PacketId, 0x1234)
	}

	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}
	if header.Topic != "TP_Test" {
		t.Errorf("header topic %s != TP_Test", header.Topic)
	}
	if header.Group != "P_G_Test" {
		t.Errorf("header group %s != P_G_Test", header.Topic)
	}
	if header.QueueId != 3 {
		t.Errorf("header query id %d != 1", header.QueueId)
	}
	if header.Offset != 123 {
		t.Errorf("header offset %d != 123", header.Offset)
	}
}

func TestPacket_RequestOffsetRequest(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x0, 0x45, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0xcf, 0xc, 0x80, 0x4b, 0x0, 0x0, 0x12, 0x34, 0x3, 0xec, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2c, 0x7b, 0x22, 0x71, 0x75, 0x65, 0x75, 0x65, 0x49, 0x64, 0x22, 0x3a, 0x33, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x22, 0x54, 0x50, 0x5f, 0x54, 0x65, 0x73, 0x74, 0x22, 0x2c, 0x22, 0x74, 0x79, 0x70, 0x65, 0x22, 0x3a, 0x22, 0x4d, 0x41, 0x58, 0x22, 0x7d}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg RequestOffsetRequest
	pkg.Decode(ioBuffer)
	if !pkg.Validate() {
		t.Errorf("this is not a RequestOffsetRequest")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("RequestOffsetRequest can not decode itself")
	}

	//// binary header
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id %#x != %#x", pkg.PacketId, 0x1234)
	}

	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}
	if header.Topic != "TP_Test" {
		t.Errorf("header topic %s != TP_Test", header.Topic)
	}
	if header.QueueId != 3 {
		t.Errorf("header query id %d != 1", header.QueueId)
	}
	if header.Type != GetOffsetType_MAX {
		t.Errorf("header offset %v != GetOffsetType_MAX", header.Type)
	}
}

func TestPacket_RequestOffsetResponse(t *testing.T) {
	byteStreams := []byte{0x0, 0x0, 0x0, 0x53, 0xda, 0xa3, 0x20, 0xa7, 0x1, 0x90, 0xb8, 0x77, 0xdf, 0x0, 0x0, 0x12, 0x34, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x3a, 0x7b, 0x22, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x22, 0x3a, 0x31, 0x30, 0x30, 0x30, 0x2c, 0x22, 0x71, 0x75, 0x65, 0x75, 0x65, 0x49, 0x64, 0x22, 0x3a, 0x33, 0x2c, 0x22, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x3a, 0x22, 0x54, 0x50, 0x5f, 0x54, 0x65, 0x73, 0x74, 0x22, 0x2c, 0x22, 0x74, 0x79, 0x70, 0x65, 0x22, 0x3a, 0x22, 0x4d, 0x41, 0x58, 0x22, 0x7d}

	ioBuffer := &buffer.IoBuffer{}
	ioBuffer.Write(byteStreams)

	var pkg RequestOffsetResponse
	pkg.Decode(ioBuffer)
	if !pkg.Validate() {
		t.Errorf("this is not a RequestOffsetResponse")
	}

	outBuffer := &buffer.IoBuffer{}
	pkg.Encode(outBuffer)
	var outByteStreams []byte
	outByteStreams = append(outByteStreams, outBuffer.Bytes()...)
	if !reflect.DeepEqual(byteStreams, outByteStreams) {
		t.Errorf("RequestOffsetResponse can not decode itself")
	}

	// binary header
	if pkg.Magic != MAGIC_CODE {
		t.Errorf("pkg crc32 value != %d", MAGIC_CODE)
	}
	if pkg.PacketId != 0x1234 {
		t.Errorf("pkg packet id %#x != %#x", pkg.PacketId, 0x1234)
	}

	// header
	header, err := pkg.GetHeader()
	if err != nil {
		t.Errorf("json.Unmarshal() = error %s", err)
	}
	if header.Topic != "TP_Test" {
		t.Errorf("header topic %s != TP_Test", header.Topic)
	}
	if header.QueueId != 3 {
		t.Errorf("header query id %d != 1", header.QueueId)
	}
	if header.Type != GetOffsetType_MAX {
		t.Errorf("header offset %v != GetOffsetType_MAX", header.Type)
	}
	if header.Offset != 1000 {
		t.Errorf("header offset %d != 1000", header.Offset)
	}
}

func TestTopicMetadata_GetFMQKey(t *testing.T) {
	metadata := TopicMetadata{
		Topic: Topic{
			Name:       "hello0",
			FixedQueue: false,
		},
	}

	nsKey := "hellokey"
	mq := MessageQueue{
		Id:      12,
		Address: "127.0.0.1:1234",
	}

	key := metadata.GetMQKey(nsKey, &mq)
	if key != (nsKey + string(MQKeySeparator) + "hello0#12#127.0.0.1:1234") {
		t.Errorf("key:%q != hello0#12#127.0.0.1:1234", key)
	}

	metadata.Topic.FixedQueue = true
	key = metadata.GetMQKey(nsKey, &mq)
	if key != (nsKey + string(MQKeySeparator) + "hello0#12") {
		t.Errorf("key:%q != hello0#12", key)
	}
}

func TestTopicMetadata_FeedQueue(t *testing.T) {
	meta := TopicMetadata{
		MessageQueues: []MessageQueue{
			{Id: 3, Permission: int32(PermName_PERM_READ_AND_WRITE)},
			{Id: 1, Permission: int32(PermName_PERM_READ)},
			{Id: 7, Permission: int32(PermName_PERM_WRITE)},
			{Id: 0, Permission: int32(PermName_PERM_READ)},
		},
	}

	meta.FeedQueue()

	if len(meta.WritableQueues) != 2 {
		t.Errorf("meta.WritableQueues %+v", meta.WritableQueues)
	}
	if len(meta.ReadableQueues) != 3 {
		t.Errorf("meta.ReadableQueues %+v", meta.ReadableQueues)
	}
}

func TestConnectionClosedResponse_GetHeader(t *testing.T) {
	headerData := []byte{0X7B, 0X22, 0X6D, 0X61, 0X78, 0X4F, 0X66, 0X66, 0X73, 0X65, 0X74, 0X22, 0X3A, 0X31, 0X35, 0X38, 0X30, 0X2C, 0X22, 0X6D, 0X69, 0X6E, 0X4F, 0X66, 0X66, 0X73, 0X65, 0X74, 0X22, 0X3A, 0X39, 0X38, 0X32, 0X2C, 0X22, 0X6E, 0X65, 0X78, 0X74, 0X4F, 0X66, 0X66, 0X73, 0X65, 0X74, 0X22, 0X3A, 0X31, 0X33, 0X38, 0X31, 0X2C, 0X22, 0X72, 0X65, 0X6D, 0X61, 0X72, 0X6B, 0X22, 0X3A, 0X22, 0X46, 0X4F, 0X55, 0X4E, 0X44, 0X22, 0X2C, 0X22, 0X73, 0X74, 0X61, 0X74, 0X65, 0X22, 0X3A, 0X22, 0X53, 0X55, 0X43, 0X43, 0X45, 0X53, 0X53, 0X22, 0X7D}
	var rsp FetchMessageResponse
	rsp.HeaderData = headerData
	rsp.HeaderLength = int16(len(headerData))
	header, err := rsp.GetHeader()
	if err != nil {
		t.Errorf("rsp.GetHeader() error %v", err)
	}
	t.Logf("header:%+v", header)

	bodyData := []byte{0X0, 0X0, 0X2, 0X50, 0XDA, 0XA3, 0X20, 0XA7, 0X6B, 0X48, 0X59, 0X82, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X5, 0XC5, 0X0, 0X0, 0X4, 0XE0, 0XCC, 0X86, 0X14, 0XEE, 0X10, 0X0, 0X0, 0X0, 0X0, 0X0, 0X1, 0X6C, 0XF2, 0XE2, 0X6B, 0XC5, 0XB, 0XA6, 0X46, 0XA, 0X0, 0X0, 0X25, 0X25, 0X0, 0X0, 0X0, 0X0, 0XA, 0X54, 0X50, 0X5F, 0X44, 0X53, 0X5F, 0X54, 0X45, 0X53, 0X54, 0X0, 0X7, 0X49, 0X4E, 0X44, 0X45, 0X58, 0X2, 0X31, 0X1, 0XEC, 0X54, 0X41, 0X47, 0X53, 0X2, 0X1, 0X44, 0X45, 0X4C, 0X49, 0X56, 0X45, 0X52, 0X5F, 0X54, 0X49, 0X4D, 0X45, 0X2, 0X30, 0X1, 0X73, 0X79, 0X73, 0X50, 0X65, 0X6E, 0X41, 0X74, 0X74, 0X72, 0X73, 0X2, 0X1, 0X56, 0X45, 0X52, 0X53, 0X49, 0X4F, 0X4E, 0X2, 0X31, 0X1, 0X73, 0X6F, 0X66, 0X61, 0X50, 0X65, 0X6E, 0X41, 0X74, 0X74, 0X72, 0X73, 0X2, 0X1, 0X73, 0X6F, 0X66, 0X61, 0X54, 0X72, 0X61, 0X63, 0X65, 0X49, 0X64, 0X2, 0X63, 0X30, 0X61, 0X38, 0X30, 0X62, 0X30, 0X31, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X37, 0X31, 0X30, 0X31, 0X31, 0X33, 0X30, 0X39, 0X38, 0X39, 0X37, 0X36, 0X1, 0X45, 0X56, 0X45, 0X4E, 0X54, 0X5F, 0X54, 0X49, 0X4D, 0X45, 0X2, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X37, 0X31, 0X30, 0X1, 0X61, 0X6E, 0X74, 0X71, 0X5F, 0X65, 0X6E, 0X74, 0X69, 0X74, 0X79, 0X5F, 0X70, 0X72, 0X6F, 0X70, 0X65, 0X72, 0X74, 0X79, 0X2, 0X7B, 0X22, 0X62, 0X6F, 0X72, 0X6E, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X37, 0X31, 0X30, 0X2C, 0X22, 0X63, 0X6F, 0X6D, 0X6D, 0X69, 0X74, 0X74, 0X65, 0X64, 0X22, 0X3A, 0X74, 0X72, 0X75, 0X65, 0X2C, 0X22, 0X64, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X79, 0X43, 0X6F, 0X75, 0X6E, 0X74, 0X22, 0X3A, 0X30, 0X2C, 0X22, 0X64, 0X6C, 0X71, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X2D, 0X31, 0X2C, 0X22, 0X66, 0X6C, 0X61, 0X67, 0X22, 0X3A, 0X33, 0X2C, 0X22, 0X67, 0X4D, 0X54, 0X43, 0X72, 0X65, 0X61, 0X74, 0X65, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X37, 0X31, 0X30, 0X2C, 0X22, 0X67, 0X4D, 0X54, 0X4C, 0X61, 0X73, 0X74, 0X44, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X79, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X37, 0X31, 0X30, 0X2C, 0X22, 0X67, 0X72, 0X6F, 0X75, 0X70, 0X49, 0X64, 0X22, 0X3A, 0X22, 0X53, 0X5F, 0X64, 0X6F, 0X6E, 0X67, 0X73, 0X68, 0X69, 0X5F, 0X74, 0X65, 0X73, 0X74, 0X22, 0X2C, 0X22, 0X68, 0X6F, 0X73, 0X74, 0X4E, 0X61, 0X6D, 0X65, 0X22, 0X3A, 0X22, 0X43, 0X30, 0X32, 0X58, 0X57, 0X35, 0X53, 0X4C, 0X4A, 0X48, 0X44, 0X32, 0X2E, 0X6C, 0X6F, 0X63, 0X61, 0X6C, 0X22, 0X2C, 0X22, 0X6D, 0X65, 0X73, 0X73, 0X61, 0X67, 0X65, 0X49, 0X64, 0X22, 0X3A, 0X22, 0X38, 0X36, 0X44, 0X42, 0X45, 0X32, 0X44, 0X41, 0X41, 0X37, 0X39, 0X33, 0X41, 0X35, 0X44, 0X44, 0X46, 0X44, 0X46, 0X30, 0X43, 0X37, 0X32, 0X41, 0X38, 0X43, 0X42, 0X38, 0X38, 0X42, 0X34, 0X31, 0X22, 0X2C, 0X22, 0X6D, 0X65, 0X73, 0X73, 0X61, 0X67, 0X65, 0X54, 0X79, 0X70, 0X65, 0X22, 0X3A, 0X22, 0X4D, 0X51, 0X5F, 0X44, 0X45, 0X46, 0X41, 0X55, 0X4C, 0X54, 0X5F, 0X54, 0X41, 0X47, 0X22, 0X2C, 0X22, 0X6E, 0X65, 0X78, 0X74, 0X44, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X30, 0X2C, 0X22, 0X70, 0X6F, 0X73, 0X74, 0X54, 0X69, 0X6D, 0X65, 0X4F, 0X75, 0X74, 0X22, 0X3A, 0X31, 0X30, 0X30, 0X30, 0X30, 0X2C, 0X22, 0X74, 0X69, 0X6D, 0X65, 0X54, 0X6F, 0X4C, 0X69, 0X76, 0X65, 0X22, 0X3A, 0X2D, 0X31, 0X7D, 0X1, 0X53, 0X48, 0X41, 0X52, 0X44, 0X5F, 0X4B, 0X45, 0X59, 0X2, 0X0, 0X0, 0X0, 0X12, 0X68, 0X65, 0X6C, 0X6C, 0X6F, 0X2D, 0X6D, 0X6F, 0X73, 0X6E, 0X2D, 0X64, 0X65, 0X6C, 0X61, 0X79, 0X2D, 0X31, 0X0, 0X0, 0X2, 0X50, 0XDA, 0XA3, 0X20, 0XA7, 0X1C, 0X4F, 0X69, 0X14, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X5, 0XC6, 0X0, 0X0, 0X4, 0XE0, 0XCC, 0X86, 0X86, 0XBF, 0X10, 0X0, 0X0, 0X0, 0X0, 0X0, 0X1, 0X6C, 0XF2, 0XE2, 0X6C, 0XD, 0XB, 0XA6, 0X46, 0XA, 0X0, 0X0, 0X25, 0X25, 0X0, 0X0, 0X0, 0X0, 0XA, 0X54, 0X50, 0X5F, 0X44, 0X53, 0X5F, 0X54, 0X45, 0X53, 0X54, 0X0, 0X7, 0X49, 0X4E, 0X44, 0X45, 0X58, 0X2, 0X30, 0X1, 0XEC, 0X54, 0X41, 0X47, 0X53, 0X2, 0X1, 0X44, 0X45, 0X4C, 0X49, 0X56, 0X45, 0X52, 0X5F, 0X54, 0X49, 0X4D, 0X45, 0X2, 0X30, 0X1, 0X73, 0X79, 0X73, 0X50, 0X65, 0X6E, 0X41, 0X74, 0X74, 0X72, 0X73, 0X2, 0X1, 0X56, 0X45, 0X52, 0X53, 0X49, 0X4F, 0X4E, 0X2, 0X31, 0X1, 0X73, 0X6F, 0X66, 0X61, 0X50, 0X65, 0X6E, 0X41, 0X74, 0X74, 0X72, 0X73, 0X2, 0X1, 0X73, 0X6F, 0X66, 0X61, 0X54, 0X72, 0X61, 0X63, 0X65, 0X49, 0X64, 0X2, 0X63, 0X30, 0X61, 0X38, 0X30, 0X62, 0X30, 0X31, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X37, 0X38, 0X34, 0X31, 0X31, 0X33, 0X32, 0X39, 0X38, 0X39, 0X37, 0X36, 0X1, 0X45, 0X56, 0X45, 0X4E, 0X54, 0X5F, 0X54, 0X49, 0X4D, 0X45, 0X2, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X37, 0X38, 0X34, 0X1, 0X61, 0X6E, 0X74, 0X71, 0X5F, 0X65, 0X6E, 0X74, 0X69, 0X74, 0X79, 0X5F, 0X70, 0X72, 0X6F, 0X70, 0X65, 0X72, 0X74, 0X79, 0X2, 0X7B, 0X22, 0X62, 0X6F, 0X72, 0X6E, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X37, 0X38, 0X34, 0X2C, 0X22, 0X63, 0X6F, 0X6D, 0X6D, 0X69, 0X74, 0X74, 0X65, 0X64, 0X22, 0X3A, 0X74, 0X72, 0X75, 0X65, 0X2C, 0X22, 0X64, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X79, 0X43, 0X6F, 0X75, 0X6E, 0X74, 0X22, 0X3A, 0X30, 0X2C, 0X22, 0X64, 0X6C, 0X71, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X2D, 0X31, 0X2C, 0X22, 0X66, 0X6C, 0X61, 0X67, 0X22, 0X3A, 0X33, 0X2C, 0X22, 0X67, 0X4D, 0X54, 0X43, 0X72, 0X65, 0X61, 0X74, 0X65, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X37, 0X38, 0X34, 0X2C, 0X22, 0X67, 0X4D, 0X54, 0X4C, 0X61, 0X73, 0X74, 0X44, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X79, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X37, 0X38, 0X34, 0X2C, 0X22, 0X67, 0X72, 0X6F, 0X75, 0X70, 0X49, 0X64, 0X22, 0X3A, 0X22, 0X53, 0X5F, 0X64, 0X6F, 0X6E, 0X67, 0X73, 0X68, 0X69, 0X5F, 0X74, 0X65, 0X73, 0X74, 0X22, 0X2C, 0X22, 0X68, 0X6F, 0X73, 0X74, 0X4E, 0X61, 0X6D, 0X65, 0X22, 0X3A, 0X22, 0X43, 0X30, 0X32, 0X58, 0X57, 0X35, 0X53, 0X4C, 0X4A, 0X48, 0X44, 0X32, 0X2E, 0X6C, 0X6F, 0X63, 0X61, 0X6C, 0X22, 0X2C, 0X22, 0X6D, 0X65, 0X73, 0X73, 0X61, 0X67, 0X65, 0X49, 0X64, 0X22, 0X3A, 0X22, 0X43, 0X37, 0X41, 0X42, 0X33, 0X36, 0X46, 0X31, 0X45, 0X43, 0X36, 0X43, 0X34, 0X32, 0X31, 0X31, 0X35, 0X44, 0X39, 0X39, 0X34, 0X35, 0X38, 0X34, 0X36, 0X37, 0X34, 0X43, 0X35, 0X33, 0X46, 0X39, 0X22, 0X2C, 0X22, 0X6D, 0X65, 0X73, 0X73, 0X61, 0X67, 0X65, 0X54, 0X79, 0X70, 0X65, 0X22, 0X3A, 0X22, 0X4D, 0X51, 0X5F, 0X44, 0X45, 0X46, 0X41, 0X55, 0X4C, 0X54, 0X5F, 0X54, 0X41, 0X47, 0X22, 0X2C, 0X22, 0X6E, 0X65, 0X78, 0X74, 0X44, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X30, 0X2C, 0X22, 0X70, 0X6F, 0X73, 0X74, 0X54, 0X69, 0X6D, 0X65, 0X4F, 0X75, 0X74, 0X22, 0X3A, 0X31, 0X30, 0X30, 0X30, 0X30, 0X2C, 0X22, 0X74, 0X69, 0X6D, 0X65, 0X54, 0X6F, 0X4C, 0X69, 0X76, 0X65, 0X22, 0X3A, 0X2D, 0X31, 0X7D, 0X1, 0X53, 0X48, 0X41, 0X52, 0X44, 0X5F, 0X4B, 0X45, 0X59, 0X2, 0X0, 0X0, 0X0, 0X12, 0X68, 0X65, 0X6C, 0X6C, 0X6F, 0X2D, 0X6D, 0X6F, 0X73, 0X6E, 0X2D, 0X64, 0X65, 0X6C, 0X61, 0X79, 0X2D, 0X30, 0X0, 0X0, 0X2, 0X50, 0XDA, 0XA3, 0X20, 0XA7, 0X6B, 0X48, 0X59, 0X82, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X5, 0XC7, 0X0, 0X0, 0X4, 0XE0, 0XCC, 0X86, 0X89, 0XF, 0X10, 0X0, 0X0, 0X0, 0X0, 0X0, 0X1, 0X6C, 0XF2, 0XE2, 0X6C, 0XD, 0XB, 0XA6, 0X46, 0XA, 0X0, 0X0, 0X25, 0X25, 0X0, 0X0, 0X0, 0X0, 0XA, 0X54, 0X50, 0X5F, 0X44, 0X53, 0X5F, 0X54, 0X45, 0X53, 0X54, 0X0, 0X7, 0X49, 0X4E, 0X44, 0X45, 0X58, 0X2, 0X31, 0X1, 0XEC, 0X54, 0X41, 0X47, 0X53, 0X2, 0X1, 0X44, 0X45, 0X4C, 0X49, 0X56, 0X45, 0X52, 0X5F, 0X54, 0X49, 0X4D, 0X45, 0X2, 0X30, 0X1, 0X73, 0X79, 0X73, 0X50, 0X65, 0X6E, 0X41, 0X74, 0X74, 0X72, 0X73, 0X2, 0X1, 0X56, 0X45, 0X52, 0X53, 0X49, 0X4F, 0X4E, 0X2, 0X31, 0X1, 0X73, 0X6F, 0X66, 0X61, 0X50, 0X65, 0X6E, 0X41, 0X74, 0X74, 0X72, 0X73, 0X2, 0X1, 0X73, 0X6F, 0X66, 0X61, 0X54, 0X72, 0X61, 0X63, 0X65, 0X49, 0X64, 0X2, 0X63, 0X30, 0X61, 0X38, 0X30, 0X62, 0X30, 0X31, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X37, 0X38, 0X34, 0X31, 0X31, 0X33, 0X32, 0X39, 0X38, 0X39, 0X37, 0X36, 0X1, 0X45, 0X56, 0X45, 0X4E, 0X54, 0X5F, 0X54, 0X49, 0X4D, 0X45, 0X2, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X37, 0X38, 0X34, 0X1, 0X61, 0X6E, 0X74, 0X71, 0X5F, 0X65, 0X6E, 0X74, 0X69, 0X74, 0X79, 0X5F, 0X70, 0X72, 0X6F, 0X70, 0X65, 0X72, 0X74, 0X79, 0X2, 0X7B, 0X22, 0X62, 0X6F, 0X72, 0X6E, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X37, 0X38, 0X34, 0X2C, 0X22, 0X63, 0X6F, 0X6D, 0X6D, 0X69, 0X74, 0X74, 0X65, 0X64, 0X22, 0X3A, 0X74, 0X72, 0X75, 0X65, 0X2C, 0X22, 0X64, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X79, 0X43, 0X6F, 0X75, 0X6E, 0X74, 0X22, 0X3A, 0X30, 0X2C, 0X22, 0X64, 0X6C, 0X71, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X2D, 0X31, 0X2C, 0X22, 0X66, 0X6C, 0X61, 0X67, 0X22, 0X3A, 0X33, 0X2C, 0X22, 0X67, 0X4D, 0X54, 0X43, 0X72, 0X65, 0X61, 0X74, 0X65, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X37, 0X38, 0X34, 0X2C, 0X22, 0X67, 0X4D, 0X54, 0X4C, 0X61, 0X73, 0X74, 0X44, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X79, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X37, 0X38, 0X34, 0X2C, 0X22, 0X67, 0X72, 0X6F, 0X75, 0X70, 0X49, 0X64, 0X22, 0X3A, 0X22, 0X53, 0X5F, 0X64, 0X6F, 0X6E, 0X67, 0X73, 0X68, 0X69, 0X5F, 0X74, 0X65, 0X73, 0X74, 0X22, 0X2C, 0X22, 0X68, 0X6F, 0X73, 0X74, 0X4E, 0X61, 0X6D, 0X65, 0X22, 0X3A, 0X22, 0X43, 0X30, 0X32, 0X58, 0X57, 0X35, 0X53, 0X4C, 0X4A, 0X48, 0X44, 0X32, 0X2E, 0X6C, 0X6F, 0X63, 0X61, 0X6C, 0X22, 0X2C, 0X22, 0X6D, 0X65, 0X73, 0X73, 0X61, 0X67, 0X65, 0X49, 0X64, 0X22, 0X3A, 0X22, 0X35, 0X42, 0X37, 0X32, 0X45, 0X35, 0X45, 0X32, 0X33, 0X31, 0X37, 0X42, 0X46, 0X46, 0X36, 0X31, 0X46, 0X30, 0X37, 0X34, 0X37, 0X38, 0X34, 0X31, 0X38, 0X45, 0X36, 0X38, 0X44, 0X37, 0X36, 0X34, 0X22, 0X2C, 0X22, 0X6D, 0X65, 0X73, 0X73, 0X61, 0X67, 0X65, 0X54, 0X79, 0X70, 0X65, 0X22, 0X3A, 0X22, 0X4D, 0X51, 0X5F, 0X44, 0X45, 0X46, 0X41, 0X55, 0X4C, 0X54, 0X5F, 0X54, 0X41, 0X47, 0X22, 0X2C, 0X22, 0X6E, 0X65, 0X78, 0X74, 0X44, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X30, 0X2C, 0X22, 0X70, 0X6F, 0X73, 0X74, 0X54, 0X69, 0X6D, 0X65, 0X4F, 0X75, 0X74, 0X22, 0X3A, 0X31, 0X30, 0X30, 0X30, 0X30, 0X2C, 0X22, 0X74, 0X69, 0X6D, 0X65, 0X54, 0X6F, 0X4C, 0X69, 0X76, 0X65, 0X22, 0X3A, 0X2D, 0X31, 0X7D, 0X1, 0X53, 0X48, 0X41, 0X52, 0X44, 0X5F, 0X4B, 0X45, 0X59, 0X2, 0X0, 0X0, 0X0, 0X12, 0X68, 0X65, 0X6C, 0X6C, 0X6F, 0X2D, 0X6D, 0X6F, 0X73, 0X6E, 0X2D, 0X64, 0X65, 0X6C, 0X61, 0X79, 0X2D, 0X31, 0X0, 0X0, 0X2, 0X50, 0XDA, 0XA3, 0X20, 0XA7, 0X1C, 0X4F, 0X69, 0X14, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X5, 0XC8, 0X0, 0X0, 0X4, 0XE0, 0XCC, 0X86, 0XFF, 0XA4, 0X10, 0X0, 0X0, 0X0, 0X0, 0X0, 0X1, 0X6C, 0XF2, 0XE2, 0X6C, 0X56, 0XB, 0XA6, 0X46, 0XA, 0X0, 0X0, 0X25, 0X25, 0X0, 0X0, 0X0, 0X0, 0XA, 0X54, 0X50, 0X5F, 0X44, 0X53, 0X5F, 0X54, 0X45, 0X53, 0X54, 0X0, 0X7, 0X49, 0X4E, 0X44, 0X45, 0X58, 0X2, 0X30, 0X1, 0XEC, 0X54, 0X41, 0X47, 0X53, 0X2, 0X1, 0X44, 0X45, 0X4C, 0X49, 0X56, 0X45, 0X52, 0X5F, 0X54, 0X49, 0X4D, 0X45, 0X2, 0X30, 0X1, 0X73, 0X79, 0X73, 0X50, 0X65, 0X6E, 0X41, 0X74, 0X74, 0X72, 0X73, 0X2, 0X1, 0X56, 0X45, 0X52, 0X53, 0X49, 0X4F, 0X4E, 0X2, 0X31, 0X1, 0X73, 0X6F, 0X66, 0X61, 0X50, 0X65, 0X6E, 0X41, 0X74, 0X74, 0X72, 0X73, 0X2, 0X1, 0X73, 0X6F, 0X66, 0X61, 0X54, 0X72, 0X61, 0X63, 0X65, 0X49, 0X64, 0X2, 0X63, 0X30, 0X61, 0X38, 0X30, 0X62, 0X30, 0X31, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X38, 0X35, 0X36, 0X31, 0X31, 0X33, 0X34, 0X39, 0X38, 0X39, 0X37, 0X36, 0X1, 0X45, 0X56, 0X45, 0X4E, 0X54, 0X5F, 0X54, 0X49, 0X4D, 0X45, 0X2, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X38, 0X35, 0X36, 0X1, 0X61, 0X6E, 0X74, 0X71, 0X5F, 0X65, 0X6E, 0X74, 0X69, 0X74, 0X79, 0X5F, 0X70, 0X72, 0X6F, 0X70, 0X65, 0X72, 0X74, 0X79, 0X2, 0X7B, 0X22, 0X62, 0X6F, 0X72, 0X6E, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X38, 0X35, 0X36, 0X2C, 0X22, 0X63, 0X6F, 0X6D, 0X6D, 0X69, 0X74, 0X74, 0X65, 0X64, 0X22, 0X3A, 0X74, 0X72, 0X75, 0X65, 0X2C, 0X22, 0X64, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X79, 0X43, 0X6F, 0X75, 0X6E, 0X74, 0X22, 0X3A, 0X30, 0X2C, 0X22, 0X64, 0X6C, 0X71, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X2D, 0X31, 0X2C, 0X22, 0X66, 0X6C, 0X61, 0X67, 0X22, 0X3A, 0X33, 0X2C, 0X22, 0X67, 0X4D, 0X54, 0X43, 0X72, 0X65, 0X61, 0X74, 0X65, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X38, 0X35, 0X36, 0X2C, 0X22, 0X67, 0X4D, 0X54, 0X4C, 0X61, 0X73, 0X74, 0X44, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X79, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X38, 0X35, 0X36, 0X2C, 0X22, 0X67, 0X72, 0X6F, 0X75, 0X70, 0X49, 0X64, 0X22, 0X3A, 0X22, 0X53, 0X5F, 0X64, 0X6F, 0X6E, 0X67, 0X73, 0X68, 0X69, 0X5F, 0X74, 0X65, 0X73, 0X74, 0X22, 0X2C, 0X22, 0X68, 0X6F, 0X73, 0X74, 0X4E, 0X61, 0X6D, 0X65, 0X22, 0X3A, 0X22, 0X43, 0X30, 0X32, 0X58, 0X57, 0X35, 0X53, 0X4C, 0X4A, 0X48, 0X44, 0X32, 0X2E, 0X6C, 0X6F, 0X63, 0X61, 0X6C, 0X22, 0X2C, 0X22, 0X6D, 0X65, 0X73, 0X73, 0X61, 0X67, 0X65, 0X49, 0X64, 0X22, 0X3A, 0X22, 0X31, 0X34, 0X38, 0X32, 0X36, 0X37, 0X45, 0X32, 0X33, 0X37, 0X31, 0X35, 0X46, 0X32, 0X43, 0X31, 0X31, 0X46, 0X34, 0X32, 0X45, 0X41, 0X42, 0X39, 0X35, 0X43, 0X42, 0X36, 0X33, 0X44, 0X46, 0X44, 0X22, 0X2C, 0X22, 0X6D, 0X65, 0X73, 0X73, 0X61, 0X67, 0X65, 0X54, 0X79, 0X70, 0X65, 0X22, 0X3A, 0X22, 0X4D, 0X51, 0X5F, 0X44, 0X45, 0X46, 0X41, 0X55, 0X4C, 0X54, 0X5F, 0X54, 0X41, 0X47, 0X22, 0X2C, 0X22, 0X6E, 0X65, 0X78, 0X74, 0X44, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X30, 0X2C, 0X22, 0X70, 0X6F, 0X73, 0X74, 0X54, 0X69, 0X6D, 0X65, 0X4F, 0X75, 0X74, 0X22, 0X3A, 0X31, 0X30, 0X30, 0X30, 0X30, 0X2C, 0X22, 0X74, 0X69, 0X6D, 0X65, 0X54, 0X6F, 0X4C, 0X69, 0X76, 0X65, 0X22, 0X3A, 0X2D, 0X31, 0X7D, 0X1, 0X53, 0X48, 0X41, 0X52, 0X44, 0X5F, 0X4B, 0X45, 0X59, 0X2, 0X0, 0X0, 0X0, 0X12, 0X68, 0X65, 0X6C, 0X6C, 0X6F, 0X2D, 0X6D, 0X6F, 0X73, 0X6E, 0X2D, 0X64, 0X65, 0X6C, 0X61, 0X79, 0X2D, 0X30, 0X0, 0X0, 0X2, 0X50, 0XDA, 0XA3, 0X20, 0XA7, 0X6B, 0X48, 0X59, 0X82, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X5, 0XC9, 0X0, 0X0, 0X4, 0XE0, 0XCC, 0X87, 0X1, 0XF4, 0X10, 0X0, 0X0, 0X0, 0X0, 0X0, 0X1, 0X6C, 0XF2, 0XE2, 0X6C, 0X56, 0XB, 0XA6, 0X46, 0XA, 0X0, 0X0, 0X25, 0X25, 0X0, 0X0, 0X0, 0X0, 0XA, 0X54, 0X50, 0X5F, 0X44, 0X53, 0X5F, 0X54, 0X45, 0X53, 0X54, 0X0, 0X7, 0X49, 0X4E, 0X44, 0X45, 0X58, 0X2, 0X31, 0X1, 0XEC, 0X54, 0X41, 0X47, 0X53, 0X2, 0X1, 0X44, 0X45, 0X4C, 0X49, 0X56, 0X45, 0X52, 0X5F, 0X54, 0X49, 0X4D, 0X45, 0X2, 0X30, 0X1, 0X73, 0X79, 0X73, 0X50, 0X65, 0X6E, 0X41, 0X74, 0X74, 0X72, 0X73, 0X2, 0X1, 0X56, 0X45, 0X52, 0X53, 0X49, 0X4F, 0X4E, 0X2, 0X31, 0X1, 0X73, 0X6F, 0X66, 0X61, 0X50, 0X65, 0X6E, 0X41, 0X74, 0X74, 0X72, 0X73, 0X2, 0X1, 0X73, 0X6F, 0X66, 0X61, 0X54, 0X72, 0X61, 0X63, 0X65, 0X49, 0X64, 0X2, 0X63, 0X30, 0X61, 0X38, 0X30, 0X62, 0X30, 0X31, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X38, 0X35, 0X36, 0X31, 0X31, 0X33, 0X34, 0X39, 0X38, 0X39, 0X37, 0X36, 0X1, 0X45, 0X56, 0X45, 0X4E, 0X54, 0X5F, 0X54, 0X49, 0X4D, 0X45, 0X2, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X38, 0X35, 0X36, 0X1, 0X61, 0X6E, 0X74, 0X71, 0X5F, 0X65, 0X6E, 0X74, 0X69, 0X74, 0X79, 0X5F, 0X70, 0X72, 0X6F, 0X70, 0X65, 0X72, 0X74, 0X79, 0X2, 0X7B, 0X22, 0X62, 0X6F, 0X72, 0X6E, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X38, 0X35, 0X36, 0X2C, 0X22, 0X63, 0X6F, 0X6D, 0X6D, 0X69, 0X74, 0X74, 0X65, 0X64, 0X22, 0X3A, 0X74, 0X72, 0X75, 0X65, 0X2C, 0X22, 0X64, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X79, 0X43, 0X6F, 0X75, 0X6E, 0X74, 0X22, 0X3A, 0X30, 0X2C, 0X22, 0X64, 0X6C, 0X71, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X2D, 0X31, 0X2C, 0X22, 0X66, 0X6C, 0X61, 0X67, 0X22, 0X3A, 0X33, 0X2C, 0X22, 0X67, 0X4D, 0X54, 0X43, 0X72, 0X65, 0X61, 0X74, 0X65, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X38, 0X35, 0X36, 0X2C, 0X22, 0X67, 0X4D, 0X54, 0X4C, 0X61, 0X73, 0X74, 0X44, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X79, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X38, 0X35, 0X36, 0X2C, 0X22, 0X67, 0X72, 0X6F, 0X75, 0X70, 0X49, 0X64, 0X22, 0X3A, 0X22, 0X53, 0X5F, 0X64, 0X6F, 0X6E, 0X67, 0X73, 0X68, 0X69, 0X5F, 0X74, 0X65, 0X73, 0X74, 0X22, 0X2C, 0X22, 0X68, 0X6F, 0X73, 0X74, 0X4E, 0X61, 0X6D, 0X65, 0X22, 0X3A, 0X22, 0X43, 0X30, 0X32, 0X58, 0X57, 0X35, 0X53, 0X4C, 0X4A, 0X48, 0X44, 0X32, 0X2E, 0X6C, 0X6F, 0X63, 0X61, 0X6C, 0X22, 0X2C, 0X22, 0X6D, 0X65, 0X73, 0X73, 0X61, 0X67, 0X65, 0X49, 0X64, 0X22, 0X3A, 0X22, 0X30, 0X31, 0X32, 0X43, 0X46, 0X43, 0X37, 0X43, 0X43, 0X31, 0X42, 0X41, 0X42, 0X39, 0X33, 0X45, 0X45, 0X46, 0X34, 0X36, 0X43, 0X34, 0X36, 0X41, 0X41, 0X43, 0X42, 0X42, 0X33, 0X42, 0X34, 0X37, 0X22, 0X2C, 0X22, 0X6D, 0X65, 0X73, 0X73, 0X61, 0X67, 0X65, 0X54, 0X79, 0X70, 0X65, 0X22, 0X3A, 0X22, 0X4D, 0X51, 0X5F, 0X44, 0X45, 0X46, 0X41, 0X55, 0X4C, 0X54, 0X5F, 0X54, 0X41, 0X47, 0X22, 0X2C, 0X22, 0X6E, 0X65, 0X78, 0X74, 0X44, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X30, 0X2C, 0X22, 0X70, 0X6F, 0X73, 0X74, 0X54, 0X69, 0X6D, 0X65, 0X4F, 0X75, 0X74, 0X22, 0X3A, 0X31, 0X30, 0X30, 0X30, 0X30, 0X2C, 0X22, 0X74, 0X69, 0X6D, 0X65, 0X54, 0X6F, 0X4C, 0X69, 0X76, 0X65, 0X22, 0X3A, 0X2D, 0X31, 0X7D, 0X1, 0X53, 0X48, 0X41, 0X52, 0X44, 0X5F, 0X4B, 0X45, 0X59, 0X2, 0X0, 0X0, 0X0, 0X12, 0X68, 0X65, 0X6C, 0X6C, 0X6F, 0X2D, 0X6D, 0X6F, 0X73, 0X6E, 0X2D, 0X64, 0X65, 0X6C, 0X61, 0X79, 0X2D, 0X31, 0X0, 0X0, 0X2, 0X50, 0XDA, 0XA3, 0X20, 0XA7, 0X1C, 0X4F, 0X69, 0X14, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X5, 0XCA, 0X0, 0X0, 0X4, 0XE0, 0XCC, 0X87, 0X41, 0X39, 0X10, 0X0, 0X0, 0X0, 0X0, 0X0, 0X1, 0X6C, 0XF2, 0XE2, 0X6C, 0X9E, 0XB, 0XA6, 0X46, 0XA, 0X0, 0X0, 0X25, 0X25, 0X0, 0X0, 0X0, 0X0, 0XA, 0X54, 0X50, 0X5F, 0X44, 0X53, 0X5F, 0X54, 0X45, 0X53, 0X54, 0X0, 0X7, 0X49, 0X4E, 0X44, 0X45, 0X58, 0X2, 0X30, 0X1, 0XEC, 0X54, 0X41, 0X47, 0X53, 0X2, 0X1, 0X44, 0X45, 0X4C, 0X49, 0X56, 0X45, 0X52, 0X5F, 0X54, 0X49, 0X4D, 0X45, 0X2, 0X30, 0X1, 0X73, 0X79, 0X73, 0X50, 0X65, 0X6E, 0X41, 0X74, 0X74, 0X72, 0X73, 0X2, 0X1, 0X56, 0X45, 0X52, 0X53, 0X49, 0X4F, 0X4E, 0X2, 0X31, 0X1, 0X73, 0X6F, 0X66, 0X61, 0X50, 0X65, 0X6E, 0X41, 0X74, 0X74, 0X72, 0X73, 0X2, 0X1, 0X73, 0X6F, 0X66, 0X61, 0X54, 0X72, 0X61, 0X63, 0X65, 0X49, 0X64, 0X2, 0X63, 0X30, 0X61, 0X38, 0X30, 0X62, 0X30, 0X31, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X39, 0X32, 0X39, 0X31, 0X31, 0X33, 0X36, 0X39, 0X38, 0X39, 0X37, 0X36, 0X1, 0X45, 0X56, 0X45, 0X4E, 0X54, 0X5F, 0X54, 0X49, 0X4D, 0X45, 0X2, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X39, 0X32, 0X39, 0X1, 0X61, 0X6E, 0X74, 0X71, 0X5F, 0X65, 0X6E, 0X74, 0X69, 0X74, 0X79, 0X5F, 0X70, 0X72, 0X6F, 0X70, 0X65, 0X72, 0X74, 0X79, 0X2, 0X7B, 0X22, 0X62, 0X6F, 0X72, 0X6E, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X39, 0X32, 0X39, 0X2C, 0X22, 0X63, 0X6F, 0X6D, 0X6D, 0X69, 0X74, 0X74, 0X65, 0X64, 0X22, 0X3A, 0X74, 0X72, 0X75, 0X65, 0X2C, 0X22, 0X64, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X79, 0X43, 0X6F, 0X75, 0X6E, 0X74, 0X22, 0X3A, 0X30, 0X2C, 0X22, 0X64, 0X6C, 0X71, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X2D, 0X31, 0X2C, 0X22, 0X66, 0X6C, 0X61, 0X67, 0X22, 0X3A, 0X33, 0X2C, 0X22, 0X67, 0X4D, 0X54, 0X43, 0X72, 0X65, 0X61, 0X74, 0X65, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X39, 0X32, 0X39, 0X2C, 0X22, 0X67, 0X4D, 0X54, 0X4C, 0X61, 0X73, 0X74, 0X44, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X79, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X39, 0X32, 0X39, 0X2C, 0X22, 0X67, 0X72, 0X6F, 0X75, 0X70, 0X49, 0X64, 0X22, 0X3A, 0X22, 0X53, 0X5F, 0X64, 0X6F, 0X6E, 0X67, 0X73, 0X68, 0X69, 0X5F, 0X74, 0X65, 0X73, 0X74, 0X22, 0X2C, 0X22, 0X68, 0X6F, 0X73, 0X74, 0X4E, 0X61, 0X6D, 0X65, 0X22, 0X3A, 0X22, 0X43, 0X30, 0X32, 0X58, 0X57, 0X35, 0X53, 0X4C, 0X4A, 0X48, 0X44, 0X32, 0X2E, 0X6C, 0X6F, 0X63, 0X61, 0X6C, 0X22, 0X2C, 0X22, 0X6D, 0X65, 0X73, 0X73, 0X61, 0X67, 0X65, 0X49, 0X64, 0X22, 0X3A, 0X22, 0X42, 0X38, 0X45, 0X39, 0X39, 0X31, 0X33, 0X39, 0X39, 0X31, 0X46, 0X30, 0X36, 0X34, 0X36, 0X34, 0X38, 0X39, 0X46, 0X44, 0X44, 0X37, 0X45, 0X35, 0X37, 0X38, 0X42, 0X43, 0X36, 0X37, 0X46, 0X46, 0X22, 0X2C, 0X22, 0X6D, 0X65, 0X73, 0X73, 0X61, 0X67, 0X65, 0X54, 0X79, 0X70, 0X65, 0X22, 0X3A, 0X22, 0X4D, 0X51, 0X5F, 0X44, 0X45, 0X46, 0X41, 0X55, 0X4C, 0X54, 0X5F, 0X54, 0X41, 0X47, 0X22, 0X2C, 0X22, 0X6E, 0X65, 0X78, 0X74, 0X44, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X30, 0X2C, 0X22, 0X70, 0X6F, 0X73, 0X74, 0X54, 0X69, 0X6D, 0X65, 0X4F, 0X75, 0X74, 0X22, 0X3A, 0X31, 0X30, 0X30, 0X30, 0X30, 0X2C, 0X22, 0X74, 0X69, 0X6D, 0X65, 0X54, 0X6F, 0X4C, 0X69, 0X76, 0X65, 0X22, 0X3A, 0X2D, 0X31, 0X7D, 0X1, 0X53, 0X48, 0X41, 0X52, 0X44, 0X5F, 0X4B, 0X45, 0X59, 0X2, 0X0, 0X0, 0X0, 0X12, 0X68, 0X65, 0X6C, 0X6C, 0X6F, 0X2D, 0X6D, 0X6F, 0X73, 0X6E, 0X2D, 0X64, 0X65, 0X6C, 0X61, 0X79, 0X2D, 0X30, 0X0, 0X0, 0X2, 0X50, 0XDA, 0XA3, 0X20, 0XA7, 0X6B, 0X48, 0X59, 0X82, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X5, 0XCB, 0X0, 0X0, 0X4, 0XE0, 0XCC, 0X87, 0X43, 0X89, 0X10, 0X0, 0X0, 0X0, 0X0, 0X0, 0X1, 0X6C, 0XF2, 0XE2, 0X6C, 0X9E, 0XB, 0XA6, 0X46, 0XA, 0X0, 0X0, 0X25, 0X25, 0X0, 0X0, 0X0, 0X0, 0XA, 0X54, 0X50, 0X5F, 0X44, 0X53, 0X5F, 0X54, 0X45, 0X53, 0X54, 0X0, 0X7, 0X49, 0X4E, 0X44, 0X45, 0X58, 0X2, 0X31, 0X1, 0XEC, 0X54, 0X41, 0X47, 0X53, 0X2, 0X1, 0X44, 0X45, 0X4C, 0X49, 0X56, 0X45, 0X52, 0X5F, 0X54, 0X49, 0X4D, 0X45, 0X2, 0X30, 0X1, 0X73, 0X79, 0X73, 0X50, 0X65, 0X6E, 0X41, 0X74, 0X74, 0X72, 0X73, 0X2, 0X1, 0X56, 0X45, 0X52, 0X53, 0X49, 0X4F, 0X4E, 0X2, 0X31, 0X1, 0X73, 0X6F, 0X66, 0X61, 0X50, 0X65, 0X6E, 0X41, 0X74, 0X74, 0X72, 0X73, 0X2, 0X1, 0X73, 0X6F, 0X66, 0X61, 0X54, 0X72, 0X61, 0X63, 0X65, 0X49, 0X64, 0X2, 0X63, 0X30, 0X61, 0X38, 0X30, 0X62, 0X30, 0X31, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X39, 0X32, 0X39, 0X31, 0X31, 0X33, 0X36, 0X39, 0X38, 0X39, 0X37, 0X36, 0X1, 0X45, 0X56, 0X45, 0X4E, 0X54, 0X5F, 0X54, 0X49, 0X4D, 0X45, 0X2, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X39, 0X32, 0X39, 0X1, 0X61, 0X6E, 0X74, 0X71, 0X5F, 0X65, 0X6E, 0X74, 0X69, 0X74, 0X79, 0X5F, 0X70, 0X72, 0X6F, 0X70, 0X65, 0X72, 0X74, 0X79, 0X2, 0X7B, 0X22, 0X62, 0X6F, 0X72, 0X6E, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X39, 0X32, 0X39, 0X2C, 0X22, 0X63, 0X6F, 0X6D, 0X6D, 0X69, 0X74, 0X74, 0X65, 0X64, 0X22, 0X3A, 0X74, 0X72, 0X75, 0X65, 0X2C, 0X22, 0X64, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X79, 0X43, 0X6F, 0X75, 0X6E, 0X74, 0X22, 0X3A, 0X30, 0X2C, 0X22, 0X64, 0X6C, 0X71, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X2D, 0X31, 0X2C, 0X22, 0X66, 0X6C, 0X61, 0X67, 0X22, 0X3A, 0X33, 0X2C, 0X22, 0X67, 0X4D, 0X54, 0X43, 0X72, 0X65, 0X61, 0X74, 0X65, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X39, 0X32, 0X39, 0X2C, 0X22, 0X67, 0X4D, 0X54, 0X4C, 0X61, 0X73, 0X74, 0X44, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X79, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X30, 0X39, 0X32, 0X39, 0X2C, 0X22, 0X67, 0X72, 0X6F, 0X75, 0X70, 0X49, 0X64, 0X22, 0X3A, 0X22, 0X53, 0X5F, 0X64, 0X6F, 0X6E, 0X67, 0X73, 0X68, 0X69, 0X5F, 0X74, 0X65, 0X73, 0X74, 0X22, 0X2C, 0X22, 0X68, 0X6F, 0X73, 0X74, 0X4E, 0X61, 0X6D, 0X65, 0X22, 0X3A, 0X22, 0X43, 0X30, 0X32, 0X58, 0X57, 0X35, 0X53, 0X4C, 0X4A, 0X48, 0X44, 0X32, 0X2E, 0X6C, 0X6F, 0X63, 0X61, 0X6C, 0X22, 0X2C, 0X22, 0X6D, 0X65, 0X73, 0X73, 0X61, 0X67, 0X65, 0X49, 0X64, 0X22, 0X3A, 0X22, 0X46, 0X32, 0X44, 0X38, 0X31, 0X38, 0X32, 0X37, 0X46, 0X42, 0X39, 0X33, 0X35, 0X31, 0X31, 0X33, 0X41, 0X43, 0X45, 0X32, 0X36, 0X32, 0X31, 0X41, 0X31, 0X33, 0X45, 0X36, 0X43, 0X37, 0X30, 0X41, 0X22, 0X2C, 0X22, 0X6D, 0X65, 0X73, 0X73, 0X61, 0X67, 0X65, 0X54, 0X79, 0X70, 0X65, 0X22, 0X3A, 0X22, 0X4D, 0X51, 0X5F, 0X44, 0X45, 0X46, 0X41, 0X55, 0X4C, 0X54, 0X5F, 0X54, 0X41, 0X47, 0X22, 0X2C, 0X22, 0X6E, 0X65, 0X78, 0X74, 0X44, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X30, 0X2C, 0X22, 0X70, 0X6F, 0X73, 0X74, 0X54, 0X69, 0X6D, 0X65, 0X4F, 0X75, 0X74, 0X22, 0X3A, 0X31, 0X30, 0X30, 0X30, 0X30, 0X2C, 0X22, 0X74, 0X69, 0X6D, 0X65, 0X54, 0X6F, 0X4C, 0X69, 0X76, 0X65, 0X22, 0X3A, 0X2D, 0X31, 0X7D, 0X1, 0X53, 0X48, 0X41, 0X52, 0X44, 0X5F, 0X4B, 0X45, 0X59, 0X2, 0X0, 0X0, 0X0, 0X12, 0X68, 0X65, 0X6C, 0X6C, 0X6F, 0X2D, 0X6D, 0X6F, 0X73, 0X6E, 0X2D, 0X64, 0X65, 0X6C, 0X61, 0X79, 0X2D, 0X31, 0X0, 0X0, 0X2, 0X50, 0XDA, 0XA3, 0X20, 0XA7, 0X1C, 0X4F, 0X69, 0X14, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X0, 0X5, 0XCC, 0X0, 0X0, 0X4, 0XE0, 0XCC, 0X87, 0XC3, 0XA, 0X10, 0X0, 0X0, 0X0, 0X0, 0X0, 0X1, 0X6C, 0XF2, 0XE2, 0X6C, 0XE7, 0XB, 0XA6, 0X46, 0XA, 0X0, 0X0, 0X25, 0X25, 0X0, 0X0, 0X0, 0X0, 0XA, 0X54, 0X50, 0X5F, 0X44, 0X53, 0X5F, 0X54, 0X45, 0X53, 0X54, 0X0, 0X7, 0X49, 0X4E, 0X44, 0X45, 0X58, 0X2, 0X30, 0X1, 0XEC, 0X54, 0X41, 0X47, 0X53, 0X2, 0X1, 0X44, 0X45, 0X4C, 0X49, 0X56, 0X45, 0X52, 0X5F, 0X54, 0X49, 0X4D, 0X45, 0X2, 0X30, 0X1, 0X73, 0X79, 0X73, 0X50, 0X65, 0X6E, 0X41, 0X74, 0X74, 0X72, 0X73, 0X2, 0X1, 0X56, 0X45, 0X52, 0X53, 0X49, 0X4F, 0X4E, 0X2, 0X31, 0X1, 0X73, 0X6F, 0X66, 0X61, 0X50, 0X65, 0X6E, 0X41, 0X74, 0X74, 0X72, 0X73, 0X2, 0X1, 0X73, 0X6F, 0X66, 0X61, 0X54, 0X72, 0X61, 0X63, 0X65, 0X49, 0X64, 0X2, 0X63, 0X30, 0X61, 0X38, 0X30, 0X62, 0X30, 0X31, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X31, 0X30, 0X30, 0X31, 0X31, 0X31, 0X33, 0X38, 0X39, 0X38, 0X39, 0X37, 0X36, 0X1, 0X45, 0X56, 0X45, 0X4E, 0X54, 0X5F, 0X54, 0X49, 0X4D, 0X45, 0X2, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X31, 0X30, 0X30, 0X31, 0X1, 0X61, 0X6E, 0X74, 0X71, 0X5F, 0X65, 0X6E, 0X74, 0X69, 0X74, 0X79, 0X5F, 0X70, 0X72, 0X6F, 0X70, 0X65, 0X72, 0X74, 0X79, 0X2, 0X7B, 0X22, 0X62, 0X6F, 0X72, 0X6E, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X31, 0X30, 0X30, 0X31, 0X2C, 0X22, 0X63, 0X6F, 0X6D, 0X6D, 0X69, 0X74, 0X74, 0X65, 0X64, 0X22, 0X3A, 0X74, 0X72, 0X75, 0X65, 0X2C, 0X22, 0X64, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X79, 0X43, 0X6F, 0X75, 0X6E, 0X74, 0X22, 0X3A, 0X30, 0X2C, 0X22, 0X64, 0X6C, 0X71, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X2D, 0X31, 0X2C, 0X22, 0X66, 0X6C, 0X61, 0X67, 0X22, 0X3A, 0X33, 0X2C, 0X22, 0X67, 0X4D, 0X54, 0X43, 0X72, 0X65, 0X61, 0X74, 0X65, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X31, 0X30, 0X30, 0X32, 0X2C, 0X22, 0X67, 0X4D, 0X54, 0X4C, 0X61, 0X73, 0X74, 0X44, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X79, 0X22, 0X3A, 0X31, 0X35, 0X36, 0X37, 0X34, 0X34, 0X33, 0X30, 0X32, 0X31, 0X30, 0X30, 0X32, 0X2C, 0X22, 0X67, 0X72, 0X6F, 0X75, 0X70, 0X49, 0X64, 0X22, 0X3A, 0X22, 0X53, 0X5F, 0X64, 0X6F, 0X6E, 0X67, 0X73, 0X68, 0X69, 0X5F, 0X74, 0X65, 0X73, 0X74, 0X22, 0X2C, 0X22, 0X68, 0X6F, 0X73, 0X74, 0X4E, 0X61, 0X6D, 0X65, 0X22, 0X3A, 0X22, 0X43, 0X30, 0X32, 0X58, 0X57, 0X35, 0X53, 0X4C, 0X4A, 0X48, 0X44, 0X32, 0X2E, 0X6C, 0X6F, 0X63, 0X61, 0X6C, 0X22, 0X2C, 0X22, 0X6D, 0X65, 0X73, 0X73, 0X61, 0X67, 0X65, 0X49, 0X64, 0X22, 0X3A, 0X22, 0X42, 0X36, 0X37, 0X39, 0X43, 0X34, 0X44, 0X39, 0X44, 0X43, 0X36, 0X36, 0X31, 0X35, 0X43, 0X34, 0X34, 0X39, 0X44, 0X31, 0X46, 0X34, 0X31, 0X44, 0X34, 0X31, 0X43, 0X41, 0X41, 0X37, 0X39, 0X39, 0X22, 0X2C, 0X22, 0X6D, 0X65, 0X73, 0X73, 0X61, 0X67, 0X65, 0X54, 0X79, 0X70, 0X65, 0X22, 0X3A, 0X22, 0X4D, 0X51, 0X5F, 0X44, 0X45, 0X46, 0X41, 0X55, 0X4C, 0X54, 0X5F, 0X54, 0X41, 0X47, 0X22, 0X2C, 0X22, 0X6E, 0X65, 0X78, 0X74, 0X44, 0X65, 0X6C, 0X69, 0X76, 0X65, 0X72, 0X54, 0X69, 0X6D, 0X65, 0X22, 0X3A, 0X30, 0X2C, 0X22, 0X70, 0X6F, 0X73, 0X74, 0X54, 0X69, 0X6D, 0X65, 0X4F, 0X75, 0X74, 0X22, 0X3A, 0X31, 0X30, 0X30, 0X30, 0X30, 0X2C, 0X22, 0X74, 0X69, 0X6D, 0X65, 0X54, 0X6F, 0X4C, 0X69, 0X76, 0X65, 0X22, 0X3A, 0X2D, 0X31, 0X7D, 0X1, 0X53, 0X48, 0X41, 0X52, 0X44, 0X5F, 0X4B, 0X45, 0X59, 0X2, 0X0, 0X0, 0X0, 0X12, 0X68, 0X65, 0X6C, 0X6C, 0X6F, 0X2D, 0X6D, 0X6F, 0X73, 0X6E, 0X2D, 0X64, 0X65, 0X6C, 0X61, 0X79, 0X2D, 0X30}

	rsp.Body = bodyData
	messages, err := rsp.GetMessages("hello")
	if err != nil {
		t.Errorf("rsp.GetMessages() = error:%v", err)
	}
	t.Logf("messages:%+v", messages)
}

func TestFetchMessageRequest_Rebuild(t *testing.T) {
	rs := NewResponse(SUCCESS, 0x14)
	var fetchMessageRsp FetchMessageResponse
	fetchMessageRsp.Packet = *rs

	rqHeader := ReadMessageResponseHeader{}
	rqHeader.State = ReadMessageResponseHeader_NO_NEW_MESSAGE
	fetchMessageRsp.Rebuild(&rqHeader)

}
