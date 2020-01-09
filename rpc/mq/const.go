package commands

const (
	// int32(0xAABBCCDD ^ 1880681586 + 8)
	MAGIC_CODE = int32(-626843481)

	DEFAULT_GROUP = "Bolt_Default_Group_Name"
)

type RequestCode = uint32

const (
	CLIENT_REGISTER RequestCode = 1000

	CLIENT_UNREGISTER RequestCode = 1001

	CLIENT_HEARTBEAT RequestCode = 1002

	GET_TOPIC_METADATA RequestCode = 1003

	/** get queue offset: min offset, max offset */
	GET_OFFSET RequestCode = 1004

	/** query consuming offset */
	QUERY_OFFSET RequestCode = 1005

	/** save consuming offset */
	SAVE_OFFSET RequestCode = 1006

	/** reset offset */
	RESET_OFFSET RequestCode = 1011

	GET_CONSUMERS RequestCode = 1007

	QUERY_OFFSET_BY_TIMESTAMP RequestCode = 1008

	CONSUMER_LIST_CHANGE RequestCode = 1009

	GET_ASSIGNED_QUEUES RequestCode = 1012

	SEND_MESSAGE RequestCode = 2000

	SUBMIT_TRANSACTIONAL_MESSAGE RequestCode = 2001

	CHECK_TRANSACTIONAL_MESSAGE RequestCode = 2002

	SCHEDULE_MESSAGE RequestCode = 2003

	SEND_BATCH_MESSAGE RequestCode = 2004

	FETCH_MESSAGE RequestCode = 3000

	DELIVERY_MESSAGE RequestCode = 3001
)

type ResponseCode = uint32

const (
	SUCCESS ResponseCode = 0

	TIMEOUT ResponseCode = 1

	CONNECTION_ERROR ResponseCode = 2

	UN_SUPPORT ResponseCode = 3

	HANDLE_FAIL ResponseCode = 4

	UNKNOWN_ERROR ResponseCode = 100
)
