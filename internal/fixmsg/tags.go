package fixmsg

// FIX 4.2 tags used by the gateway.
const (
	TagAccount          = 1
	TagBeginSeqNo       = 7
	TagBeginString      = 8
	TagBodyLength       = 9
	TagCheckSum         = 10
	TagClientOrderID    = 11
	TagCumQty           = 14
	TagCurrency         = 15
	TagEndSeqNo         = 16
	TagExecID           = 17
	TagExecInst         = 18
	TagLastMarket       = 30
	TagLastPrice        = 31
	TagLastShares       = 32
	TagMsgSeqNum        = 34
	TagMsgType          = 35
	TagOrderID          = 37
	TagOrderQty         = 38
	TagOrdStatus        = 39
	TagOrderType        = 40
	TagOrigClientOrdID  = 41
	TagPrice            = 44
	TagRefSeqNum        = 45
	TagSenderCompID     = 49
	TagSendingTime      = 52
	TagSide             = 54
	TagSymbol           = 55
	TagTargetCompID     = 56
	TagText             = 58
	TagTimeInForce      = 59
	TagTransactTime     = 60
	TagAvgPrice         = 6
	TagPossDupFlag      = 43
	TagNewSeqNo         = 36
	TagGapFillFlag      = 123
	TagEncryptMethod    = 98
	TagHeartBtInt       = 108
	TagTestReqID        = 112
	TagCxlRejReason     = 102
	TagOrdRejReason     = 103
	TagExDestination    = 100
	TagLeavesQty        = 151
	TagExecType         = 150
	TagSecurityExchange = 207
	TagCxlRejResponseTo = 434
	TagFillProbability  = 9001
	TagQueueModel       = 9002
)

// Message types.
const (
	MsgTypeHeartbeat          = "0"
	MsgTypeTestRequest        = "1"
	MsgTypeResendRequest      = "2"
	MsgTypeReject             = "3"
	MsgTypeSequenceReset      = "4"
	MsgTypeLogout             = "5"
	MsgTypeExecutionReport    = "8"
	MsgTypeOrderCancelReject  = "9"
	MsgTypeLogon              = "A"
	MsgTypeNewOrderSingle     = "D"
	MsgTypeOrderCancelRequest = "F"
	MsgTypeOrderCancelReplace = "G"
)
