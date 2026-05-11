package app

const (
	kafkaConsumedPref = "mq:kafka:consumed:"
	voucherRetryTopic = "voucher.order.retry"
	voucherDLTTopic   = "voucher.order.dlt"
	maxRetryBeforeDLT = 2
)
