package consumer

import "context"

type Message struct {
	MsgID      int64                  `json:"msg_id"`
	ReadCT     int64                  `json:"read_ct"`
	EnqueuedAt string                 `json:"enqueued_at"`
	VT         string                 `json:"vt"`
	Message    map[string]interface{} `json:"message"`
}

type handler func(msg map[string]interface{}) error

type ConsumerOptions struct {
	QueueName                   string
	VisibilityTime              int
	ConsumerType                string
	PoolSize                    int
	TimeMsWaitBeforeNextPolling int
	EnabledPolling              bool
	QueueNameDlq                string
	TotalRetriesBeforeSendToDlq int64
	EventListeners              map[string]func(msg Message, err error)
}

type QueueDriver interface {
	Send(
		queueName string,
		message map[string]interface{},
		signal context.Context,
	) error

	Get(
		queueName string,
		visibilityTime int,
		totalMessages int,
	) ([]Message, error)

	Pop(
		queueName string,
	) ([]Message, error)

	Delete(
		queueName string,
		messageID int64,
	) error
}

const EVENT_LISTENER_FINISH = "finish"
const EVENT_LISTENER_ERROR = "error"
const EVENT_LISTENER_ABORT_ERROR = "abort-error"
const EVENT_LISTENER_SEND_TO_DLQ = "send-to-dlq"
