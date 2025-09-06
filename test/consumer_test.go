package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/tiago123456789/consumer-pgmq-go/consumer"
	"github.com/tiago123456789/consumer-pgmq-go/fakeMock"
)

func TestConsumer_NewConsumer(t *testing.T) {

	_, err := consumer.NewConsumer(func(msg map[string]interface{}) error {
		return nil
	}, consumer.ConsumerOptions{
		QueueName:                   "subscriptions",
		VisibilityTime:              30,
		ConsumerType:                "test",
		PoolSize:                    1,
		TimeMsWaitBeforeNextPolling: 1,
		EnabledPolling:              true,
		QueueNameDlq:                "subscriptions_dlq",
		TotalRetriesBeforeSendToDlq: 2,
		EventListeners: map[string]func(msg consumer.Message, err error){
			consumer.EVENT_LISTENER_SEND_TO_DLQ: func(msg consumer.Message, err error) {
				fmt.Println("message sent to dlq", msg, err)
			},
			consumer.EVENT_LISTENER_ABORT_ERROR: func(msg consumer.Message, err error) {
				fmt.Println("abort error processing message", msg, err)
			},
			consumer.EVENT_LISTENER_FINISH: func(msg consumer.Message, err error) {
				fmt.Println("message processed and finish", msg, err)
			},
			consumer.EVENT_LISTENER_ERROR: func(msg consumer.Message, err error) {
				fmt.Println("error processing message", msg, err)
			},
		},
	}, nil)

	if err == nil {
		t.Fatal("Expected error, because consumer type is invalid")
	}

	_, err = consumer.NewConsumer(func(msg map[string]interface{}) error {
		return nil
	}, consumer.ConsumerOptions{
		QueueName:                   "subscriptions",
		VisibilityTime:              30,
		ConsumerType:                "read",
		PoolSize:                    1,
		TimeMsWaitBeforeNextPolling: 1,
		EnabledPolling:              true,
		QueueNameDlq:                "subscriptions_dlq",
		EventListeners: map[string]func(msg consumer.Message, err error){
			consumer.EVENT_LISTENER_SEND_TO_DLQ: func(msg consumer.Message, err error) {
				fmt.Println("message sent to dlq", msg, err)
			},
			consumer.EVENT_LISTENER_ABORT_ERROR: func(msg consumer.Message, err error) {
				fmt.Println("abort error processing message", msg, err)
			},
			consumer.EVENT_LISTENER_FINISH: func(msg consumer.Message, err error) {
				fmt.Println("message processed and finish", msg, err)
			},
			consumer.EVENT_LISTENER_ERROR: func(msg consumer.Message, err error) {
				fmt.Println("error processing message", msg, err)
			},
		},
	}, nil)

	if err == nil {
		t.Fatal("Expected error, because setted dead letter queue, but forgot to set total retries before send to dlq")
	}
}

func TestConsumer_StartGetMessageAndDelete(t *testing.T) {
	queueDriver := new(fakeMock.MockQueueDriver)
	queueDriver.On("Get", "subscriptions", 2, 1).Return([]consumer.Message{
		{
			MsgID:      1,
			ReadCT:     1,
			EnqueuedAt: "2025-09-05T23:20:00Z",
			VT:         "2025-09-05T23:20:00Z",
			Message:    map[string]interface{}{"msg": "hi"},
		},
	}, nil)

	queueDriver.On("Delete", "subscriptions", int64(1)).Return(nil)
	consumer, _ := consumer.NewConsumer(func(msg map[string]interface{}) error {
		return nil
	}, consumer.ConsumerOptions{
		QueueName:                   "subscriptions",
		VisibilityTime:              2,
		ConsumerType:                "read",
		PoolSize:                    1,
		TimeMsWaitBeforeNextPolling: 1,
		EnabledPolling:              false,
		QueueNameDlq:                "subscriptions_dlq",
		TotalRetriesBeforeSendToDlq: 2,
		EventListeners:              map[string]func(msg consumer.Message, err error){},
	}, queueDriver)

	consumer.Start()

	queueDriver.AssertCalled(t, "Get", "subscriptions", 2, 1)
	queueDriver.AssertCalled(t, "Delete", "subscriptions", int64(1))
}

func TestConsumer_StartUsingConsumerTypePop(t *testing.T) {
	queueDriver := new(fakeMock.MockQueueDriver)
	queueDriver.On("Pop", "subscriptions").Return([]consumer.Message{
		{
			MsgID:      1,
			ReadCT:     1,
			EnqueuedAt: "2025-09-05T23:20:00Z",
			VT:         "2025-09-05T23:20:00Z",
			Message:    map[string]interface{}{"msg": "hi"},
		},
	}, nil)

	queueDriver.On("Delete", "subscriptions", int64(1)).Return(nil)
	consumer, _ := consumer.NewConsumer(func(msg map[string]interface{}) error {
		return nil
	}, consumer.ConsumerOptions{
		QueueName:                   "subscriptions",
		VisibilityTime:              2,
		ConsumerType:                "pop",
		PoolSize:                    1,
		TimeMsWaitBeforeNextPolling: 1,
		EnabledPolling:              false,
		QueueNameDlq:                "subscriptions_dlq",
		TotalRetriesBeforeSendToDlq: 2,
		EventListeners:              map[string]func(msg consumer.Message, err error){},
	}, queueDriver)

	consumer.Start()

	queueDriver.AssertCalled(t, "Pop", "subscriptions")
	queueDriver.AssertNotCalled(t, "Delete", "subscriptions", int64(1))
}

func TestConsumer_StartContextCanceledLongTask(t *testing.T) {
	queueDriver := new(fakeMock.MockQueueDriver)
	queueDriver.On("Get", "subscriptions", 1, 1).Return([]consumer.Message{
		{
			MsgID:      1,
			ReadCT:     1,
			EnqueuedAt: "2025-09-05T23:20:00Z",
			VT:         "2025-09-05T23:20:00Z",
			Message:    map[string]interface{}{"msg": "hi"},
		},
	}, nil)

	queueDriver.On("Delete", "subscriptions", int64(1)).Return(nil)
	consumer, _ := consumer.NewConsumer(func(msg map[string]interface{}) error {
		time.Sleep(2 * time.Second)
		return nil
	}, consumer.ConsumerOptions{
		QueueName:                   "subscriptions",
		VisibilityTime:              1,
		ConsumerType:                "read",
		PoolSize:                    1,
		TimeMsWaitBeforeNextPolling: 1,
		EnabledPolling:              false,
		EventListeners:              map[string]func(msg consumer.Message, err error){},
	}, queueDriver)

	consumer.Start()

	queueDriver.AssertCalled(t, "Get", "subscriptions", 1, 1)
	queueDriver.AssertNotCalled(t, "Delete", "subscriptions", int64(1))
}

func TestConsumer_StartSendToDlq(t *testing.T) {
	queueDriver := new(fakeMock.MockQueueDriver)
	queueDriver.On("Get", "subscriptions", 1, 1).Return([]consumer.Message{
		{
			MsgID:      1,
			ReadCT:     3,
			EnqueuedAt: "2025-09-05T23:20:00Z",
			VT:         "2025-09-05T23:20:00Z",
			Message:    map[string]interface{}{"msg": "hi"},
		},
	}, nil)

	queueDriver.On("Delete", "subscriptions", int64(1)).Return(nil)
	queueDriver.On("Send", "subscriptions_dlq", map[string]interface{}{"msg": "hi"}, context.Background()).Return(nil)
	consumer, _ := consumer.NewConsumer(func(msg map[string]interface{}) error {
		time.Sleep(2 * time.Second)
		return nil
	}, consumer.ConsumerOptions{
		QueueName:                   "subscriptions",
		VisibilityTime:              1,
		ConsumerType:                "read",
		PoolSize:                    1,
		TimeMsWaitBeforeNextPolling: 1,
		EnabledPolling:              false,
		QueueNameDlq:                "subscriptions_dlq",
		TotalRetriesBeforeSendToDlq: 2,
		EventListeners:              map[string]func(msg consumer.Message, err error){},
	}, queueDriver)

	consumer.Start()

	queueDriver.AssertCalled(t, "Get", "subscriptions", 1, 1)
	queueDriver.AssertCalled(t, "Send", "subscriptions_dlq", map[string]interface{}{"msg": "hi"}, context.Background())
	queueDriver.AssertCalled(t, "Delete", "subscriptions", int64(1))
}

func TestConsumer_StartNoDeleteHandlerReturnedError(t *testing.T) {
	queueDriver := new(fakeMock.MockQueueDriver)
	queueDriver.On("Get", "subscriptions", 10, 1).Return([]consumer.Message{
		{
			MsgID:      1,
			ReadCT:     1,
			EnqueuedAt: "2025-09-05T23:20:00Z",
			VT:         "2025-09-05T23:20:00Z",
			Message:    map[string]interface{}{"msg": "hi"},
		},
	}, nil)

	queueDriver.On("Delete", "subscriptions", int64(1)).Return(nil)
	consumer, _ := consumer.NewConsumer(func(msg map[string]interface{}) error {
		return errors.New("error processing message")
	}, consumer.ConsumerOptions{
		QueueName:                   "subscriptions",
		VisibilityTime:              10,
		ConsumerType:                "read",
		PoolSize:                    1,
		TimeMsWaitBeforeNextPolling: 1,
		EnabledPolling:              false,
		QueueNameDlq:                "subscriptions_dlq",
		TotalRetriesBeforeSendToDlq: 2,
		EventListeners:              map[string]func(msg consumer.Message, err error){},
	}, queueDriver)

	consumer.Start()

	queueDriver.AssertCalled(t, "Get", "subscriptions", 10, 1)
	queueDriver.AssertNotCalled(t, "Delete", "subscriptions", int64(1))
}
