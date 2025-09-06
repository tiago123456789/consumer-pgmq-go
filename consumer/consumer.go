package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Consumer struct {
	handler        handler
	options        ConsumerOptions
	channelMessage chan Message
	queueDriver    QueueDriver
}

func NewConsumer(
	handler handler,
	options ConsumerOptions,
	queueDriver QueueDriver,
) (*Consumer, error) {
	channelMessage := make(chan Message, options.PoolSize)

	if options.ConsumerType != "pop" && options.ConsumerType != "read" {
		return nil, errors.New("ConsumerType must be 'pop' or 'read'")
	}

	if options.QueueNameDlq != "" && options.TotalRetriesBeforeSendToDlq == 0 {
		return nil, errors.New("TotalRetriesBeforeSendToDlq must be set if QueueNameDlq is set")
	}

	return &Consumer{
		handler:        handler,
		options:        options,
		channelMessage: channelMessage,
		queueDriver:    queueDriver,
	}, nil
}

func (c *Consumer) notifyEventListener(event string, msg Message, err error) {
	if c.options.EventListeners[event] != nil {
		c.options.EventListeners[event](msg, err)
	}
}

func (c *Consumer) removeMessage(msg Message) {
	if c.options.ConsumerType == "pop" {
		return
	}

	c.queueDriver.Delete(c.options.QueueName, msg.MsgID)
}

func (c *Consumer) getMessages() []Message {
	if c.options.ConsumerType == "pop" {
		result, err := c.queueDriver.Pop(c.options.QueueName)
		if err != nil {
			fmt.Println("error getting messages", err)
			return nil
		}

		return result
	}

	result, err := c.queueDriver.Get(
		c.options.QueueName,
		c.options.VisibilityTime,
		c.options.PoolSize,
	)
	if err != nil {
		fmt.Println("error getting messages", err)
		return nil
	}

	return result
}

func (c *Consumer) polling() {
	messages := c.getMessages()
	if len(messages) == 0 && c.options.EnabledPolling {
		time.Sleep(time.Duration(c.options.TimeMsWaitBeforeNextPolling) * time.Millisecond)
		c.polling()
	} else {
		for _, msg := range messages {
			c.channelMessage <- msg
		}

		if c.options.EnabledPolling {
			c.polling()
		} else {
			time.Sleep(time.Duration(c.options.VisibilityTime) * time.Second)
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		err := c.handler(msg.Message)
		if err != nil {
			c.notifyEventListener(EVENT_LISTENER_ERROR, msg, err)
			return nil
		}

		if err := ctx.Err(); err != nil {
			return nil
		}

		c.removeMessage(msg)
		c.notifyEventListener(EVENT_LISTENER_FINISH, msg, nil)
		return nil
	}
}

func (c *Consumer) sendToDlq(ctx context.Context, msg Message) error {
	select {
	case <-ctx.Done():
		fmt.Println("timeout processing message")
		return ctx.Err()
	default:
		c.queueDriver.Send(c.options.QueueNameDlq, msg.Message, context.Background())
		if err := ctx.Err(); err != nil {
			fmt.Println("context canceled")
			return nil
		}

		c.removeMessage(msg)
		c.notifyEventListener(EVENT_LISTENER_SEND_TO_DLQ, msg, nil)
		return nil
	}

}

func (c *Consumer) startWorker(i int) {
	fmt.Println("Worker", i, "started")
	for msg := range c.channelMessage {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			time.Duration(c.options.VisibilityTime)*time.Second,
		)

		timerToCancel := time.AfterFunc(time.Duration(c.options.VisibilityTime)*time.Second, func() {
			cancel()
			c.notifyEventListener(EVENT_LISTENER_ABORT_ERROR, msg, ctx.Err())
		})

		if c.options.TotalRetriesBeforeSendToDlq > 0 &&
			c.options.QueueNameDlq != "" &&
			msg.ReadCT > c.options.TotalRetriesBeforeSendToDlq {
			if err := c.sendToDlq(ctx, msg); err == nil {
				timerToCancel.Stop()
			}
		} else {
			if err := c.processMessage(ctx, msg); err == nil {
				timerToCancel.Stop()
			}
		}
	}
}

func (c *Consumer) Start() {
	for i := 0; i < c.options.PoolSize; i++ {
		go c.startWorker(i)
	}
	c.polling()
}
