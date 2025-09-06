package queuedriver

import (
	"context"
	"encoding/json"

	"github.com/supabase-community/supabase-go"
	"github.com/tiago123456789/consumer-pgmq-go/consumer"
)

type SupabaseQueueDriver struct {
	client *supabase.Client
}

func NewSupabaseQueueDriver(client *supabase.Client) *SupabaseQueueDriver {
	return &SupabaseQueueDriver{
		client: client,
	}
}

func (s *SupabaseQueueDriver) Send(
	queueName string,
	message map[string]interface{},
	signal context.Context,
) error {
	s.client.Rpc("send", "", map[string]interface{}{
		"queue_name": queueName,
		"message":    message,
	})
	return nil
}

func (s *SupabaseQueueDriver) Get(
	queueName string,
	visibilityTime int,
	totalMessages int,
) ([]consumer.Message, error) {
	result := s.client.Rpc("read", "", map[string]interface{}{
		"queue_name":    queueName,
		"sleep_seconds": visibilityTime,
		"n":             totalMessages,
	})

	if result == "" {
		return nil, nil
	}

	var messages []consumer.Message
	err := json.Unmarshal([]byte(result), &messages)
	if err != nil {
		return nil, err
	}
	return messages, nil
}

func (s *SupabaseQueueDriver) Pop(
	queueName string,
) ([]consumer.Message, error) {
	result := s.client.Rpc("pop", "", map[string]interface{}{
		"queue_name": queueName,
	})

	var messages []consumer.Message
	err := json.Unmarshal([]byte(result), &messages)
	if err != nil {
		return nil, err
	}

	return messages, nil
}

func (s *SupabaseQueueDriver) Delete(
	queueName string,
	messageID int64,
) error {
	s.client.Rpc("delete", "", map[string]interface{}{
		"queue_name": queueName,
		"message_id": messageID,
	})
	return nil
}
