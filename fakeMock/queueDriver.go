package fakeMock

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/tiago123456789/consumer-pgmq-go/consumer"
)

type MockQueueDriver struct {
	mock.Mock
}

func (m *MockQueueDriver) Get(queueName string, visibilityTime int, totalMessages int) ([]consumer.Message, error) {
	args := m.Called(queueName, visibilityTime, totalMessages)
	return args.Get(0).([]consumer.Message), args.Error(1)
}

func (m *MockQueueDriver) Pop(queueName string) ([]consumer.Message, error) {
	args := m.Called(queueName)
	return args.Get(0).([]consumer.Message), args.Error(1)
}

func (m *MockQueueDriver) Delete(queueName string, msgID int64) error {
	args := m.Called(queueName, msgID)
	return args.Error(0)
}

func (m *MockQueueDriver) Send(queueName string, message map[string]interface{}, signal context.Context,
) error {
	args := m.Called(queueName, message, signal)
	return args.Error(0)
}
