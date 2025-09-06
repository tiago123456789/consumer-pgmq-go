package queuedriver

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/tiago123456789/consumer-pgmq-go/consumer"
)

type PostgresQueueDriver struct {
	db     *sql.DB
	schema string
}

func NewPostgresQueueDriver(db *sql.DB, schema string) *PostgresQueueDriver {
	if schema == "" {
		schema = "pgmq"
	}
	return &PostgresQueueDriver{db: db, schema: schema}
}

func (p *PostgresQueueDriver) Get(queueName string, visibilityTime int, totalMessages int) ([]consumer.Message, error) {
	sqlStatement, err := p.db.Query(fmt.Sprintf(`SELECT msg_id, read_ct, enqueued_at, vt, message FROM %s.read(
		queue_name => $1,
		vt         => $2,
		qty        => $3
	);`, p.schema), queueName, visibilityTime, totalMessages)
	if err != nil {
		return nil, err
	}
	defer sqlStatement.Close()

	var messages []consumer.Message
	for sqlStatement.Next() {

		var message consumer.Message
		var messageBody []byte

		err = sqlStatement.Scan(&message.MsgID, &message.ReadCT, &message.EnqueuedAt, &message.VT, &messageBody)
		if err != nil {
			return nil, err
		}

		json.Unmarshal(messageBody, &message.Message)

		messages = append(messages, message)
	}

	return messages, nil
}

func (p *PostgresQueueDriver) Pop(queueName string) ([]consumer.Message, error) {
	sqlStatement, err := p.db.Query(fmt.Sprintf(`SELECT msg_id, read_ct, enqueued_at, vt, message FROM %s.pop(
		queue_name => $1
	);`, p.schema), queueName)
	if err != nil {
		return nil, err
	}
	defer sqlStatement.Close()

	var messages []consumer.Message
	for sqlStatement.Next() {

		var message consumer.Message
		var messageBody []byte

		err = sqlStatement.Scan(&message.MsgID, &message.ReadCT, &message.EnqueuedAt, &message.VT, &messageBody)
		if err != nil {
			panic(err.Error())
		}

		json.Unmarshal(messageBody, &message.Message)

		messages = append(messages, message)
	}

	return messages, nil
}

func (p *PostgresQueueDriver) Delete(queueName string, msgID int64) error {
	_, err := p.db.Exec(fmt.Sprintf(` SELECT * FROM %s.delete(
	            queue_name => $1,
	            msg_id     => $2
	        );`, p.schema), queueName, msgID)
	if err != nil {
		return err
	}

	return nil
}

func (p *PostgresQueueDriver) Send(queueName string, message map[string]interface{}, signal context.Context,
) error {
	jsonMessage, err := json.Marshal(message)
	if err != nil {
		panic(err.Error())
	}
	_, err = p.db.Exec(fmt.Sprintf(` SELECT * FROM %s.send(
	            queue_name => $1,
	            msg        => $2,
	            delay      => $3
	        );`, p.schema), queueName, jsonMessage, 0)
	if err != nil {
		return err
	}

	return nil
}
