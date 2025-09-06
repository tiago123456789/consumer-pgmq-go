## About

This project is a consumer of Supabase/Postgresql queue(using pgmq extension) to simplify the process of consuming messages.

## Features

- Consumer message from Supabase queue. PS: instructions to setup https://supabase.com/blog/supabase-queues
- Consumer message from Postgresql queue. PS: instructions to setup https://github.com/pgmq/pgmq
- Support for both read and pop consume types
   - Read consume type is when the consumer gets the message and the message is not deleted from queue until the callback is executed with success.
   - Pop consume type is when the consumer gets the message and the message is deleted from queue.
- Support for both Supabase and Postgresql
- Support for both visibility time and pool size

## Installation

```bash
go get github.com/tiago123456789/consumer-pgmq-go
```

## Options

- queueName: The name of the queue. 
- visibilityTime: The time in seconds that the message will be invisible to other consumers. PS: 
    - Your handler must finish in this time or the message will be visible again to other consumers.
    - Is used too to abort the message if the handler takes too long to finish. For example, if you set visibilityTime to 15 seconds and your handler didnt finish in 15 seconds the handler will be aborted and the message will be visible again to other consumers.
- consumeType: The type of consume. Can be "read" or "pop"
    - Read consume type is when the consumer gets the message and the message is not deleted from queue until the callback is executed with success. 
    - Pop consume type is when the consumer gets the message and delete from queue in the moment get the message.
- poolSize: The number of consumers. PS: this is the number of consumers that will be created to consume the messages and 
if you use read consume type, the pool size is the number of messages will get at the same time.
- timeMsWaitBeforeNextPolling: The time in milliseconds to wait before the next polling
- enabledPolling: The enabled polling. PS: if true, the consumer will poll the message, if false, the consumer will consume the message one time and stop. PS: is required to the versions more than 1.0.5.
- queueNameDlq: The name of the dead letter queue. PS: recommended to set the same name of the queue, but suffix with '_dlq'. For example: **messages_dlq**
- totalRetriesBeforeSendToDlq: The total retries before send to dlq. For example: if you set totalRetriesBeforeSendToDlq equal 2, the message will be sent to dlq if the handler fails 2 times, so the third time the message will be sent to dlq and remove the main queue to avoid infinite retries.

## Extra points to know when use the dlq feature
- The dead letter queue no work If you setted the consumerType option with value 'pop', because the pop get the message and remove from queue at same time, so if failed when you are processing you lose the message.
- Recommendation no set lower value to the option 'visibilityTime' if you are using the dead letter queue feature. For example: set visibilityTime value lower than 30 seconds, because if the message wasn't delete and the message be available again the consumer application can consume the message again.

## Events

- send-to-dlq: When the message is sent to dlq
- finish: When the message is consumed with success
- abort-error: When the message is aborted
- error: When an error occurs

## Examples how to use

- Consuming messages from Supabase queue
```go

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/tiago123456789/consumer-pgmq-go/consumer"
	queuedriver "github.com/tiago123456789/consumer-pgmq-go/consumer/queueDriver"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	client, err := supabase.NewClient(API_URL, API_KEY, &supabase.ClientOptions{
		Schema: "pgmq_public",
	})
	if err != nil {
		fmt.Println("cannot initalize client", err)
	}

	supabaseQueueDriver := queuedriver.NewSupabaseQueueDriver(client)

	consumer, err := consumer.NewConsumer(func(msg map[string]interface{}) error {
		fmt.Println(msg)
		return nil
	}, consumer.ConsumerOptions{
		QueueName:                   "subscriptions",
		VisibilityTime:              30,
		ConsumerType:                "read", // "read or pop"
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
	}, supabaseQueueDriver)

	if err != nil {
		fmt.Println("cannot initalize consumer", err)
		return
	}
	consumer.Start()

}
```

- Consuming messages from Postgresql queue
```go
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/tiago123456789/consumer-pgmq-go/consumer"
	queuedriver "github.com/tiago123456789/consumer-pgmq-go/consumer/queueDriver"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	var dataSourceName = fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		os.Getenv("POSTGRES_HOST"), os.Getenv("POSTGRES_PORT"),
		os.Getenv("POSTGRES_USER"), os.Getenv("POSTGRES_PASSWORD"),
		os.Getenv("POSTGRES_DATABASE"))

	db, err := sql.Open("postgres", dataSourceName)
	defer db.Close()

	if err != nil {
		panic(err.Error())
	} else {
		fmt.Println("Connected!")
	}

	postgresQueueDriver := queuedriver.NewPostgresQueueDriver(db, "pgmq")

	err = postgresQueueDriver.Send("subscriptions", map[string]interface{}{"msg": "hi"}, context.Background())
	if err != nil {
		panic(err.Error())
	}

	err = postgresQueueDriver.Send("subscriptions", map[string]interface{}{"msg": "hi"}, context.Background())
	if err != nil {
		panic(err.Error())
	}

	err = postgresQueueDriver.Send("subscriptions", map[string]interface{}{"msg": "hi"}, context.Background())
	if err != nil {
		panic(err.Error())
	}

	err = postgresQueueDriver.Send("subscriptions", map[string]interface{}{"msg": "hi"}, context.Background())
	if err != nil {
		panic(err.Error())
	}

	consumer, err := consumer.NewConsumer(func(msg map[string]interface{}) error {
		fmt.Println(msg)
		return nil
	}, consumer.ConsumerOptions{
		QueueName:                   "subscriptions",
		VisibilityTime:              30,
		ConsumerType:                "read", // "read or pop"
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
	}, postgresQueueDriver)

	if err != nil {
		fmt.Println("cannot initalize consumer", err)
		return
	}
	consumer.Start()

}

```




