package main

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/supabase-community/supabase-go"
	"github.com/tiago123456789/consumer-pgmq-go/consumer"
	queuedriver "github.com/tiago123456789/consumer-pgmq-go/consumer/queueDriver"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	client, err := supabase.NewClient(os.Getenv("SUPABASE_URL"), os.Getenv("SUPABASE_ANON_KEY"), &supabase.ClientOptions{
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
