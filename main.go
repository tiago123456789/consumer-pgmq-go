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

	// API_URL := os.Getenv("SUPABASE_URL")
	// API_KEY := os.Getenv("SUPABASE_ANON_KEY")
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

	messages, err := postgresQueueDriver.Get("subscriptions", 30, 5)
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("%v\n", messages)

	// messages, err := postgresQueueDriver.Pop("subscriptions")
	// if err != nil {
	// 	panic(err.Error())
	// }
	// fmt.Printf("%v\n", messages)

	// err = postgresQueueDriver.Delete("subscriptions", fmt.Sprintf("%d", messages[0].MsgID))
	// if err != nil {
	// 	panic(err.Error())
	// }

	// _, err = db.Exec(` SELECT * FROM pgmq.delete(
	//             queue_name => $1,
	//             msg_id     => $2
	//         );`, "subscriptions", messages[0].MsgID)
	// if err != nil {
	// 	panic(err.Error())
	// }

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

	// client, err := supabase.NewClient(API_URL, API_KEY, &supabase.ClientOptions{
	// 	Schema: "pgmq_public",
	// })
	// if err != nil {
	// 	fmt.Println("cannot initalize client", err)
	// }

	// supabaseQueueDriver := queuedriver.NewSupabaseQueueDriver(client)

	consumer, err := consumer.NewConsumer(func(msg map[string]interface{}) error {
		fmt.Println(msg)
		return errors.New("error processing message")
		// return nil
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
