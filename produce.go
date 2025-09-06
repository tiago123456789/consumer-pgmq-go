package main

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/supabase-community/supabase-go"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	API_URL := os.Getenv("SUPABASE_URL")
	API_KEY := os.Getenv("SUPABASE_ANON_KEY")

	client, err := supabase.NewClient(API_URL, API_KEY, &supabase.ClientOptions{
		Schema: "pgmq_public",
	})
	if err != nil {
		fmt.Println("cannot initalize client", err)
	}

	for i := 0; i < 1000; i++ {
		client.Rpc("send", "", map[string]interface{}{
			"queue_name": "subscriptions",
			"message": map[string]interface{}{
				"message": fmt.Sprintf("Hello World %d", i),
			},
		})
	}

}
