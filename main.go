package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "fmt"
    "log"

    "github.com/segmentio/kafka-go"
	_ "github.com/lib/pq"
)

type Message struct {
    Key   string `json:"key"`
    Value string `json:"value"`
}

func main() {
    // Set up a Kafka reader
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers:  []string{"localhost:9092"},
        Topic:    "mytopic",
        GroupID:  "mygroup",
        MinBytes: 10e3, // 10KB
        MaxBytes: 10e6, // 10MB
    })

    // Set up a PostgreSQL database connection
    db, err := sql.Open("postgres", "postgres://user_mytopic:password@localhost:5432/kafka_responses?sslmode=disable")
    if err != nil {
        log.Fatalf("Failed to connect to database: %v", err)
    }
    defer db.Close()
	log.Print("Connection Successfull")

    // Process Kafka messages
	log.Print("Processing kafka messages: ")
    for {
        m, err := reader.ReadMessage(context.Background())
        if err != nil {
            log.Printf("Error reading message: %v", err)
            continue
        }

        // Parse the JSON message into a Message struct
        var msg Message
        err = json.Unmarshal(m.Value, &msg)
        if err != nil {
            log.Printf("Error parsing message: %v", err)
            continue
        }

        // Insert the message record into the database
        _, err = db.Exec("INSERT INTO kafka_mytopic (key, value) VALUES ($1, $2)", msg.Key, msg.Value)
        if err != nil {
            log.Printf("Error inserting record: %v", err)
            continue
        }

        fmt.Printf("Processed message: %v\n", msg)
    }
}