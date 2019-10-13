package main

import (
	"context"
	"github.com/segmentio/kafka-go"
	"time"
)

const TopicTest = "test"

func getMessage () ([]byte) {
	return []byte("Message at " + time.Now().Format(time.RFC3339))
}

func main() {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   TopicTest,
		Balancer: &kafka.LeastBytes{},
	})

	w.WriteMessages(context.Background(),
		kafka.Message{
			Value: getMessage(),
		},
		kafka.Message{
			Value: getMessage(),
		},
		kafka.Message{
			Value: getMessage(),
		},
	)

	w.Close()
}
