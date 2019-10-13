package main

import (
	"context"
	"fmt"
	"time"
	"github.com/segmentio/kafka-go"
)

const TopicTest = "test"

func main() {
	// to consume messages
	partition := 0

	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", TopicTest, partition)

	//readByBatch(conn)

	fmt.Println("=== Use reader === ")

	useReader()

	defer func() {
		conn.Close()
	}()
}

func readByBatch (conn *kafka.Conn) {

	conn.SetReadDeadline(time.Now().Add(1*time.Second))

	fmt.Println("Run batch")

	batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max
	defer func () {
		batch.Close()
	}()

	b := make([]byte, 10e3) // 10KB max per message

	fmt.Println("Run read")

	for {
		_, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Println(string(b))
	}
}

func useReader () {
	dialer := &kafka.Dialer{
		Timeout:   1 * time.Millisecond,
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     TopicTest,
		Partition: 0,
		MinBytes: 1,
		MaxBytes: 1000,
		CommitInterval: time.Nanosecond,
		Dialer:dialer,
	})

	reader.SetOffset(0)
	fmt.Println("==== Run read ")
	for {
		message, err := reader.ReadMessage(context.Background())

		if err != nil {break}

		fmt.Printf("===== Message at offset %d: %s = %s\n", message.Offset, string(message.Key), string(message.Value))
	}

	reader.Close()
}



