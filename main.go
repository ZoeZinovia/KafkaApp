package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

const (
	broker1Address = "localhost:9092"
	broker2Address = "localhost:29092"
	topic          = "KafkaApp"
)

func main() {
	ctx := context.Background()

	fmt.Println("Starting consumer and producer")

	// Start Kafka Producer
	go producer(ctx)

	// Start Kafka Consumer
	consumer(ctx)
}

func producer(ctx context.Context) {
	keyValue := 0

	// Start new Kafka writer
	kafkaWriter := kafka.NewWriter(
		kafka.WriterConfig{
			Brokers: []string{broker1Address},
			Topic:   topic,
		})

	for {

		if err := kafkaWriter.WriteMessages(ctx, kafka.Message{
			Key:   []byte(strconv.Itoa(keyValue)),
			Value: []byte(fmt.Sprintf("Hello Kafka number %d\n", keyValue)),
		}); err != nil {
			panic(fmt.Sprintf("Could not write message! Error: %v\n", err))
		}

		fmt.Printf("Successfully logged message %d\n", keyValue)
		keyValue++

		time.Sleep(time.Second * 5)

	}
}

func consumer(ctx context.Context) {
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker1Address},
		Topic:   topic,
		GroupID: "group1",
	})

	for {

		msg, err := kafkaReader.ReadMessage(ctx)
		if err != nil {
			fmt.Printf("Could not read message! Err: %v\n", err)
		}

		fmt.Printf("Received message %s: %s\n", string(msg.Key), string(msg.Value))

	}
}
