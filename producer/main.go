package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "orders",
		Balancer: &kafka.LeastBytes{},
	}

	defer writer.Close()

	message := kafka.Message{
		Key:   []byte("order-1"),
		Value: []byte(`{"id":1, "status":"created"}`),
	}

	for {
		err := writer.WriteMessages(context.Background(), message)
		if err != nil {
			log.Fatal()
		}

		fmt.Println("message sent")
	}
}
