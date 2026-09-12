package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddr = "localhost:9092"
	topic      = "orders"
	groupID    = "orders-consumer-group"
)

type order struct {
	ID        int     `json:"id"`
	Status    string  `json:"status"`
	CreatedAt string  `json:"created_at"`
	Amount    float64 `json:"amount"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{brokerAddr},
		Topic:          topic,
		GroupID:        groupID,
		StartOffset:    kafka.FirstOffset,
		MaxWait:        2 * time.Second,
		CommitInterval: time.Second,
	})
	defer reader.Close()

	for {
		select {
		case <-ctx.Done():
			log.Println("consumer stopped")
			return
		default:
		}

		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return
			}
			log.Printf("fetch error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var payload order
		if err := json.Unmarshal(message.Value, &payload); err != nil {
			log.Printf("invalid payload partition=%d offset=%d: %v", message.Partition, message.Offset, err)
			if commitErr := reader.CommitMessages(ctx, message); commitErr != nil {
				log.Printf("commit invalid payload failed: %v", commitErr)
			}
			continue
		}

		fmt.Printf("worker=1 partition=%d offset=%d order=%d status=%s amount=%.2f\n",
			message.Partition,
			message.Offset,
			payload.ID,
			payload.Status,
			payload.Amount,
		)

		time.Sleep(2 * time.Second)

		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Printf("commit error for partition=%d offset=%d: %v", message.Partition, message.Offset, err)
			continue
		}

		fmt.Printf("committed partition=%d offset=%d\n", message.Partition, message.Offset)
	}
}
