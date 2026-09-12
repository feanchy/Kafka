package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddr = "localhost:9092"
	topic      = "orders"
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

	if err := ensureTopic(ctx, brokerAddr, topic); err != nil {
		log.Fatalf("topic setup failed: %v", err)
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokerAddr),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    1,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
	defer writer.Close()

	for i := 1; ; i++ {
		select {
		case <-ctx.Done():
			log.Println("producer stopped")
			return
		default:
		}

		payload, err := json.Marshal(order{
			ID:        i,
			Status:    "created",
			CreatedAt: time.Now().UTC().Format(time.RFC3339),
			Amount:    float64(i) * 10.5,
		})
		if err != nil {
			log.Fatalf("marshal order failed: %v", err)
		}

		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("order-%d", i)),
			Value: payload,
		}

		if err := writer.WriteMessages(ctx, msg); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("write failed for order=%d: %v", i, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		fmt.Printf("message sent: key=%s order=%d status=%s\n", string(msg.Key), i, "created")

		select {
		case <-ctx.Done():
			log.Println("producer stopped")
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func ensureTopic(ctx context.Context, brokerAddr, topic string) error {
	conn, err := kafka.DialContext(ctx, "tcp", brokerAddr)
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get controller: %w", err)
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer controllerConn.Close()

	if err := controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
		return fmt.Errorf("create topic %q: %w", topic, err)
	}

	return nil
}
