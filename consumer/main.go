package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type Job struct {
	Message kafka.Message
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "orders",
		GroupID: "orders-consumer-group",
	})

	defer reader.Close()

	jobs := make(chan Job)

	var wg sync.WaitGroup

	// 3 workers
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go worker(i, jobs, reader, ctx, &wg)
	}

	// Consumer
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Fatal(err)
		}

		jobs <- Job{
			Message: message,
		}
	}

	close(jobs)
	wg.Wait()

}

func worker(
	id int,
	jobs <-chan Job,
	reader *kafka.Reader,
	ctx context.Context,
	wg *sync.WaitGroup,

) {
	defer wg.Done()

	for job := range jobs {
		message := job.Message

		fmt.Printf(
			"worker=%d partition=%d offset=%d value=%s\n",
			id,
			message.Partition,
			message.Offset,
			string(message.Value),
		)

		time.Sleep(2 * time.Second)

		err := reader.CommitMessages(ctx, message)
		if err != nil {
			log.Printf(
				"worker=%d commit error: %v\n",
				id,
				err,
			)
			continue
		}

		fmt.Printf(
			"worker=%d committed partition=%d offset=%d\n",
			id,
			message.Partition,
			message.Offset,
		)
	}
}
