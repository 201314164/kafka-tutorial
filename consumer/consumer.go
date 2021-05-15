package main

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

const (
	topic          = "my-kafka-topic"
	broker1Address = "localhost:9093"
	broker2Address = "localhost:9094"
	broker3Address = "localhost:9095"
)

func consume(ctx context.Context) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker1Address, broker2Address, broker3Address},
		Topic:   topic,
		GroupID: "my-group",
	})

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic("Could not read message " + err.Error())
		}

		fmt.Println("Recieved : ", string(msg.Value))
	}
}

func main() {
	ctx := context.Background()

	consume(ctx)
}
