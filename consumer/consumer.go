package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/segmentio/kafka-go"
)

const (
	topic          = "my-kafka-topic"
	broker1Address = "kafka-strimzi-kafka-bootstrap.kafka-demo.svc:9092"
)

func consume(ctx context.Context) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker1Address},
		Topic:   topic,
		GroupID: "my-group",
	})

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic("Could not read message " + err.Error())
		}

		if jsonMsg := string(msg.Value); strings.Contains(jsonMsg, "JSON:") {
			jsonMsg = strings.ReplaceAll(jsonMsg, "JSON:", "")
			info := struct {
				Name     string `json:"name"`
				Location string `json:"location"`
				Gender   string `json:"gender"`
				Age      int    `json:"age"`
				Vaccine  string `json:"vaccine_type"`
				Route    string `json:"route"`
			}{}
			_ = json.Unmarshal([]byte(jsonMsg), &info)

			responseBody := bytes.NewBuffer([]byte(jsonMsg))

			respMongo, err := http.Post("http://34.123.46.26/add", "application/json", responseBody)
			if err != nil {
				log.Fatalf("An error occured %v", err)
			}
			defer respMongo.Body.Close()

			fmt.Printf("Body: %s", responseBody)

			respRedis, err := http.Post("http://146.148.106.74:3000/add", "application/json", responseBody)
			if err != nil {
				log.Fatalf("An error occured %v", err)
			}
			defer respRedis.Body.Close()

			fmt.Printf("Body: %s", responseBody)

			bodyMongo, err := ioutil.ReadAll(respMongo.Body)
			if err != nil {
				log.Fatalln(err)
			}

			bodyRedis, err := ioutil.ReadAll(respRedis.Body)
			if err != nil {
				log.Fatalln(err)
			}

			fmt.Printf("Mongo: %s - Redis: %s", string(bodyMongo), string(bodyRedis))
		} else {
			fmt.Println("Recieved : ", string(msg.Value))
		}

	}
}

func main() {
	ctx := context.Background()

	consume(ctx)
}
