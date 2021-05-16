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

			resp, err := http.Post("http://34.123.46.26/add", "application/json", responseBody)
			if err != nil {
				log.Fatalf("An error occured %v", err)
			}
			defer resp.Body.Close()

			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Fatalln(err)
			}

			sb := string(body)

			fmt.Printf("Recieved: %+v\n - %s", info, sb)
		} else {
			fmt.Println("Recieved : ", string(msg.Value))
		}

	}
}

func main() {
	ctx := context.Background()

	consume(ctx)
}
