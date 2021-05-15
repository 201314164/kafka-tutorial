package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/segmentio/kafka-go"
)

const (
	topic          = "my-kafka-topic"
	broker1Address = "localhost:9093"
	broker2Address = "localhost:9094"
	broker3Address = "localhost:9095"
)

type App struct {
	Context context.Context
	Router  *mux.Router
	Writer  *kafka.Writer
	Key     int
}

func respondWithError(w http.ResponseWriter, code int, msg string) {
	respondWithJSON(w, code, map[string]string{"error": msg})
}

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, _ := json.Marshal(payload)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

func (a *App) initialize() {

	a.Context = context.Background()

	a.Key = 0

	a.Writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker1Address, broker2Address, broker3Address},
		Topic:   topic,
	})

	a.Router = mux.NewRouter()
	a.Router.HandleFunc("/", a.send).Methods("POST")
}

func (a *App) serve() {
	fmt.Printf("Listening on port %s\n", ":7000")
	log.Fatal(http.ListenAndServe(":7000", a.Router))
}

func (a *App) send(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Peticion POST /")
	info := struct {
		Mensaje string `json:"msg"`
	}{}
	_ = json.NewDecoder(r.Body).Decode(&info)

	err := a.Writer.WriteMessages(a.Context, kafka.Message{
		Key:   []byte(strconv.Itoa(a.Key)),
		Value: []byte(info.Mensaje),
	})

	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		panic("Could not write message " + err.Error())
	}

	fmt.Println("Writes: "+info.Mensaje, a.Key)
	a.Key++

	respondWithJSON(w, http.StatusOK, "Vacunado Agregado")

}

func main() {
	app := App{}
	app.initialize()
	app.serve()
}
