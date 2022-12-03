package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type edge struct {
	Vertex1 int
	Vertex2 int
}

func check(e error) {
	if e != nil {
		log.Panic(e)
	}
}

func produce(conn *kafka.Conn, e edge) {
	js, err := json.Marshal(e)
	check(err)
	conn.WriteMessages(kafka.Message{Value: js})
}

func source(conn *kafka.Conn, istream string) {
	file, err := os.Open(istream + ".requests")
	check(err)
	defer file.Close()

	var e edge
	for {
		_, err = fmt.Fscanf(file, "%d%d\n", &e.Vertex1, &e.Vertex2)
		if err == io.EOF {
			e.Vertex1 = -1
			e.Vertex2 = -1
			produce(conn, e)
			break
		}
		check(err)
		if e.Vertex1 <= 0 || e.Vertex2 <= 0 {
			fmt.Println("Incorrect edge format on input file")
			break
		}
		produce(conn, e)
	}
}

func main() {
	topic := os.Args[1]
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, 0)
	check(err)
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	source(conn, os.Args[2])
	fmt.Println("Succesfuly wrote on " + topic)

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
