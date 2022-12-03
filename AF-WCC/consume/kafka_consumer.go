package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

func check(e error) {
	if e != nil {
		log.Panic(e)
	}
}

func output(b []byte, batch *kafka.Batch, ostream string) {
	file, err := os.Create(ostream + ".wcc")
	check(err)
	defer file.Close()

	_, err = file.WriteString("Weakly Connected components of graph:\n\n")
	check(err)

	for {
		_, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Println(string(b))
		/*file.WriteString("{")
		i := 0
		for node := range cc {
			file.WriteString(strconv.Itoa(node))
			i++
			if i != len(cc) {
				file.WriteString(", ")
			}
		}
		file.WriteString("}\n\n")*/
	}
}

func main() {
	topic := os.Args[1]

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, 0)
	check(err)
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	batch := conn.ReadBatch(1e3, 1e9)
	b := make([]byte, 1e3)
	output(b, batch, os.Args[2])

}
