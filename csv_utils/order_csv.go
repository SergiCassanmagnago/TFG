package main

import (
	"encoding/csv"
	"log"
	"os"
	"sort"
	"strconv"
)

func check(e error) {
	if e != nil {
		log.Panic(e)
	}
}

func main() {
	file, err := os.Open(os.Args[1])
	check(err)
	defer file.Close()

	r := csv.NewReader(file)
	records, err := r.ReadAll()
	check(err)

	sort.Slice(records, func(i, j int) bool {
		a, _ := strconv.ParseInt(records[i][2], 10, 64)
		b, _ := strconv.ParseInt(records[j][2], 10, 64)
		return a < b
	})

	for i := 0; i < len(records); i++ {
		records[i] = append(records[i], records[i][2])
		records[i][2] = strconv.Itoa(i + 1)
	}

	fout, err := os.Create(os.Args[1] + ".csv")
	check(err)
	defer fout.Close()

	w := csv.NewWriter(fout)
	w.WriteAll(records)
}
