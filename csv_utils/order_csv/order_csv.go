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

func remove(slice []string, s int) []string {
	return append(slice[:s], slice[s+1:]...)
}

func main() {
	file, err := os.Open(os.Args[1])
	check(err)
	defer file.Close()

	r := csv.NewReader(file)
	records, err := r.ReadAll()
	check(err)

	for i := 0; i < len(records); i++ {
		remove(records[i], 2)
	}

	sort.Slice(records, func(i, j int) bool {
		a, _ := strconv.ParseFloat(records[i][2], 64)
		b, _ := strconv.ParseFloat(records[j][2], 64)
		return a < b
	})

	for i := 0; i < len(records); i++ {
		records[i][2] = strconv.Itoa(i + 1)
	}

	fout, err := os.Create(os.Args[1] + ".csv")
	check(err)
	defer fout.Close()

	w := csv.NewWriter(fout)
	w.WriteAll(records)
}
