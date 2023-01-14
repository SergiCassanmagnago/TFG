package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
)

func check(e error) {
	if e != nil {
		log.Panic(e)
	}
}

func main() {
	test := os.Args[1]
	counter := make(map[string]int)
	data := [][]string{}
	row := []string{}

	// Open traces file & read its content
	input, err := os.Open("../../results/" + test + ".csv")
	check(err)
	csvReader := csv.NewReader(input)
	records, err := csvReader.ReadAll()
	check(err)
	defer input.Close()

	// Create metrics file & initialize csv writer
	file, err := os.Create("../../results/" + test + "-metrics.csv")
	check(err)
	w := csv.NewWriter(file)
	defer file.Close()

	for i := 0; i < len(records); i++ {
		if _, ok := counter[records[i][1]]; ok {
			counter[records[i][1]]++
		}
		if _, ok := counter[records[i][1]]; !ok || i == len(records)-1 {
			if i != 0 {
				row = append(row, records[i-1][3])
				row = append(row, fmt.Sprint(counter[records[i-1][1]]))
				data = append(data, row)
			}
			counter[records[i][1]] = 1
			row = []string{test, records[i][1], records[i][3]}
		}

	}
	w.WriteAll(data)
}
