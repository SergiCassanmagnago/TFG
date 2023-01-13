package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	set "github.com/deckarep/golang-set/v2"
)

// Edges are formed by pairs of vertexes (x,y)
type edge struct {
	x, y int
}

func check(e error) {
	if e != nil {
		log.Panic(e)
	}
}

// Function that checks if two connected components intersect
func intersection(cc map[int]bool, ccin map[int]bool) bool {
	if len(cc) > len(ccin) {
		cc, ccin = ccin, cc
	}
	for k := range cc {
		if ccin[k] {
			return true
		}
	}
	return false
}

// Function that returns the union of two connected components
func union(cc map[int]bool, ccin map[int]bool) map[int]bool {
	ccunion := map[int]bool{}
	for k := range cc {
		ccunion[k] = true
	}
	for k := range ccin {
		ccunion[k] = true
	}
	return ccunion
}

func openFile(istream string) []string {
	content, err := ioutil.ReadFile("../tests/" + istream + ".requests")
	check(err)
	return strings.Split(string(content), "\n")
}

// Source stage
func source(istream string, ine chan<- edge, inv chan<- set.Set[int]) {

	temp := openFile(istream)

	var e edge
	for i := 0; i < len(temp); i++ {
		_, err := fmt.Sscanf(temp[i], "%d\t%d", &e.x, &e.y)
		if err == io.EOF {
			break
		}
		check(err)
		if e.x < 0 || e.y < 0 {
			fmt.Println("Incorrect edge format on input file")
			break
		}
		ine <- e
	}
	close(ine)
	// Send EOF signal to the next stage when all edges have been sent
	v := set.NewSet[int]()
	v.Add(-1)
	inv <- v
	close(inv)
}

// Sink stage
func sink(start time.Time, ostream string, istream string, mode string, inv <-chan set.Set[int], endchan chan<- string) {

	// Print all connected components separated by newline
	if mode == "print" {

		// Create output file and close it after executing
		file, err := os.Create("../results/" + ostream + ".wcc")
		check(err)
		defer file.Close()

		for {
			cc, ok := <-inv
			if !ok {
				break
			}
			file.WriteString(cc.String()[3:])
			file.WriteString("\n")
		}
	} else if mode == "test" {

		// Create output file and close it after executing
		file, err := os.Create("../results/dpwcc" + istream + ostream + ".csv")
		check(err)
		defer file.Close()
		// Initialize csv writer, approach, data structure and row counter
		w := csv.NewWriter(file)
		approach := "DP-WCC"
		data := [][]string{}
		counter := 1
		for {
			_, ok := <-inv
			if !ok {
				break
			}
			t := time.Since(start)
			row := []string{istream, approach, strconv.Itoa(counter), strconv.FormatFloat(t.Seconds(), 'f', -1, 64)}
			data = append(data, row)
			counter++
		}
		w.WriteAll(data)
	}

	// Send message through endchan to conclude the execution
	endchan <- "Execution complete"
	close(endchan)
}

// Generator stage
func generator(ine <-chan edge, inv <-chan set.Set[int], outv chan<- set.Set[int]) {

	// Actor1 Phase: Creates a new filter stage upon receiving a new edge
	for {
		e, ok := <-ine
		if !ok {
			break
		}
		ineNew := make(chan edge)
		invNew := make(chan set.Set[int])
		cc := set.NewSet[int]()
		cc.Add(e.x)
		cc.Add(e.y)
		go filter(ine, inv, ineNew, invNew, cc)
		ine = ineNew
		inv = invNew
	}

	// Actor2 Phase: Sends connected components to the sink stage
	for {
		g, ok := <-inv
		if b := g.Contains(-1); !b && ok {
			outv <- g
		} else {
			break
		}
	}
	close(outv)
}

// Filter stage used for grouping edges into connected components
func filter(ine <-chan edge, inv <-chan set.Set[int],
	oute chan<- edge, outv chan<- set.Set[int], cc set.Set[int]) {

	/* Actor1 Phase: Receives edges and adds them to its connected component if they are connected,
	otherwise they are sent to the next stage */
	for {
		e, ok := <-ine
		if !ok {
			break
		}

		// if cc contains x, then y is added to cc
		if b := cc.Contains(e.x); b {
			cc.Add(e.y)
		} else if b := cc.Contains(e.y); b { // otherwise, if cc contains y, then x is added to cc
			cc.Add(e.x)
		} else { // otherwise r is passed to the next stage
			oute <- e
		}
	}
	close(oute)

	/* Actor2 Phase: Receives connected components from previous stages; it combines them with its
	current connected component if the two intersect, otherwise it sends the received connected
	component to the next stage */
	for {
		g, _ := <-inv
		// EOF signal received, so no more sets of vertices will be received
		if b := g.Contains(-1); b {
			break
		} else {
			if s := cc.Intersect(g); s.Cardinality() == 0 { // the components are connected so they are merged
				cc = cc.Union(g)
			} else { // the components are not connected so they are passed separately
				outv <- g
			}
		}
	}

	// Sends its own connected component and EOF signal to the next stage before finishing its execution
	outv <- cc
	v := set.NewSet[int]()
	v.Add(-1)
	outv <- v
	close(outv)
}

func main() {

	start := time.Now()

	// Channel transporting edges
	ine := make(chan edge)

	// Channels transporting sets of connected vertices
	inv := make(chan set.Set[int])
	outv := make(chan set.Set[int])

	// Channel used for waiting for all the results to be generated
	endchan := make(chan string)

	// Launch input, generator and sink stages
	go source(os.Args[1], ine, inv)
	go generator(ine, inv, outv)
	go sink(start, os.Args[2], os.Args[1], os.Args[3], outv, endchan)

	// Wait for all the results to be generated and produce results
	<-endchan
	t := time.Since(start)
	fmt.Println("TotalExecutionTime,", t, ",", t.Microseconds(), ",", t.Milliseconds(), ",", t.Seconds())
}
