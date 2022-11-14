package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

type edge struct {
	x, y int
}

func check(e error) {
	if e != nil {
		log.Panic(e)
	}
}

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

// Source stage
func source(istream string, ine chan<- edge, inv chan<- map[int]bool) {
	file, err := os.Open(istream + ".requests")
	check(err)
	defer file.Close()

	var e edge
	for {
		_, err = fmt.Fscanf(file, "%d%d\n", &e.x, &e.y)
		if err == io.EOF {
			break
		}
		check(err)
		if e.x <= 0 || e.y <= 0 {
			fmt.Println("Incorrect edge format on input file")
			break
		}
		ine <- e
	}
	close(ine)
	inv <- map[int]bool{-1: true}
	close(inv)
	file.Close()
}

// Sink stage
func sink(ostream string, istream string, inv <-chan map[int]bool, endchan chan<- string) {
	f, err := os.Create(ostream + ".wcc")
	check(err)
	defer f.Close()

	_, err = f.WriteString("Weakly Connected components of graph " + istream + ":\n\n")
	check(err)

	for {
		cc, ok := <-inv
		if ok {
			f.WriteString("{")
			i := 0
			for node := range cc {
				f.WriteString(strconv.Itoa(node))
				i++
				if i != len(cc) {
					f.WriteString(", ")
				}
			}
			f.WriteString("}\n\n")
		} else {
			break
		}
	}
	endchan <- "Execution complete"
	close(endchan)
}

// Generator stage
func generator(ine <-chan edge, inv <-chan map[int]bool, outv chan<- map[int]bool) {
	for {
		e, ok := <-ine
		if ok {
			fmt.Printf("Filter instance created with x = %v, y = %v\n", e.x, e.y)
			ineNew := make(chan edge)
			invNew := make(chan map[int]bool)
			go filter(ine, inv, ineNew, invNew, map[int]bool{e.x: true, e.y: true})
			ine = ineNew
			inv = invNew
		} else {
			break
		}
	}
	for {
		g, ok := <-inv
		if _, b := g[-1]; b == false && ok {
			fmt.Printf("Generator: received eof %v\n", g)
			outv <- g
		} else {
			break
		}
	}
	close(outv)
}

// Filter stage
func filter(ine <-chan edge, inv <-chan map[int]bool,
	oute chan<- edge, outv chan<- map[int]bool, cc map[int]bool) {

	//Actor1 Phase
	for {
		e, ok := <-ine
		if ok {
			if _, ok := cc[e.x]; ok == true { //if cc contains x, then y is added to cc
				cc[e.y] = true
			} else if _, ok := cc[e.y]; ok == true { //otherwise, if cc contains y, then x is added to cc
				cc[e.x] = true
			} else { //otherwise r is passed to the next stage
				oute <- e
			}
		} else {
			break
		}
	}
	close(oute)

	//Actor2 Phase
	for {
		g, _ := <-inv
		if _, ok := g[-1]; ok == true { //eof so no more sets of vertices will be received
			fmt.Printf("Actor2 eof: Passing %v\n", cc)
			break
		} else {
			if intersection(cc, g) == true { //the components are connected so they are merged
				cc = union(cc, g)
				fmt.Printf("Actor2 merge: %v\n", cc)
			} else { //the components are not connected so they are passed separately
				fmt.Printf("Actor2 %v no intersection: Passing %v\n", cc, g)
				outv <- g
			}
		}
	}
	outv <- cc
	outv <- map[int]bool{-1: true}
	close(outv)
}

func main() {
	ine := make(chan edge)          //channel transporting requests
	inv := make(chan map[int]bool)  //channel transporting sets of connected vertices
	outv := make(chan map[int]bool) //channel transporting sets of connected vertices
	endchan := make(chan string)    //channel transporting sets of connected vertices
	start := time.Now()
	go source(os.Args[1], ine, inv) // Launch Input.
	go generator(ine, inv, outv)
	go sink(os.Args[2], os.Args[1], outv, endchan)
	<-endchan
	t := time.Since(start)
	fmt.Println("TotalExecutionTime,", t, ",", t.Microseconds(), ",", t.Milliseconds(), ",", t.Seconds())
}
