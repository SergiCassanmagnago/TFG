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

type request struct {
	op string
	e  edge
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
func source(istream string, ine chan<- request, inv chan<- map[int]bool) {
	file, err := os.Open(istream + ".requests")
	check(err)
	defer file.Close()

	var r request
	for {
		_, err = fmt.Fscanf(file, "%s", &r.op)
		if err == io.EOF {
			break
		}
		check(err)
		if r.op == "eof" {
			r.e = edge{x: -1, y: -1}
			ine <- r
			inv <- map[int]bool{-1: true}
			break
		} else {
			_, err = fmt.Fscanf(file, "%d%d\n", &r.e.x, &r.e.y)
			ine <- r
			check(err)
		}
	}
	close(ine)
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
func generator(ine <-chan request, inv <-chan map[int]bool, outv chan<- map[int]bool) {
	for {
		r, ok := <-ine
		if ok {
			switch r.op {
			case "insert":
				fmt.Printf("Filter instance created with x = %v, y = %v\n", r.e.x, r.e.y)
				ineNew := make(chan request)
				invNew := make(chan map[int]bool)
				go filter(ine, inv, ineNew, invNew, map[int]bool{r.e.x: true, r.e.y: true})
				ine = ineNew
				inv = invNew
			case "actor2":
				g, _ := <-inv
				fmt.Printf("Generator: received actor2 %v\n", g)
				outv <- g
			case "eof":
				g, _ := <-inv
				fmt.Printf("Generator: received eof %v\n", g)
				outv <- g
				break
			default: //something's wrong
				fmt.Println("Unknown operation in generator")
				break
			}
		} else {
			break
		}
	}
	close(outv)
}

// Filter stage
func filter(ine <-chan request, inv <-chan map[int]bool,
	oute chan<- request, outv chan<- map[int]bool, cc map[int]bool) {
	merged := false //Variable used to discard filter instances that have already been united with an actor2
	for {
		r, ok := <-ine
		if ok {
			switch r.op {
			case "eof":
				g, _ := <-inv
				if len(g) == 1 { //g is sent from the source stage so no union must be made
					fmt.Printf("eof: Passing %v\n", cc)
					oute <- r
					outv <- cc
				} else if merged == false {
					if intersection(cc, g) == true { //the components are connected so they are merged
						ccunion := union(cc, g)
						fmt.Printf("eof: Passing union %v\n", ccunion)
						oute <- r
						outv <- ccunion
					} else { //the components are not connected so they are passed separately
						r.op = "actor2"
						fmt.Printf("eof: No intersection, passing actor2 with %v\n", g)
						oute <- r
						outv <- g
						r.op = "eof"
						fmt.Printf("eof: No intersection, passing eof with %v\n", cc)
						oute <- r
						outv <- cc
					}
				} else {
					fmt.Printf("Deactivated stage %v. Passing %v", cc, g)
					oute <- r
					outv <- g
				}
				break
			case "actor2":
				g, _ := <-inv
				oute <- r
				if intersection(cc, g) == true && merged == false { //the components are connected so they are merged
					ccunion := union(cc, g)
					merged = true
					fmt.Printf("actor2 with %v: Passing union %v. Deactivated stage\n", cc, ccunion)
					outv <- ccunion
				} else { //the components are not connected so they are not merged
					fmt.Printf("actor2 with %v: No intersection, passing %v\n", cc, g)
					outv <- g
				}
			case "insert":
				if _, ok := cc[r.e.x]; ok == true { //if cc contains x, then y is added to cc
					cc[r.e.y] = true
					//fmt.Printf("Inserted edge with x = %v, y = %v, to %v\n", r.e.x, r.e.y, cc)
				} else if _, ok := cc[r.e.y]; ok == true { //otherwise, if cc contains y, then x is added to cc
					cc[r.e.x] = true
					//fmt.Printf("Inserted edge with x = %v, y = %v, to %v\n", r.e.x, r.e.y, cc)
				} else { //otherwise r is passed to the next stage
					oute <- r
				}
			default: //something's wrong
				fmt.Println("Unknown operation in generator")
				break
			}
		} else {
			break
		}
	}
	close(oute)
	close(outv)
}

func main() {
	ine := make(chan request)       //channel transporting requests
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
