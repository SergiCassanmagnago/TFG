package main

import (
	"net/http"

	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun"
)

func main() {

	builder := statefun.StatefulFunctionsBuilder()
	handler := builder.AsHandler()
	http.Handle("/", handler)
	_ = http.ListenAndServe(":8000", nil)
}
