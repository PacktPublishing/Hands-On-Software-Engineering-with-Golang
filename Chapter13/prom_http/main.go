package main

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// Create a prometheus counter to keep track of ping requests.
	numPings := promauto.NewCounter(prometheus.CounterOpts{
		Name: "pingapp_pings_total",
		Help: "The total number of incoming ping requests",
	})

	// Register prometheus handler for exporting metrics.
	http.Handle("/metrics", promhttp.Handler())

	// Register /ping handler to increment ping counter.
	http.Handle("/ping", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		numPings.Inc()
		numPings.Add(42)
		_, _ = w.Write([]byte("pong!\n"))
	}))

	log.Println("serving prometheus metrics at: http://localhost:8080/metrics")
	log.Println("to collect some data try running: curl localhost:8080/ping")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
