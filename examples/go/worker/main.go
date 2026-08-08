package main

import (
	"log"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/jl178/temporal-testing/examples/go/greeting"
)

func main() {
	hostPort := os.Getenv("TEMPORAL_ADDRESS")
	if hostPort == "" {
		hostPort = "localhost:7233"
	}
	c, err := client.Dial(client.Options{
		HostPort:  hostPort,
		Namespace: os.Getenv("TEMPORAL_NAMESPACE"), // empty string means "default"
	})
	if err != nil {
		log.Fatalf("unable to create Temporal client: %v", err)
	}
	defer c.Close()

	w := worker.New(c, greeting.TaskQueue, worker.Options{})
	w.RegisterWorkflow(greeting.GreetingWorkflow)
	w.RegisterActivity(greeting.ComposeGreeting)

	log.Printf("Worker listening on task queue %q", greeting.TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("worker exited: %v", err)
	}
}
