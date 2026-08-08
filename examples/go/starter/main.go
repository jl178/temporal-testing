package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.temporal.io/sdk/client"

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

	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        fmt.Sprintf("greeting-go-%d", time.Now().UnixNano()),
		TaskQueue: greeting.TaskQueue,
	}, greeting.GreetingWorkflow, "Temporal")
	if err != nil {
		log.Fatalf("unable to start workflow: %v", err)
	}

	var result string
	if err := run.Get(context.Background(), &result); err != nil {
		log.Fatalf("workflow failed: %v", err)
	}
	fmt.Printf("Workflow result: %s\n", result)
	if result != "Hello, Temporal!" {
		log.Fatalf("unexpected result: %q", result)
	}
	fmt.Println("GO EXAMPLE: PASS")
}
