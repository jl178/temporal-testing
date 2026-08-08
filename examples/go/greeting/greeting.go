package greeting

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "greeting-tasks-go"

func ComposeGreeting(ctx context.Context, name string) (string, error) {
	return fmt.Sprintf("Hello, %s!", name), nil
}

func GreetingWorkflow(ctx workflow.Context, name string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})
	var result string
	if err := workflow.ExecuteActivity(ctx, ComposeGreeting, name).Get(ctx, &result); err != nil {
		return "", err
	}
	return result, nil
}
