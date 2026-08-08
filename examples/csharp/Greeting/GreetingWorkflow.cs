using Temporalio.Workflows;

namespace Greeting;

[Workflow]
public class GreetingWorkflow
{
    public const string TaskQueue = "greeting-tasks-csharp";

    [WorkflowRun]
    public async Task<string> RunAsync(string name) =>
        await Workflow.ExecuteActivityAsync(
            () => GreetingActivities.ComposeGreeting(name),
            new() { StartToCloseTimeout = TimeSpan.FromSeconds(10) });
}
