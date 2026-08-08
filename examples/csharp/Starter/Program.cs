using Greeting;
using Temporalio.Client;

var address = Environment.GetEnvironmentVariable("TEMPORAL_ADDRESS") ?? "localhost:7233";
var client = await TemporalClient.ConnectAsync(new(address));

var result = await client.ExecuteWorkflowAsync(
    (GreetingWorkflow wf) => wf.RunAsync("Temporal"),
    new(id: $"greeting-csharp-{Guid.NewGuid()}", taskQueue: GreetingWorkflow.TaskQueue));

Console.WriteLine($"Workflow result: {result}");
if (result != "Hello, Temporal!")
{
    Console.Error.WriteLine($"unexpected result: {result}");
    Environment.Exit(1);
}
Console.WriteLine("CSHARP EXAMPLE: PASS");
