using Greeting;
using Temporalio.Client;
using Temporalio.Worker;

var address = Environment.GetEnvironmentVariable("TEMPORAL_ADDRESS") ?? "localhost:7233";
var client = await TemporalClient.ConnectAsync(new(address)
{
    Namespace = Environment.GetEnvironmentVariable("TEMPORAL_NAMESPACE") ?? "default",
});

using var tokenSource = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    tokenSource.Cancel();
};
AppDomain.CurrentDomain.ProcessExit += (_, _) => tokenSource.Cancel();

using var worker = new TemporalWorker(
    client,
    new TemporalWorkerOptions(GreetingWorkflow.TaskQueue)
        .AddActivity(GreetingActivities.ComposeGreeting)
        .AddWorkflow<GreetingWorkflow>());

Console.WriteLine($"Worker listening on task queue '{GreetingWorkflow.TaskQueue}'");
try
{
    await worker.ExecuteAsync(tokenSource.Token);
}
catch (OperationCanceledException)
{
}
